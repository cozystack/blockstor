/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package drbd_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestResolveOptions_LowerOverrides pins the override order:
// controller is the weakest, resource the strongest. Two stronger
// scopes setting the same key both win over the controller's value;
// between them the more-specific scope wins.
func TestResolveOptions_LowerOverrides(t *testing.T) {
	t.Parallel()

	got := drbd.ResolveOptions(
		map[string]string{"DrbdOptions/Net/protocol": "A"},                      // controller
		map[string]string{"DrbdOptions/Net/protocol": "B"},                      // RG
		map[string]string{"DrbdOptions/Net/protocol": "C"},                      // RD
		map[string]string{"DrbdOptions/Net/protocol": "C", "StorPoolName": "p"}, // resource
	)

	if got["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("protocol: got %q, want C", got["DrbdOptions/Net/protocol"])
	}

	// Non-DRBD prop (StorPoolName) survives the merge.
	if got["StorPoolName"] != "p" {
		t.Errorf("StorPoolName: got %q, want p", got["StorPoolName"])
	}
}

// TestResolveOptions_PartialInheritance: each scope contributes its
// own keys; defaults stay defaulted (controller-level), resource-level
// overrides only the keys it sets.
func TestResolveOptions_PartialInheritance(t *testing.T) {
	t.Parallel()

	got := drbd.ResolveOptions(
		map[string]string{
			"DrbdOptions/Net/max-buffers": "1024",
			"DrbdOptions/Net/protocol":    "C",
		},
		nil, nil,
		map[string]string{"DrbdOptions/Net/max-buffers": "8192"},
	)

	if got["DrbdOptions/Net/max-buffers"] != "8192" {
		t.Errorf("max-buffers: got %q, want 8192", got["DrbdOptions/Net/max-buffers"])
	}

	if got["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("protocol from controller scope dropped: got %q", got["DrbdOptions/Net/protocol"])
	}
}

// TestResolveOptions_ControllerNonDRBDPropsDropped: only DRBD props
// flow through from upper scopes. A controller-level `StorPoolName`
// (nonsensical, but possible) MUST NOT pollute the resource's prop
// bag — that's a placement decision, not a DRBD knob.
func TestResolveOptions_ControllerNonDRBDPropsDropped(t *testing.T) {
	t.Parallel()

	got := drbd.ResolveOptions(
		map[string]string{"StorPoolName": "from-controller"},
		nil, nil,
		map[string]string{"DrbdOptions/Net/protocol": "C"},
	)

	if _, present := got["StorPoolName"]; present {
		t.Errorf("non-DRBD controller prop leaked: %v", got)
	}
}

func TestSectionFor(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"DrbdOptions/Net/protocol":             drbd.SectionNet,
		"DrbdOptions/Disk/on-io-error":         drbd.SectionDisk,
		"DrbdOptions/PeerDevice/c-max-rate":    drbd.SectionPeerDevice,
		"DrbdOptions/peer-device/c-min-rate":   drbd.SectionPeerDevice,
		"DrbdOptions/Handlers/fence-peer":      drbd.SectionHandlers,
		"DrbdOptions/auto-promote":             drbd.SectionOptions,
		"DrbdOptions/UnknownSection/something": drbd.SectionOptions,
		"NotADrbdOption":                       drbd.SectionOptions,
	}

	for in, want := range cases {
		if got := drbd.SectionFor(in); got != want {
			t.Errorf("SectionFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFilterDRBD(t *testing.T) {
	t.Parallel()

	got := drbd.FilterDRBD(map[string]string{
		"DrbdOptions/Net/protocol": "C",
		"StorPoolName":             "p",
		"Aux/zone":                 "z1",
	})

	if got["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("DRBD prop missing: %v", got)
	}

	if _, ok := got["StorPoolName"]; ok {
		t.Errorf("non-DRBD prop leaked: %v", got)
	}

	if _, ok := got["Aux/zone"]; ok {
		t.Errorf("Aux prop leaked: %v", got)
	}
}
