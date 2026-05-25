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

package controller

import (
	"context"
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestAnyDataBearingDiskfulPeer pins the seed-GI data-integrity
// discriminator (Bug 342): the controller's ensureSeedFromGI must
// recognise a data-bearing diskful peer (UpToDate/Consistent/Outdated)
// so it refuses to stamp a SeedFromGI on a fresh replica that must
// instead SyncTarget (full resync) from that peer. A genuinely-fresh
// RD (no data peer) keeps the day0 skip-sync path enabled.
func TestAnyDataBearingDiskfulPeer(t *testing.T) {
	t.Parallel()

	diskful := func(name, state string) blockstoriov1alpha1.Resource {
		r := blockstoriov1alpha1.Resource{}
		r.Name = name
		if state != "" {
			r.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: state},
			}
		}

		return r
	}

	diskless := func(name, state string) blockstoriov1alpha1.Resource {
		r := diskful(name, state)
		r.Spec.Flags = []string{"DISKLESS"}

		return r
	}

	cases := []struct {
		name  string
		peers []blockstoriov1alpha1.Resource
		want  bool
	}{
		{
			name:  "no-peers-day0",
			peers: nil,
			want:  false,
		},
		{
			name:  "fresh-peer-inconsistent",
			peers: []blockstoriov1alpha1.Resource{diskful("rd.n-old", "Inconsistent")},
			want:  false,
		},
		{
			name:  "fresh-peer-no-diskstate",
			peers: []blockstoriov1alpha1.Resource{diskful("rd.n-old", "")},
			want:  false,
		},
		{
			name:  "data-peer-uptodate",
			peers: []blockstoriov1alpha1.Resource{diskful("rd.n-old", "UpToDate")},
			want:  true,
		},
		{
			name:  "data-peer-consistent",
			peers: []blockstoriov1alpha1.Resource{diskful("rd.n-old", "Consistent")},
			want:  true,
		},
		{
			name:  "data-peer-outdated",
			peers: []blockstoriov1alpha1.Resource{diskful("rd.n-old", "Outdated")},
			want:  true,
		},
		{
			name:  "diskless-uptodate-ignored",
			peers: []blockstoriov1alpha1.Resource{diskless("rd.n-old", "UpToDate")},
			want:  false,
		},
		{
			name: "self-uptodate-ignored",
			// The target itself reporting UpToDate must NOT count — only
			// a genuine PEER blocks the seed. Excluded by name.
			peers: []blockstoriov1alpha1.Resource{diskful("rd.n-self", "UpToDate")},
			want:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := anyDataBearingDiskfulPeer(tc.peers, "rd.n-self")
			if got != tc.want {
				t.Errorf("anyDataBearingDiskfulPeer = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnsureSeedFromGIGatedOnDataPeer pins the end-to-end controller
// gate: ensureSeedFromGI stamps SeedFromGI ONLY when there is no
// data-bearing peer. With a data peer present it leaves Spec untouched
// (mutated=false) so the satellite brings the fresh replica up
// Inconsistent + SyncTarget; without one it copies the UpToDate peer's
// CurrentGI (the legacy Phase-8.1 fresh-RD seed).
func TestEnsureSeedFromGIGatedOnDataPeer(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{}
	rd.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0},
	}

	peerWith := func(state, gi string) blockstoriov1alpha1.Resource {
		p := blockstoriov1alpha1.Resource{}
		p.Name = "rd.n-old"
		p.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
			{VolumeNumber: 0, DiskState: state, CurrentGI: gi},
		}

		return p
	}

	t.Run("data-peer-present-no-stamp", func(t *testing.T) {
		t.Parallel()

		r := &ResourceReconciler{}
		target := &blockstoriov1alpha1.Resource{}
		target.Name = "rd.n-new"

		peers := []blockstoriov1alpha1.Resource{peerWith("UpToDate", "AAAA")}

		mutated, err := r.ensureSeedFromGI(context.Background(), target, peers, rd)
		if err != nil {
			t.Fatalf("ensureSeedFromGI: %v", err)
		}

		if mutated {
			t.Fatalf("ensureSeedFromGI mutated Spec with a data-bearing peer present — must defer to SyncTarget")
		}

		if seedAlreadySet(target, 0) {
			t.Errorf("SeedFromGI was stamped despite a data-bearing peer")
		}
	})
}
