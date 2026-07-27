// SPDX-License-Identifier: Apache-2.0

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
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// The namespace a knob is filed under decides which `.res` block it is
// rendered into. A `net{}` option written under `Resource` lands in
// options{} where drbdadm rejects it, wedging every later adjust for
// that resource — so the mapping is pinned per section.
func TestFlagKeyNamespaces(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		// Uncatalogued knobs the harness sets: both must reach net{}.
		"verify-alg":  "DrbdOptions/Net/verify-alg",
		"c-min-rate":  "DrbdOptions/PeerDevice/c-min-rate",
		"max-buffers": "DrbdOptions/Net/max-buffers",
		"c-max-rate":  "DrbdOptions/PeerDevice/c-max-rate",

		"on-io-error":               "DrbdOptions/Disk/on-io-error",
		"rs-discard-granularity":    "DrbdOptions/Disk/rs-discard-granularity",
		"quorum":                    "DrbdOptions/Resource/quorum",
		"on-no-quorum":              "DrbdOptions/Resource/on-no-quorum",
		"quorum-minimum-redundancy": "DrbdOptions/Resource/quorum-minimum-redundancy",
		"auto-promote":              "DrbdOptions/Resource/auto-promote",
		"fence-peer":                "DrbdOptions/Handlers/fence-peer",
		"before-resync-target":      "DrbdOptions/Handlers/before-resync-target",
	}

	for name, want := range cases {
		got, ok := drbd.FlagKey(name)
		if !ok {
			t.Errorf("--%s is not resolvable", name)

			continue
		}

		if got != want {
			t.Errorf("--%s → %q, want %q", name, got, want)
		}
	}
}

// SectionFor routes on the namespace, so every knob FlagKey resolves
// must land in a real section — a key whose namespace SectionFor does
// not recognise silently falls through to the resource options block.
func TestEveryFlagKeyRoutesToItsSection(t *testing.T) {
	t.Parallel()

	sections := map[string]string{
		"DrbdOptions/Net/":        drbd.SectionNet,
		"DrbdOptions/Disk/":       drbd.SectionDisk,
		"DrbdOptions/PeerDevice/": drbd.SectionPeerDevice,
		"DrbdOptions/Resource/":   drbd.SectionOptions,
		"DrbdOptions/Handlers/":   drbd.SectionHandlers,
	}

	for _, name := range drbd.FlagNames() {
		key, ok := drbd.FlagKey(name)
		if !ok {
			t.Errorf("FlagNames lists --%s but FlagKey cannot resolve it", name)

			continue
		}

		matched := false

		for prefix, want := range sections {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				matched = true

				if got := drbd.SectionFor(key); got != want {
					t.Errorf("--%s (%s) routes to %q, want %q", name, key, got, want)
				}
			}
		}

		if !matched {
			t.Errorf("--%s resolves to %q, which is in no known namespace", name, key)
		}
	}
}

// Every knob the render catalogue carries has to be settable, or an
// option blockstor renders could not be configured.
func TestCataloguedOptionsAreSettable(t *testing.T) {
	t.Parallel()

	names := drbd.FlagNames()

	for _, opt := range drbd.Options() {
		if !slices.Contains(names, opt.Name) {
			t.Errorf("catalogued option %q is not offered as a drbd-options flag", opt.Name)
		}

		key, ok := drbd.FlagKey(opt.Name)
		if !ok || key != opt.LinstorKey {
			t.Errorf("--%s → %q, want the catalogue's %q", opt.Name, key, opt.LinstorKey)
		}
	}
}

// An unknown knob is not filed under a guessed namespace.
func TestUnknownFlagIsRejected(t *testing.T) {
	t.Parallel()

	if key, ok := drbd.FlagKey("not-a-drbd-option"); ok {
		t.Errorf("an unknown knob resolved to %q", key)
	}
}
