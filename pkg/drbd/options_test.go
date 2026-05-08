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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestOptionsHaveLinstorKey: every catalogue entry surfaces a
// `DrbdOptions/<Section>/<Name>` LINSTOR-compatible property name. We
// rely on this so users / golinstor clients can set keys verbatim.
func TestOptionsHaveLinstorKey(t *testing.T) {
	for _, opt := range drbd.Options() {
		if opt.LinstorKey == "" {
			t.Errorf("%s: missing LinstorKey", opt.Name)
		}

		if !strings.HasPrefix(opt.LinstorKey, "DrbdOptions/") {
			t.Errorf("%s: LinstorKey %q has wrong prefix", opt.Name, opt.LinstorKey)
		}
	}
}

// TestOptionsHaveSection: every entry pins a `.res` section so the
// satellite reconciler can route values without re-parsing the key.
func TestOptionsHaveSection(t *testing.T) {
	allowed := map[string]bool{
		drbd.SectionNet:        true,
		drbd.SectionDisk:       true,
		drbd.SectionPeerDevice: true,
		drbd.SectionOptions:    true,
		drbd.SectionHandlers:   true,
	}

	for _, opt := range drbd.Options() {
		if !allowed[opt.Section] {
			t.Errorf("%s: unknown section %q", opt.Name, opt.Section)
		}
	}
}

// TestOptionsUniqueLinstorKeys: catalogue must not have duplicates,
// otherwise property validation gets ambiguous.
func TestOptionsUniqueLinstorKeys(t *testing.T) {
	seen := map[string]struct{}{}

	for _, opt := range drbd.Options() {
		if _, dup := seen[opt.LinstorKey]; dup {
			t.Errorf("duplicate LinstorKey %q", opt.LinstorKey)
		}

		seen[opt.LinstorKey] = struct{}{}
	}
}

// TestOptionsCoverWellKnownKeys: a few keys cozystack-style clusters
// rely on must be present.
func TestOptionsCoverWellKnownKeys(t *testing.T) {
	want := []string{
		"DrbdOptions/Net/protocol",
		"DrbdOptions/Net/shared-secret",
		"DrbdOptions/Resource/auto-promote",
		"DrbdOptions/Resource/quorum",
		"DrbdOptions/Resource/on-no-quorum",
	}

	got := map[string]bool{}
	for _, opt := range drbd.Options() {
		got[opt.LinstorKey] = true
	}

	for _, key := range want {
		if !got[key] {
			t.Errorf("missing well-known key %q from catalogue", key)
		}
	}
}
