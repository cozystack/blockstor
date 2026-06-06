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

package satellite

import (
	"testing"
)

// Upstream-issue U337 / U302 — persisted DRBD-options must not break a
// subsequent resource render, and a configured `verify-alg` must reach
// the rendered `net { }` block VERBATIM.
//
// The real-world report (C-package regression family): an operator sets
// an assortment of DRBD knobs at the RD / RG scope —
//
//	linstor rd drbd-options --verify-alg crc32c <rd>
//	linstor rd drbd-options --max-buffers 36864 <rd>
//	linstor controller drbd-options --protocol C        # (or RG/RD)
//
// then runs `vd c` + `r c` to add replicas. The C-package surface
// folds those persisted props into the Resource's `DrbdOptions/*` map
// (effectiveprops merge), and splitDRBDOptions routes each one to its
// `.res` section. The bug class this guards: a non-catalog Net key like
// `verify-alg` (not present in pkg/drbd's static netOptions() table)
// must still route to the net{} block — SectionFor keys off the
// `DrbdOptions/Net/` prefix, NOT off catalog membership, so an
// uncatalogued knob must NOT silently fall through to the catch-all
// resource-level options{} block (where drbdadm would reject it and
// wedge every subsequent reconcile on that RD).

// TestSplitDRBDOptionsRoutesVerifyAlgToNet pins U302: a configured
// `DrbdOptions/Net/verify-alg` lands in the net-section map (and thus
// renders verbatim into net{}), not in the resource-options bucket.
// verify-alg is deliberately NOT in pkg/drbd's static net catalogue —
// this asserts the prefix-based routing, not table membership.
func TestSplitDRBDOptionsRoutesVerifyAlgToNet(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/Net/verify-alg": "crc32c",
	})

	if v, ok := got.Net["verify-alg"]; !ok || v != "crc32c" {
		t.Errorf("net[verify-alg]=%q, ok=%v; want %q, true", v, ok, "crc32c")
	}

	// Must NOT leak into the catch-all resource options{} block, where
	// drbdadm rejects `verify-alg` ("expected: cpu-mask | ... but got:
	// verify-alg") and wedges the reconcile.
	if _, ok := got.Resource["verify-alg"]; ok {
		t.Errorf("verify-alg leaked into Resource (resource-options) map; would land in options{} and trip drbdadm")
	}
}

// TestSplitDRBDOptionsAssortedPersistedSet pins U337: an assorted set
// of persisted RD/RG drbd-options (verify-alg + max-buffers + protocol)
// all route to the net{} section together. The render must remain
// stable — no key dropped, none misrouted — so the subsequent `r c`
// renders a valid .res rather than wedging on a mis-bucketed knob.
func TestSplitDRBDOptionsAssortedPersistedSet(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/Net/verify-alg":  "crc32c",
		"DrbdOptions/Net/max-buffers": "36864",
		"DrbdOptions/Net/protocol":    "C",
	})

	wantNet := map[string]string{
		"verify-alg":  "crc32c",
		"max-buffers": "36864",
		"protocol":    "C",
	}

	for k, want := range wantNet {
		if v, ok := got.Net[k]; !ok || v != want {
			t.Errorf("net[%s]=%q, ok=%v; want %q, true", k, v, ok, want)
		}

		if _, ok := got.Resource[k]; ok {
			t.Errorf("%s leaked into Resource (resource-options) map", k)
		}
	}

	// No spurious extra Net keys beyond the three we set.
	if len(got.Net) != len(wantNet) {
		t.Errorf("net map size=%d, want %d: %v", len(got.Net), len(wantNet), got.Net)
	}
}
