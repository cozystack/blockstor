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

// Regression coverage for Bug 258 — splitDRBDOptions misrouted
// `DrbdOptions/Disk/*`, `DrbdOptions/PeerDevice/*` and
// `DrbdOptions/Handlers/*` keys into the top-level resource `options{}`
// block. drbd-9 rejects these keys at that scope ("expected: cpu-mask
// | on-no-data-accessible | ... but got: on-io-error"), wedging the
// reconciler on any `linstor rd sp <rd> DrbdOptions/Disk/on-io-error
// detach` — a common operator action.
//
// These tests pin the post-fix routing: each section's keys land on
// the matching per-section map, NOT on resOpts.

// TestSplitDRBDOptionsRoutesDiskKeys: `DrbdOptions/Disk/on-io-error`
// must land in the disk-section map. The pre-fix path stuffed it into
// resOpts, which the renderer would emit inside the top-level
// `options { }` block — drbd-9 rejects `on-io-error` at that scope.
func TestSplitDRBDOptionsRoutesDiskKeys(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/Disk/on-io-error": "detach",
	})

	if v, ok := got.Disk["on-io-error"]; !ok || v != "detach" {
		t.Errorf("disk[on-io-error]=%q, ok=%v; want %q, true", v, ok, "detach")
	}

	if _, ok := got.Resource["on-io-error"]; ok {
		t.Errorf("on-io-error leaked into Resource (resource-options) map; would land in options{} and trip drbdadm")
	}
}

// TestSplitDRBDOptionsRoutesHandlersKeys: `DrbdOptions/Handlers/fence-peer`
// must land in the handlers-section map. drbd-9 rejects `fence-peer`
// in `options{}` — handlers live in a dedicated `handlers{}` block.
func TestSplitDRBDOptionsRoutesHandlersKeys(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/Handlers/fence-peer": "/usr/lib/drbd/crm-fence-peer.9.sh",
	})

	if v, ok := got.Handlers["fence-peer"]; !ok || v != "/usr/lib/drbd/crm-fence-peer.9.sh" {
		t.Errorf("handlers[fence-peer]=%q, ok=%v; want script path, true", v, ok)
	}

	if _, ok := got.Resource["fence-peer"]; ok {
		t.Errorf("fence-peer leaked into Resource (resource-options) map; would land in options{} and trip drbdadm")
	}
}

// TestSplitDRBDOptionsRoutesPeerDeviceKeys:
// `DrbdOptions/PeerDevice/c-fill-target` must land in the
// peer-device-section map. drbd-9 rejects `c-fill-target` in
// `options{}` — it belongs in `disk{}` inside a `connection{}` block.
func TestSplitDRBDOptionsRoutesPeerDeviceKeys(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/PeerDevice/c-fill-target": "1M",
	})

	if v, ok := got.PeerDevice["c-fill-target"]; !ok || v != "1M" {
		t.Errorf("peerDevice[c-fill-target]=%q, ok=%v; want %q, true", v, ok, "1M")
	}

	if _, ok := got.Resource["c-fill-target"]; ok {
		t.Errorf("c-fill-target leaked into Resource (resource-options) map; would land in options{} and trip drbdadm")
	}
}

// TestSplitDRBDOptionsRoutesPeerDeviceKeysKebabAlias: the LINSTOR
// namespace is "PeerDevice", but upstream also accepts the kebab
// alias "peer-device" (case-insensitive). Both forms must route to
// the same per-connection disk{} block.
func TestSplitDRBDOptionsRoutesPeerDeviceKeysKebabAlias(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/peer-device/c-max-rate": "100M",
	})

	if v, ok := got.PeerDevice["c-max-rate"]; !ok || v != "100M" {
		t.Errorf("peerDevice[c-max-rate]=%q, ok=%v; want %q, true", v, ok, "100M")
	}

	if _, ok := got.Resource["c-max-rate"]; ok {
		t.Errorf("c-max-rate (peer-device alias) leaked into Resource map")
	}
}

// TestSplitDRBDOptionsKeepsNetRouting: regression guard that Bug 138's
// `DrbdOptions/Net/*` routing still works after the section-routing
// rewrite. Net options must land on `got.Net`, not anywhere else.
func TestSplitDRBDOptionsKeepsNetRouting(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/Net/protocol": "C",
	})

	if v, ok := got.Net["protocol"]; !ok || v != "C" {
		t.Errorf("net[protocol]=%q, ok=%v; want %q, true", v, ok, "C")
	}

	if _, ok := got.Resource["protocol"]; ok {
		t.Errorf("protocol leaked into Resource (resource-options) map")
	}
}

// TestSplitDRBDOptionsKeepsResourceRouting: Resource-scope options
// (the catch-all that drbd-9 DOES accept at top level) must still land
// on `got.Resource`. Pre-fix this was the entire bucket; post-fix it
// MUST keep working for the keys that genuinely belong here.
func TestSplitDRBDOptionsKeepsResourceRouting(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/Resource/on-no-quorum": "suspend-io",
	})

	if v, ok := got.Resource["on-no-quorum"]; !ok || v != "suspend-io" {
		t.Errorf("resource[on-no-quorum]=%q, ok=%v; want %q, true", v, ok, "suspend-io")
	}
}

// TestSplitDRBDOptionsDropsLinstorOnlyKeys: section-less keys are
// LINSTOR-controller-only (e.g. `DrbdOptions/AutoEvictAllowEviction`);
// they have no DRBD section, so the renderer must NOT write them
// anywhere in the .res. Pre-fix behaviour kept and re-asserted here.
func TestSplitDRBDOptionsDropsLinstorOnlyKeys(t *testing.T) {
	t.Parallel()

	got := splitDRBDOptions(map[string]string{
		"DrbdOptions/AutoEvictAllowEviction": "false",
	})

	for name, m := range map[string]map[string]string{
		"Net":        got.Net,
		"Disk":       got.Disk,
		"PeerDevice": got.PeerDevice,
		"Handlers":   got.Handlers,
		"Resource":   got.Resource,
	} {
		if _, ok := m["AutoEvictAllowEviction"]; ok {
			t.Errorf("LINSTOR-only key leaked into %s map", name)
		}
	}
}
