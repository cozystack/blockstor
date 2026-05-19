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

package rest

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourcesViewEmpty: empty list, never nil — golinstor iterates blindly.
func TestResourcesViewEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	got, err := c.Resources.GetResourceView(t.Context())
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestResourcesViewAcrossNodes: replicas on different nodes are all returned.
func TestResourcesViewAcrossNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-1", NodeName: "n2"},
		{Name: "pvc-2", NodeName: "n2"},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)

	got, err := c.Resources.GetResourceView(t.Context())
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if len(got) != 3 {
		t.Errorf("len: got %d, want 3", len(got))
	}
}

// TestResourcesViewNodeFilter: ?nodes=n1 returns only the n1 replicas.
// Case-insensitive matching mirrors Java LINSTOR behaviour so a client
// sending mixed-case node names still gets results.
func TestResourcesViewNodeFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-1", NodeName: "n2"},
		{Name: "pvc-2", NodeName: "n2"},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?nodes=N1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1; entries=%v", len(got), got)
	}

	if got[0].NodeName != "n1" {
		t.Errorf("filter leaked: got node %q, want n1", got[0].NodeName)
	}
}

// TestResourcesViewRDFilter: ?resources=pvc-1 returns only that RD's replicas.
func TestResourcesViewRDFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-2", NodeName: "n2"},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?resources=pvc-1")
	defer func() { _ = resp.Body.Close() }()

	var got []apiv1.ResourceWithVolumes

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 || got[0].Name != "pvc-1" {
		t.Errorf("got %v, want one entry for pvc-1", got)
	}
}

// TestFaultyFilterPrioritizesZeroUpToDate exercises the recovery-
// copilot ranking: `?faulty=true` excludes fully-healthy RDs and
// orders the remainder so the RDs with ZERO UpToDate copies come
// first (operators have to intervene there — DRBD has no good
// replica to seed from), followed by RDs that still have at least
// one UpToDate replica.
//
// Seed:
//   - rd-1: 3 replicas, all Inconsistent  → 0 UpToDate, faulty (FIRST)
//   - rd-2: 1 UpToDate + 1 StandAlone     → 1 UpToDate, faulty (SECOND)
//   - rd-3: 3 replicas, all UpToDate      → 3 UpToDate, healthy (EXCLUDED)
func TestFaultyFilterPrioritizesZeroUpToDate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seed := []apiv1.Resource{
		// rd-1 — all replicas Inconsistent: 0 UpToDate, must come first.
		{Name: "rd-1", NodeName: "n1", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}}},
		{Name: "rd-1", NodeName: "n2", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}}},
		{Name: "rd-1", NodeName: "n3", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}}},
		// rd-2 — one UpToDate replica + one StandAlone peer: 1 UpToDate.
		{Name: "rd-2", NodeName: "n1", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
		{Name: "rd-2", NodeName: "n2", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "StandAlone"},
		}}},
		// rd-3 — fully healthy: must be filtered out entirely.
		{Name: "rd-3", NodeName: "n1", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
		{Name: "rd-3", NodeName: "n2", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
		{Name: "rd-3", NodeName: "n3", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
	}

	for i := range seed {
		if err := st.Resources().Create(ctx, &seed[i]); err != nil {
			t.Fatalf("seed %s/%s: %v", seed[i].Name, seed[i].NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?faulty=true")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// rd-3 (3 replicas, all UpToDate) must be excluded; only rd-1
	// (3 replicas) + rd-2 (2 replicas) survive = 5 entries.
	if len(got) != 5 {
		t.Fatalf("len: got %d, want 5; entries=%v", len(got), got)
	}

	// rd-3 must not appear anywhere — defence in depth against
	// regressions where the filter weakens to "any non-empty
	// disk_state" or similar.
	for i := range got {
		if got[i].Name == "rd-3" {
			t.Errorf("rd-3 (all UpToDate) leaked into faulty view at index %d", i)
		}
	}

	// First three entries belong to rd-1 (0 UpToDate); next two to
	// rd-2 (1 UpToDate). Bucket boundary is the load-bearing
	// invariant — within each bucket the deterministic Name+NodeName
	// tiebreak keeps order stable for pagination.
	for i := range 3 {
		if got[i].Name != "rd-1" {
			t.Errorf("position %d: got %q, want rd-1 (0 UpToDate first)", i, got[i].Name)
		}
	}

	for i := 3; i < 5; i++ {
		if got[i].Name != "rd-2" {
			t.Errorf("position %d: got %q, want rd-2 (1 UpToDate second)", i, got[i].Name)
		}
	}
}

// TestResourcesViewWithoutStore: 503 when store is nil.
func TestResourcesViewWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
}

// TestResourceShapeIncludesReversibilityHint is a cross-project contract test
// for the ccp drbd-recovery copilot SKILL §Core-Principles #8 "ASK before
// destructive op" prompt. The copilot needs four fields to render an
// actionable approval prompt for a target resource:
//
//  1. Resource name + volume(s)             — already exposed via Resource/Volume.
//  2. All replica states (UpToDate / Inconsistent / Diskless flags)
//     — already exposed via Resource.Flags + VolumeState.DiskState
//     (`?faulty=true` filter, Bug 16 fix).
//  3. The exact LINSTOR/drbdadm command the copilot wants to run
//     — MISSING. The copilot currently has to hand-roll the command
//     string from heuristics, which makes the approval prompt fragile
//     (e.g. is it `drbdadm primary --force` or `drbdadm -- --discard-my-data
//     connect`?).
//  4. The reversibility classification of that command
//     — MISSING. Without it the prompt cannot distinguish
//     read-only (safe to retry) from interrupts-I/O (causes brief
//     outage) from destroys-data (irreversible).
//
// Wire shape expected (proposal, pending review):
//
//	{
//	  "name": "pvc-1",
//	  "node_name": "n1",
//	  ...
//	  "recovery_metadata": {
//	    "actionable_commands": [
//	      {
//	        "cmd": "drbdadm -- --discard-my-data connect pvc-1",
//	        "reversibility_class": "destroys-data"
//	      },
//	      {
//	        "cmd": "linstor resource toggle-disk n1 pvc-1 --diskless",
//	        "reversibility_class": "interrupts-io"
//	      }
//	    ]
//	  }
//	}
//
// `reversibility_class` is one of: `read-only`, `interrupts-io`,
// `destroys-data` — mirrors the SKILL's destructive-op taxonomy.
//
// Today the field is absent on every code path: there is no
// `RecoveryMetadata` type in pkg/api/v1, no producer in pkg/rest, and no
// consumer in the copilot beyond the SKILL spec itself. This test stands
// as the contract: when the hint engine lands, drop the t.Skip and the
// assertions below become the green-bar definition of "done".
func TestResourceShapeIncludesReversibilityHint(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// One faulty replica is enough to make the hint engine
	// non-trivial: a single Inconsistent replica + an UpToDate
	// peer is the canonical "ASK before forcing primary" case.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Flags:    []string{},
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Decode loosely so the test does not depend on the as-yet
	// unwritten Go type. When the field lands the loose decode
	// will still succeed; only the Skip-vs-assert branch flips.
	var raw []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(raw) != 1 {
		t.Fatalf("len: got %d, want 1", len(raw))
	}

	meta, ok := raw[0]["recovery_metadata"].(map[string]any)
	if !ok {
		t.Skip("spec gap: Resource wire shape has no `recovery_metadata` " +
			"block yet — the ccp drbd-recovery copilot SKILL §Core " +
			"Principles #8 approval prompt currently has to hand-roll " +
			"the destructive-op command + reversibility class from " +
			"heuristics. Follow-up: add api/v1 RecoveryMetadata type " +
			"with ActionableCommands []{Cmd, ReversibilityClass} and a " +
			"producer in pkg/rest/resources.go that classifies the " +
			"recommended next step from replica flags + disk_state. " +
			"reversibility_class taxonomy: read-only | interrupts-io | " +
			"destroys-data.")
	}

	cmds, ok := meta["actionable_commands"].([]any)
	if !ok || len(cmds) == 0 {
		t.Fatalf("recovery_metadata.actionable_commands: got %v, want non-empty list", meta["actionable_commands"])
	}

	for i, c := range cmds {
		entry, ok := c.(map[string]any)
		if !ok {
			t.Errorf("actionable_commands[%d]: not an object: %T", i, c)

			continue
		}

		cmd, _ := entry["cmd"].(string)
		if cmd == "" {
			t.Errorf("actionable_commands[%d].cmd: empty", i)
		}

		rev, _ := entry["reversibility_class"].(string)
		switch rev {
		case "read-only", "interrupts-io", "destroys-data":
			// ok
		default:
			t.Errorf("actionable_commands[%d].reversibility_class: got %q, want one of read-only|interrupts-io|destroys-data", i, rev)
		}
	}
}

// cliStateFromFlags models the upstream linstor Python CLI's `State`
// column rendering for `linstor r list`. The CLI's rsc_state derivation
// keys off Resource.Flags membership:
//
//   - DISKLESS + TIE_BREAKER → "TieBreaker" (autoplacer-stamped witness)
//   - DISKLESS (only)        → "Diskless"   (operator-placed diskless)
//   - neither                → "" (caller falls back to per-volume
//     disk_state, e.g. "UpToDate" / "Inconsistent")
//
// The order matters: TIE_BREAKER always implies DISKLESS, so the
// TieBreaker branch must be checked first. Without that ordering the
// witness would render as "Diskless" and the operator-placed diskless
// would be indistinguishable from a tiebreaker — which is the exact
// regression scenario 5.7 guards against.
func cliStateFromFlags(flags []string) string {
	hasDiskless := slices.Contains(flags, apiv1.ResourceFlagDiskless)
	hasTieBreaker := slices.Contains(flags, apiv1.ResourceFlagTieBreaker)

	switch {
	case hasDiskless && hasTieBreaker:
		return "TieBreaker"
	case hasDiskless:
		return "Diskless"
	default:
		return ""
	}
}

// TestViewResourcesDistinguishesDisklessFromTiebreaker is the scenario
// 5.7 regression guard for the TIE_BREAKER vs DISKLESS wire shape.
//
// Two Resources can both carry the DISKLESS flag but mean very different
// things to the operator:
//
//  1. Operator-placed diskless — `linstor r c <node> <rd> --diskless`
//     plants `Flags: [DISKLESS]`. The Python CLI renders this as
//     `State=Diskless` in `linstor r list`.
//  2. Autoplacer-stamped tiebreaker — when place_count < peers the
//     RD reconciler (and the placer) drop a witness with
//     `Flags: [DISKLESS, TIE_BREAKER]`. The CLI renders this as
//     `State=TieBreaker`.
//
// The two MUST be distinguishable across:
//   - the REST `flags` array (raw, exact membership round-trip), and
//   - the derived CLI `State` column the Python client computes from
//     those flags.
//
// If TIE_BREAKER ever gets dropped on the way through the store / wire,
// the operator can no longer tell an explicit diskless from an
// auto-cleanup-eligible witness — and the autoplace.go
// promoteOrCreateReplica path uses that distinction to decide whether
// to strip the flag (witness) or preserve it (operator intent).
func TestViewResourcesDistinguishesDisklessFromTiebreaker(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// pvc-1/n1: operator-placed diskless. Flags = [DISKLESS] only.
	// pvc-1/n2: autoplacer tiebreaker. Flags = [DISKLESS, TIE_BREAKER].
	seed := []apiv1.Resource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Flags:    []string{apiv1.ResourceFlagDiskless},
		},
		{
			Name:     "pvc-1",
			NodeName: "n2",
			Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
	}

	for i := range seed {
		if err := st.Resources().Create(ctx, &seed[i]); err != nil {
			t.Fatalf("seed %s/%s: %v", seed[i].Name, seed[i].NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2; entries=%v", len(got), got)
	}

	// Index by node so the test is independent of sort order — the
	// default sort is Name+NodeName so n1 comes before n2, but
	// asserting via map lookup keeps the contract on the wire-shape,
	// not on sort order (a separate test guards sort stability).
	byNode := map[string]apiv1.ResourceWithVolumes{}

	for i := range got {
		byNode[got[i].NodeName] = got[i]
	}

	diskless, ok := byNode["n1"]
	if !ok {
		t.Fatalf("missing pvc-1/n1 (operator-placed diskless) in view: %v", got)
	}

	tiebreaker, ok := byNode["n2"]
	if !ok {
		t.Fatalf("missing pvc-1/n2 (autoplacer tiebreaker) in view: %v", got)
	}

	// --- Flags round-trip --------------------------------------------------
	//
	// Operator-placed diskless: exactly [DISKLESS] — no TIE_BREAKER
	// leakage. If TIE_BREAKER ever appears here, the autoplace.go
	// cleanup heuristics would mis-classify an operator-intentional
	// diskless as a stale witness and strip it.
	if !slices.Contains(diskless.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("operator-placed diskless: DISKLESS missing from flags=%v", diskless.Flags)
	}

	if slices.Contains(diskless.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("operator-placed diskless: TIE_BREAKER leaked into flags=%v", diskless.Flags)
	}

	// Autoplacer tiebreaker: BOTH DISKLESS and TIE_BREAKER must
	// survive the wire round-trip. Dropping TIE_BREAKER here is the
	// exact regression that turns a cleanup-eligible witness into
	// what looks like operator intent.
	if !slices.Contains(tiebreaker.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("autoplacer tiebreaker: DISKLESS missing from flags=%v", tiebreaker.Flags)
	}

	if !slices.Contains(tiebreaker.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("autoplacer tiebreaker: TIE_BREAKER missing from flags=%v", tiebreaker.Flags)
	}

	// --- CLI State rendering -----------------------------------------------
	//
	// The two MUST collapse to different `State` strings under the
	// Python CLI's rsc_state derivation. If both render as the same
	// label, an operator running `linstor r list` cannot tell them
	// apart at the only place they look — the State column.
	disklessState := cliStateFromFlags(diskless.Flags)
	tiebreakerState := cliStateFromFlags(tiebreaker.Flags)

	if disklessState != "Diskless" {
		t.Errorf("operator-placed diskless: State=%q, want %q", disklessState, "Diskless")
	}

	if tiebreakerState != "TieBreaker" {
		t.Errorf("autoplacer tiebreaker: State=%q, want %q", tiebreakerState, "TieBreaker")
	}

	if disklessState == tiebreakerState {
		t.Fatalf("CLI State collision: both render as %q — operator cannot distinguish "+
			"explicit diskless from autoplacer witness", disklessState)
	}
}

// TestResourceDeleteUnknownRDReturns200Warning pins half of the CSI
// idempotence contract for Bug 56: DELETE on a {rd} path segment that
// never existed returns 200 + an ApiCallRc envelope carrying the
// WARN bit and an "already absent" message, NOT 404 / 500.
// linstor-csi's DeleteVolume retries until it sees success; the
// previous 404 broke the second-delete-after-success path and
// surfaced as a divergence against upstream LINSTOR (cli-parity-audit
// row #42: upstream → 200 WARN, blockstor → 500 ERROR).
func TestResourceDeleteUnknownRDReturns200Warning(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/ghost-rd/resources/any-node")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("ApiCallRc envelope: got empty, want one entry")
	}

	// WARN bit (maskWarn = 0x0002_0000_0000) MUST be set so python-
	// linstor surfaces the advisory line in `linstor r d` output.
	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}
}

// TestResourceDeleteUnknownNodeReturns200Warning pins the other half
// of the Bug 56 idempotence contract: with the RD present but no
// replica on the requested node, the handler must STILL fold the
// per-replica NotFound into the 200 + WARN envelope rather than
// 404 / 500. Upstream LINSTOR returns `WARNING: Node: X, Resource: Y
// not found.` exit 0 on this exact input.
func TestResourceDeleteUnknownNodeReturns200Warning(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-real"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-real/resources/ghost-node")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("ApiCallRc envelope: got empty, want one entry")
	}

	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "pvc-real") || !strings.Contains(rc[0].Message, "ghost-node") {
		t.Errorf("message: got %q, want it to name both pvc-real and ghost-node", rc[0].Message)
	}
}

// TestResourceDeleteSuccessUsesInfoMaskNotWarn pins the success-path
// distinction: a DELETE that actually drops a real replica must
// reply with the MASK_INFO + RC_RSC_DELETED entry (NO warn bit), so
// operators reading API logs can tell a real drop from a no-op
// replay. Without this guard, a regression that always tagged the
// envelope with WARN would silently make every successful delete
// look like an idempotent re-try in audit logs.
func TestResourceDeleteSuccessUsesInfoMaskNotWarn(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-live"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-live", NodeName: "n1"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-live/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("ApiCallRc envelope: got empty, want one entry")
	}

	// Success path: MASK_INFO bit set, MASK_WARN bit NOT set.
	if rc[0].RetCode&apiCallRcInfo == 0 {
		t.Errorf("ret_code: got %#x, want MASK_INFO (%#x) set", rc[0].RetCode, apiCallRcInfo)
	}

	if rc[0].RetCode&maskWarn != 0 {
		t.Errorf("ret_code: got %#x, MASK_WARN (%#x) must NOT be set on a real drop",
			rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "resource deleted") {
		t.Errorf("message: got %q, want 'resource deleted' marker", rc[0].Message)
	}

	// Belt + braces: the row really left the store, not just the
	// envelope. Without this, a buggy handler that always emitted
	// the success envelope without calling Delete would pass the
	// status/mask checks while leaking entries on every CSI retry.
	_, err := st.Resources().Get(ctx, "pvc-live", "n1")
	if err == nil {
		t.Errorf("replica still present after DELETE; want it gone")
	}
}

// walkResourceLayerStack walks the single-branch children chain of
// `layer_object` and returns the discriminator type at each level.
// Mirrors the Python LINSTOR client's `rsc.layer_data.layer_stack`
// derivation — the CLI's `linstor r list` Layers column joins this
// list with commas. Used by the F19 tests below.
func walkResourceLayerStack(top *apiv1.ResourceLayer) []string {
	if top == nil {
		return nil
	}

	out := []string{top.Type}
	cursor := top

	for len(cursor.Children) > 0 {
		cursor = &cursor.Children[0]
		out = append(out, cursor.Type)
	}

	return out
}

// findResourceStorageLeaf walks `layer_object` for the STORAGE leaf
// (always the bottom of the chain in DRBD-over-STORAGE deployments).
// Returns nil when no STORAGE entry exists.
func findResourceStorageLeaf(top *apiv1.ResourceLayer) *apiv1.ResourceLayer {
	for cursor := top; cursor != nil; {
		if cursor.Type == apiv1.LayerKindStorage {
			return cursor
		}

		if len(cursor.Children) == 0 {
			return nil
		}

		cursor = &cursor.Children[0]
	}

	return nil
}

// TestTieBreakerHasSTORAGEChildLayer pins F19's wire-shape: a Resource
// with `Flags=[DISKLESS, TIE_BREAKER]` must still expose a STORAGE
// child layer under `layer_object.children[0]`, with the storage
// payload marked `provider_kind=DISKLESS`. Upstream LINSTOR keeps the
// leaf for diskless replicas — stripping it makes the Python CLI's
// `linstor r l` Layers column render `DRBD` instead of the upstream
// `DRBD,STORAGE`, breaking CLI parity. Bug F19.
func TestTieBreakerHasSTORAGEChildLayer(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed with the upstream-parity wire shape so the REST surface
	// preserves it across encode/decode. The k8s store's
	// `crdToWireResource` builds the same shape from CRD spec — the
	// internal_test in pkg/store/k8s pins THAT synthesis; here we
	// pin that the REST layer never re-strips or mutates it.
	seed := apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n-witness",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Children: []apiv1.ResourceLayer{{
				Type: apiv1.LayerKindStorage,
				Storage: &apiv1.StorageResourceLayer{
					ProviderKind: apiv1.StoragePoolKindDiskless,
				},
			}},
		},
	}

	if err := st.Resources().Create(ctx, &seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1", len(got))
	}

	res := got[0]
	if res.LayerObject == nil {
		t.Fatal("layer_object missing — CLI would crash on AttributeError")
	}

	if res.LayerObject.Type != apiv1.LayerKindDRBD {
		t.Errorf("top layer: got %q, want DRBD", res.LayerObject.Type)
	}

	if len(res.LayerObject.Children) != 1 {
		t.Fatalf("children: got %v, want [STORAGE]", res.LayerObject.Children)
	}

	storage := &res.LayerObject.Children[0]
	if storage.Type != apiv1.LayerKindStorage {
		t.Errorf("children[0].type: got %q, want %q (F19 — upstream keeps STORAGE leaf on tiebreakers)",
			storage.Type, apiv1.LayerKindStorage)
	}

	if storage.Storage == nil {
		t.Fatal("children[0].storage payload nil — Layers column has no provider_kind to render")
	}

	if storage.Storage.ProviderKind != apiv1.StoragePoolKindDiskless {
		t.Errorf("children[0].storage.provider_kind: got %q, want %q",
			storage.Storage.ProviderKind, apiv1.StoragePoolKindDiskless)
	}

	if len(storage.Storage.StorageVolumes) != 0 {
		t.Errorf("children[0].storage.storage_volumes: got %v, want empty (no backing on witness)",
			storage.Storage.StorageVolumes)
	}
}

// TestTieBreakerLayersColumnRendersDRBDSTORAGE walks the wire
// `layer_object` the way the Python CLI's `rsc.layer_data.layer_stack`
// derivation does and confirms that a tiebreaker emits BOTH `DRBD`
// AND `STORAGE` in the chain — matching upstream LINSTOR's
// `linstor r l` Layers column rendering. Without this guard, a
// regression that re-strips the STORAGE leaf on diskless would slip
// past the type-only assertion in the previous test (children empty
// vs. children[0].type==STORAGE collapse to similar-looking failures
// at first glance). F19.
func TestTieBreakerLayersColumnRendersDRBDSTORAGE(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seed := apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n-witness",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Children: []apiv1.ResourceLayer{{
				Type: apiv1.LayerKindStorage,
				Storage: &apiv1.StorageResourceLayer{
					ProviderKind: apiv1.StoragePoolKindDiskless,
				},
			}},
		},
	}

	if err := st.Resources().Create(ctx, &seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1", len(got))
	}

	stack := walkResourceLayerStack(got[0].LayerObject)
	want := []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage}

	if len(stack) != len(want) {
		t.Fatalf("layer_stack: got %v, want %v (`linstor r l` Layers column would render %q)",
			stack, want, strings.Join(stack, ","))
	}

	for i := range want {
		if stack[i] != want[i] {
			t.Errorf("layer_stack[%d]: got %q, want %q", i, stack[i], want[i])
		}
	}

	if rendered := strings.Join(stack, ","); rendered != "DRBD,STORAGE" {
		t.Errorf("Layers column: got %q, want %q (upstream LINSTOR wire-parity)",
			rendered, "DRBD,STORAGE")
	}
}

// TestDiskfulResourceUnchanged is the regression guard for the F19
// fix: a normal diskful Resource (no DISKLESS/TIE_BREAKER flags) must
// STILL carry the STORAGE child with real `storage_volumes` derived
// from the satellite-observed per-volume state. Without this, the
// F19 fix could accidentally collapse the diskful path into the same
// empty-payload shape as the witness, hiding `device_path` from the
// CLI's `linstor v l` fallback.
func TestDiskfulResourceUnchanged(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Diskful seed: no DISKLESS / TIE_BREAKER flag; the STORAGE leaf
	// carries a real `device_path` per volume so the CLI's fallback
	// rendering works when the DRBD-layer device_path is empty.
	seed := apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n-storage",
		// No DISKLESS / TIE_BREAKER — this is a diskful replica.
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Children: []apiv1.ResourceLayer{{
				Type: apiv1.LayerKindStorage,
				Storage: &apiv1.StorageResourceLayer{
					ProviderKind: apiv1.StoragePoolKindZFS,
					StorageVolumes: []apiv1.StorageVolumeLayer{{
						VolumeNumber:     0,
						DevicePath:       "/dev/zvol/tank/pvc-1_00000",
						AllocatedSizeKib: 4096,
						UsableSizeKib:    4096,
						DiskState:        "UpToDate",
					}},
				},
			}},
		},
	}

	if err := st.Resources().Create(ctx, &seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1", len(got))
	}

	storage := findResourceStorageLeaf(got[0].LayerObject)
	if storage == nil {
		t.Fatal("diskful: STORAGE leaf missing — F19 regression collapsed diskful into witness shape")
	}

	if storage.Storage == nil {
		t.Fatal("diskful: STORAGE payload nil — `linstor v l` device_path fallback broken")
	}

	// Diskful MUST NOT have provider_kind=DISKLESS — that's the
	// exact discriminator the F19 fix uses to tell witnesses from
	// real backings on the wire.
	if storage.Storage.ProviderKind == apiv1.StoragePoolKindDiskless {
		t.Errorf("diskful: provider_kind=%q, want anything except DISKLESS (witness collision)",
			storage.Storage.ProviderKind)
	}

	if storage.Storage.ProviderKind != apiv1.StoragePoolKindZFS {
		t.Errorf("diskful: provider_kind=%q, want %q (seeded value must round-trip)",
			storage.Storage.ProviderKind, apiv1.StoragePoolKindZFS)
	}

	if len(storage.Storage.StorageVolumes) != 1 {
		t.Fatalf("diskful: storage_volumes len=%d, want 1 (seeded volume should round-trip)",
			len(storage.Storage.StorageVolumes))
	}

	vol := storage.Storage.StorageVolumes[0]
	if vol.VolumeNumber != 0 || vol.DevicePath != "/dev/zvol/tank/pvc-1_00000" {
		t.Errorf("diskful: storage_volumes[0]=%+v, want vol=0 device=/dev/zvol/tank/pvc-1_00000", vol)
	}

	if vol.DiskState != "UpToDate" {
		t.Errorf("diskful: storage_volumes[0].disk_state=%q, want UpToDate", vol.DiskState)
	}
}

// TestResourceListPropertiesRoundTripAllNamespaces pins scenario
// 1.W01 (P0, unit) for the Resource scope: `linstor resource
// list-properties` reads the `props` field of `GET
// /v1/resource-definitions/{rd}/resources/{node}`. Every LINSTOR-
// known namespace (`DrbdOptions/`, `Aux/`, `FileSystem/`,
// `StorDriver/`) must round-trip verbatim — per-replica overrides
// only take effect on a specific (rd, node) pair, so any
// normalisation drift would silently miss the replica it was
// meant to target.
func TestResourceListPropertiesRoundTripAllNamespaces(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	seed := map[string]string{
		"DrbdOptions/Net/protocol": "C",
		"DrbdOptions/PeerSlots":    "7",
		"Aux/cozystack.io/replica": "primary",
		"FileSystem/Type":          "ext4",
		"StorDriver/StorPoolName":  "blockstor-zfs",
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Props:    maps.Clone(seed),
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Props == nil {
		t.Fatalf("Props: got nil, want a {Key,Value} map")
	}

	for k, want := range seed {
		if got.Props[k] != want {
			t.Errorf("Props[%q]: got %q, want %q (namespace round-trip drift)", k, got.Props[k], want)
		}
	}
}

// TestResourceListPropertiesUnknownReturns404 pins the unknown-scope
// half of scenario 1.W01 for resources: an absent (rd, node) pair
// must 404 rather than fold into a 200 with empty Props.
func TestResourceListPropertiesUnknownReturns404(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost-rd/resources/ghost-node")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceListPropertiesEmptyDecodes pins the "empty scope
// returns empty map (not nil)" clause for the Resource scope: a
// replica with no Props decodes cleanly so the CLI's range-over-map
// renders zero rows instead of crashing on a nil dereference.
func TestResourceListPropertiesEmptyDecodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-empty"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-empty",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-empty/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	for k, v := range got.Props {
		t.Errorf("Props: unexpected entry %q=%q on an empty seed", k, v)
	}
}

// TestAffinityControllerContract pins the `/v1/view/resources` wire
// shape the piraeus `linstor-affinity-controller` polls to keep PV
// `spec.nodeAffinity` in lock-step with where DRBD actually has data.
//
// Scenario 11.W04 (wave2-11-kubernetes.md). Without the affinity
// controller a PV's nodeAffinity is set ONCE at provisioning and
// never updated; if LINSTOR later moves a replica (evacuation /
// auto-rebalance / operator action) the Pod cannot reschedule onto
// the node that now carries the diskful copy. The affinity controller
// closes this gap by polling LINSTOR's resource view and rewriting
// the PV's nodeAffinity to match the current set of nodes that hold
// a usable replica.
//
// blockstor is the LINSTOR-controller drop-in here. The contract the
// affinity controller relies on, narrowed to fields it actually reads:
//
//  1. The view enumerates one entry per replica with `node_name`
//     populated (so the controller can list the nodes that hold the
//     resource at all).
//
//  2. Per-replica `flags` round-trips intact — DISKLESS / TIE_BREAKER
//     must be visible because the affinity controller MUST exclude
//     witness replicas: a pod scheduled onto a tiebreaker-only node
//     has no local data and would attach over the network, defeating
//     the locality the affinity controller exists to enforce.
//
//  3. Per-volume `state.disk_state` is the diskful-readiness gate.
//     Only `UpToDate` (or the sync-progress-annotated `UpToDate(NN%)`
//     variant) means "the replica on this node is safe to mount":
//     Inconsistent / Outdated / Failed replicas exist on disk but are
//     not authoritative, and including them in `nodeAffinity` would
//     let kubelet mount stale data.
//
// Three replica shapes therefore must round-trip distinguishably on
// the wire:
//
//   - diskful-UpToDate on n-good   → eligible target
//   - diskful-Inconsistent on n-bad → NOT eligible (data not authoritative)
//   - DISKLESS+TIE_BREAKER on n-tb → NOT eligible (no local data)
//
// If any of these collapse on the wire — Inconsistent indistinguishable
// from UpToDate, TIE_BREAKER flag lost, node_name blank — the affinity
// controller cannot do its job and Pods get stuck Pending after every
// evacuate / rebalance. This test is the regression guard for the wire
// shape; the actual controller loop lives in piraeus and is covered by
// tests/e2e/affinity-controller.sh against a live stand.
func TestAffinityControllerContract(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Three replicas of the same RD, one per node, each modelling
	// one of the cases the affinity controller must distinguish.
	seed := []apiv1.Resource{
		{
			Name:     "pvc-affinity",
			NodeName: "n-good",
			Flags:    []string{}, // diskful, no DISKLESS
			Volumes: []apiv1.Volume{{
				VolumeNumber: 0,
				State:        apiv1.VolumeState{DiskState: "UpToDate"},
			}},
		},
		{
			Name:     "pvc-affinity",
			NodeName: "n-bad",
			Flags:    []string{}, // diskful but not authoritative
			Volumes: []apiv1.Volume{{
				VolumeNumber: 0,
				State:        apiv1.VolumeState{DiskState: "Inconsistent"},
			}},
		},
		{
			Name:     "pvc-affinity",
			NodeName: "n-tb",
			Flags: []string{
				apiv1.ResourceFlagDiskless,
				apiv1.ResourceFlagTieBreaker,
			},
			// Witness — no local volume backing, hence no disk_state.
			Volumes: []apiv1.Volume{{VolumeNumber: 0}},
		},
	}

	for i := range seed {
		if err := st.Resources().Create(ctx, &seed[i]); err != nil {
			t.Fatalf("seed %s/%s: %v", seed[i].Name, seed[i].NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// The affinity controller filters by RD name in its polling
	// loop — mirror that here so the test stays scoped even if
	// other parallel tests bleed Resources into the store.
	resp := httpGet(t, base+"/v1/view/resources?resources=pvc-affinity")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Contract #1: one entry per replica. Index by node so the
	// assertions key on the wire shape, not on sort order.
	if len(got) != 3 {
		t.Fatalf("len: got %d, want 3 (one entry per replica); entries=%v", len(got), got)
	}

	byNode := map[string]apiv1.ResourceWithVolumes{}

	for i := range got {
		if got[i].NodeName == "" {
			t.Errorf("replica %d: node_name empty — affinity controller cannot map replica to K8s node", i)
		}

		byNode[got[i].NodeName] = got[i]
	}

	good, ok := byNode["n-good"]
	if !ok {
		t.Fatalf("missing pvc-affinity/n-good (UpToDate diskful) in view: %v", got)
	}

	bad, ok := byNode["n-bad"]
	if !ok {
		t.Fatalf("missing pvc-affinity/n-bad (Inconsistent diskful) in view: %v", got)
	}

	tb, ok := byNode["n-tb"]
	if !ok {
		t.Fatalf("missing pvc-affinity/n-tb (tiebreaker witness) in view: %v", got)
	}

	// Contract #2: per-volume disk_state must round-trip. The
	// affinity controller's "is this node eligible?" predicate is
	// `disk_state == "UpToDate"`; collapsing Inconsistent into
	// UpToDate would silently expand nodeAffinity to nodes whose
	// replica is not authoritative.
	if len(good.Volumes) == 0 || good.Volumes[0].State.DiskState != "UpToDate" {
		t.Errorf("n-good: volumes[0].state.disk_state=%q, want %q (eligible-replica gate)",
			volDiskState(good), "UpToDate")
	}

	if len(bad.Volumes) == 0 || bad.Volumes[0].State.DiskState != "Inconsistent" {
		t.Errorf("n-bad: volumes[0].state.disk_state=%q, want %q (must NOT collapse to UpToDate)",
			volDiskState(bad), "Inconsistent")
	}

	// Contract #3: TIE_BREAKER round-trips, so the affinity
	// controller can exclude witnesses by flag membership without
	// having to second-guess from disk_state alone (a witness has
	// no volume backing, so its disk_state is naturally empty — but
	// the same empty disk_state appears transiently on a fresh
	// diskful replica before the satellite observer first reports,
	// so flag-based exclusion is the load-bearing signal).
	if !slices.Contains(tb.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n-tb: DISKLESS missing from flags=%v — witness indistinguishable from diskful", tb.Flags)
	}

	if !slices.Contains(tb.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n-tb: TIE_BREAKER missing from flags=%v — controller cannot exclude autoplaced witness", tb.Flags)
	}

	// And the diskful entries MUST NOT carry DISKLESS — otherwise
	// the controller's "exclude all DISKLESS" filter eats the very
	// nodes it should be selecting.
	if slices.Contains(good.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n-good: DISKLESS leaked onto a diskful replica flags=%v", good.Flags)
	}

	if slices.Contains(bad.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n-bad: DISKLESS leaked onto a diskful replica flags=%v", bad.Flags)
	}

	// Composite contract: the eligible-node set the affinity
	// controller derives from this view is exactly {n-good}. Encode
	// the derivation here so a future regression on ANY of the
	// individual contracts above still trips this last assertion.
	eligible := eligibleAffinityNodes(got)

	if len(eligible) != 1 || eligible[0] != "n-good" {
		t.Errorf("eligible node set: got %v, want [n-good] only "+
			"(UpToDate diskful is the only mountable-with-locality replica)", eligible)
	}
}

// volDiskState is a nil-safe accessor used by the error messages in
// TestAffinityControllerContract — keeps the assertion site readable
// even when the volumes slice is empty (which is itself a failure
// mode the test catches).
func volDiskState(r apiv1.ResourceWithVolumes) string {
	if len(r.Volumes) == 0 {
		return "<no volumes>"
	}

	return r.Volumes[0].State.DiskState
}

// eligibleAffinityNodes models the piraeus linstor-affinity-controller's
// node-selection predicate, in the narrowest form that exercises the
// blockstor REST contract: a node is eligible iff it carries a replica
// that is (a) diskful (no DISKLESS flag — excludes both operator
// diskless and TIE_BREAKER witnesses) and (b) reports UpToDate on
// every volume. The real controller has additional knobs (storage-pool
// labels, allow/deny lists) but those layer on top of this predicate.
func eligibleAffinityNodes(rs []apiv1.ResourceWithVolumes) []string {
	out := []string{}

	for i := range rs {
		if slices.Contains(rs[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		allUpToDate := len(rs[i].Volumes) > 0
		for j := range rs[i].Volumes {
			if !isUpToDateDiskState(rs[i].Volumes[j].State.DiskState) {
				allUpToDate = false

				break
			}
		}

		if allUpToDate {
			out = append(out, rs[i].NodeName)
		}
	}

	slices.Sort(out)

	return out
}

// TestAnnotateSyncProgressStateBug348 pins the upstream-shaped State
// column for the SyncSource / SyncTarget transient window.
//
// Bug 348: blockstor used to render `linstor r l` during a sync
// window as `UpToDate(NN%)` on the source side — visually plausible
// (the data IS uptodate) but losing the operator-facing signal that
// this replica is actively sending data. Upstream LINSTOR renders
// the source side as `SyncSource` and the target as `SyncTarget`,
// sourced directly from drbdsetup events2's replication_state field.
// Bug 331 closed Connecting / NetworkFailure column shapes but
// missed the SyncSource / SyncTarget pair — these cases pin the
// closed shape so a future refactor cannot silently regress.
//
// The fix lives in annotateSyncProgress: when any peer's
// Volume.State.ReplicationStates entry is `SyncSource` or
// `SyncTarget`, the literal token wins over the `(NN%)` annotation.
// When replication settles into `Established` (or the map is empty
// because no peer is mid-sync), the legacy disk_state-with-progress
// path takes over so existing tests stay green.
func TestAnnotateSyncProgressStateBug348(t *testing.T) {
	t.Parallel()

	const sizeKib = int64(1024 * 1024) // 1 GiB

	sizes := map[int32]int64{0: sizeKib}

	type tc struct {
		name string
		in   apiv1.Volume
		want string
	}

	cases := []tc{
		{
			name: "source side mid-sync renders SyncSource literal",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "UpToDate",
					OutOfSyncKib: sizeKib / 2, // 50% out-of-sync
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-b": {ReplicationState: "SyncSource"},
					},
				},
			},
			want: "SyncSource",
		},
		{
			name: "target side mid-sync renders SyncTarget literal",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: sizeKib / 2,
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-a": {ReplicationState: "SyncTarget"},
					},
				},
			},
			want: "SyncTarget",
		},
		{
			name: "both peers Established with UpToDate disk renders UpToDate clean",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "UpToDate",
					OutOfSyncKib: 0,
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-b": {ReplicationState: "Established"},
					},
				},
			},
			want: "UpToDate",
		},
		{
			name: "Inconsistent with no replication-state info stays Inconsistent (no NN%)",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: 0, // no progress signal → no suffix
				},
			},
			want: "Inconsistent",
		},
		{
			name: "Inconsistent with progress but no replication-state still annotates legacy (NN%)",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: sizeKib / 4, // 75% synced
				},
			},
			want: "Inconsistent(75%)",
		},
		{
			name: "PausedSyncS falls through to legacy annotation (Bug 348 scope is SyncSource/SyncTarget only)",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "UpToDate",
					OutOfSyncKib: sizeKib / 2,
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-b": {ReplicationState: "PausedSyncS"},
					},
				},
			},
			want: "UpToDate(50%)",
		},
		{
			name: "3-replica mixed: SyncSource peer wins over Established peer",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "UpToDate",
					OutOfSyncKib: sizeKib / 2,
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-b": {ReplicationState: "Established"},
						"peer-c": {ReplicationState: "SyncSource"},
					},
				},
			},
			want: "SyncSource",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := annotateSyncProgress([]apiv1.Volume{c.in}, sizes)
			if len(got) != 1 {
				t.Fatalf("annotateSyncProgress returned %d volumes, want 1", len(got))
			}

			if got[0].State.DiskState != c.want {
				t.Errorf("State.DiskState = %q, want %q (in=%+v)",
					got[0].State.DiskState, c.want, c.in)
			}
		})
	}
}

// TestActiveSyncReplicationState exercises the small helper in
// isolation: source preference, target fallback, ignore-others, and
// the nil-map fast path. Behaviour is locked here because Bug 348's
// "literal token wins over (NN%)" gate funnels every replica through
// this helper.
func TestActiveSyncReplicationStateBug348(t *testing.T) {
	t.Parallel()

	type tc struct {
		name   string
		states map[string]apiv1.ReplicationState
		want   string
	}

	cases := []tc{
		{name: "nil map", states: nil, want: ""},
		{name: "empty map", states: map[string]apiv1.ReplicationState{}, want: ""},
		{
			name: "single SyncSource peer",
			states: map[string]apiv1.ReplicationState{
				"b": {ReplicationState: "SyncSource"},
			},
			want: "SyncSource",
		},
		{
			name: "single SyncTarget peer",
			states: map[string]apiv1.ReplicationState{
				"a": {ReplicationState: "SyncTarget"},
			},
			want: "SyncTarget",
		},
		{
			name: "Established only — not promoted",
			states: map[string]apiv1.ReplicationState{
				"b": {ReplicationState: "Established"},
			},
			want: "",
		},
		{
			name: "PausedSyncS not promoted — Bug 348 scope is the two literal tokens only",
			states: map[string]apiv1.ReplicationState{
				"b": {ReplicationState: "PausedSyncS"},
			},
			want: "",
		},
		{
			name: "SyncSource wins over SyncTarget in the same map",
			states: map[string]apiv1.ReplicationState{
				"b": {ReplicationState: "SyncTarget"},
				"c": {ReplicationState: "SyncSource"},
			},
			want: "SyncSource",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if got := activeSyncReplicationState(c.states); got != c.want {
				t.Errorf("activeSyncReplicationState(%+v) = %q, want %q",
					c.states, got, c.want)
			}
		})
	}
}
