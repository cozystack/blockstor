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
