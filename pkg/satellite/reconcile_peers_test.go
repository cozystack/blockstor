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
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// fakePeerUIDsStamper is the test-side AppliedPeerUIDsStamper. It
// records every StampAppliedPeerUIDs invocation so adoption-mode
// tests can pin the (resourceName, uids) shape that landed on the
// stamper without wiring a real apiserver. Mirrors the fake stamper
// pattern in reconciler_metadata_created_test.go.
type fakePeerUIDsStamper struct {
	mu    sync.Mutex
	calls []fakePeerUIDsCall
	// failNext, when true, causes the next call to return an error
	// (used by tests that need to assert adoption falls through to
	// the normal diff when the stamper hard-fails).
	failNext bool
}

type fakePeerUIDsCall struct {
	ResourceName string
	UIDs         map[string]string
}

func (f *fakePeerUIDsStamper) StampAppliedPeerUIDs(_ context.Context, resourceName string, uids map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	copied := make(map[string]string, len(uids))
	for k, v := range uids {
		copied[k] = v
	}

	f.calls = append(f.calls, fakePeerUIDsCall{ResourceName: resourceName, UIDs: copied})

	if f.failNext {
		f.failNext = false

		return context.DeadlineExceeded
	}

	return nil
}

func (f *fakePeerUIDsStamper) Calls() []fakePeerUIDsCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]fakePeerUIDsCall, len(f.calls))
	copy(out, f.calls)

	return out
}

// newReconcilerForPeers builds a minimal Reconciler that's wired only
// with the bits reconcilePeers / maybeAdoptPeers touch: Adm (around a
// FakeExec), NodeName, and optionally a stamper. The test never goes
// through Apply, so providers / state dir / cryptsetup can stay nil.
func newReconcilerForPeers(t *testing.T, fx *storage.FakeExec, stamper AppliedPeerUIDsStamper) *Reconciler {
	t.Helper()

	return NewReconciler(ReconcilerConfig{
		Adm:                    drbd.NewAdm(fx),
		NodeName:               "n1",
		AppliedPeerUIDsStamper: stamper,
	})
}

// drFor builds a small DesiredResource with one volume and the given
// peer set. Tests bolt on extra fields (AppliedPeerUIDs, more volumes,
// …) by mutating the returned struct directly.
func drFor(name string, peers []intent.DesiredPeer, vols []int32) *intent.DesiredResource {
	dv := make([]*intent.DesiredVolume, 0, len(vols))
	for _, v := range vols {
		dv = append(dv, &intent.DesiredVolume{VolumeNumber: v})
	}

	return &intent.DesiredResource{
		Name:     name,
		NodeName: "n1",
		Peers:    peers,
		Volumes:  dv,
	}
}

// devicesMap mimics the per-volume backing-device table the dispatcher
// passes into reconcilePeers (volNum → /dev/...). forget-peer fans out
// across these entries.
func devicesMap(volNums []int32) map[int32]string {
	out := make(map[int32]string, len(volNums))
	for _, v := range volNums {
		out[v] = "/dev/dm-test"
	}

	return out
}

// hasCmd is the small contains-helper the recorded-call assertions
// lean on. Used over slices.Contains directly so a missed match prints
// the full command stream — debugging a Pass-3 zombie probe with two
// FakeExec call lists side-by-side is otherwise painful.
func hasCmd(cmds []string, want string) bool {
	return slices.Contains(cmds, want)
}

// -----------------------------------------------------------------------
// Pass 1: kernel-not-in-K8s
// -----------------------------------------------------------------------

// Bug 342: pins Pass-1 contract — kernel has a slot for a peer that
// K8s no longer names (peer Resource was deleted) → satellite must
// del-peer + forget-peer to free the kernel-side connection and the
// on-disk metadata slot.
func TestReconcilePeers_Pass1_KernelOnlyDelPeer(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	// K8s desired has only n2 (n3 was removed); kernel has BOTH n2
	// (steady-state) and a leftover n3 slot.
	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-n2"},
	}
	actual := map[string]drbd.KernelSlot{
		"n2": {Name: "n2", NodeID: 1, ConnectionState: "Connected"},
		"n3": {Name: "n3", NodeID: 2, ConnectionState: "Connected"},
	}

	dr := drFor("pvc-pass1", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-n2"},
	}, []int32{0})

	err := rec.diffKernelNotInK8s(t.Context(), dr, expected, actual, devicesMap([]int32{0}))
	if err != nil {
		t.Fatalf("diffKernelNotInK8s: %v", err)
	}

	cmds := fx.CommandLines()
	if !hasCmd(cmds, "drbdadm del-peer n3:pvc-pass1") {
		t.Errorf("Pass 1: expected del-peer for n3 (kernel-only); got: %v", cmds)
	}

	// forget-peer must be keyed on the kernel-observed node-id (2).
	if !hasCmd(cmds, "drbdmeta --force pvc-pass1/0 v09 /dev/dm-test internal forget-peer 2") {
		t.Errorf("Pass 1: expected forget-peer on node-id 2; got: %v", cmds)
	}

	// n2 is steady-state — must NOT be torn down.
	if hasCmd(cmds, "drbdadm del-peer n2:pvc-pass1") {
		t.Errorf("Pass 1: must NOT del-peer n2 (still in K8s desired); got: %v", cmds)
	}
}

// Bug 342: empty kernel state (drbdsetup show returned nil / RD not
// loaded yet) must produce zero teardown commands — Pass 1 is a no-op
// in the pre-up steady state.
func TestReconcilePeers_Pass1_NoKernelSlots_Noop(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-n2"},
	}

	dr := drFor("pvc-empty", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-n2"},
	}, []int32{0})

	err := rec.diffKernelNotInK8s(t.Context(), dr, expected, nil, devicesMap([]int32{0}))
	if err != nil {
		t.Fatalf("diffKernelNotInK8s: %v", err)
	}

	for _, c := range fx.CommandLines() {
		if strings.HasPrefix(c, "drbdadm del-peer") || strings.HasPrefix(c, "drbdmeta") {
			t.Errorf("Pass 1: empty kernel must not fire any teardown; saw %q", c)
		}
	}
}

// -----------------------------------------------------------------------
// Pass 2: UID mismatch (Bug 342 core)
// -----------------------------------------------------------------------

// Bug 342: same peer name in K8s, but ResourceUID changed → peer was
// deleted + re-created sub-second; kernel still has the OLD identity's
// slot. Satellite must force del-peer + forget-peer keyed on the
// KERNEL-observed node-id (because the allocator may have reissued the
// new UID a fresh node-id, Bug 87).
func TestReconcilePeers_Pass2_UIDChangedForceTeardown(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-NEW"},
	}
	applied := map[string]string{"n2": "uid-OLD"}
	actual := map[string]drbd.KernelSlot{
		"n2": {Name: "n2", NodeID: 7, ConnectionState: "Connecting"},
	}

	dr := drFor("pvc-pass2", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-NEW"},
	}, []int32{0})

	err := rec.diffUIDMismatch(t.Context(), dr, expected, applied, actual, devicesMap([]int32{0}))
	if err != nil {
		t.Fatalf("diffUIDMismatch: %v", err)
	}

	cmds := fx.CommandLines()
	if !hasCmd(cmds, "drbdadm del-peer n2:pvc-pass2") {
		t.Errorf("Pass 2: UID mismatch must force del-peer; got: %v", cmds)
	}

	// forget-peer keyed on the kernel-observed node-id (7), NOT the
	// K8s-desired node-id (1) — Bug 87 follow-up.
	if !hasCmd(cmds, "drbdmeta --force pvc-pass2/0 v09 /dev/dm-test internal forget-peer 7") {
		t.Errorf("Pass 2: forget-peer must use kernel node-id 7; got: %v", cmds)
	}
}

// Bug 342: empty AppliedPeerUIDs entry (rollout / fresh pod / pre-
// adoption-mode-stamp window) must NOT trigger Pass-2 teardown — we
// have no known identity, so we can't claim a mismatch. Pass 3
// debounce + zombie probe is the safety net for this branch.
func TestReconcilePeers_Pass2_AppliedEmptyForUid_Skip(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-current"},
	}
	applied := map[string]string{"n2": ""} // intentionally empty
	actual := map[string]drbd.KernelSlot{
		"n2": {Name: "n2", NodeID: 1, ConnectionState: "Connected"},
	}

	dr := drFor("pvc-pass2-empty", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-current"},
	}, []int32{0})

	err := rec.diffUIDMismatch(t.Context(), dr, expected, applied, actual, devicesMap([]int32{0}))
	if err != nil {
		t.Fatalf("diffUIDMismatch: %v", err)
	}

	for _, c := range fx.CommandLines() {
		if strings.HasPrefix(c, "drbdadm del-peer") || strings.HasPrefix(c, "drbdmeta") {
			t.Errorf("Pass 2: empty applied entry must skip teardown; saw %q", c)
		}
	}
}

// Bug 342: UID matches the last-applied stamp → steady state, no
// teardown. This is the hot path on every reconcile after the first.
func TestReconcilePeers_Pass2_UIDMatch_Noop(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-current"},
	}
	applied := map[string]string{"n2": "uid-current"}
	actual := map[string]drbd.KernelSlot{
		"n2": {Name: "n2", NodeID: 1, ConnectionState: "Connected"},
	}

	dr := drFor("pvc-pass2-match", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-current"},
	}, []int32{0})

	err := rec.diffUIDMismatch(t.Context(), dr, expected, applied, actual, devicesMap([]int32{0}))
	if err != nil {
		t.Fatalf("diffUIDMismatch: %v", err)
	}

	for _, c := range fx.CommandLines() {
		if strings.HasPrefix(c, "drbdadm del-peer") || strings.HasPrefix(c, "drbdmeta") {
			t.Errorf("Pass 2: UID match must be a no-op; saw %q", c)
		}
	}
}

// Bug 342 + Bug 87: even when K8s desired carries a NEW node-id for
// the same peer name (allocator re-issued an id between delete and
// re-create), forget-peer MUST target the kernel-observed node-id —
// that's where the zombie slot lives in the on-disk metadata. If the
// satellite forgot the K8s-desired id, the OLD slot would leak
// forever and exhaust the MaxPeers-1 budget.
func TestReconcilePeers_Pass2_ForgetPeerOnKernelNodeID(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	// K8s-desired node-id = 3 (newly allocated for the new UID).
	// Kernel-observed node-id = 42 (the OLD slot's id).
	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 3, ResourceUID: "uid-NEW"},
	}
	applied := map[string]string{"n2": "uid-OLD"}
	actual := map[string]drbd.KernelSlot{
		"n2": {Name: "n2", NodeID: 42, ConnectionState: "Connecting"},
	}

	dr := drFor("pvc-pass2-id", []intent.DesiredPeer{
		{Name: "n2", NodeID: 3, ResourceUID: "uid-NEW"},
	}, []int32{0})

	err := rec.diffUIDMismatch(t.Context(), dr, expected, applied, actual, devicesMap([]int32{0}))
	if err != nil {
		t.Fatalf("diffUIDMismatch: %v", err)
	}

	cmds := fx.CommandLines()
	if !hasCmd(cmds, "drbdmeta --force pvc-pass2-id/0 v09 /dev/dm-test internal forget-peer 42") {
		t.Errorf("Pass 2: forget-peer must key on kernel node-id 42 (not K8s-desired 3); got: %v", cmds)
	}

	if hasCmd(cmds, "drbdmeta --force pvc-pass2-id/0 v09 /dev/dm-test internal forget-peer 3") {
		t.Errorf("Pass 2: must NOT use K8s-desired node-id 3 for forget-peer; got: %v", cmds)
	}
}

// -----------------------------------------------------------------------
// Pass 3: zombie probe
// -----------------------------------------------------------------------

// Bug 342: kernel slot is Connecting, no peer-device for any volume,
// state-change age > grace → tear down. This is the "applied empty
// but kernel wedged" branch that pass 2 can't cover.
func TestReconcilePeers_Pass3_StaleConnecting_Teardown(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	// State changed 5 minutes ago — far past zombieGraceDefault (30s).
	stale := time.Now().Add(-5 * time.Minute)
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name:                "n2",
			NodeID:              1,
			ConnectionState:     "Connecting",
			LastStateChangeTime: stale,
			// PeerDevicesByVolNum intentionally empty → HasAny=false
			// across any volume → zombie shape.
		},
	}

	dr := drFor("pvc-pass3-stale", nil, []int32{0})

	err := rec.diffZombieSlots(t.Context(), dr, actual, devicesMap([]int32{0}), []int32{0})
	if err != nil {
		t.Fatalf("diffZombieSlots: %v", err)
	}

	cmds := fx.CommandLines()
	if !hasCmd(cmds, "drbdadm del-peer n2:pvc-pass3-stale") {
		t.Errorf("Pass 3: stale Connecting must trigger del-peer; got: %v", cmds)
	}

	if !hasCmd(cmds, "drbdmeta --force pvc-pass3-stale/0 v09 /dev/dm-test internal forget-peer 1") {
		t.Errorf("Pass 3: stale Connecting must trigger forget-peer; got: %v", cmds)
	}
}

// Bug 342: kernel slot is Connecting BUT state-change is fresh
// (within debounce window) → don't tear down; let the handshake
// retry path finish.
func TestReconcilePeers_Pass3_FreshConnecting_Skip(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	// State changed 1 second ago — well inside zombieGraceDefault.
	fresh := time.Now().Add(-1 * time.Second)
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name:                "n2",
			NodeID:              1,
			ConnectionState:     "Connecting",
			LastStateChangeTime: fresh,
		},
	}

	dr := drFor("pvc-pass3-fresh", nil, []int32{0})

	err := rec.diffZombieSlots(t.Context(), dr, actual, devicesMap([]int32{0}), []int32{0})
	if err != nil {
		t.Fatalf("diffZombieSlots: %v", err)
	}

	for _, c := range fx.CommandLines() {
		if strings.HasPrefix(c, "drbdadm del-peer") || strings.HasPrefix(c, "drbdmeta") {
			t.Errorf("Pass 3: fresh Connecting must be debounced; saw %q", c)
		}
	}
}

// Bug 342: kernel slot is Connecting, but HAS a peer-device for at
// least one volume → partial handshake mid-flight, NOT zombie. Must
// skip even after the grace window (DRBD is still progressing).
func TestReconcilePeers_Pass3_PartialPeerDevicePresence_Skip(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	// Stale enough by time, but vol 0 has a registered peer-device →
	// not zombie shape.
	stale := time.Now().Add(-5 * time.Minute)
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name:                "n2",
			NodeID:              1,
			ConnectionState:     "Connecting",
			LastStateChangeTime: stale,
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, DiskState: "Outdated", Configured: true},
			},
		},
	}

	dr := drFor("pvc-pass3-partial", nil, []int32{0, 1})

	err := rec.diffZombieSlots(t.Context(), dr, actual, devicesMap([]int32{0, 1}), []int32{0, 1})
	if err != nil {
		t.Fatalf("diffZombieSlots: %v", err)
	}

	for _, c := range fx.CommandLines() {
		if strings.HasPrefix(c, "drbdadm del-peer") || strings.HasPrefix(c, "drbdmeta") {
			t.Errorf("Pass 3: partial peer-device must skip teardown; saw %q", c)
		}
	}
}

// Bug 342: StandAlone variant of the zombie shape — same conditions
// as stale Connecting, just a different state token. Must also tear
// down (drbd's StandAlone is the terminal "won't retry" state).
func TestReconcilePeers_Pass3_StandAlone_Teardown(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	stale := time.Now().Add(-5 * time.Minute)
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name:                "n2",
			NodeID:              4,
			ConnectionState:     "StandAlone",
			LastStateChangeTime: stale,
		},
	}

	dr := drFor("pvc-pass3-sa", nil, []int32{0})

	err := rec.diffZombieSlots(t.Context(), dr, actual, devicesMap([]int32{0}), []int32{0})
	if err != nil {
		t.Fatalf("diffZombieSlots: %v", err)
	}

	cmds := fx.CommandLines()
	if !hasCmd(cmds, "drbdadm del-peer n2:pvc-pass3-sa") {
		t.Errorf("Pass 3: StandAlone zombie must trigger del-peer; got: %v", cmds)
	}

	if !hasCmd(cmds, "drbdmeta --force pvc-pass3-sa/0 v09 /dev/dm-test internal forget-peer 4") {
		t.Errorf("Pass 3: StandAlone zombie must trigger forget-peer; got: %v", cmds)
	}
}

// Bug 342: BSTOR_ZOMBIE_GRACE_S env var must shrink the debounce
// window. Setting grace=5s + age=10s → teardown fires (where it
// would have skipped at default 30s).
func TestReconcilePeers_Pass3_GraceEnvOverride(t *testing.T) {
	// Cannot t.Parallel — env var is process-global and a parallel
	// test could observe it leak across runs. t.Setenv handles
	// cleanup but not visibility ordering.
	t.Setenv("BSTOR_ZOMBIE_GRACE_S", "5")

	if got := zombieGrace(); got != 5*time.Second {
		t.Fatalf("zombieGrace=%s, want 5s after env override", got)
	}

	fx := storage.NewFakeExec()
	rec := newReconcilerForPeers(t, fx, nil)

	// Age = 10s: would skip under default 30s grace, must teardown
	// under the 5s override.
	stale := time.Now().Add(-10 * time.Second)
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name:                "n2",
			NodeID:              1,
			ConnectionState:     "Connecting",
			LastStateChangeTime: stale,
		},
	}

	dr := drFor("pvc-pass3-env", nil, []int32{0})

	err := rec.diffZombieSlots(t.Context(), dr, actual, devicesMap([]int32{0}), []int32{0})
	if err != nil {
		t.Fatalf("diffZombieSlots: %v", err)
	}

	cmds := fx.CommandLines()
	if !hasCmd(cmds, "drbdadm del-peer n2:pvc-pass3-env") {
		t.Errorf("Pass 3: grace=5s, age=10s must trigger del-peer; got: %v", cmds)
	}
}

// -----------------------------------------------------------------------
// Adoption-mode gate
// -----------------------------------------------------------------------

// Bug 342: applied empty + kernel empty → nothing to adopt; fall
// through to normal diff (which will be a complete no-op).
func TestAdoption_NoAppliedNotConfigured_NoAdopt(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	stamper := &fakePeerUIDsStamper{}
	rec := newReconcilerForPeers(t, fx, stamper)

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-A"},
	}

	dr := drFor("pvc-adopt-empty", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-A"},
	}, []int32{0})

	got := rec.maybeAdoptPeers(t.Context(), dr, expected, nil, nil)
	if got {
		t.Fatalf("maybeAdoptPeers: empty kernel must not adopt (returned true)")
	}

	if calls := stamper.Calls(); len(calls) != 0 {
		t.Errorf("stamper must not be invoked when there's nothing to adopt; got %v", calls)
	}
}

// Bug 342: applied empty + kernel has same peer set / node-ids / per-
// vol peer-devices as K8s desired → adopt: stamp current UIDs, do
// not touch kernel connections.
func TestAdoption_NoAppliedConfiguredAgree_Adopt(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	stamper := &fakePeerUIDsStamper{}
	rec := newReconcilerForPeers(t, fx, stamper)

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-A"},
	}
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name:            "n2",
			NodeID:          1,
			ConnectionState: "Connected",
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, DiskState: "UpToDate", Configured: true},
			},
		},
	}

	dr := drFor("pvc-adopt-agree", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-A"},
	}, []int32{0})

	got := rec.maybeAdoptPeers(t.Context(), dr, expected, nil, actual)
	if !got {
		t.Fatalf("maybeAdoptPeers: agreement must adopt (returned false)")
	}

	calls := stamper.Calls()
	if len(calls) != 1 {
		t.Fatalf("stamper: want 1 call, got %d (%v)", len(calls), calls)
	}

	// Bug 344: stamper key is "<rd>.<node>", not the RD-only name.
	if calls[0].ResourceName != "pvc-adopt-agree.n1" {
		t.Errorf("stamper resourceName = %q, want %q", calls[0].ResourceName, "pvc-adopt-agree.n1")
	}

	if got, want := calls[0].UIDs["n2"], "uid-A"; got != want {
		t.Errorf("stamped UIDs[n2] = %q, want %q", got, want)
	}

	// Adoption must NOT have touched connections.
	for _, c := range fx.CommandLines() {
		if strings.HasPrefix(c, "drbdadm del-peer") || strings.HasPrefix(c, "drbdmeta") {
			t.Errorf("Adoption: must not mutate kernel state; saw %q", c)
		}
	}
}

// Bug 342: peer-set disagrees (kernel has an extra peer K8s doesn't
// name) → decline adoption; return false so caller falls through to
// the normal three-pass diff.
func TestAdoption_Disagree_Decline(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	stamper := &fakePeerUIDsStamper{}
	rec := newReconcilerForPeers(t, fx, stamper)

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1, ResourceUID: "uid-A"},
	}
	// Kernel has n2 AND n4 — disagrees with K8s desired (n2 only).
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name: "n2", NodeID: 1, ConnectionState: "Connected",
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, Configured: true},
			},
		},
		"n4": {
			Name: "n4", NodeID: 5, ConnectionState: "Connected",
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, Configured: true},
			},
		},
	}

	dr := drFor("pvc-adopt-disagree", []intent.DesiredPeer{
		{Name: "n2", NodeID: 1, ResourceUID: "uid-A"},
	}, []int32{0})

	got := rec.maybeAdoptPeers(t.Context(), dr, expected, nil, actual)
	if got {
		t.Fatalf("maybeAdoptPeers: disagreement must decline (returned true)")
	}

	if calls := stamper.Calls(); len(calls) != 0 {
		t.Errorf("stamper must not be invoked on adoption decline; got %v", calls)
	}
}

// Bug 342: jitter is process-global and best-tested via the
// firstReconcileAfterBoot flip. We pin two invariants:
//
//   - first call for (rd, node) returns true (jitter fires)
//   - subsequent calls return false (skip jitter)
//
// The actual sleep duration is not asserted — there's no clock
// abstraction in the production code path and adding one purely for
// the test would inflate scope. The deterministic flag behaviour is
// the load-bearing half; the random sleep is bounded by
// zombieGraceDefault and exercised in integration.
func TestAdoption_Jitter_OnFirstReconcileAfterBoot(t *testing.T) {
	t.Parallel()

	rec := newReconcilerForPeers(t, storage.NewFakeExec(), nil)

	first := rec.firstReconcileAfterBoot("pvc-jitter", "n1")
	if !first {
		t.Fatalf("firstReconcileAfterBoot: initial call must be true")
	}

	second := rec.firstReconcileAfterBoot("pvc-jitter", "n1")
	if second {
		t.Errorf("firstReconcileAfterBoot: subsequent call must be false (jitter skipped)")
	}

	// A different (rd, node) key still counts as first.
	other := rec.firstReconcileAfterBoot("pvc-jitter-other", "n1")
	if !other {
		t.Errorf("firstReconcileAfterBoot: per-(rd,node) key — different RD must return true")
	}

	// Jitter bound: zombieGraceDefault sets the upper limit (rand
	// Intn(zombieGraceDefault) ∈ [0, 30s)). Pin the constant so a
	// regression to e.g. 5min sleep gets caught at test time.
	if zombieGraceDefault > 60*time.Second {
		t.Errorf("zombieGraceDefault = %s exceeds 60s — adoption jitter would block reconciles", zombieGraceDefault)
	}
}

// -----------------------------------------------------------------------
// peerSetsAgree
// -----------------------------------------------------------------------

// Bug 342: name-set mismatch (kernel has n4, K8s has n3) must reject
// adoption. n2's peer-device is populated so the iteration falls
// through to the n3-vs-n4 mismatch (peerSetsAgree iterates `expected`
// in map-random order; populating n2's peer-device prevents the
// loop from short-circuiting on peer_device_absent before it reaches
// the genuine name-set check).
func TestPeerSetsAgree_NameSetMismatch_False(t *testing.T) {
	t.Parallel()

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1},
		"n3": {Name: "n3", NodeID: 2},
	}
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name: "n2", NodeID: 1,
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, Configured: true},
			},
		},
		"n4": {
			Name: "n4", NodeID: 5,
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, Configured: true},
			},
		},
	}

	ok, reason := peerSetsAgree(expected, actual, []int32{0})
	if ok {
		t.Fatalf("peerSetsAgree: name-set mismatch must return false")
	}

	if !strings.Contains(reason, "n3") && !strings.Contains(reason, "name_set") {
		t.Errorf("reason %q must mention the offending peer n3 or name_set mismatch", reason)
	}
}

// Bug 342: same peer names on both sides, but the kernel-observed
// node-id disagrees with K8s desired → adoption can't bridge that;
// must reject.
func TestPeerSetsAgree_NodeIDMismatch_False(t *testing.T) {
	t.Parallel()

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1},
	}
	// Same name, different node-id (allocator reissued).
	actual := map[string]drbd.KernelSlot{
		"n2": {Name: "n2", NodeID: 99},
	}

	ok, reason := peerSetsAgree(expected, actual, []int32{0})
	if ok {
		t.Fatalf("peerSetsAgree: node-id mismatch must return false")
	}

	if !strings.Contains(reason, "node_id") {
		t.Errorf("reason %q must mention node_id", reason)
	}
}

// Bug 342: name + node-id agree, but the kernel has no peer-device
// registered for any of the resource's volumes → zombie shape; must
// reject adoption (the Pass-3 probe will tear it down in the normal
// diff path).
func TestPeerSetsAgree_PeerDeviceMissing_False(t *testing.T) {
	t.Parallel()

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1},
	}
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name: "n2", NodeID: 1, ConnectionState: "Connecting",
			// PeerDevicesByVolNum intentionally empty.
		},
	}

	ok, reason := peerSetsAgree(expected, actual, []int32{0})
	if ok {
		t.Fatalf("peerSetsAgree: missing peer-device must return false")
	}

	if !strings.Contains(reason, "peer_device") {
		t.Errorf("reason %q must mention peer_device", reason)
	}
}

// Bug 342: name set, node-ids and per-vol peer-device presence all
// match → adoption can stamp UIDs without disrupting connections.
func TestPeerSetsAgree_AllAgree_True(t *testing.T) {
	t.Parallel()

	expected := map[string]intent.DesiredPeer{
		"n2": {Name: "n2", NodeID: 1},
		"n3": {Name: "n3", NodeID: 2},
	}
	actual := map[string]drbd.KernelSlot{
		"n2": {
			Name: "n2", NodeID: 1, ConnectionState: "Connected",
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, Configured: true},
			},
		},
		"n3": {
			Name: "n3", NodeID: 2, ConnectionState: "Connected",
			PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
				0: {VolumeNumber: 0, Configured: true},
			},
		},
	}

	ok, reason := peerSetsAgree(expected, actual, []int32{0})
	if !ok {
		t.Fatalf("peerSetsAgree: full agreement must return true; reason=%q", reason)
	}

	if reason != "" {
		t.Errorf("peerSetsAgree: agreement reason must be empty; got %q", reason)
	}
}
