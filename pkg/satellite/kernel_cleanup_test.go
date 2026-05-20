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
	"slices"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// kernelCleanupFixture is the standard `drbdsetup show -j pvc-1`
// fixture: worker-1 healthy (Connected, one peer-device), worker-2
// stuck (Connecting, no peer-device — the Bug 342 signature). Pass-1
// tests strip one of these from `expectedPeerNames`; Pass-3 tests
// keep both expected and drive the debounce timer.
const kernelCleanupFixture = `[
  {
    "_name": "pvc-1",
    "connections": [
      {
        "peer_node_id": 1,
        "_peer_node_name": "worker-1",
        "connection": "Connected",
        "peer_devices": [ { "volume_nr": 0 } ]
      },
      {
        "peer_node_id": 2,
        "_peer_node_name": "worker-2",
        "connection": "Connecting",
        "peer_devices": []
      }
    ]
  }
]`

// newKernelCleanupReconciler builds a Reconciler wired to a FakeExec
// pre-loaded with the show fixture. Each test gets its own FakeExec
// so command-line assertions don't bleed across cases.
func newKernelCleanupReconciler(t *testing.T) (*Reconciler, *storage.FakeExec) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup show -j pvc-1", storage.FakeResponse{
		Stdout: []byte(kernelCleanupFixture),
	})

	r := NewReconciler(ReconcilerConfig{
		NodeName: "worker-0",
		Adm:      drbd.NewAdm(fx),
	})

	return r, fx
}

// TestPruneStaleKernelSlots_Pass1RemovesUnexpectedPeer: when a kernel
// slot exists for a peer name K8s no longer expects, Pass 1 must
// issue `drbdadm del-peer` (correctness — live connection) plus
// `drbdmeta forget-peer` per volume (slow-leak — metadata slot).
// The healthy expected peer must NOT be touched.
func TestPruneStaleKernelSlots_Pass1RemovesUnexpectedPeer(t *testing.T) {
	r, fx := newKernelCleanupReconciler(t)

	// Expect ONLY worker-1; worker-2 should be torn down.
	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1"},
		[]int32{0},
		map[int32]string{0: "/dev/zvol/pool/pvc-1"})
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}

	cmds := fx.CommandLines()

	// del-peer + forget-peer for worker-2, nothing for worker-1.
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")
	mustContain(t, cmds, "drbdmeta --force pvc-1/0 v09 /dev/zvol/pool/pvc-1 internal forget-peer 2")
	mustNotContain(t, cmds, "drbdadm del-peer worker-1:pvc-1")
}

// TestPruneStaleKernelSlots_Pass3DebouncesStuckSlot: a slot that
// matches the zombie signature must NOT be torn down on the first
// observation — the in-memory debounce timer hasn't elapsed yet. A
// second invocation after the grace window must tear it down.
func TestPruneStaleKernelSlots_Pass3DebouncesStuckSlot(t *testing.T) {
	r, fx := newKernelCleanupReconciler(t)

	// Both peers expected — Pass 1 won't fire. Pass 3 sees worker-2
	// (Connecting, no peer-device) match the zombie signature.
	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1", "worker-2"},
		[]int32{0},
		map[int32]string{0: "/dev/zvol/pool/pvc-1"})
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots (first call): %v", err)
	}

	cmds := fx.CommandLines()
	mustNotContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")

	// First sighting recorded the timestamp — back-date it past
	// the grace window directly. Driving real wall-clock would
	// make the test flaky on slow CI.
	r.mu.Lock()
	key := "pvc-1/worker-2"

	if _, ok := r.seenStuckAt[key]; !ok {
		r.mu.Unlock()
		t.Fatalf("expected debounce entry %s after first probe; got %+v", key, r.seenStuckAt)
	}

	r.seenStuckAt[key] = time.Now().Add(-2 * defaultStuckSlotGrace)
	r.mu.Unlock()

	// Re-register the show fixture — FakeExec consumed the canned
	// response on the first call.
	fx.Reset()
	fx.Expect("drbdsetup show -j pvc-1", storage.FakeResponse{
		Stdout: []byte(kernelCleanupFixture),
	})

	err = r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1", "worker-2"},
		[]int32{0},
		map[int32]string{0: "/dev/zvol/pool/pvc-1"})
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots (second call): %v", err)
	}

	cmds = fx.CommandLines()
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")
	mustContain(t, cmds, "drbdmeta --force pvc-1/0 v09 /dev/zvol/pool/pvc-1 internal forget-peer 2")

	// After successful teardown, the debounce entry must be cleared
	// so a re-incarnated peer of the same name starts a fresh timer.
	r.mu.Lock()
	_, stillThere := r.seenStuckAt[key]
	r.mu.Unlock()

	if stillThere {
		t.Errorf("debounce entry should be cleared after teardown; still present")
	}
}

// TestPruneStaleKernelSlots_Pass3HealthyClearsDebounce: a slot
// previously observed as stuck that becomes healthy on the next probe
// must clear the debounce entry. Otherwise a flapping connection
// accumulates false grace credit and eventually false-trips Pass 3
// on an actually-healthy peer.
func TestPruneStaleKernelSlots_Pass3HealthyClearsDebounce(t *testing.T) {
	r, fx := newKernelCleanupReconciler(t)

	// First call: worker-2 is stuck, debounce entry created.
	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1", "worker-2"},
		[]int32{0},
		map[int32]string{0: "/dev/zvol/pool/pvc-1"})
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots (first call): %v", err)
	}

	r.mu.Lock()
	_, present := r.seenStuckAt["pvc-1/worker-2"]
	r.mu.Unlock()

	if !present {
		t.Fatalf("expected debounce entry after first probe")
	}

	// Second call with a fixture where worker-2 is now Connected
	// with a peer-device — healthy. The debounce entry must clear.
	const healthyFixture = `[
  {
    "_name": "pvc-1",
    "connections": [
      {
        "peer_node_id": 1,
        "_peer_node_name": "worker-1",
        "connection": "Connected",
        "peer_devices": [ { "volume_nr": 0 } ]
      },
      {
        "peer_node_id": 2,
        "_peer_node_name": "worker-2",
        "connection": "Connected",
        "peer_devices": [ { "volume_nr": 0 } ]
      }
    ]
  }
]`

	fx.Reset()
	fx.Expect("drbdsetup show -j pvc-1", storage.FakeResponse{
		Stdout: []byte(healthyFixture),
	})

	err = r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1", "worker-2"},
		[]int32{0},
		map[int32]string{0: "/dev/zvol/pool/pvc-1"})
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots (second call): %v", err)
	}

	r.mu.Lock()
	_, stillThere := r.seenStuckAt["pvc-1/worker-2"]
	r.mu.Unlock()

	if stillThere {
		t.Errorf("healthy observation MUST clear the debounce entry; still present")
	}

	// Healthy peer must NOT be torn down.
	cmds := fx.CommandLines()
	mustNotContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")
}

// TestPruneStaleKernelSlots_NoKernelSlotsIsNoop: when drbdsetup
// reports no resource (kernel module loaded but slot absent — the
// post-down / pre-up steady state), the prune must succeed with no
// shell-outs beyond the show probe. Also clears any stale debounce
// state so a future re-up starts from a clean timer.
func TestPruneStaleKernelSlots_NoKernelSlotsIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup show -j pvc-1", storage.FakeResponse{
		Stdout: []byte("[]"),
	})

	r := NewReconciler(ReconcilerConfig{
		NodeName: "worker-0",
		Adm:      drbd.NewAdm(fx),
	})

	// Pre-load a stale debounce entry; it must clear.
	r.seenStuckAt["pvc-1/worker-2"] = time.Now()

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1"}, []int32{0}, nil)
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}

	// Only the show probe, nothing else.
	cmds := fx.CommandLines()
	if len(cmds) != 1 || cmds[0] != "drbdsetup show -j pvc-1" {
		t.Errorf("expected only the show probe, got: %v", cmds)
	}

	if _, ok := r.seenStuckAt["pvc-1/worker-2"]; ok {
		t.Errorf("stale debounce entry must clear when kernel reports no slots")
	}
}

// TestPruneStaleKernelSlots_NilAdmIsNoop: a Reconciler with no Adm
// (storage-only RD / unit test wiring) must short-circuit before
// touching the kernel.
func TestPruneStaleKernelSlots_NilAdmIsNoop(t *testing.T) {
	r := NewReconciler(ReconcilerConfig{NodeName: "worker-0"})

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1"}, []int32{0}, nil)
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots with nil Adm: %v", err)
	}
}

// TestPruneStaleKernelSlots_Pass1ForgetPeerSkippedWithoutDevice:
// Pass-1 del-peer must still fire for an unexpected peer when no
// device path is known (DISKLESS local case), but forget-peer is
// per-volume and must be skipped — there's no metadata block to
// clean. The kernel-side connection leak is the loud half; metadata
// slot leak is the slow half and per-RD recoverable.
func TestPruneStaleKernelSlots_Pass1ForgetPeerSkippedWithoutDevice(t *testing.T) {
	r, fx := newKernelCleanupReconciler(t)

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1"},
		[]int32{0},
		nil) // no devices map
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}

	cmds := fx.CommandLines()
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")

	// No forget-peer because devices map is nil.
	for _, c := range cmds {
		if len(c) >= len("drbdmeta") && c[:len("drbdmeta")] == "drbdmeta" {
			t.Errorf("forget-peer must be skipped without device path; got: %s", c)
		}
	}
}

// TestStuckSlotGrace_EnvOverride: BSTOR_ZOMBIE_GRACE_S=N flips the
// debounce window to N seconds; invalid / non-positive values fall
// back to the default. Pins the stand-side iter contract — the e2e
// validation cranks the window down to a couple seconds to fit
// inside the catcher's 240s timeout.
func TestStuckSlotGrace_EnvOverride(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset → default", "", defaultStuckSlotGrace},
		{"valid override", "5", 5 * time.Second},
		{"zero rejected", "0", defaultStuckSlotGrace},
		{"negative rejected", "-3", defaultStuckSlotGrace},
		{"non-numeric rejected", "abc", defaultStuckSlotGrace},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(stuckSlotGraceEnv, tc.env)

			got := stuckSlotGrace()
			if got != tc.want {
				t.Errorf("stuckSlotGrace = %v, want %v", got, tc.want)
			}
		})
	}
}

func mustContain(t *testing.T, cmds []string, want string) {
	t.Helper()

	if !slices.Contains(cmds, want) {
		t.Errorf("missing %q in calls: %v", want, cmds)
	}
}

func mustNotContain(t *testing.T, cmds []string, banned string) {
	t.Helper()

	if slices.Contains(cmds, banned) {
		t.Errorf("forbidden %q present in calls: %v", banned, cmds)
	}
}
