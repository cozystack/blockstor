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
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestAdmDisconnectThenConnectOrdering pins the observer's recovery
// cycle shape: when both Disconnect and Connect are issued back-to-back
// on the same target, the disconnect MUST land BEFORE the connect.
// Reversing the order leaves the wedged StandAlone state intact and
// the follow-up connect is a no-op against the existing slot — the
// whole recovery cycle silently regresses to "do nothing".
//
// The observer's attemptReconnect always runs Disconnect first
// (best-effort) then Connect; this regression guard pins that order
// at the drbdadm wrapper boundary so a future refactor that swaps the
// two doesn't silently break recovery.
func TestAdmDisconnectThenConnectOrdering(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	target := "pvc-1:n2"

	if err := adm.Disconnect(t.Context(), target); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	if err := adm.Connect(t.Context(), target); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	cmds := fx.CommandLines()

	wantDisconnect := "drbdadm disconnect pvc-1:n2"
	wantConnect := "drbdadm connect pvc-1:n2"

	idxDisc := slices.Index(cmds, wantDisconnect)
	idxConn := slices.Index(cmds, wantConnect)

	if idxDisc < 0 {
		t.Fatalf("disconnect not seen: %v", cmds)
	}

	if idxConn < 0 {
		t.Fatalf("connect not seen: %v", cmds)
	}

	if idxDisc >= idxConn {
		t.Errorf("disconnect must precede connect; disc=%d conn=%d (cmds=%v)",
			idxDisc, idxConn, cmds)
	}
}

// TestAdmConnectPropagatesError pins the recovery-path failure
// surface: when `drbdadm connect` exits non-zero (kernel refused the
// handshake, target malformed, ...) the wrapper MUST surface the error
// wrapped with the subcommand context. The observer's attemptReconnect
// logs the wrapped error at Error level so triage can distinguish a
// failed reconnect from a successful one.
func TestAdmConnectPropagatesError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm connect pvc-1:n2",
		storage.FakeResponse{Err: errFakeFailure})

	adm := drbd.NewAdm(fx)

	err := adm.Connect(t.Context(), "pvc-1:n2")
	if err == nil {
		t.Fatalf("Connect: expected error, got nil")
	}
}

// TestAdmDisconnectPropagatesError pins the same surface for
// disconnect. The observer's attemptReconnect treats Disconnect errors
// as non-fatal (logs at V(1) and proceeds to Connect anyway), but the
// wrapper itself MUST still surface the error so callers that DO care
// (the agent-side reconciler's tear-down path) can distinguish "kernel
// refused" from "ran fine".
func TestAdmDisconnectPropagatesError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm disconnect pvc-1:n2",
		storage.FakeResponse{Err: errFakeFailure})

	adm := drbd.NewAdm(fx)

	err := adm.Disconnect(t.Context(), "pvc-1:n2")
	if err == nil {
		t.Fatalf("Disconnect: expected error, got nil")
	}
}

// TestAdmConnectAcceptsBareResourceTarget pins the bare-resource form
// `drbdadm connect <res>` (no `:<peer>` qualifier) which targets every
// peer on a resource. The observer always uses the per-peer form, but
// the wrapper must accept either shape — drbdadm itself differentiates
// based on the `:` separator, not a flag, so the wrapper is unchanged
// and we pin that bare-target invocations still serialise correctly.
func TestAdmConnectAcceptsBareResourceTarget(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Connect(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	want := "drbdadm connect pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmDisconnectAcceptsBareResourceTarget mirrors the Connect
// variant: bare-resource disconnect quiesces every peer on a resource
// at once. The wrapper must serialise the bare form unchanged so a
// caller that wants the all-peers shape doesn't get the per-peer
// behaviour by accident.
func TestAdmDisconnectAcceptsBareResourceTarget(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Disconnect(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	want := "drbdadm disconnect pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmConnectIsNotForce pins the absence of `--force` on the
// reconnect verb. A regression that adopted `--force` would silently
// override split-brain auto-recovery refusals — the kernel uses the
// non-force connect to reject mismatched data and force adoption
// would clobber the operator's discard-my-data decision. The
// observer's auto-recovery MUST stay polite at the wrapper layer; only
// the operator's manual runbook flips the override flag.
func TestAdmConnectIsNotForce(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Connect(t.Context(), "pvc-1:n2"); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if line == "drbdadm connect --force pvc-1:n2" ||
			line == "drbdadm connect --discard-my-data pvc-1:n2" {
			t.Errorf("Connect added override flag (unsafe): %s", line)
		}
	}
}
