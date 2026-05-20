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

// TestNewReconcilerWiresExec pins Bug 311 regression (#501 reopen):
// the production agent's `newReconciler` MUST wire `ReconcilerConfig.Exec`
// with a non-nil `storage.Exec`. The reconciler's auto-mkfs path
// (`runAutoMkfs`) and the Bug-311 retry predicate (`needsAutoMkfsRetry`)
// both early-exit when `cfg.Exec == nil` — the no-op branch used by unit
// tests that mock half a Reconciler. An agent that forgot to pass Exec
// silently disables auto-mkfs for every diskful resource with
// `FileSystem/Type` set, which is exactly how the Bug 311 fix landed in
// the unit-test world (mkfs runs) but never reached production (mkfs
// silently skipped). The agent-side wiring fix is one line in
// `newReconciler`; this test is the trip-wire that keeps it wired.
//
// Live-stand reproducer:
//   linstor rd c <rd>; linstor rd sp <rd> FileSystem/Type ext4
//   linstor vd c <rd> 256M; linstor r c <rd> --auto-place=2 -s lvm-thin
//   # wait until UpToDate on every replica
//   blkid -o export /dev/drbd<minor>   # → exit 2 (no signature)
//   ls /etc/drbd.d/*.mkfs.done         # → none
//
// Companion of TestApplyAutoMkfsRetryAfterMissedFirstActivation and
// the cli-matrix `rwx-ganesha-data-vol-mkfs.sh` cell (kernel-truth
// half — only the real DRBD stack catches the missing `Exec` wiring
// because every existing unit test passes Exec explicitly).
func TestNewReconcilerWiresExec(t *testing.T) {
	t.Parallel()

	agent := NewAgent(Config{
		NodeName:     "n1",
		StateDir:     "/tmp/test-state",
		LocalAddress: "10.0.0.1",
	})

	rec := agent.newReconciler()

	if rec.cfg.Exec == nil {
		t.Fatal("Bug 311 regression: Agent.newReconciler MUST wire ReconcilerConfig.Exec " +
			"with a non-nil storage.Exec; nil Exec disables auto-mkfs entirely " +
			"(runAutoMkfs + needsAutoMkfsRetry both early-exit on nil Exec)")
	}

	if rec.cfg.StateDir == "" {
		t.Fatal("Agent.newReconciler MUST propagate cfg.StateDir; empty StateDir " +
			"also disables auto-mkfs (same no-op branch as nil Exec)")
	}

	if rec.cfg.Adm == nil {
		t.Fatal("Agent.newReconciler MUST wire ReconcilerConfig.Adm; nil Adm " +
			"disables the DRBD half entirely")
	}

	if rec.cfg.Cryptsetup == nil {
		t.Fatal("Agent.newReconciler MUST wire ReconcilerConfig.Cryptsetup; " +
			"nil Cryptsetup makes LUKS-layered RDs unfulfillable")
	}
}
