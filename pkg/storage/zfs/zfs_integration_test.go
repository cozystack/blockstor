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

package zfs_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// TestZFSAgainstRealPool runs the Provider through CreateVolume →
// VolumeStatus → CreateSnapshot → DeleteSnapshot → DeleteVolume on
// a real `zfs` binary against a pre-created pool whose name is
// passed via the `BLOCKSTOR_ZFS_POOL` env var.
//
// Skipped if either:
//   - `zfs` is not on PATH (developer laptop without ZFS)
//   - BLOCKSTOR_ZFS_POOL is unset (CI/regression doesn't auto-touch
//     a real pool — the dev stand opts in by setting it)
//
// On the dev stand: `sudo zpool create blockstor-test /dev/loopN` →
// `BLOCKSTOR_ZFS_POOL=blockstor-test go test -run TestZFSAgainst -v`.
func TestZFSAgainstRealPool(t *testing.T) {
	if _, err := exec.LookPath("zfs"); err != nil {
		t.Skip("zfs binary not on PATH; skipping integration smoke")
	}

	pool := requireEnv(t, "BLOCKSTOR_ZFS_POOL")

	provider := zfs.NewProvider(zfs.Config{Pool: pool}, storage.RealExec{})
	ctx := t.Context()

	vol := storage.Volume{
		ResourceName: "blockstor-it",
		VolumeNumber: 0,
		SizeKib:      8 * 1024, // 8 MiB — keep it tiny so a small pool fits
	}

	// Cleanup whatever a prior aborted run left behind.
	t.Cleanup(func() {
		_ = provider.DeleteVolume(t.Context(), vol)
	})

	err := provider.CreateVolume(ctx, vol)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	// Idempotent — second CreateVolume must not blow up on the
	// already-existing dataset.
	err = provider.CreateVolume(ctx, vol)
	if err != nil {
		t.Fatalf("CreateVolume (idempotent): %v", err)
	}

	st, err := provider.VolumeStatus(ctx, vol)
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if st.State != "PROVISIONED" {
		t.Errorf("State: got %q, want PROVISIONED", st.State)
	}

	if !strings.HasPrefix(st.DevicePath, "/dev/zvol/"+pool+"/") {
		t.Errorf("DevicePath: got %q, want /dev/zvol/%s/...", st.DevicePath, pool)
	}

	if st.UsableKib < vol.SizeKib {
		t.Errorf("UsableKib: got %d, want >= %d", st.UsableKib, vol.SizeKib)
	}

	// Snapshot round-trip.
	snap := storage.Snapshot{ResourceName: vol.ResourceName, SnapshotName: "snap-1"}

	err = provider.CreateSnapshot(ctx, snap)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	err = provider.DeleteSnapshot(ctx, snap)
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	err = provider.DeleteVolume(ctx, vol)
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	// After delete, status reports NOT_PROVISIONED.
	st, err = provider.VolumeStatus(ctx, vol)
	if err != nil {
		t.Fatalf("VolumeStatus (post-delete): %v", err)
	}

	if st.State != "NOT_PROVISIONED" {
		t.Errorf("post-delete State: got %q, want NOT_PROVISIONED", st.State)
	}
}

// TestZFSPoolStatusAgainstRealPool just walks PoolStatus once and
// verifies the numbers look sensible.
func TestZFSPoolStatusAgainstRealPool(t *testing.T) {
	if _, err := exec.LookPath("zpool"); err != nil {
		t.Skip("zpool binary not on PATH; skipping integration smoke")
	}

	pool := requireEnv(t, "BLOCKSTOR_ZFS_POOL")

	provider := zfs.NewProvider(zfs.Config{Pool: pool}, storage.RealExec{})

	ps, err := provider.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if ps.TotalCapacityKib <= 0 {
		t.Errorf("TotalCapacityKib: got %d, want > 0", ps.TotalCapacityKib)
	}

	if ps.FreeCapacityKib < 0 || ps.FreeCapacityKib > ps.TotalCapacityKib {
		t.Errorf("FreeCapacityKib: got %d, want 0..%d", ps.FreeCapacityKib, ps.TotalCapacityKib)
	}

	if !ps.SupportsSnapshots {
		t.Errorf("SupportsSnapshots: got false, want true (ZFS always supports snapshots)")
	}
}

// requireEnv reads the env var or skips the test (rather than fails)
// when missing — these are opt-in integration tests.
func requireEnv(t *testing.T, key string) string {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set; skipping integration smoke", key)
	}

	return v
}
