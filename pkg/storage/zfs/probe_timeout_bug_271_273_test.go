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
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Bug 271+272+273 (P1, class-extension of Bug 270 to ZFS): a stuck
// `zpool list` / `zfs list` call in kernel I/O-wait (suspended pool,
// SAN drop, etc.) must NOT consume the satellite reconcile worker
// forever. controller-runtime hands Reconcile an unbounded ctx;
// without the withProbeTimeout wrapper inside each probe, the
// REST plane for the affected node goes blackout until reboot —
// same wedge surface as the LVM Bug 270 incident reproduced on
// dev-kvaps in May.

// ctxDeadlineRecorder records whether the ctx it receives carries
// a Deadline. PoolStatus, VolumeStatus, and datasetExists are the
// three call sites the bug-hunt v38 report flagged.
type ctxDeadlineRecorder struct {
	mu          sync.Mutex
	hadDeadline []bool
	stdout      map[string][]byte
}

func newCtxDeadlineRecorder() *ctxDeadlineRecorder {
	return &ctxDeadlineRecorder{stdout: map[string][]byte{}}
}

func (f *ctxDeadlineRecorder) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return f.RunWithStdin(ctx, nil, name, args...)
}

func (f *ctxDeadlineRecorder) RunWithStdin(
	ctx context.Context,
	_ io.Reader,
	name string,
	args ...string,
) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := ctx.Deadline()
	f.hadDeadline = append(f.hadDeadline, ok)

	key := name
	if len(args) > 0 {
		key += " " + strings.Join(args, " ")
	}

	if out, ok := f.stdout[key]; ok {
		return out, nil
	}

	return nil, nil
}

// TestPoolStatusBoundsZpoolList pins Bug 271: PoolStatus must
// derive a deadline before shelling out to `zpool list`.
func TestPoolStatusBoundsZpoolList(t *testing.T) {
	t.Parallel()

	fx := newCtxDeadlineRecorder()
	fx.stdout["zpool list -H -p -o size,free tank"] = []byte("104857600000\t78643200000\n")

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.PoolStatus(context.Background())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if len(fx.hadDeadline) == 0 {
		t.Fatalf("no exec calls recorded")
	}

	if !fx.hadDeadline[0] {
		t.Errorf("PoolStatus must bound the zpool list ctx with a deadline (Bug 271)")
	}
}

// TestVolumeStatusBoundsZfsList pins Bug 273: VolumeStatus must
// derive a deadline before shelling out to `zfs list`.
func TestVolumeStatusBoundsZfsList(t *testing.T) {
	t.Parallel()

	fx := newCtxDeadlineRecorder()
	fx.stdout["zfs list -H -p -o name,volsize,used tank/pvc-1_00000"] = []byte("tank/pvc-1_00000\t1073741824\t1024\n")

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.VolumeStatus(context.Background(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if len(fx.hadDeadline) == 0 {
		t.Fatalf("no exec calls recorded")
	}

	if !fx.hadDeadline[0] {
		t.Errorf("VolumeStatus must bound the zfs list ctx with a deadline (Bug 273)")
	}
}

// TestDatasetExistsBoundsZfsList pins Bug 272: datasetExists is
// not exported, but DeleteVolume runs it as the idempotency check
// and short-circuits on missing-LV without further exec calls — the
// cleanest probe-only surface to assert the deadline propagates.
func TestDatasetExistsBoundsZfsList(t *testing.T) {
	t.Parallel()

	fx := newCtxDeadlineRecorder()
	// datasetExists returns empty (no dataset) so DeleteVolume
	// short-circuits with nil error. The one recorded call is
	// datasetExists itself.
	fx.stdout["zfs list -H -o name tank/pvc-1_00000"] = []byte("")

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteVolume(context.Background(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if len(fx.hadDeadline) < 1 {
		t.Fatalf("expected at least one exec call; got %d", len(fx.hadDeadline))
	}

	if !fx.hadDeadline[0] {
		t.Errorf("datasetExists must bound the zfs list ctx with a deadline (Bug 272)")
	}
}
