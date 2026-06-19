// SPDX-License-Identifier: Apache-2.0

package satellite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errFakeNoDevice is the canned "backing device not present yet" error the
// fake exec returns for blockdev probes before the (simulated) udev
// /dev/zvol symlink appears.
var errFakeNoDevice = errors.New("blockdev: cannot open device: No such file or directory")

// zvolWaitFakeProvider is a no-op storage.Provider whose VolumeStatus
// reports a fixed device path. It lets the applyStorage device-wait tests
// drive the post-`zfs create` udev race without a real ZFS/LVM backend.
// UsableKib is large so applyStorage never takes the resize branch.
type zvolWaitFakeProvider struct{ dev string }

func (zvolWaitFakeProvider) Kind() string { return ProviderKindZFSThin }

func (zvolWaitFakeProvider) PoolStatus(context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{}, nil
}

func (zvolWaitFakeProvider) CreateVolume(context.Context, storage.Volume) error { return nil }
func (zvolWaitFakeProvider) DeleteVolume(context.Context, storage.Volume) error { return nil }
func (zvolWaitFakeProvider) ResizeVolume(context.Context, storage.Volume) error { return nil }

func (p zvolWaitFakeProvider) VolumeStatus(context.Context, storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{DevicePath: p.dev, UsableKib: 1 << 30, State: "PROVISIONED"}, nil
}

func (zvolWaitFakeProvider) CreateSnapshot(context.Context, storage.Snapshot) error { return nil }
func (zvolWaitFakeProvider) DeleteSnapshot(context.Context, storage.Snapshot) error { return nil }

func (zvolWaitFakeProvider) RestoreVolumeFromSnapshot(
	context.Context, storage.Volume, storage.Snapshot,
) error {
	return nil
}

// countCalls counts how many recorded FakeExec calls match name + args
// exactly. Safe to read fx.Calls directly here: applyStorage runs
// synchronously in the test goroutine, no product goroutine is live.
func countCalls(fx *storage.FakeExec, name string, args ...string) int {
	want := strings.Join(args, " ")

	n := 0
	for _, c := range fx.Calls {
		if c.Name == name && strings.Join(c.Args, " ") == want {
			n++
		}
	}

	return n
}

// TestApplyStorageWaitsForZvolDevice is the regression guard for the
// zfs-create/udev race: `zfs create -V` returns before udev has created
// the /dev/zvol/<pool>/<vol> symlink, so the storage-only (local SC) path
// used to run mkfs immediately on a device that did not exist yet
// ("mkfs.ext4 ...: The file ... does not exist"). applyStorage must now
// poll `blockdev --getsize64` until the backing device is usable before
// returning it to the mkfs / DRBD-attach consumers.
//
// On the pre-fix code applyStorage never probes the device, so the
// blockdev call count is 0 and this test fails — exactly the fail-on-bug
// property the fix must carry.
func TestApplyStorageWaitsForZvolDevice(t *testing.T) {
	t.Parallel()

	const dev = "/dev/zvol/data/pvc-zvolwait_00000"

	fx := storage.NewFakeExec()
	// blockdev fails twice (udev has not made the symlink yet), then the
	// device appears and it succeeds. Later calls fall through to fx
	// (empty success). The sequencedFakeExec helper still records every
	// forwarded call in fx.Calls for the count assertion below.
	seq := &sequencedFakeExec{
		fx:     fx,
		seqKey: "blockdev --getsize64 " + dev,
		seqResps: []storage.FakeResponse{
			{Err: errFakeNoDevice},
			{Err: errFakeNoDevice},
			{Stdout: []byte("1073741824\n")},
		},
	}

	rec := NewReconciler(ReconcilerConfig{
		Providers:         map[string]storage.Provider{"p1": zvolWaitFakeProvider{dev: dev}},
		Exec:              seq,
		DeviceWaitPoll:    time.Millisecond,
		DeviceWaitTimeout: 5 * time.Second,
	})

	dr := &intent.DesiredResource{
		Name:    "pvc-zvolwait",
		Volumes: []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024, StoragePool: "p1"}},
	}

	devices, _, _, err := rec.applyStorage(context.Background(), dr)
	if err != nil {
		t.Fatalf("applyStorage: unexpected error: %v", err)
	}

	if devices[0] != dev {
		t.Fatalf("device path: got %q, want %q", devices[0], dev)
	}

	got := countCalls(fx, "blockdev", "--getsize64", dev)
	if got < 3 {
		t.Fatalf("blockdev --getsize64 calls: got %d, want >=3 "+
			"(applyStorage must poll for the backing device before returning)", got)
	}
}

// TestApplyStorageFailsWhenZvolDeviceNeverAppears pins the timeout side of
// the wait: if the backing device never materialises, applyStorage must
// surface an error so the resource is re-queued rather than mkfs-ing /
// attaching a path that is not there.
func TestApplyStorageFailsWhenZvolDeviceNeverAppears(t *testing.T) {
	t.Parallel()

	const dev = "/dev/zvol/data/pvc-never_00000"

	fx := storage.NewFakeExec()
	fx.Expect("blockdev --getsize64 "+dev, storage.FakeResponse{Err: errFakeNoDevice})

	rec := NewReconciler(ReconcilerConfig{
		Providers:         map[string]storage.Provider{"p1": zvolWaitFakeProvider{dev: dev}},
		Exec:              fx,
		DeviceWaitPoll:    time.Millisecond,
		DeviceWaitTimeout: 50 * time.Millisecond,
	})

	dr := &intent.DesiredResource{
		Name:    "pvc-never",
		Volumes: []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024, StoragePool: "p1"}},
	}

	_, _, _, err := rec.applyStorage(context.Background(), dr) //nolint:dogsled // only the returned error is relevant here
	if err == nil {
		t.Fatal("applyStorage: want error when the backing device never appears, got nil")
	}

	if !strings.Contains(err.Error(), "did not appear") {
		t.Fatalf("error: got %v, want it to report the device never appeared", err)
	}
}
