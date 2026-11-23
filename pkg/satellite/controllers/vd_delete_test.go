// SPDX-License-Identifier: Apache-2.0

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

package controllers

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

const (
	vdDelNode = "n-vdd"
	vdDelRD   = "pvc-vdd"
	vdDelPool = "thin"
	vdDelID   = "delete-1"
)

var (
	errDatasetBusy = errors.New("dataset is busy")
	errHeldOpen    = errors.New("device is held open by someone")
)

// recordingProvider records the volumes deleted through it.
type recordingProvider struct {
	mu      sync.Mutex
	deleted []storage.Volume
	fail    error
}

func (p *recordingProvider) Kind() string { return "FAKE" }

func (p *recordingProvider) PoolStatus(context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{TotalCapacityKib: 1 << 20, FreeCapacityKib: 1 << 20}, nil
}

func (p *recordingProvider) CreateVolume(context.Context, storage.Volume) error { return nil }

func (p *recordingProvider) DeleteVolume(_ context.Context, vol storage.Volume) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.fail != nil {
		return p.fail
	}

	p.deleted = append(p.deleted, vol)

	return nil
}

func (p *recordingProvider) ResizeVolume(context.Context, storage.Volume) error { return nil }

func (p *recordingProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{DevicePath: "/dev/fake/" + vol.ResourceName}, nil
}

func (p *recordingProvider) CreateSnapshot(context.Context, storage.Snapshot) error { return nil }

func (p *recordingProvider) DeleteSnapshot(context.Context, storage.Snapshot) error { return nil }

func (p *recordingProvider) RestoreVolumeFromSnapshot(context.Context, storage.Volume, storage.Snapshot) error {
	return nil
}

func (p *recordingProvider) deletedNumbers() []int32 {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]int32, 0, len(p.deleted))
	for _, v := range p.deleted {
		out = append(out, v.VolumeNumber)
	}

	return out
}

// vdDeleteFixture is one replica of a storage-only definition with volumes
// 0 and 1, and a satellite whose pools are the given providers.
type vdDeleteFixture struct {
	cli client.Client
	rec *ResourceReconciler
	res *blockstoriov1alpha1.Resource
	rd  *blockstoriov1alpha1.ResourceDefinition
}

func newVDDeleteFixture(t *testing.T, providers map[string]storage.Provider, adm *drbd.Adm, edit func(*blockstoriov1alpha1.ResourceDefinition, *blockstoriov1alpha1.Resource)) *vdDeleteFixture {
	t.Helper()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: vdDelRD},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
				{VolumeNumber: 1, SizeKib: 65536},
			},
		},
	}
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: vdDelRD + "." + vdDelNode, Finalizers: []string{SatelliteResourceFinalizer}},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: vdDelRD,
			NodeName:               vdDelNode,
			StoragePool:            vdDelPool,
		},
	}

	if edit != nil {
		edit(rd, res)
	}

	cli := fake.NewClientBuilder().
		WithScheme(newToggleDiskTestScheme(t)).
		WithObjects(rd, res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	rec := &ResourceReconciler{Client: cli, Config: Config{
		NodeName:  vdDelNode,
		Apply:     satellite.NewReconciler(satellite.ReconcilerConfig{NodeName: vdDelNode, Providers: providers, Adm: adm}),
		Exec:      storage.NewFakeExec(),
		APIReader: cli,
	}}

	return &vdDeleteFixture{cli: cli, rec: rec, res: res, rd: rd}
}

func flag(vd *blockstoriov1alpha1.ResourceDefinitionVolume, id string) {
	vd.Flags = append(vd.Flags, blockstoriov1alpha1.VolumeDefinitionFlagDelete)
	vd.DeleteID = id
}

func (f *vdDeleteFixture) run(t *testing.T) error {
	t.Helper()

	var res blockstoriov1alpha1.Resource
	if err := f.cli.Get(t.Context(), client.ObjectKeyFromObject(f.res), &res); err != nil {
		t.Fatal(err)
	}

	var rd blockstoriov1alpha1.ResourceDefinition
	if err := f.cli.Get(t.Context(), client.ObjectKeyFromObject(f.rd), &rd); err != nil {
		t.Fatal(err)
	}

	return f.rec.deleteFlaggedVolumes(t.Context(), &res, &rd)
}

func (f *vdDeleteFixture) confirmations(t *testing.T) []blockstoriov1alpha1.DeletedVolumeStatus {
	t.Helper()

	var res blockstoriov1alpha1.Resource
	if err := f.cli.Get(t.Context(), client.ObjectKeyFromObject(f.res), &res); err != nil {
		t.Fatal(err)
	}

	return res.Status.DeletedVolumes
}

// A volume definition flagged DELETE is deleted on this replica through the
// pool it lives in, and confirmed under its deleteID; the volume next to it
// is left alone.
func TestDeleteFlaggedVolumesDeletesAndConfirms(t *testing.T) {
	t.Parallel()

	provider := &recordingProvider{}
	f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, nil,
		func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
			flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
		})

	if err := f.run(t); err != nil {
		t.Fatalf("deleteFlaggedVolumes: %v", err)
	}

	if got := provider.deletedNumbers(); !slices.Equal(got, []int32{1}) {
		t.Errorf("deleted volumes %v, want only volume 1", got)
	}

	want := []blockstoriov1alpha1.DeletedVolumeStatus{{VolumeNumber: 1, DeleteID: vdDelID}}
	if got := f.confirmations(t); !slices.Equal(got, want) {
		t.Errorf("confirmations %+v, want %+v", got, want)
	}

	// A second pass has nothing left to do.
	if err := f.run(t); err != nil {
		t.Fatal(err)
	}

	if got := provider.deletedNumbers(); len(got) != 1 {
		t.Errorf("a confirmed volume was deleted again: %v", got)
	}
}

// Only the DELETE flag deletes a volume. A volume the replica still records
// in its annotation and status but the definition no longer lists is not
// deleted here: that would be a delete nobody asked for.
func TestDeleteFlaggedVolumesIgnoresVolumesMissingFromTheDefinition(t *testing.T) {
	t.Parallel()

	provider := &recordingProvider{}
	f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, nil,
		func(rd *blockstoriov1alpha1.ResourceDefinition, res *blockstoriov1alpha1.Resource) {
			rd.Spec.VolumeDefinitions = rd.Spec.VolumeDefinitions[:1]
			res.Annotations = map[string]string{blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0,1"}
			res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0}, {VolumeNumber: 1}}
		})

	if err := f.run(t); err != nil {
		t.Fatal(err)
	}

	if got := provider.deletedNumbers(); len(got) != 0 {
		t.Errorf("deleted %v without a DELETE flag", got)
	}
}

// A confirmation whose definition is gone, or flagged under another
// deleteID, is dropped, and the new delete is carried out instead of being
// taken as done.
func TestDeleteFlaggedVolumesReplacesStaleConfirmations(t *testing.T) {
	t.Parallel()

	provider := &recordingProvider{}
	f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, nil,
		func(rd *blockstoriov1alpha1.ResourceDefinition, res *blockstoriov1alpha1.Resource) {
			flag(&rd.Spec.VolumeDefinitions[1], "delete-2")
			res.Status.DeletedVolumes = []blockstoriov1alpha1.DeletedVolumeStatus{
				{VolumeNumber: 1, DeleteID: "delete-1"},
				{VolumeNumber: 7, DeleteID: "long-gone"},
			}
		})

	if err := f.run(t); err != nil {
		t.Fatal(err)
	}

	if got := provider.deletedNumbers(); !slices.Equal(got, []int32{1}) {
		t.Errorf("deleted %v, want volume 1 deleted for the new delete", got)
	}

	want := []blockstoriov1alpha1.DeletedVolumeStatus{{VolumeNumber: 1, DeleteID: "delete-2"}}
	if got := f.confirmations(t); !slices.Equal(got, want) {
		t.Errorf("confirmations %+v, want %+v", got, want)
	}
}

// A volume the provider could not delete, or whose pool this satellite does
// not have, is not confirmed: the definition waits for it.
func TestDeleteFlaggedVolumesDoesNotConfirmAFailedDelete(t *testing.T) {
	t.Parallel()

	for name, providers := range map[string]map[string]storage.Provider{
		"provider fails":    {vdDelPool: &recordingProvider{fail: errDatasetBusy}},
		"pool unregistered": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newVDDeleteFixture(t, providers, nil,
				func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
					flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
				})

			if err := f.run(t); err == nil {
				t.Error("deleteFlaggedVolumes succeeded over a volume it did not delete")
			}

			if got := f.confirmations(t); len(got) != 0 {
				t.Errorf("confirmed %+v without deleting", got)
			}
		})
	}
}

// A diskless replica has no storage for the volume; it confirms once DRBD
// has let go of it.
func TestDeleteFlaggedVolumesOnADisklessReplica(t *testing.T) {
	t.Parallel()

	provider := &recordingProvider{}
	f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, nil,
		func(rd *blockstoriov1alpha1.ResourceDefinition, res *blockstoriov1alpha1.Resource) {
			flag(&rd.Spec.VolumeDefinitions[0], vdDelID)
			res.Spec.Flags = []string{apiv1.ResourceFlagDiskless}
		})

	if err := f.run(t); err != nil {
		t.Fatal(err)
	}

	if got := provider.deletedNumbers(); len(got) != 0 {
		t.Errorf("deleted storage %v on a diskless replica", got)
	}

	if got := f.confirmations(t); len(got) != 1 {
		t.Errorf("confirmations %+v, want the diskless replica's", got)
	}
}

// A volume definition that names its own pool is deleted through that pool.
func TestDeleteFlaggedVolumesUsesTheVolumesOwnPool(t *testing.T) {
	t.Parallel()

	replicaPool, ownPool := &recordingProvider{}, &recordingProvider{}
	f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: replicaPool, "fast": ownPool}, nil,
		func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
			rd.Spec.VolumeDefinitions[1].Props = map[string]string{"StorPoolName": "fast"}
			flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
		})

	if err := f.run(t); err != nil {
		t.Fatal(err)
	}

	if len(replicaPool.deletedNumbers()) != 0 || !slices.Equal(ownPool.deletedNumbers(), []int32{1}) {
		t.Errorf("replica pool deleted %v, own pool %v; want volume 1 through its own pool",
			replicaPool.deletedNumbers(), ownPool.deletedNumbers())
	}
}

const statusWithMinor = `pvc-vdd role:Secondary
  volume:0 minor:1000 disk:UpToDate
  volume:1 minor:1001 disk:UpToDate
`

// When the volume is still a DRBD device, which is the case for the
// resource's last volume since no .res is rendered for zero volumes, it is
// taken out of DRBD before its storage is touched. A device DRBD refuses to
// drop, because it is open, keeps its storage.
func TestDeleteVolumesRemovesTheMinorBeforeTheStorage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		delMinorErr error
		wantDeleted bool
	}{
		{"released", nil, true},
		{"held open", errHeldOpen, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exec := storage.NewFakeExec()
			exec.Responses["drbdsetup status --verbose"] = storage.FakeResponse{Stdout: []byte(statusWithMinor)}
			exec.Responses["drbdsetup del-minor 1001"] = storage.FakeResponse{Err: tc.delMinorErr}

			provider := &recordingProvider{}
			f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, drbd.NewAdm(exec),
				func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
					rd.Spec.LayerStack = []string{"DRBD", "STORAGE"}
					minor := int32(1001)
					rd.Spec.VolumeDefinitions[1].DRBDMinor = &minor
					flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
				})

			err := f.run(t)
			if (err == nil) != tc.wantDeleted {
				t.Fatalf("deleteFlaggedVolumes: %v", err)
			}

			if got := len(provider.deletedNumbers()) == 1; got != tc.wantDeleted {
				t.Errorf("storage deleted = %v, want %v", got, tc.wantDeleted)
			}

			calls := exec.CallsSnapshot()
			lines := make([]string, 0, len(calls))

			for _, c := range calls {
				lines = append(lines, c.Name+" "+strings.Join(c.Args, " "))
			}

			if !slices.Contains(lines, "drbdsetup detach 1001") || !slices.Contains(lines, "drbdsetup del-minor 1001") {
				t.Errorf("commands %q, want detach and del-minor of minor 1001", lines)
			}
		})
	}
}

// A replica whose delete keeps failing holds the definition flagged DELETE,
// so it says why in a Warning Event on the Resource, not only in its log.
func TestAFailedVolumeDeleteIsAWarningEvent(t *testing.T) {
	t.Parallel()

	f := newVDDeleteFixture(t, map[string]storage.Provider{}, nil,
		func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
			flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
		})

	recorder := events.NewFakeRecorder(4)
	f.rec.Config.Recorder = recorder

	ok := []*intent.ResourceApplyResult{{Name: vdDelRD, Ok: true}}
	if applyFailed, _ := f.rec.afterApply(t.Context(), f.res, f.rd, ok, logr.Discard()); !applyFailed {
		t.Fatal("afterApply reported success for a delete that failed")
	}

	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning "+volumeDeleteFailedReason) || !strings.Contains(ev, vdDelPool) {
			t.Errorf("Event = %q, want a Warning %s naming the pool", ev, volumeDeleteFailedReason)
		}
	default:
		t.Error("no Event for a volume delete that failed")
	}
}

// An apply that failed may have left the volume in DRBD, so its storage is
// not touched until an apply goes through.
func TestAfterApplyDeletesNothingWhenTheApplyFailed(t *testing.T) {
	t.Parallel()

	provider := &recordingProvider{}
	f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, nil,
		func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
			flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
		})

	failed := []*intent.ResourceApplyResult{{Name: vdDelRD, Ok: false, Message: "drbdadm adjust failed"}}
	if applyFailed, _ := f.rec.afterApply(t.Context(), f.res, f.rd, failed, logr.Discard()); !applyFailed {
		t.Error("afterApply reported success for a failed apply")
	}

	if got := provider.deletedNumbers(); len(got) != 0 {
		t.Errorf("deleted %v after a failed apply", got)
	}

	ok := []*intent.ResourceApplyResult{{Name: vdDelRD, Ok: true}}
	if applyFailed, _ := f.rec.afterApply(t.Context(), f.res, f.rd, ok, logr.Discard()); applyFailed {
		t.Error("afterApply reported a failure for an apply that went through")
	}

	if got := provider.deletedNumbers(); !slices.Equal(got, []int32{1}) {
		t.Errorf("deleted %v after the apply went through, want volume 1", got)
	}
}

// vdDelSuspendedStatus is `drbdsetup status pvc-vdd --json` on a replica
// left suspended on quorum by the removal of volume 1, with volume 0, which
// stays, reporting quorum.
const vdDelSuspendedStatus = `[{"name": "pvc-vdd", "role": "Primary", "suspended": true,
  "suspended-user": false, "suspended-no-data": false, "suspended-fencing": false,
  "suspended-quorum": true,
  "devices": [{"volume": 0, "minor": 1000, "disk-state": "UpToDate", "quorum": true}]}]`

// vdDelSuspendIOShow is `drbdsetup show pvc-vdd --json` for a resource with
// quorum majority and on-no-quorum suspend-io.
const vdDelSuspendIOShow = `[{"resource": "pvc-vdd",
  "options": {"quorum": "majority", "on-no-quorum": "suspend-io"},
  "connections": [{"_peer_node_id": 0}]}]`

// The on-no-quorum changes the satellite makes around and after a removal.
const (
	vdDelToIOError   = "drbdsetup resource-options pvc-vdd --on-no-quorum=io-error"
	vdDelToSuspendIO = "drbdsetup resource-options pvc-vdd --on-no-quorum=suspend-io"
)

// `drbdadm adjust` can remove the minor of a volume flagged DELETE and then
// fail for another reason, in the same run or in later ones. Its storage
// stays until an apply goes through, but the quorum recheck the removal
// needs must not wait for that: until it runs, the remaining volumes of a
// Primary stay suspended. While the minor is still in the kernel nothing is
// rechecked.
func TestAfterAFailedApplyTheRemovedMinorsQuorumIsRechecked(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		verbose     string
		wantRecheck bool
	}{
		{name: "adjust removed the minor", verbose: "pvc-vdd role:Primary\n  volume:0 minor:1000 disk:UpToDate\n", wantRecheck: true},
		{name: "the minor is still there", verbose: statusWithMinor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exec := storage.NewFakeExec()
			exec.Responses["drbdsetup status --verbose"] = storage.FakeResponse{Stdout: []byte(tc.verbose)}
			exec.Responses["drbdsetup status pvc-vdd --json"] = storage.FakeResponse{Stdout: []byte(vdDelSuspendedStatus)}
			exec.Responses["drbdsetup show pvc-vdd --json"] = storage.FakeResponse{Stdout: []byte(vdDelSuspendIOShow)}

			provider := &recordingProvider{}
			f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, drbd.NewAdm(exec),
				func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
					rd.Spec.LayerStack = []string{"DRBD", "STORAGE"}
					minor := int32(1001)
					rd.Spec.VolumeDefinitions[1].DRBDMinor = &minor
					flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
				})

			failed := []*intent.ResourceApplyResult{{Name: vdDelRD, Ok: false, Message: "drbdadm adjust failed"}}
			if applyFailed, _ := f.rec.afterApply(t.Context(), f.res, f.rd, failed, logr.Discard()); !applyFailed {
				t.Fatal("afterApply reported success for a failed apply")
			}

			if got := provider.deletedNumbers(); len(got) != 0 {
				t.Errorf("deleted %v after a failed apply", got)
			}

			lines := exec.CommandLines()
			if got := slices.Contains(lines, vdDelToIOError); got != tc.wantRecheck {
				t.Errorf("quorum recomputed %t, want %t; commands %q", got, tc.wantRecheck, lines)
			}

			if !tc.wantRecheck && slices.Contains(lines, "drbdsetup status pvc-vdd --json") {
				t.Errorf("quorum read while the minor is still in the kernel; commands %q", lines)
			}
		})
	}
}

// vdDelSecondaryStatus is `drbdsetup status pvc-vdd --json` on a Secondary
// that still has both volumes, neither suspended nor open.
const vdDelSecondaryStatus = `[{"name": "pvc-vdd", "role": "Secondary", "suspended": false,
  "suspended-user": false, "suspended-no-data": false, "suspended-fencing": false,
  "suspended-quorum": false,
  "devices": [{"volume": 0, "minor": 1000, "disk-state": "UpToDate", "open": false, "quorum": true},
    {"volume": 1, "minor": 1001, "disk-state": "UpToDate", "open": false, "quorum": true}]}]`

// Inside `drbdadm adjust` the volume of a definition flagged DELETE is
// removed while on-no-quorum suspend-io is in force, which suspends the
// volumes that stay on the replica that removes it last. Ahead of the
// apply, which re-renders the .res without the volume, the satellite takes
// the volume out itself under io-error and sets suspend-io back, and
// leaves its storage to the delete that follows the apply.
func TestBeforeTheApplyFlaggedVolumesLeaveDRBDKeepingQuorum(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		flagged bool
		want    []string
	}{
		{
			name: "a volume flagged DELETE", flagged: true,
			want: []string{vdDelToIOError, "drbdsetup detach 1001", "drbdsetup del-minor 1001", vdDelToSuspendIO},
		},
		{name: "nothing flagged"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exec := storage.NewFakeExec()
			exec.Responses["drbdsetup status --verbose"] = storage.FakeResponse{Stdout: []byte(statusWithMinor)}
			exec.Responses["drbdsetup status pvc-vdd --json"] = storage.FakeResponse{Stdout: []byte(vdDelSecondaryStatus)}
			exec.Responses["drbdsetup show pvc-vdd --json"] = storage.FakeResponse{Stdout: []byte(vdDelSuspendIOShow)}

			provider := &recordingProvider{}
			f := newVDDeleteFixture(t, map[string]storage.Provider{vdDelPool: provider}, drbd.NewAdm(exec),
				func(rd *blockstoriov1alpha1.ResourceDefinition, _ *blockstoriov1alpha1.Resource) {
					rd.Spec.LayerStack = []string{"DRBD", "STORAGE"}
					minor := int32(1001)
					rd.Spec.VolumeDefinitions[1].DRBDMinor = &minor
					if tc.flagged {
						flag(&rd.Spec.VolumeDefinitions[1], vdDelID)
					}
				})

			f.rec.prepareDRBDApply(t.Context(), f.res, f.rd, nil, logr.Discard())

			var got []string

			for _, line := range exec.CommandLines() {
				if strings.HasPrefix(line, "drbdsetup resource-options ") ||
					strings.HasPrefix(line, "drbdsetup detach ") || strings.HasPrefix(line, "drbdsetup del-minor ") {
					got = append(got, line)
				}
			}

			if !slices.Equal(got, tc.want) {
				t.Errorf("commands %q, want %q", got, tc.want)
			}

			if deleted := provider.deletedNumbers(); len(deleted) != 0 {
				t.Errorf("storage of volumes %v deleted before the apply", deleted)
			}
		})
	}
}
