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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
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
