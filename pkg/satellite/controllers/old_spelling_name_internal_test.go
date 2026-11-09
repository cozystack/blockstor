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
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// A replica written before the store kept CR names in spec names its
// definition Pvc-Mix. The satellite lays it down under the definition's CR
// name, pvc-mix, so a snapshot of it is taken under that name whichever
// spelling the snapshot row keeps.
func TestAnOldMixedCaseReplicaIsSnapshottedUnderItsCRName(t *testing.T) {
	t.Parallel()

	for _, snapSpelling := range []string{"pvc-mix", "Pvc-Mix"} {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				LayerStack:        []string{"STORAGE"},
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 4 * 1024}},
			},
		}
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-a"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "Pvc-Mix",
				NodeName:               "node-a",
				StoragePool:            "thin1",
			},
		}

		fx := storage.NewFakeExec()
		apply := satellite.NewReconciler(satellite.ReconcilerConfig{
			Providers: map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
			NodeName:  "node-a",
		})

		r := newPassphraseTestReconciler(t, rd, res)
		r.Config.Exec = fx
		r.Config.Apply = apply

		if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
			t.Fatalf("runApply: %v", err)
		}

		snap := &blockstoriov1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.snap-1", Finalizers: []string{SatelliteSnapshotFinalizer}},
			Spec: blockstoriov1alpha1.SnapshotSpec{
				ResourceDefinitionName: snapSpelling,
				SnapshotName:           "snap-1",
				Nodes:                  []string{"node-a"},
				TakeSnapshot:           true,
			},
		}

		cli := fake.NewClientBuilder().WithScheme(r.Client.Scheme()).
			WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build()

		snapshots := &SnapshotReconciler{Client: cli, Config: Config{NodeName: "node-a", Apply: apply, Exec: fx}}
		if _, err := snapshots.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err != nil {
			t.Fatalf("snapshot %s: %v", snapSpelling, err)
		}

		var got blockstoriov1alpha1.Snapshot
		if err := cli.Get(t.Context(), client.ObjectKey{Name: snap.Name}, &got); err != nil {
			t.Fatalf("get snapshot: %v", err)
		}

		if slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
			t.Errorf("snapshot row %s: FAILED %v", snapSpelling, got.Status)
		}

		if !slices.ContainsFunc(fx.CommandLines(), func(c string) bool {
			return c == "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-mix_snap-1_00000 vg/pvc-mix_00000"
		}) {
			t.Errorf("snapshot row %s: no lvcreate of pvc-mix_snap-1_00000 from vg/pvc-mix_00000; commands %v", snapSpelling, fx.CommandLines())
		}

		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-mix_snap-1_00000",
			storage.FakeResponse{Stdout: []byte("pvc-mix_snap-1_00000\n")})

		if err := cli.Delete(t.Context(), &got); err != nil {
			t.Fatalf("delete snapshot: %v", err)
		}

		if _, err := snapshots.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err != nil {
			t.Fatalf("snapshot %s delete: %v", snapSpelling, err)
		}

		if !slices.ContainsFunc(fx.CommandLines(), func(c string) bool {
			return strings.HasPrefix(c, "lvremove ") && strings.Contains(c, "vg/pvc-mix_snap-1_00000")
		}) {
			t.Errorf("snapshot row %s: no lvremove of vg/pvc-mix_snap-1_00000; commands %v", snapSpelling, fx.CommandLines())
		}
	}
}

// An old mixed-case replica is torn down under the name it was laid down
// under, by the finalizer delete and by a toggle-disk cancel alike.
func TestAnOldMixedCaseReplicaIsTornDownUnderItsCRName(t *testing.T) {
	t.Parallel()

	for name, teardown := range map[string]func(*ResourceReconciler, *blockstoriov1alpha1.Resource) error{
		"delete": func(r *ResourceReconciler, res *blockstoriov1alpha1.Resource) error {
			_, err := r.handleDelete(t.Context(), res)

			return err
		},
		"toggle-disk cancel": func(r *ResourceReconciler, res *blockstoriov1alpha1.Resource) error {
			_, err := r.handleToggleDiskCancel(t.Context(), res, logr.Discard())

			return err
		},
	} {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				LayerStack:        []string{"STORAGE"},
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 4 * 1024}},
			},
		}
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-a", Finalizers: []string{SatelliteResourceFinalizer}},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "Pvc-Mix",
				NodeName:               "node-a",
				StoragePool:            "thin1",
			},
		}

		fx := storage.NewFakeExec()
		r := newPassphraseTestReconciler(t, rd, res)
		r.Config.Exec = fx
		r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
			Providers: map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
			NodeName:  "node-a",
		})

		if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
			t.Fatalf("%s: runApply: %v", name, err)
		}

		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-mix_00000",
			storage.FakeResponse{Stdout: []byte("pvc-mix_00000\n")})

		var live blockstoriov1alpha1.Resource
		if err := r.Get(t.Context(), client.ObjectKey{Name: res.Name}, &live); err != nil {
			t.Fatalf("%s: get replica: %v", name, err)
		}

		if err := teardown(r, &live); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if !slices.ContainsFunc(fx.CommandLines(), func(c string) bool {
			return strings.HasPrefix(c, "lvremove ") && strings.Contains(c, "vg/pvc-mix_00000")
		}) {
			t.Errorf("%s: no lvremove of vg/pvc-mix_00000; commands %v", name, fx.CommandLines())
		}
	}
}

// A snapshot row that keeps the definition as Pvc-Mix suspends, flushes
// and resumes the resource under the CR name it runs under.
func TestAnOldMixedCaseSnapshotRowDrivesTheResourceByItsCRName(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack:        []string{"STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 4 * 1024}},
		},
	}
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-a"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-mix", NodeName: "node-a", StoragePool: "thin1"},
	}

	fx := storage.NewFakeExec()
	apply := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		NodeName:  "node-a",
	})

	r := newPassphraseTestReconciler(t, rd, res)
	r.Config.Exec = fx
	r.Config.Apply = apply

	if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-mix_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-mix_00000|4096\n")})

	spec := blockstoriov1alpha1.SnapshotSpec{
		ResourceDefinitionName: "Pvc-Mix",
		SnapshotName:           "snap-1",
		Nodes:                  []string{"node-a"},
		VolumeDefinitions:      []blockstoriov1alpha1.SnapshotVolumeRef{{VolumeNumber: 0}},
	}

	for _, phase := range []struct {
		name   string
		spec   func(*blockstoriov1alpha1.SnapshotSpec)
		status blockstoriov1alpha1.SnapshotPerNodeStatus
		want   string
	}{
		{"suspend", func(s *blockstoriov1alpha1.SnapshotSpec) { s.SuspendIO = true }, blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "node-a"}, "drbdadm suspend-io pvc-mix"},
		{"flush", func(s *blockstoriov1alpha1.SnapshotSpec) { s.SuspendIO, s.TakeSnapshot = true, true }, blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "node-a", SuspendIOAcked: true}, "blockdev --flushbufs /dev/vg/pvc-mix_00000"},
		{"resume", func(*blockstoriov1alpha1.SnapshotSpec) {}, blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "node-a", Ready: true, CreateTimestamp: 1, SuspendIOAcked: true}, "drbdadm resume-io pvc-mix"},
	} {
		s := spec
		phase.spec(&s)

		snap := &blockstoriov1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.snap-1", Finalizers: []string{SatelliteSnapshotFinalizer}},
			Spec:       s,
			Status:     blockstoriov1alpha1.SnapshotStatus{NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{phase.status}},
		}

		cli := fake.NewClientBuilder().WithScheme(r.Client.Scheme()).
			WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build()

		snapshots := &SnapshotReconciler{Client: cli, Config: Config{NodeName: "node-a", Apply: apply, Exec: fx}}
		if _, err := snapshots.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err != nil {
			t.Fatalf("%s: %v", phase.name, err)
		}

		if !slices.Contains(fx.CommandLines(), phase.want) {
			t.Errorf("%s: missing %q; commands %v", phase.name, phase.want, fx.CommandLines())
		}
	}
}

// The orphan sweepers count an old mixed-case replica as owning the DRBD
// resource and the volumes it runs under, which carry the CR name.
func TestTheSweepersOwnAnOldMixedCaseReplicaByItsCRName(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 4 * 1024}},
		},
	}
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-a"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "node-a"},
	}

	cli := newPassphraseTestReconciler(t, rd, res).Client

	volumes, err := (&StorageOrphanSweeperRunnable{Client: cli, NodeName: "node-a"}).listOwnedVolumeRefs(t.Context())
	if err != nil {
		t.Fatalf("owned volumes: %v", err)
	}

	if _, ok := volumes[ownedKey("pvc-mix", 0)]; !ok {
		t.Errorf("owned volumes %v; want pvc-mix volume 0", volumes)
	}

	resources, err := (&OrphanSweeperRunnable{Client: cli, NodeName: "node-a"}).listOwnedResourceNames(t.Context())
	if err != nil {
		t.Fatalf("owned resources: %v", err)
	}

	if _, ok := resources["pvc-mix"]; !ok {
		t.Errorf("owned resources %v; want pvc-mix", resources)
	}
}
