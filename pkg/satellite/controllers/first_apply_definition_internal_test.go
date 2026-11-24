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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

func blockSizeRD(props map[string]string) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-first"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 4 * 1024, Props: props},
			},
		},
	}
}

func firstApplyReplica() *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-first.node-a"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-first",
			NodeName:               "node-a",
			StoragePool:            "pool-a",
		},
	}
}

// staleCacheReconciler serves a definition without the block size from
// its cache and the API server's copy, which has it, from its reader:
// the state right after the controller wrote the size and then gave the
// replica its identity.
func staleCacheReconciler(t *testing.T) *ResourceReconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 to scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor to scheme: %v", err)
	}

	r := newPassphraseTestReconciler(t, blockSizeRD(nil), firstApplyReplica())
	r.Config.APIReader = fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(blockSizeRD(map[string]string{blocksize.PropBlockSize: "4096"})).
		Build()

	return r
}

// A replica that was never applied takes the definition from the API
// server, so the first attach renders the block size the controller
// wrote even when the cached definition predates it. drbdsetup never
// changes the block size of an attached device, so a size missed here
// stays missed until the next detach.
func TestFirstApplyReadsTheDefinitionPastTheCache(t *testing.T) {
	t.Parallel()

	r := staleCacheReconciler(t)
	rd := blockSizeRD(nil)
	res := firstApplyReplica()

	r.refreshDefinitionForFirstApply(t.Context(), res, rd, logr.Discard())

	desired, err := r.buildDesiredFromCRD(t.Context(), res, rd, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	vols := desired.GetVolumes()
	if len(vols) != 1 {
		t.Fatalf("volumes: %d, want 1", len(vols))
	}

	if got := vols[0].GetDiskOptions()["block-size"]; got != "4096" {
		t.Fatalf("first apply renders block-size %q, want 4096 from the API server", got)
	}
}

// A replica already applied keeps the cached definition: the read is
// for the first attach only.
func TestAppliedReplicaKeepsTheCachedDefinition(t *testing.T) {
	t.Parallel()

	r := staleCacheReconciler(t)
	rd := blockSizeRD(nil)
	res := firstApplyReplica()
	res.Annotations = map[string]string{blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0"}

	r.refreshDefinitionForFirstApply(t.Context(), res, rd, logr.Discard())

	if _, ok := rd.Spec.VolumeDefinitions[0].Props[blocksize.PropBlockSize]; ok {
		t.Fatalf("an applied replica re-read the definition: %v", rd.Spec.VolumeDefinitions[0].Props)
	}
}

// TestRunApplyLaysTheFirstApplyDownFromTheAPIServer drives the apply
// itself, so the refresh is pinned where it is called and not only as a
// helper: the cached definition is a size behind the API server's, and
// the first apply must carve the volume at the API server's size. A
// runApply that stopped refreshing would carve the cached one, and on a
// DRBD stack render the cached definition's block size with it.
func TestRunApplyLaysTheFirstApplyDownFromTheAPIServer(t *testing.T) {
	t.Parallel()

	stale := blockSizeRD(nil)
	stale.Spec.LayerStack = []string{"STORAGE"}

	fresh := blockSizeRD(map[string]string{blocksize.PropBlockSize: "4096"})
	fresh.Spec.LayerStack = []string{"STORAGE"}
	fresh.Spec.VolumeDefinitions[0].SizeKib = 8 * 1024

	res := firstApplyReplica()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 to scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor to scheme: %v", err)
	}

	fx := storage.NewFakeExec()
	r := newPassphraseTestReconciler(t, stale, res)
	r.Config.Exec = fx
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		NodeName:  "node-a",
	})
	r.Config.APIReader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(fresh, firstApplyReplica()).Build()

	if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	creates := slices.DeleteFunc(fx.CommandLines(), func(c string) bool { return !strings.HasPrefix(c, "lvcreate ") })
	if len(creates) != 1 || !strings.Contains(creates[0], "--virtualsize 8192k") {
		t.Fatalf("first apply carved %v, want one volume at the API server's 8MiB", creates)
	}
}
