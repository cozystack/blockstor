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

package controller_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The prune deleted by age alone. A definition restored from an
// auto-snapshot lost its source as soon as enough newer ones piled up, and
// its next replica came up blank. The prune keeps an auto-snapshot a
// definition still reads from.
func TestAutoSnapshotPruneKeepsTheSourceOfARestore(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-keep-src", 15, "2")

	restored := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-restored"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{store.RestoreFromSnapshotProp: "pvc-keep-src:auto-snap-00001"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd, restored).Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	for i := range 4 {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}

		clk.advance(15*time.Minute + time.Second)
	}

	kept := false

	for _, snap := range listAutoSnapshotsByRD(t, cli, "pvc-keep-src") {
		if snap.Spec.SnapshotName == "auto-snap-00001" {
			kept = true
		}
	}

	if !kept {
		t.Fatal("the prune deleted the auto-snapshot a definition was restored from")
	}
}

// The production prune judges the restore through the store
// (store.ReleaseRestoreSource): a restore that is not finished keeps its
// auto-snapshot.
func TestAutoSnapshotPruneWithAStoreKeepsTheSourceOfAnUnfinishedRestore(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-keep-st", 15, "2")
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(context.Background(), &apiv1.ResourceDefinition{
		Name:  "pvc-restored-st",
		Props: map[string]string{store.RestoreFromSnapshotProp: "pvc-keep-st:auto-snap-00001"},
	}); err != nil {
		t.Fatalf("seed restored definition: %v", err)
	}

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk, Store: st}

	for i := range 4 {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}

		clk.advance(15*time.Minute + time.Second)
	}

	for _, snap := range listAutoSnapshotsByRD(t, cli, "pvc-keep-st") {
		if snap.Spec.SnapshotName == "auto-snap-00001" {
			return
		}
	}

	t.Fatal("the prune deleted the auto-snapshot an unfinished restore reads from")
}
