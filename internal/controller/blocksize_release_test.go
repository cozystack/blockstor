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
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestLastReplicaGoneReleasesSize pins the release: with no replica left
// the definition loses both properties, as LINSTOR removes them with its
// last resource; with one left it keeps them.
func TestLastReplicaGoneReleasesSize(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		replicas []client.Object
		want     bool
	}{
		{name: "none-left", want: false},
		{name: "one-left", replicas: []client.Object{running(bsReplica("n2"))}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			f := newBSFixture(map[string]string{
				blocksize.PropBlockSize: "4096",
				blocksize.PropFreeze:    "True",
				"Other/Prop":            "kept",
			})

			cli := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithObjects(append([]client.Object{f.rd}, tc.replicas...)...).
				Build()

			rec := &controllerpkg.BlockSizeReleaseReconciler{Client: cli}

			_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: bsRD}})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			var rd blockstoriov1alpha1.ResourceDefinition

			err = cli.Get(ctx, client.ObjectKey{Name: bsRD}, &rd)
			if err != nil {
				t.Fatalf("get rd: %v", err)
			}

			props := rd.Spec.VolumeDefinitions[0].Props
			_, hasSize := props[blocksize.PropBlockSize]

			if hasSize != tc.want || blocksize.Frozen(props) != tc.want {
				t.Fatalf("props %v: want block size kept = %v", props, tc.want)
			}

			if props["Other/Prop"] != "kept" {
				t.Fatalf("an unrelated prop was dropped: %v", props)
			}
		})
	}
}

// TestReleaseHoldsTheDecisionLock: the release holds the lock the block
// size decision for a new replica takes from listing the replicas until
// its patch. A replica decided in between under the size it kept writes
// nothing, so no conflict stops the patch, and the release used to take
// the size and the stamp from under a replica that had just come up at
// it.
func TestReleaseHoldsTheDecisionLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"})

	var mu sync.Mutex

	unlockedAt := []string{}

	checkHeld := func(step string) {
		if mu.TryLock() {
			unlockedAt = append(unlockedAt, step)
			mu.Unlock()
		}
	}

	base := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(f.rd).Build()
	cli := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			checkHeld("list")

			return c.List(ctx, list, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			checkHeld("get")

			return c.Get(ctx, key, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			checkHeld("patch")

			return c.Patch(ctx, obj, patch, opts...)
		},
	})

	rec := &controllerpkg.BlockSizeReleaseReconciler{
		Client: cli,
		Lock:   func(string) *sync.Mutex { return &mu },
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: bsRD}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(unlockedAt) != 0 {
		t.Fatalf("the decision lock was free at %v", unlockedAt)
	}
}

// TestReleaseKeepsARestoringDefinition: a definition still being restored
// keeps the size its snapshot gave it when a replica placed meanwhile is
// deleted; the data it restores was written at that size.
func TestReleaseKeepsARestoringDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"})
	f.rd.Spec.Props = map[string]string{store.RestoreFromSnapshotProp: "src:snap"}

	cli := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(f.rd).Build()

	_, err := (&controllerpkg.BlockSizeReleaseReconciler{Client: cli}).
		Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: bsRD}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if props := volumeProps(t, cli, bsRD); props[blocksize.PropBlockSize] != "4096" {
		t.Fatalf("a restoring definition lost its size: %v", props)
	}
}

// TestSweepReleasesWhatTheEventsMissed: the last replica of a frozen
// definition went while the controller was down, so no deletion event
// reached the release. The sweep releases it the second time it finds it
// empty, and leaves alone a size nobody froze, which may be an operator's
// set before the first replica.
func TestSweepReleasesWhatTheEventsMissed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	frozen := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"}).rd
	sized := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096"}).rd
	sized.Name = "pvc-sized"
	placed := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"}).rd
	placed.Name = "pvc-placed"
	replica := running(bsReplica("n2"))
	replica.Name = "pvc-placed.n2"
	replica.Spec.ResourceDefinitionName = "pvc-placed"

	cli := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(frozen, sized, placed, replica).Build()
	rec := &controllerpkg.BlockSizeReleaseReconciler{Client: cli}

	seen, err := rec.Sweep(ctx, nil)
	if err != nil {
		t.Fatalf("first sweep: %v", err)
	}

	if !blocksize.Frozen(volumeProps(t, cli, bsRD)) {
		t.Fatal("the first sweep released a definition it had not seen empty before")
	}

	_, err = rec.Sweep(ctx, seen)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}

	if props := volumeProps(t, cli, bsRD); len(props) != 0 {
		t.Fatalf("the frozen empty definition kept %v", props)
	}

	if props := volumeProps(t, cli, "pvc-sized"); props[blocksize.PropBlockSize] != "4096" {
		t.Fatalf("an unfrozen size was released: %v", props)
	}

	if props := volumeProps(t, cli, "pvc-placed"); !blocksize.Frozen(props) {
		t.Fatalf("a placed definition was released: %v", props)
	}
}

func volumeProps(t *testing.T, cli client.Client, name string) map[string]string {
	t.Helper()

	var rd blockstoriov1alpha1.ResourceDefinition

	err := cli.Get(context.Background(), client.ObjectKey{Name: name}, &rd)
	if err != nil {
		t.Fatalf("get rd %s: %v", name, err)
	}

	return rd.Spec.VolumeDefinitions[0].Props
}
