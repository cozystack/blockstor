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

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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

var errBlockSizeReleaseBroken = errors.New("apiserver refused the write")

// releaseClient fails every definition patch with what fail returns,
// and lets the rest through.
func releaseClient(t *testing.T, fail func(name string) error, objs ...client.Object) client.Client {
	t.Helper()

	base := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()

	return interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if err := fail(obj.GetName()); err != nil {
				return err
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})
}

func releaseRaced(name string) error {
	return apierrors.NewConflict(schema.GroupResource{Group: "blockstor.org", Resource: "resourcedefinitions"}, name,
		errors.New("the object has been modified; please apply your changes to the latest version and try again"))
}

// TestReleaseRetriesAPatchThatLostARace: a release whose patch lost an
// optimistic-concurrency race is retried, not reported. Reported,
// controller-runtime logged it as "Reconciler error" at ERROR on every
// burst of volume deletions, and the next pass always went through.
func TestReleaseRetriesAPatchThatLostARace(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"})
	cli := releaseClient(t, releaseRaced, f.rd)

	result, err := (&controllerpkg.BlockSizeReleaseReconciler{Client: cli}).
		Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: bsRD}})
	if err != nil {
		t.Fatalf("Reconcile: %v, want the conflict retried without an error", err)
	}

	if !result.Requeue { //nolint:staticcheck // the rate-limited retry an error would get
		t.Fatalf("result %+v, want a requeue", result)
	}
}

// TestReleaseReportsAPatchThatFailed: any other failed patch is still a
// failure.
func TestReleaseReportsAPatchThatFailed(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"})
	cli := releaseClient(t, func(string) error { return errBlockSizeReleaseBroken }, f.rd)

	_, err := (&controllerpkg.BlockSizeReleaseReconciler{Client: cli}).
		Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: bsRD}})
	if !errors.Is(err, errBlockSizeReleaseBroken) {
		t.Fatalf("Reconcile: %v, want the patch error", err)
	}
}

// TestSweepGoesOnPastALostRace: a definition whose release lost a race
// does not end the sweep with "block size sweep failed" at ERROR, and
// the definitions after it are still released.
func TestSweepGoesOnPastALostRace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	raced := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"}).rd
	raced.Name = "pvc-a-raced"
	later := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"}).rd
	later.Name = "pvc-b-later"

	cli := releaseClient(t, func(name string) error {
		if name == raced.Name {
			return releaseRaced(name)
		}

		return nil
	}, raced, later)
	rec := &controllerpkg.BlockSizeReleaseReconciler{Client: cli}

	seen, err := rec.Sweep(ctx, nil)
	if err != nil {
		t.Fatalf("first sweep: %v", err)
	}

	seen, err = rec.Sweep(ctx, seen)
	if err != nil {
		t.Fatalf("second sweep: %v, want the lost race retried later", err)
	}

	if props := volumeProps(t, cli, later.Name); len(props) != 0 {
		t.Fatalf("a definition after the lost race kept %v", props)
	}

	if !seen[raced.Name] {
		t.Fatalf("the raced definition %q is no longer a candidate: %v", raced.Name, seen)
	}
}

// raceLevels records the verbosity of every Info line.
type raceLevels struct {
	mu   sync.Mutex
	info []int
}

func (l *raceLevels) last() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.info[len(l.info)-1]
}

type raceSink struct{ levels *raceLevels }

func (raceSink) Init(logr.RuntimeInfo) {}
func (raceSink) Enabled(int) bool      { return true }
func (s raceSink) Info(level int, _ string, _ ...any) {
	s.levels.mu.Lock()
	defer s.levels.mu.Unlock()

	s.levels.info = append(s.levels.info, level)
}
func (raceSink) Error(error, string, ...any)      {}
func (s raceSink) WithValues(...any) logr.LogSink { return s }
func (s raceSink) WithName(string) logr.LogSink   { return s }

// TestReleaseStarvedByAWriterIsLogged: a release that keeps losing its
// race to a writer rewriting the definition in a loop starves forever,
// and with the conflict out of the error log and the reconcile-error
// metric nothing showed it. The tenth lost race in a row on the same
// definition is logged at the default verbosity; a pass that does not
// conflict starts the count again.
func TestReleaseStarvedByAWriterIsLogged(t *testing.T) {
	t.Parallel()

	var levels raceLevels

	ctx := logf.IntoContext(context.Background(), logr.New(raceSink{levels: &levels}))

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"})

	var (
		mu    sync.Mutex
		raced = true
	)

	cli := releaseClient(t, func(name string) error {
		mu.Lock()
		defer mu.Unlock()

		if raced {
			return releaseRaced(name)
		}

		return errBlockSizeReleaseBroken
	}, f.rd)
	rec := &controllerpkg.BlockSizeReleaseReconciler{Client: cli}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: bsRD}}

	race := func(want int) {
		t.Helper()

		_, err := rec.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile: %v, want the conflict retried", err)
		}

		if got := levels.last(); got != want {
			t.Fatalf("lost race logged at V(%d), want V(%d)", got, want)
		}
	}

	for range 9 {
		race(1)
	}

	race(0)
	race(0)

	mu.Lock()
	raced = false
	mu.Unlock()

	_, err := rec.Reconcile(ctx, req)
	if !errors.Is(err, errBlockSizeReleaseBroken) {
		t.Fatalf("Reconcile: %v, want the patch error", err)
	}

	mu.Lock()
	raced = true
	mu.Unlock()

	race(1)
}

// TestSweepForgetsTheRacesOfADeletedDefinition: a lost race in the
// sweep is retried by the next sweep, not by a requeue, so a definition
// deleted after its sweeps kept losing never got the pass that forgets
// its count. The count then outlived it, and a definition recreated
// under the same name started its first race already at the default
// verbosity.
func TestSweepForgetsTheRacesOfADeletedDefinition(t *testing.T) {
	t.Parallel()

	var levels raceLevels

	ctx := logf.IntoContext(context.Background(), logr.New(raceSink{levels: &levels}))

	rd := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"}).rd
	cli := releaseClient(t, releaseRaced, rd)
	rec := &controllerpkg.BlockSizeReleaseReconciler{Client: cli}

	var (
		seen map[string]bool
		err  error
	)

	for range 10 {
		seen, err = rec.Sweep(ctx, seen)
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
	}

	err = cli.Delete(ctx, rd.DeepCopy())
	if err != nil {
		t.Fatalf("delete rd: %v", err)
	}

	_, err = rec.Sweep(ctx, seen)
	if err != nil {
		t.Fatalf("sweep after the delete: %v", err)
	}

	again := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096", blocksize.PropFreeze: "True"}).rd

	err = cli.Create(ctx, again)
	if err != nil {
		t.Fatalf("recreate rd: %v", err)
	}

	_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: bsRD}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := levels.last(); got != 1 {
		t.Fatalf("the recreated definition's first lost race logged at V(%d), want V(1)", got)
	}
}
