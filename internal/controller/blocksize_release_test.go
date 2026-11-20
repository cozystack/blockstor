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
