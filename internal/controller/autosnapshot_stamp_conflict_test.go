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

package controller

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestAutoSnapshotStampSurvivesRDUpdateConflict pins the bookkeeping
// half of the auto-snapshot tick against the reconciler race observed
// in the L-integration stack (TestGroupG/AutoSnapshotPeriodicTick):
// the RD object the runnable stamps comes from Tick's List, and the
// Snapshot create wakes reconcilers that update the RD concurrently —
// so the stamp's bare Update hits a 409 Conflict. Pre-fix the conflict
// aborted the stamp (Tick swallows per-RD errors), leaving NextAutoId
// and the last-at annotation unset: the next tick re-derived the SAME
// id every interval and only the createAutoSnapshot AlreadyExists
// short-circuit kept the loop from duplicating snapshots. The stamp
// must instead re-read the RD fresh and retry the conflict away.
func TestAutoSnapshotStampSurvivesRDUpdateConflict(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-stamp-conflict"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{PropAutoSnapshotRunEvery: "1"},
		},
	}

	// Model the racing reconciler: the FIRST RD Update lands on a
	// stale resourceVersion and 409s; the retry (against a fresh
	// read) succeeds.
	conflicted := false
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				_, isRD := obj.(*blockstoriov1alpha1.ResourceDefinition)
				if isRD && !conflicted {
					conflicted = true

					return apierrors.NewConflict(
						schema.GroupResource{Group: "blockstor.cozystack.io", Resource: "resourcedefinitions"},
						obj.GetName(),
						nil,
					)
				}

				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()

	runnable := &AutoSnapshotRunnable{Client: cli}

	if err := runnable.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if !conflicted {
		t.Fatalf("interceptor never fired — the test no longer models the stamp Update")
	}

	var updated blockstoriov1alpha1.ResourceDefinition
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "rd-stamp-conflict"}, &updated); err != nil {
		t.Fatalf("re-fetch RD: %v", err)
	}

	if got := updated.Spec.Props[PropAutoSnapshotNextID]; got != "2" {
		t.Errorf("NextAutoId after conflicted stamp: got %q, want \"2\"", got)
	}

	if updated.Annotations[AnnotationAutoSnapshotLastAt] == "" {
		t.Errorf("AnnotationAutoSnapshotLastAt unset after conflicted stamp; the tick would re-fire and re-derive the same id")
	}
}
