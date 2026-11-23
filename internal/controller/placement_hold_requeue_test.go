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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A placement that dies before it removes its hold leaves the
// definition with two diskful replicas and no witness, the shape that
// splits the brain, and nothing writes to the definition again to wake
// the reconciler. The reconcile that skipped the witness comes back by
// the time the hold runs out, whatever else schedules the next one, and
// reads the hold as the list of placements it is (PlacementInProgressUntil).
func TestReconcileComesBackWhenAPlacementHoldRunsOut(t *testing.T) {
	t.Parallel()

	const hold = time.Minute

	stamp := func(d time.Duration) string { return time.Now().Add(d).UTC().Format(time.RFC3339Nano) }

	for _, tc := range []struct {
		name  string
		holds string
	}{
		{name: "one placement", holds: stamp(hold)},
		{name: "a placement next to an expired one", holds: stamp(-hold) + " " + stamp(hold)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reconcileUnderAPlacementHold(t, tc.holds, hold)
		})
	}
}

func reconcileUnderAPlacementHold(t *testing.T, holds string, hold time.Duration) {
	t.Helper()

	ctx := t.Context()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-held", NodeName: n}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-held",
			Annotations: map[string]string{
				apiv1.PlacementInProgressUntilAnnotation: holds,
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	result, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-held"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if _, err := st.Resources().Get(ctx, "pvc-held", "n3"); err == nil {
		t.Fatalf("a witness was added while the placement holds it")
	}

	// A second of slack for a deadline rounded up to the next second.
	if result.RequeueAfter <= 0 || result.RequeueAfter > hold+2*time.Second {
		t.Errorf("RequeueAfter = %v, want a requeue within the %v hold", result.RequeueAfter, hold)
	}
}
