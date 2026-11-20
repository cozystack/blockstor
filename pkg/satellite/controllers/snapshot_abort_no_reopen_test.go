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

package controllers_test

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// A suspend order next to a recorded outcome is stale: the controller
// never orders a suspend for a take that ended. The satellite opens no
// window for it, whichever form the record takes, so a pass working
// from a stale view cannot freeze I/O again after an abort.
func TestSnapshotSatelliteOpensNoWindowForAnEndedTake(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*blockstoriov1alpha1.Snapshot){
		"spec abort reason": func(s *blockstoriov1alpha1.Snapshot) {
			s.Spec.AbortReason = "snapshot aborted: deadline"
		},
		"abort annotation": func(s *blockstoriov1alpha1.Snapshot) {
			s.Annotations = map[string]string{blockstoriov1alpha1.AnnotationSnapshotAbortReason: "snapshot aborted: deadline"}
		},
		"FAILED_DISCONNECT": func(s *blockstoriov1alpha1.Snapshot) {
			s.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect}
		},
		"FAILED": func(s *blockstoriov1alpha1.Snapshot) {
			s.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailed}
		},
		"SUCCESSFUL": func(s *blockstoriov1alpha1.Snapshot) {
			s.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagSuccessful}
		},
	}

	for name, mark := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			snap := &blockstoriov1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "pvc-1.snap-1",
					Finalizers: []string{controllers.SnapshotNodeFinalizer("n1")},
				},
				Spec: blockstoriov1alpha1.SnapshotSpec{
					ResourceDefinitionName: "pvc-1",
					SnapshotName:           "snap-1",
					Nodes:                  []string{"n1"},
					SuspendIO:              true,
				},
			}
			mark(snap)

			cli := fake.NewClientBuilder().
				WithScheme(newBug351Scheme(t)).
				WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
				WithObjects(snap).
				Build()

			fx := storage.NewFakeExec()
			fx.Expect("drbdadm suspend-io pvc-1", storage.FakeResponse{})

			reconciler := &controllers.SnapshotReconciler{
				Client: cli,
				Config: controllers.Config{
					NodeName: "n1",
					Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
						Providers: map[string]storage.Provider{},
						Adm:       drbd.NewAdm(fx),
					}),
					Exec: fx,
				},
			}

			for range 2 {
				if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
					NamespacedName: types.NamespacedName{Name: snap.Name},
				}); err != nil {
					t.Fatalf("Reconcile: %v", err)
				}
			}

			for _, line := range fx.CommandLines() {
				if strings.Contains(line, "suspend-io") {
					t.Fatalf("suspended I/O for a take that ended: %v", fx.CommandLines())
				}
			}
		})
	}
}
