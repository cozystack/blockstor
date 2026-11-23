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

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestSnapshotDeleteFailureIsRecorded: an online satellite whose
// backend refuses the delete must say so on the Snapshot. LINSTOR
// answers the snapshot delete with the error its satellite returned;
// with the failure only in the satellite log, the REST call had
// nothing to answer with and kept waiting on a node that is alive.
func TestSnapshotDeleteFailureIsRecorded(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)
	now := metav1.Now()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "rdfail.snap",
			Finalizers:        []string{controllers.SatelliteSnapshotFinalizer},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "rdfail",
			SnapshotName:           "snap",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/rdfail_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/rdfail_snap_00000",
		storage.FakeResponse{Stdout: []byte("rdfail_snap_00000\n")})
	fx.Expect("lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/rdfail_snap_00000",
		storage.FakeResponse{Err: errors.New("logical volume vg/rdfail_snap_00000 in use")})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    seedThinResource(t, fx, "rdfail", "thin1"),
			Exec:     fx,
		},
	}

	_, _ = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rdfail.snap"},
	})

	got := getSnapU32(t, cli, "rdfail.snap")
	if got == nil {
		t.Fatal("snapshot reaped although its delete failed")
	}

	if len(got.Status.DeleteFailures) != 1 {
		t.Fatalf("delete failures: got %+v, want one for n1", got.Status.DeleteFailures)
	}

	failure := got.Status.DeleteFailures[0]
	if failure.NodeName != "n1" || !strings.Contains(failure.Message, "in use") {
		t.Errorf("delete failure: got %+v, want n1 with the backend's error", failure)
	}
}
