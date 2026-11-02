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

package k8s_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A child written straight to the API server under the folded name, the
// way the autosnapshot controller names a snapshot after the definition's
// CR, is found under the definition's registered mixed-case spelling too.
func TestChildrenUnderTheFoldedSpellingOfAMixedCaseDefinition(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()
	st := k8s.New(fixture.client)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "Pvc-Fold"}); err != nil {
		t.Fatalf("create the definition: %v", err)
	}

	if err := fixture.client.Create(ctx, &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-fold.auto-1"},
		Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-fold", SnapshotName: "auto-1"},
	}); err != nil {
		t.Fatalf("raw create the snapshot: %v", err)
	}

	if err := fixture.client.Create(ctx, &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-fold.node-a"},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-fold", NodeName: "node-a"},
	}); err != nil {
		t.Fatalf("raw create the replica: %v", err)
	}

	snaps, err := st.Snapshots().ListByDefinitionUncached(ctx, "Pvc-Fold")
	if err != nil || len(snaps) != 1 {
		t.Errorf("snapshots of Pvc-Fold = %v, %v; want the one written under pvc-fold", snaps, err)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "Pvc-Fold")
	if err != nil || len(replicas) != 1 {
		t.Errorf("replicas of Pvc-Fold = %v, %v; want the one written under pvc-fold", replicas, err)
	}
}

// Where the field selector is not served the listing falls back to a full
// scan, and the scan keeps the same spellings: a mixed-case definition's
// snapshot written under the folded name is found, another definition's is
// not.
func TestSnapshotScanFallbackKeepsEverySpelling(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	rd := crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Pvc-Scan")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "Pvc-Scan")

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&rd,
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-scan.auto-1"},
			Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-scan", SnapshotName: "auto-1"},
		},
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-other.auto-1"},
			Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-other", SnapshotName: "auto-1"},
		},
	).Build()

	snaps, err := k8s.New(cli).Snapshots().ListByDefinition(context.Background(), "Pvc-Scan")
	if err != nil || len(snaps) != 1 || snaps[0].Name != "auto-1" {
		t.Errorf("snapshots of Pvc-Scan = %v, %v; want only the one written under pvc-scan", snaps, err)
	}
}
