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

// A row written before the store kept CR names in spec carries the
// registered spelling, Pvc-Reg on Node-Reg. Asked in a third spelling, the
// listings find it through the registered one.
func TestARowStoredUnderTheRegisteredSpellingIsFound(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	rd := crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Pvc-Reg")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "Pvc-Reg")

	node := crdv1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Node-Reg")}}
	k8s.SetOriginalName(&node.ObjectMeta, "Node-Reg")

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&rd, &node,
		&crdv1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-reg.node-reg"},
			Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Reg", NodeName: "Node-Reg"},
		},
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-reg.snap-1"},
			Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "Pvc-Reg", SnapshotName: "snap-1"},
		},
	).Build()

	ctx := context.Background()
	st := k8s.New(cli)

	replicas, err := st.Resources().ListByDefinition(ctx, "PVC-REG")
	if err != nil || len(replicas) != 1 {
		t.Errorf("replicas of PVC-REG = %v, %v; want the one stored under Pvc-Reg", replicas, err)
	}

	onNode, err := st.Resources().ListByNode(ctx, "NODE-REG")
	if err != nil || len(onNode) != 1 {
		t.Errorf("replicas on NODE-REG = %v, %v; want the one stored on Node-Reg", onNode, err)
	}

	snaps, err := st.Snapshots().ListByDefinition(ctx, "PVC-REG")
	if err != nil || len(snaps) != 1 {
		t.Errorf("snapshots of PVC-REG = %v, %v; want the one stored under Pvc-Reg", snaps, err)
	}
}
