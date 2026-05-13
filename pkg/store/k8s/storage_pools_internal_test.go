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

package k8s

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestCrdToWireStoragePoolEmptyPoolNameFallback: when a CRD has an
// empty Spec.PoolName (older CRDs that pre-date the field, or
// hand-edited via kubectl), the converter must recover the pool
// name from the label set by Create. Without this fallback,
// /v1/view/storage-pools would surface entries with empty
// StoragePoolName — confusing the autoplacer's pool registry.
//
// Internal test (package k8s) so we can construct a CRD directly,
// bypassing wireToCRDStoragePool's normalisation.
func TestCrdToWireStoragePoolEmptyPoolNameFallback(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "thin.n1",
			Labels: map[string]string{
				LabelPoolName: "thin",
				LabelNodeName: "n1",
			},
		},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName:     "n1",
			ProviderKind: "LVM_THIN",
			// PoolName intentionally empty.
		},
	}

	got := crdToWireStoragePool(crd)
	if got.StoragePoolName != "thin" {
		t.Errorf("StoragePoolName: got %q, want \"thin\" (label fallback failed)",
			got.StoragePoolName)
	}
}

// TestCrdToWireStoragePoolEmptyPoolNameNoLabel: CRD without the
// label and without Spec.PoolName ends up with an empty name —
// the converter shouldn't crash, but the result is operator-
// visible empty (a sign of corrupt CRD state).
func TestCrdToWireStoragePoolEmptyPoolNameNoLabel(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "thin.n1"},
		Spec:       crdv1alpha1.StoragePoolSpec{NodeName: "n1"},
	}

	got := crdToWireStoragePool(crd)
	// No panic, empty name surfaces.
	if got.StoragePoolName != "" {
		t.Errorf("got %q, want empty (no label and no spec.PoolName)", got.StoragePoolName)
	}

	if got.NodeName != "n1" {
		t.Errorf("NodeName: got %q, want n1", got.NodeName)
	}
}

// TestCRDNameUsesPoolDotNodeOrder pins the canonical name encoding:
// `<pool>.<node>`. Matches the CRD-level CEL rule on the StoragePool
// type (`metadata.name == spec.poolName + '.' + spec.nodeName`) and
// the cluster-wide naming convention every other node-bound CRD in
// the project follows. Flipping the order silently breaks the CEL
// rule on Create — k8s rejects the write with a 422 and the wire-
// side error is hard to trace back to the wrong helper.
func TestCRDNameUsesPoolDotNodeOrder(t *testing.T) {
	t.Parallel()

	got := crdName("w1", "zfs-thin")
	want := "zfs-thin.w1"

	if got != want {
		t.Errorf("crdName(\"w1\", \"zfs-thin\"): got %q, want %q (must be <pool>.<node>)",
			got, want)
	}
}

// TestWireToCRDStoragePoolProducesCanonicalName pins the wire→CRD
// converter to the canonical `<pool>.<node>` shape — same rule the
// apiserver CEL validation enforces. Without this test a regression
// in `crdName` ordering would only surface against a real cluster
// (the InMemory store keys on the wire tuple and would still work).
func TestWireToCRDStoragePoolProducesCanonicalName(t *testing.T) {
	t.Parallel()

	in := &apiv1.StoragePool{NodeName: "w1", StoragePoolName: "zfs-thin"}
	crd := wireToCRDStoragePool(in)

	want := "zfs-thin.w1"
	if crd.Name != want {
		t.Errorf("CRD metadata.name: got %q, want %q", crd.Name, want)
	}

	if crd.Spec.PoolName != "zfs-thin" || crd.Spec.NodeName != "w1" {
		t.Errorf("spec: got (pool=%q, node=%q), want (zfs-thin, w1)",
			crd.Spec.PoolName, crd.Spec.NodeName)
	}

	// CEL pin: the rule on the CRD is `self.metadata.name ==
	// self.spec.poolName + '.' + self.spec.nodeName`. Replicate it
	// here so a future converter rewrite that drifts from the rule
	// fails this test rather than a much-harder-to-trace apiserver
	// 422 on Create.
	if crd.Name != crd.Spec.PoolName+"."+crd.Spec.NodeName {
		t.Errorf("CEL invariant broken: name=%q, expected %q",
			crd.Name, crd.Spec.PoolName+"."+crd.Spec.NodeName)
	}
}
