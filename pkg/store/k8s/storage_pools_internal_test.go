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
	"strings"
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

// TestCrdToWireStoragePoolFaultyStampsReports pins the Bug 83 fix:
// when the CRD's Status.PoolMissing is true, the wire-shape value
// MUST carry both `state == "Faulty"` AND a non-empty `Reports`
// slice with an ERROR-severity ApiCallRc whose message identifies
// the missing pool. Without the reports[] entry the Python linstor-
// client's State column derivation (`get_replies_state` over an
// empty reports[]) collapses to "Ok" — defeating the whole reason
// state="Faulty" was added in Bug 50.
//
// We assert structurally (not on the exact wording) so the message
// copy can evolve without breaking the test, but the wording MUST
// contain "missing" + the pool name so audit-log greppers and
// operator dashboards can pattern-match on the failure mode.
func TestCrdToWireStoragePoolFaultyStampsReports(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "zfs-thin.w3"},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName:     "w3",
			PoolName:     "zfs-thin",
			ProviderKind: apiv1.StoragePoolKindZFSThin,
		},
		Status: crdv1alpha1.StoragePoolStatus{
			PoolMissing: true,
		},
	}

	got := crdToWireStoragePool(crd)

	if got.State != "Faulty" {
		t.Errorf("State: got %q, want %q", got.State, "Faulty")
	}

	if len(got.Reports) == 0 {
		t.Fatalf("Reports: got empty slice; want >=1 entry so " +
			"python-linstor renders the State column as Faulty")
	}

	rc := got.Reports[0]

	// MASK_ERROR bit must be set so python-linstor's
	// `get_replies_state` classifies the entry as ERROR-severity.
	if rc.RetCode&apiv1.APICallRcMaskError == 0 {
		t.Errorf("RetCode missing MASK_ERROR bit: %#x", rc.RetCode)
	}

	// Sub-code must be the storpool-configuration-error mirror.
	if rc.RetCode&0xFFFF != apiv1.APICallRcFailStorPoolConfigurationError {
		t.Errorf("RetCode sub-code: got %d, want %d (FAIL_STOR_POOL_CONFIGURATION_ERROR)",
			rc.RetCode&0xFFFF, apiv1.APICallRcFailStorPoolConfigurationError)
	}

	// Message must mention both "missing" and the pool name so the
	// operator can correlate from `linstor sp l` output alone.
	if !strings.Contains(rc.Message, "missing") {
		t.Errorf("Message: %q, want substring %q", rc.Message, "missing")
	}

	if !strings.Contains(rc.Message, "zfs-thin") {
		t.Errorf("Message: %q, want substring %q (pool name)",
			rc.Message, "zfs-thin")
	}

	// ObjRefs must tag the entry with the (node, pool) pair so audit
	// log queries can filter by either axis.
	if rc.ObjRefs["Node"] != "w3" {
		t.Errorf("ObjRefs[Node]: got %q, want %q", rc.ObjRefs["Node"], "w3")
	}

	if rc.ObjRefs["StorPool"] != "zfs-thin" {
		t.Errorf("ObjRefs[StorPool]: got %q, want %q",
			rc.ObjRefs["StorPool"], "zfs-thin")
	}
}

// TestCrdToWireStoragePoolHealthyHasNoReports pins the inverse:
// a pool whose Status.PoolMissing is false MUST emit an empty
// Reports slice. Stamping a no-op entry on a healthy pool would
// trip python-linstor's `get_replies_state` into rendering the
// State column as the first-entry severity rather than Ok.
func TestCrdToWireStoragePoolHealthyHasNoReports(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "zfs-thin.w3"},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName:     "w3",
			PoolName:     "zfs-thin",
			ProviderKind: apiv1.StoragePoolKindZFSThin,
		},
	}

	got := crdToWireStoragePool(crd)

	if got.State != "Ok" {
		t.Errorf("State: got %q, want %q", got.State, "Ok")
	}

	if len(got.Reports) != 0 {
		t.Errorf("Reports: got %d entries, want 0 (healthy pool)",
			len(got.Reports))
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
