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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// TestProbeKubectlAppliedSourceListByDefinition reproduces the e2e
// clone.sh / snapshot-restore-cross-node.sh source shape: a Resource
// applied via `kubectl apply` carries the pool ONLY in Spec.Props
// (raw "StorPoolName"), with NO typed Spec.StoragePool and NO
// blockstor.io/resource-definition label (the store's Create() sets
// both, but a raw apply does not). The restore handler's
// storPoolsByNodeFromSourceRD reads the wire .Props["StorPoolName"];
// this probe confirms ListByDefinition surfaces it via the label-less
// fallback path so the restored replicas inherit the source pool
// instead of being stamped pool-less (satellite "unknown storage
// pool" -> never converges).
func TestProbeKubectlAppliedSourceListByDefinition(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()

	// Raw apply: NO label, NO typed StoragePool, pool only in Props.
	crd := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: k8s.Name("probe-src.node-a"),
		},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "probe-src",
			NodeName:               "node-a",
			Props:                  map[string]string{"StorPoolName": "zfs-thin"},
		},
	}

	if err := fixture.client.Create(ctx, crd); err != nil {
		t.Fatalf("raw create Resource CRD: %v", err)
	}

	st := k8s.New(fixture.client)

	list, err := st.Resources().ListByDefinition(ctx, "probe-src")
	if err != nil {
		t.Fatalf("ListByDefinition: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("ListByDefinition returned %d resources, want 1 (the "+
			"label-less fallback scan must find a kubectl-applied source)", len(list))
	}

	if got := list[0].Props["StorPoolName"]; got != "zfs-thin" {
		t.Fatalf("wire Props[StorPoolName] = %q, want %q "+
			"(the restore handler reads exactly this to seed the clone "+
			"replica's pool; empty here => satellite 'unknown storage pool')", got, "zfs-thin")
	}
}

// TestListByDefinitionMixedLabeledAndUnlabeled is the Bug 038
// root-cause regression: a source RD with UNLABELED diskful replicas
// (kubectl-applied source) AND a LABELED auto-tiebreaker witness (the
// controller stamps it through the store, which sets the label). The
// old ListByDefinition trusted the label-selector and skipped its
// full-scan fallback whenever the selector returned ANY item — so the
// labeled witness alone satisfied the "non-empty" check and the
// unlabeled diskful replicas vanished from the result. The
// snapshot-restore handler then resolved an EMPTY source pool and the
// clone replicas were stamped pool-less (satellite "unknown storage
// pool"). ListByDefinition must return EVERY replica regardless of the
// label, so the diskful source pool is always visible.
func TestListByDefinitionMixedLabeledAndUnlabeled(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()
	st := k8s.New(fixture.client)

	// Two UNLABELED diskful replicas (kubectl-applied source shape:
	// pool in Props, no typed StoragePool, no label).
	for _, node := range []string{"node-a", "node-b"} {
		crd := &crdv1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("mixed-src." + node)},
			Spec: crdv1alpha1.ResourceSpec{
				ResourceDefinitionName: "mixed-src",
				NodeName:               node,
				Props:                  map[string]string{"StorPoolName": "zfs-thin"},
			},
		}
		if err := fixture.client.Create(ctx, crd); err != nil {
			t.Fatalf("raw create diskful %s: %v", node, err)
		}
	}

	// One LABELED diskless tiebreaker witness, created through the
	// store the way the controller's ensureTiebreaker does (Create
	// sets the LabelResourceDefinition label).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "mixed-src",
		NodeName: "node-c",
		Flags:    []string{"DISKLESS", "TIE_BREAKER"},
	}); err != nil {
		t.Fatalf("store create tiebreaker: %v", err)
	}

	list, err := st.Resources().ListByDefinition(ctx, "mixed-src")
	if err != nil {
		t.Fatalf("ListByDefinition: %v", err)
	}

	if len(list) != 3 {
		t.Fatalf("ListByDefinition returned %d replicas, want 3 "+
			"(2 unlabeled diskful + 1 labeled witness); a labeled witness "+
			"must NOT hide the unlabeled diskful source replicas", len(list))
	}

	diskful := 0

	for i := range list {
		hasFlag := false
		for _, f := range list[i].Flags {
			if f == "DISKLESS" {
				hasFlag = true
			}
		}

		if hasFlag {
			continue
		}

		diskful++

		if got := list[i].Props["StorPoolName"]; got != "zfs-thin" {
			t.Errorf("diskful replica %s: pool %q, want zfs-thin", list[i].NodeName, got)
		}
	}

	if diskful != 2 {
		t.Fatalf("diskful replicas visible: got %d, want 2 (the source pool "+
			"resolution depends on every diskful replica being listed)", diskful)
	}
}
