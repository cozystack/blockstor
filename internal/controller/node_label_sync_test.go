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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// TestNodeLabelSyncToAuxProps pins scenario 2.13 from
// tests/scenarios/02-placement.md: a label set on the *Kubernetes*
// Node (e.g. `topology.kubernetes.io/zone=us-east-1`) must surface
// on the matching blockstor `Node` CRD as
// `Spec.Props["Aux/topology.kubernetes.io/zone"] = "us-east-1"`
// within a reconcile loop, so that RG selectors like
// `replicasOnSame: "topology.kubernetes.io/zone=us-east-1"` resolve
// to actual nodes.
//
// Audit (2026-05-13): blockstor currently has the *reverse* shim
// only — pkg/store/k8s/nodes.go.foldTopologyLabels copies labels
// already set on the **blockstor Node CRD** under the
// `topology.blockstor.io/<key>` prefix into `Props["Aux/<key>"]`
// on the wire. There is no reconciler that watches `corev1.Node`
// objects and propagates their labels onto the blockstor Node CRD
// at all — see cmd/controller/main.go (only NodeReconciler +
// NodeHeartbeatReconciler are wired, both watch the CRD, not
// corev1.Node), and the lack of any `For(&corev1.Node{})` /
// `Watches(&corev1.Node{})` anywhere under internal/controller.
//
// Until that reconciler lands, this test is skipped. The
// assertion block below documents the contract the future
// implementation must satisfy: the three sub-cases (create-with-
// label, label-update, label-removal) are the operator-visible
// behaviours called out in scenario 2.13.
func TestNodeLabelSyncToAuxProps(t *testing.T) {
	t.Parallel()

	const (
		nodeName    = "worker-1"
		labelKey    = "topology.kubernetes.io/zone"
		auxKey      = "Aux/topology.kubernetes.io/zone"
		initialZone = "us-east-1"
		updatedZone = "us-west-1"
	)

	scheme := newScheme(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kubeNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeName,
			Labels: map[string]string{labelKey: initialZone},
		},
	}

	blockstorNode := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: "SATELLITE",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeNode, blockstorNode).
		Build()

	rec := &controller.NodeLabelSyncReconciler{Client: cli, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeName}}

	// --- Sub-case 1: label is present on creation -> Aux/ prop set. ---
	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Node{}
	if err := cli.Get(ctx, types.NamespacedName{Name: nodeName}, got); err != nil {
		t.Fatalf("get blockstor Node after initial reconcile: %v", err)
	}

	if got.Spec.Props[auxKey] != initialZone {
		t.Errorf("initial sync: Props[%q] = %q, want %q",
			auxKey, got.Spec.Props[auxKey], initialZone)
	}

	// --- Sub-case 2: label value changes -> Aux/ prop updates. ---
	kubeNode.Labels[labelKey] = updatedZone
	if err := cli.Update(ctx, kubeNode); err != nil {
		t.Fatalf("update k8s Node label: %v", err)
	}

	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatalf("post-update reconcile: %v", err)
	}

	got = &blockstoriov1alpha1.Node{}
	if err := cli.Get(ctx, types.NamespacedName{Name: nodeName}, got); err != nil {
		t.Fatalf("get blockstor Node after label update: %v", err)
	}

	if got.Spec.Props[auxKey] != updatedZone {
		t.Errorf("update sync: Props[%q] = %q, want %q",
			auxKey, got.Spec.Props[auxKey], updatedZone)
	}

	// --- Sub-case 3: label removed -> Aux/ prop removed. ---
	delete(kubeNode.Labels, labelKey)
	if err := cli.Update(ctx, kubeNode); err != nil {
		t.Fatalf("remove k8s Node label: %v", err)
	}

	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatalf("post-removal reconcile: %v", err)
	}

	got = &blockstoriov1alpha1.Node{}
	if err := cli.Get(ctx, types.NamespacedName{Name: nodeName}, got); err != nil {
		t.Fatalf("get blockstor Node after label removal: %v", err)
	}

	if _, present := got.Spec.Props[auxKey]; present {
		t.Errorf("removal sync: Props[%q] still set to %q, want absent",
			auxKey, got.Spec.Props[auxKey])
	}
}
