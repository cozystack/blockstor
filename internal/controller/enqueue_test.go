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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestEnqueueResourcesForRD: an RD watch event must fan out to every
// Resource that references the RD via Spec.ResourceDefinitionName.
// Pins the path that drives sibling re-Apply when an RD's
// VolumeDefinition or LayerStack changes — without this fan-out, a
// resize bumping size_kib would only land on whichever replica
// happened to reconcile first.
func TestEnqueueResourcesForRD(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	resources := []client.Object{
		&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-1",
				NodeName:               "n1",
			},
		},
		&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n2"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-1",
				NodeName:               "n2",
			},
		},
		// Different RD — must NOT be enqueued.
		&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-2.n1"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-2",
				NodeName:               "n1",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(resources...).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
	}

	got := rec.EnqueueResourcesForRD(context.Background(), rd)

	if len(got) != 2 {
		t.Fatalf("requests: got %d, want 2 (replicas of pvc-1); got %+v", len(got), got)
	}

	names := map[string]bool{}
	for _, req := range got {
		names[req.Name] = true
	}

	for _, want := range []string{"pvc-1.n1", "pvc-1.n2"} {
		if !names[want] {
			t.Errorf("missing %q in requests; got %+v", want, got)
		}
	}

	if names["pvc-2.n1"] {
		t.Errorf("pvc-2 replica must not be enqueued for an RD-pvc-1 event")
	}
}

// TestEnqueueResourcesForRDWrongType: handler must be defensive
// against being called with the wrong object type (controller-runtime
// can technically deliver any client.Object via the watch channel).
// Wrong type → empty result, no panic.
func TestEnqueueResourcesForRDWrongType(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	// Hand a Pod (or any non-RD object); shouldn't panic.
	got := rec.EnqueueResourcesForRD(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "wrong"},
	})

	if got != nil {
		t.Errorf("wrong-type event must yield nil; got %+v", got)
	}
}

// TestEnqueueSiblings: a Resource event fans out to every OTHER
// Resource of the same RD. The originator's own reconcile already
// fires through For() on the controller-runtime builder, so we
// exclude it here to avoid the redundant requeue.
func TestEnqueueSiblings(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	resources := []client.Object{
		&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-1",
				NodeName:               "n1",
			},
		},
		&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n2"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-1",
				NodeName:               "n2",
			},
		},
		&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n3"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-1",
				NodeName:               "n3",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(resources...).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	originator := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
		},
	}

	got := rec.EnqueueSiblings(context.Background(), originator)

	if len(got) != 2 {
		t.Fatalf("requests: got %d, want 2 (siblings of pvc-1.n1); got %+v", len(got), got)
	}

	for _, req := range got {
		if req.Name == "pvc-1.n1" {
			t.Errorf("originator pvc-1.n1 must NOT appear in its own siblings list")
		}
	}
}

// TestEnqueueSiblingsEmptyRDName: a Resource without
// ResourceDefinitionName (shouldn't happen in production but the
// CRD field is technically optional) → empty result, no fan-out.
func TestEnqueueSiblingsEmptyRDName(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	orphan := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan"},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}

	got := rec.EnqueueSiblings(context.Background(), orphan)
	if len(got) != 0 {
		t.Errorf("orphan Resource must not fan out: got %+v", got)
	}
}
