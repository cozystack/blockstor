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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestLookupRDFound: the happy path returns the seeded RD pointer
// with its Spec preserved.
func TestLookupRDFound(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "STORAGE"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	got, err := rec.LookupRD(context.Background(), "pvc-1")
	if err != nil {
		t.Fatalf("LookupRD: %v", err)
	}

	if got == nil {
		t.Fatalf("got nil pointer; want RD")
	}

	if len(got.Spec.LayerStack) != 2 || got.Spec.LayerStack[0] != "DRBD" {
		t.Errorf("Spec round-trip: got %+v", got.Spec)
	}
}

// TestLookupRDNotFoundIsSoftFail: a missing RD returns (nil, nil),
// not an error. The Resource reconciler hits this whenever the RD
// is being deleted concurrently with one of its replicas — the
// dispatcher still pushes a `.res` for connection setup so peers
// can finish replicating before kube-apiserver finalises the
// cascade. Pins the soft-fail invariant.
func TestLookupRDNotFoundIsSoftFail(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	got, err := rec.LookupRD(context.Background(), "ghost")
	if err != nil {
		t.Errorf("missing RD must NOT error; got %v", err)
	}

	if got != nil {
		t.Errorf("missing RD must yield nil pointer; got %+v", got)
	}
}
