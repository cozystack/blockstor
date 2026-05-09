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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestSetQuorumWritesValue: setQuorum writes the
// `DrbdOptions/Resource/quorum` prop onto the RD's Spec.Props,
// initialising the map if nil. The underlying CRD reflects the
// change after the call returns.
func TestSetQuorumWritesValue(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		// Props intentionally nil — exercises the lazy-init branch.
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority", got.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}

// TestSetQuorumIdempotent: calling setQuorum with the same value
// must NOT issue a redundant Update — protects against requeue
// storms when the RD reconciler runs back-to-back. Detected by
// asserting ResourceVersion stays unchanged.
func TestSetQuorumIdempotent(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				"DrbdOptions/Resource/quorum": "majority",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	pre := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, pre); err != nil {
		t.Fatalf("get pre: %v", err)
	}

	if err := rec.SetQuorum(context.Background(), pre, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	post := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, post); err != nil {
		t.Fatalf("get post: %v", err)
	}

	if pre.ResourceVersion != post.ResourceVersion {
		t.Errorf("ResourceVersion changed after no-op SetQuorum: %s → %s",
			pre.ResourceVersion, post.ResourceVersion)
	}
}

// TestSetQuorumReplacesExistingValue: changing from "off" to
// "majority" overwrites the prop in place. Ensures the RD reconciler
// can flip the quorum mode when a witness lands or evaporates.
func TestSetQuorumReplacesExistingValue(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				"DrbdOptions/Resource/quorum": "off",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority", got.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}
