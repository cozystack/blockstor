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

// TestResolveEffectivePropsNilRD: with no RD pointer the resolver
// must still walk the cluster ControllerProps + Resource Spec.Props
// without panicking. Returns the merged map (Resource wins).
func TestResolveEffectivePropsNilRD(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			Props:                  map[string]string{"DrbdOptions/Net/protocol": "C"},
		},
	}

	got, err := rec.ResolveEffectiveProps(context.Background(), target, nil)
	if err != nil {
		t.Fatalf("ResolveEffectiveProps: %v", err)
	}

	if got["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("Resource-level prop dropped: got %v", got)
	}
}

// TestResolveEffectivePropsRGMissing: RD points at an RG that
// doesn't exist — soft-fail and skip that tier rather than refusing
// to dispatch. The hierarchy still produces a usable .res from the
// remaining levels (controller + RD + Resource).
func TestResolveEffectivePropsRGMissing(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			Props:                  map[string]string{"DrbdOptions/Net/protocol": "A"},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "ghost-rg", // doesn't exist in the fake client
			Props:             map[string]string{"DrbdOptions/Resource/quorum": "majority"},
		},
	}

	got, err := rec.ResolveEffectiveProps(context.Background(), target, rd)
	if err != nil {
		t.Fatalf("ResolveEffectiveProps: %v", err)
	}

	// RD-level + Resource-level props must still appear; missing RG
	// silently skipped.
	if got["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("RD prop missing in soft-fail path: %v", got)
	}

	if got["DrbdOptions/Net/protocol"] != "A" {
		t.Errorf("Resource prop missing in soft-fail path: %v", got)
	}
}

// TestResolveEffectivePropsRGOverridesRD: RD prop overrides RG prop
// (lower-tier wins). Pins the precedence ladder upstream LINSTOR's
// `linstor rd set-property` users rely on.
func TestResolveEffectivePropsRDOverridesRG(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			Props: map[string]string{"DrbdOptions/Net/protocol": "A"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rg).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg",
			Props:             map[string]string{"DrbdOptions/Net/protocol": "C"},
		},
	}

	got, err := rec.ResolveEffectiveProps(context.Background(), target, rd)
	if err != nil {
		t.Fatalf("ResolveEffectiveProps: %v", err)
	}

	if got["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("RD must override RG: got %q, want C", got["DrbdOptions/Net/protocol"])
	}
}
