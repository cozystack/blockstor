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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestResolveLayerStackRDWins: when an RD specifies its own
// LayerStack, that wins over the parent RG's. Pins the precedence
// the dispatcher's CSI pass-through relies on (linstor-csi sets
// layer_list on autoplace; the REST handler persists it onto rd
// spec, and the resolver here picks it up unchanged).
func TestResolveLayerStackRDWins(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				LayerStack: []string{"DRBD", "STORAGE"},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rg).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg",
			LayerStack:        []string{"LUKS", "STORAGE"},
		},
	}

	got := rec.ResolveLayerStack(context.Background(), rd)
	if !slices.Equal(got, []string{"LUKS", "STORAGE"}) {
		t.Errorf("RD wins over RG: got %v, want [LUKS STORAGE]", got)
	}
}

// TestResolveLayerStackFallsBackToRG: RD with no LayerStack inherits
// from its parent RG. This is the path operators use to set defaults
// (e.g. `linstor rg create encrypted-rg --layer-list LUKS,STORAGE`).
func TestResolveLayerStackFallsBackToRG(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg-luks"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rg).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg-luks",
			// LayerStack intentionally absent.
		},
	}

	got := rec.ResolveLayerStack(context.Background(), rd)
	if !slices.Equal(got, []string{"DRBD", "LUKS", "STORAGE"}) {
		t.Errorf("inherit from RG: got %v, want [DRBD LUKS STORAGE]", got)
	}
}

// TestResolveLayerStackNilWithoutRG: RD with no LayerStack and no
// ResourceGroupName → nil (dispatcher's default-fall-through).
// Pins the legacy single-RD path that pre-Phase-9 clients rely on.
func TestResolveLayerStackNilWithoutRG(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			// No LayerStack, no ResourceGroupName.
		},
	}

	if got := rec.ResolveLayerStack(context.Background(), rd); got != nil {
		t.Errorf("nil-fallback: got %v, want nil", got)
	}
}

// TestResolveLayerStackMissingRG: RD points at an RG that doesn't
// exist (deleted under us, race) → nil rather than blowing up. The
// dispatcher then falls through to the satellite default.
func TestResolveLayerStackMissingRG(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "ghost-rg",
		},
	}

	if got := rec.ResolveLayerStack(context.Background(), rd); got != nil {
		t.Errorf("missing-RG: got %v, want nil (no panic, no error)", got)
	}
}

// TestResolveLayerStackNilRD: defensive — handing a nil RD must yield
// nil, not panic.
func TestResolveLayerStackNilRD(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	if got := rec.ResolveLayerStack(context.Background(), nil); got != nil {
		t.Errorf("nil RD: got %v, want nil", got)
	}
}
