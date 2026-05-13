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

package rest

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestEffectivePropsRDInheritsFromRG: a key set on the parent
// ResourceGroup but absent on the ResourceDefinition surfaces on the
// RD with scope=RG. This is the inheritance path python-linstor-client
// renders as `(R)` (inherited) in `linstor rd lp`.
func TestEffectivePropsRDInheritsFromRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-1",
		Props: map[string]string{"a": "1"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	rd := apiv1.ResourceDefinition{Name: "rd-1", ResourceGroupName: "rg-1"}

	got, err := effectivePropsForRD(ctx, nil, st, &rd)
	if err != nil {
		t.Fatalf("effectivePropsForRD: %v", err)
	}

	entry, ok := got["a"]
	if !ok {
		t.Fatalf("missing key 'a' in effective_props: %#v", got)
	}

	if entry.Value != "1" || entry.Scope != apiv1.EffectivePropScopeResourceGroup {
		t.Errorf("got {value:%q scope:%q}, want {value:1 scope:%s}",
			entry.Value, entry.Scope, apiv1.EffectivePropScopeResourceGroup)
	}
}

// TestEffectivePropsRDOverridesRG: a key set on both the
// ResourceGroup and the ResourceDefinition resolves to the RD's
// value with scope=RD. RD overrides RG per upstream LINSTOR's
// scope precedence.
func TestEffectivePropsRDOverridesRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-1",
		Props: map[string]string{"a": "1"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	rd := apiv1.ResourceDefinition{
		Name:              "rd-1",
		ResourceGroupName: "rg-1",
		Props:             map[string]string{"a": "2"},
	}

	got, err := effectivePropsForRD(ctx, nil, st, &rd)
	if err != nil {
		t.Fatalf("effectivePropsForRD: %v", err)
	}

	entry := got["a"]
	if entry.Value != "2" || entry.Scope != apiv1.EffectivePropScopeResourceDefinition {
		t.Errorf("got {value:%q scope:%q}, want {value:2 scope:%s}",
			entry.Value, entry.Scope, apiv1.EffectivePropScopeResourceDefinition)
	}
}

// TestEffectivePropsResourceLayersAll: a Resource inherits from RG +
// RD + sets its own local key. The merged map carries the RD-level
// override for `a` (RD beats RG) and the Resource-level addition for
// `b`. Mirrors the full Controller→RG→RD→Resource precedence chain
// upstream LINSTOR walks for the `(R)` marker.
func TestEffectivePropsResourceLayersAll(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-1",
		Props: map[string]string{"a": "1"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd-1",
		ResourceGroupName: "rg-1",
		Props:             map[string]string{"a": "2"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	rsc := apiv1.Resource{
		Name:     "rd-1",
		NodeName: "node-1",
		Props:    map[string]string{"b": "3"},
	}

	got, err := effectivePropsForResource(ctx, nil, st, &rsc)
	if err != nil {
		t.Fatalf("effectivePropsForResource: %v", err)
	}

	a := got["a"]
	if a.Value != "2" || a.Scope != apiv1.EffectivePropScopeResourceDefinition {
		t.Errorf("key a: got {value:%q scope:%q}, want {value:2 scope:%s}",
			a.Value, a.Scope, apiv1.EffectivePropScopeResourceDefinition)
	}

	b := got["b"]
	if b.Value != "3" || b.Scope != apiv1.EffectivePropScopeResource {
		t.Errorf("key b: got {value:%q scope:%q}, want {value:3 scope:%s}",
			b.Value, b.Scope, apiv1.EffectivePropScopeResource)
	}
}
