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

package store

import (
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// errPatchTestRefusal stands in for whatever a real mutator refuses with.
var errPatchTestRefusal = errors.New("mutator said no")

// A mutator that edits and then refuses must change nothing. The store used
// to hand it a shallow struct copy, whose maps still addressed the stored
// object, so the edit landed anyway and the refusal was only apparent — which
// is the whole value of a refusal that lives inside a patch.
func TestPatchRefusalLeavesThePropsAlone(t *testing.T) {
	t.Parallel()

	s := NewInMemory()
	ctx := t.Context()

	err := s.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "node-1",
		StoragePoolName: "data",
		Props:           map[string]string{"StorDriver/LvmVg": "vg-real"},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = s.StoragePools().PatchStoragePoolSpec(ctx, "node-1", "data",
		func(pool *apiv1.StoragePool) error {
			pool.Props["StorDriver/LvmVg"] = "hijacked"

			return errPatchTestRefusal
		})
	if !errors.Is(err, errPatchTestRefusal) {
		t.Fatalf("patch error = %v, want the mutator's own", err)
	}

	got, err := s.StoragePools().Get(ctx, "node-1", "data")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Props["StorDriver/LvmVg"] != "vg-real" {
		t.Errorf("a refused patch changed the stored props: %v", got.Props)
	}
}

// The copy has to carry fields the wire format does not: PoolMissing is
// tagged `json:"-"`, and a copy made by round-tripping through JSON drops it,
// so an unrelated property patch would silently clear the flag the placer and
// the Faulty column read.
func TestPatchKeepsFieldsAbsentFromJSON(t *testing.T) {
	t.Parallel()

	s := NewInMemory()
	ctx := t.Context()

	err := s.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "node-1",
		StoragePoolName: "data",
		PoolMissing:     true,
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = s.StoragePools().PatchStoragePoolSpec(ctx, "node-1", "data",
		func(pool *apiv1.StoragePool) error {
			if pool.Props == nil {
				pool.Props = map[string]string{}
			}

			pool.Props["Aux/unrelated"] = "1"

			return nil
		})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	got, err := s.StoragePools().Get(ctx, "node-1", "data")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !got.PoolMissing {
		t.Error("an unrelated property patch cleared PoolMissing")
	}
}

// Bug-021: a mutator that leaves Annotations nil means "untouched", not
// "clear". The carry has to land on the value the store keeps — writing it to
// a copy the function discards restores the very behaviour it guards against.
func TestPatchCarriesAnnotationsWhenTheMutatorLeavesThemNil(t *testing.T) {
	t.Parallel()

	s := NewInMemory()
	ctx := t.Context()

	err := s.Resources().Create(ctx, &apiv1.Resource{
		Name:        "pvc-x",
		NodeName:    "node-1",
		Annotations: map[string]string{"blockstor.io/keep": "yes"},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = s.Resources().PatchResourceSpec(ctx, "pvc-x", "node-1",
		func(res *apiv1.Resource) error {
			res.Annotations = nil

			return nil
		})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	got, err := s.Resources().Get(ctx, "pvc-x", "node-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Annotations["blockstor.io/keep"] != "yes" {
		t.Errorf("a patch that nilled Annotations cleared them: %v", got.Annotations)
	}
}

// The node property patch is the eighth path, and it was still handing the
// mutator the live stored map after the other seven were fixed.
func TestNodePatchPropsRefusalLeavesThePropsAlone(t *testing.T) {
	t.Parallel()

	s := NewInMemory()
	ctx := t.Context()

	err := s.Nodes().Create(ctx, &apiv1.Node{
		Name:  "node-1",
		Props: map[string]string{"Aux/keep": "original"},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = s.Nodes().PatchProps(ctx, "node-1", func(props map[string]string) error {
		props["Aux/keep"] = "hijacked"

		return errPatchTestRefusal
	})
	if !errors.Is(err, errPatchTestRefusal) {
		t.Fatalf("patch error = %v, want the mutator's own", err)
	}

	got, err := s.Nodes().Get(ctx, "node-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Props["Aux/keep"] != "original" {
		t.Errorf("a refused node property patch changed the store: %v", got.Props)
	}
}

// The same contract on the other two stores. The code was right in all three,
// but only the Resources() path was held by a test — reverting either of the
// other two to the discarded copy left the whole suite green.
func TestPatchCarriesAnnotationsOnEveryStore(t *testing.T) {
	t.Parallel()

	t.Run("resource definition", func(t *testing.T) {
		t.Parallel()

		s := NewInMemory()
		ctx := t.Context()

		err := s.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:        "pvc-x",
			Annotations: map[string]string{"blockstor.io/keep": "yes"},
		})
		if err != nil {
			t.Fatalf("seed: %v", err)
		}

		err = s.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "pvc-x",
			func(def *apiv1.ResourceDefinition) error {
				def.Annotations = nil

				return nil
			})
		if err != nil {
			t.Fatalf("patch: %v", err)
		}

		got, err := s.ResourceDefinitions().Get(ctx, "pvc-x")
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		if got.Annotations["blockstor.io/keep"] != "yes" {
			t.Errorf("a patch that nilled Annotations cleared them: %v", got.Annotations)
		}
	})

	t.Run("resource group", func(t *testing.T) {
		t.Parallel()

		s := NewInMemory()
		ctx := t.Context()

		err := s.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:        "grp",
			Annotations: map[string]string{"blockstor.io/keep": "yes"},
		})
		if err != nil {
			t.Fatalf("seed: %v", err)
		}

		err = s.ResourceGroups().PatchResourceGroup(ctx, "grp",
			func(rg *apiv1.ResourceGroup) error {
				rg.Annotations = nil

				return nil
			})
		if err != nil {
			t.Fatalf("patch: %v", err)
		}

		got, err := s.ResourceGroups().Get(ctx, "grp")
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		if got.Annotations["blockstor.io/keep"] != "yes" {
			t.Errorf("a patch that nilled Annotations cleared them: %v", got.Annotations)
		}
	})
}
