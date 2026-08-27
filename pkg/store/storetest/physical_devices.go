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

package storetest

import (
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// RunPhysicalDeviceStore exercises store.PhysicalDeviceStore against any
// implementation.
//
// The patch entry point in particular carries logic that is written
// twice — retry-on-conflict, NotFound translation, the label the k8s
// side maintains alongside the spec — so without a shared suite the two
// implementations can drift apart unnoticed.
func RunPhysicalDeviceStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("PatchMutates", func(t *testing.T) { testPhysDevPatchMutates(t, newStore) })
	t.Run("PatchMissingIsNotFound", func(t *testing.T) { testPhysDevPatchMissing(t, newStore) })
	t.Run("PatchErrorLeavesDeviceAlone", func(t *testing.T) { testPhysDevPatchError(t, newStore) })
	t.Run("PatchCannotEditThroughPointers", func(t *testing.T) { testPhysDevPatchAliasing(t, newStore) })
}

// testPhysDevPatchAliasing: a mutator that edits through a pointer field
// and then fails must leave the store untouched.
//
// A struct copy is shallow, so handing the mutator one whose pointers
// still address the stored value makes the rollback only apparent: the
// edit already landed before the error was returned.
func testPhysDevPatchAliasing(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t)
	ctx := t.Context()

	err := s.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
		Name:     "node-1-sdb",
		NodeName: "node-1",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "data",
			ProviderKind:    "ZFS",
			ZPoolName:       "tank",
		},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	sentinel := errors.New("mutator said no")

	err = s.PhysicalDevices().PatchPhysicalDeviceSpec(ctx, "node-1-sdb",
		func(dev *apiv1.PhysicalDevice) error {
			if dev.AttachTo != nil {
				dev.AttachTo.ZPoolName = "hijacked"
			}

			return sentinel
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("PatchPhysicalDeviceSpec error = %v, want the mutator's own", err)
	}

	got, err := s.PhysicalDevices().Get(ctx, "node-1-sdb")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.AttachTo == nil || got.AttachTo.ZPoolName != "tank" {
		t.Errorf("an edit through a pointer survived a failed mutation: %+v", got.AttachTo)
	}
}

func seedPhysDev(t *testing.T, s store.Store) {
	t.Helper()

	// Only AttachTo lives in this kind's spec; size, paths and phase
	// are status, written by the satellite's discovery rather than by
	// the store. Seeding them here would assert a round trip the store
	// deliberately does not make.
	err := s.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:     "node-1-sdb",
		NodeName: "node-1",
	})
	if err != nil {
		t.Fatalf("seed physical device: %v", err)
	}
}

// testPhysDevPatchMutates: the change the caller describes lands, and
// the fields it did not name survive.
func testPhysDevPatchMutates(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t)
	seedPhysDev(t, s)

	err := s.PhysicalDevices().PatchPhysicalDeviceSpec(t.Context(), "node-1-sdb",
		func(dev *apiv1.PhysicalDevice) error {
			dev.AttachTo = &apiv1.PhysicalDeviceAttachTo{
				StoragePoolName: "data",
				ProviderKind:    "ZFS",
				ZPoolName:       "tank",
			}

			return nil
		})
	if err != nil {
		t.Fatalf("PatchPhysicalDeviceSpec: %v", err)
	}

	got, err := s.PhysicalDevices().Get(t.Context(), "node-1-sdb")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.AttachTo == nil || got.AttachTo.ZPoolName != "tank" {
		t.Errorf("attach did not land: %+v", got.AttachTo)
	}

	// The node binding is carried outside the spec — a label on the
	// k8s side — so a patch that rebuilds the spec has to maintain it
	// rather than let it fall off.
	if got.Name != "node-1-sdb" || got.NodeName != "node-1" {
		t.Errorf("the patch lost the device's identity: %+v", got)
	}
}

// testPhysDevPatchMissing: a patch against a device that is not there
// has to come back as store.ErrNotFound, the error every caller
// branches on.
func testPhysDevPatchMissing(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t)

	err := s.PhysicalDevices().PatchPhysicalDeviceSpec(t.Context(), "no-such-device",
		func(*apiv1.PhysicalDevice) error { return nil })

	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("PatchPhysicalDeviceSpec on a missing device = %v, want store.ErrNotFound", err)
	}
}

// testPhysDevPatchError: a mutator that fails must not half-apply.
// Callers use the error to abort — an attach refused because the device
// already belongs to another pool, say — so a partial write would be
// the one state nobody handles.
func testPhysDevPatchError(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t)
	seedPhysDev(t, s)

	sentinel := errors.New("mutator said no")

	err := s.PhysicalDevices().PatchPhysicalDeviceSpec(t.Context(), "node-1-sdb",
		func(dev *apiv1.PhysicalDevice) error {
			dev.AttachTo = &apiv1.PhysicalDeviceAttachTo{
				StoragePoolName: "ghost",
				ProviderKind:    "ZFS",
				ZPoolName:       "ghost",
			}

			return sentinel
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("PatchPhysicalDeviceSpec error = %v, want the mutator's own", err)
	}

	got, err := s.PhysicalDevices().Get(t.Context(), "node-1-sdb")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.AttachTo != nil {
		t.Errorf("a failed mutation leaked into the store: %+v", got.AttachTo)
	}
}
