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

package store_test

import (
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestPhysicalDeviceUpdateRejectsConcurrentAttach pins the
// CAS guard for Phase 10.7 race-matrix line 767: two REST
// `POST /v1/physical-storage/<node>` requests both pick the
// same free PhysicalDevice (each saw `AttachTo=nil` at List
// time). The Update path MUST refuse the second one with
// `ErrAlreadyExists` so the REST handler can surface 409 to
// the loser instead of silently overwriting the first attach.
func TestPhysicalDeviceUpdateRejectsConcurrentAttach(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	first := &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		Phase:      "Available",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "pool-A",
			ProviderKind:    "LVM_THIN",
		},
	}

	if err := st.PhysicalDevices().Update(ctx, first); err != nil {
		t.Fatalf("first attach: got %v, want success", err)
	}

	second := &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		Phase:      "Available",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "pool-B",
			ProviderKind:    "ZFS",
		},
	}

	err := st.PhysicalDevices().Update(ctx, second)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("second attach: got %v, want ErrAlreadyExists", err)
	}

	// Sanity: the first attach's AttachTo must still be intact —
	// the rejected second request can't have leaked through.
	got, err := st.PhysicalDevices().Get(ctx, "n1-sda")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.AttachTo == nil || got.AttachTo.StoragePoolName != "pool-A" {
		t.Errorf("first attach overwritten by losing request: %+v", got.AttachTo)
	}
}
