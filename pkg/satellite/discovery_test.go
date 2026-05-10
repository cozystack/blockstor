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

package satellite_test

import (
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/satellite"
)

// TestPickStableIDPrefersWWN pins the preference order: when WWN
// is present it always wins, regardless of transport / model /
// serial. Operators rely on WWN-derived names surviving drive
// re-cabling, so a regression that fell through to scsi-SATA
// would silently break re-discovery on reboot.
func TestPickStableIDPrefersWWN(t *testing.T) {
	t.Parallel()

	dev := &satellite.LsblkDevice{
		WWN:       "0x5000c500a3b1c2d3",
		Transport: "sata",
		Model:     "Samsung SSD 980 PRO",
		Serial:    "S6BUNS0R123456",
	}

	got := satellite.PickStableID(dev)
	if got != "wwn-0x5000c500a3b1c2d3" {
		t.Errorf("got %q, want wwn-0x5000c500a3b1c2d3", got)
	}
}

// TestPickStableIDFallsBackToScsiSATA pins the second-tier path:
// no WWN, but model+serial reported on a SATA drive — assemble
// the scsi-SATA-style ID. Spaces in MODEL get normalised to
// underscores (matching udev's `60-persistent-storage.rules`).
func TestPickStableIDFallsBackToScsiSATA(t *testing.T) {
	t.Parallel()

	dev := &satellite.LsblkDevice{
		Transport: "sata",
		Model:     "Samsung SSD 980 PRO",
		Serial:    "S6BUNS0R123456",
	}

	got := satellite.PickStableID(dev)
	want := "scsi-SATA_Samsung_SSD_980_PRO_S6BUNS0R123456"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestPickStableIDNVMe pins the NVMe path.
func TestPickStableIDNVMe(t *testing.T) {
	t.Parallel()

	dev := &satellite.LsblkDevice{
		Transport: "nvme",
		Model:     "Samsung SSD 980 PRO",
		Serial:    "S6BUNS0R123456",
	}

	got := satellite.PickStableID(dev)
	if !strings.HasPrefix(got, "nvme-Samsung_SSD_980_PRO") {
		t.Errorf("got %q, want nvme- prefix with model+serial", got)
	}
}

// TestPickStableIDByPathFallback pins the last-resort branch:
// virtio without serial passthrough — no WWN, no SCSI, no NVMe
// identifier — falls through to the by-path-derived placeholder.
// Regression target: a virtio host that returns "" for all
// stable signals would otherwise crash the satellite later when
// PhysicalDeviceCRDName tries to derive a k8s name from "".
func TestPickStableIDByPathFallback(t *testing.T) {
	t.Parallel()

	dev := &satellite.LsblkDevice{
		Transport: "virtio",
		KName:     "vdb",
	}

	got := satellite.PickStableID(dev)
	if got != "by-path-vdb" {
		t.Errorf("got %q, want by-path-vdb", got)
	}
}

// TestDiscoveryDiffCreate pins the new-device case: a device in
// `discovered` that isn't in `existing` becomes an OpCreate.
func TestDiscoveryDiffCreate(t *testing.T) {
	t.Parallel()

	discovered := []apiv1.PhysicalDevice{
		{Name: "n1-sda", NodeName: "n1", DevicePath: "/dev/disk/by-id/wwn-X"},
	}

	actions := satellite.DiscoveryDiff(discovered, nil)
	if len(actions) != 1 {
		t.Fatalf("got %d actions, want 1", len(actions))
	}

	if actions[0].Op != satellite.OpCreate {
		t.Errorf("Op: got %v, want OpCreate", actions[0].Op)
	}

	if actions[0].Device.Name != "n1-sda" {
		t.Errorf("Device.Name: got %q, want n1-sda", actions[0].Device.Name)
	}
}

// TestDiscoveryDiffDelete pins the removal case: a device in
// `existing` that isn't in `discovered` becomes an OpDelete —
// either physically removed or consumed by a pool. The latter
// case the satellite-side attach reconciler delivers as a
// post-success delete; this OpDelete is the catch-all for
// unattached cases (e.g. drive yanked, controller removed).
func TestDiscoveryDiffDelete(t *testing.T) {
	t.Parallel()

	existing := []apiv1.PhysicalDevice{
		{Name: "n1-old", NodeName: "n1"},
	}

	actions := satellite.DiscoveryDiff(nil, existing)
	if len(actions) != 1 {
		t.Fatalf("got %d actions, want 1", len(actions))
	}

	if actions[0].Op != satellite.OpDelete {
		t.Errorf("Op: got %v, want OpDelete", actions[0].Op)
	}
}

// TestDiscoveryDiffSkipsAttaching pins the in-flight rule: a
// device that's currently being attached (Spec.AttachTo set)
// MUST NOT be deleted by the discovery loop even if lsblk-side
// scan no longer surfaces it (which it won't — the LV ate the
// raw device, the underlying path is gone). The satellite-side
// attach reconciler owns the lifecycle of attaching / failed
// CRDs; discovery stays out of its way.
func TestDiscoveryDiffSkipsAttaching(t *testing.T) {
	t.Parallel()

	existing := []apiv1.PhysicalDevice{
		{
			Name:     "n1-attaching",
			NodeName: "n1",
			AttachTo: &apiv1.PhysicalDeviceAttachTo{StoragePoolName: "thin1", ProviderKind: "LVM_THIN"},
		},
	}

	actions := satellite.DiscoveryDiff(nil, existing)
	if len(actions) != 0 {
		t.Errorf("got %d actions, want 0 (attaching device must not be deleted)", len(actions))
	}
}

// TestDiscoveryDiffNoOpOnIdenticalState pins the convergent
// behaviour: same discovered + existing → empty action list.
// A regression that always emitted an Update would churn the
// apiserver on every tick.
func TestDiscoveryDiffNoOpOnIdenticalState(t *testing.T) {
	t.Parallel()

	dev := apiv1.PhysicalDevice{
		Name:           "n1-sda",
		NodeName:       "n1",
		SizeBytes:      1000204886016,
		CurrentDevPath: "/dev/sda",
		Model:          "Samsung",
	}

	actions := satellite.DiscoveryDiff([]apiv1.PhysicalDevice{dev}, []apiv1.PhysicalDevice{dev})
	if len(actions) != 0 {
		t.Errorf("got %d actions on identical state, want 0", len(actions))
	}
}

// TestDiscoveryDiffUpdateOnSizeChange pins the "drive replaced
// behind a stable WWN" case: same name, different size →
// OpUpdate. Catches a hardware swap where the same slot now
// houses a bigger drive.
func TestDiscoveryDiffUpdateOnSizeChange(t *testing.T) {
	t.Parallel()

	prev := apiv1.PhysicalDevice{Name: "n1-sda", SizeBytes: 1000204886016}
	cur := apiv1.PhysicalDevice{Name: "n1-sda", SizeBytes: 2000398934016}

	actions := satellite.DiscoveryDiff([]apiv1.PhysicalDevice{cur}, []apiv1.PhysicalDevice{prev})
	if len(actions) != 1 {
		t.Fatalf("got %d actions, want 1", len(actions))
	}

	if actions[0].Op != satellite.OpUpdate {
		t.Errorf("Op: got %v, want OpUpdate", actions[0].Op)
	}
}

// TestDiscoveryDiffUpdateOnCurrentDevPath pins the sdN-relettering
// case: same drive (same WWN-derived Name), different /dev/sdN
// after a reboot. The CRD's CurrentDevPath should refresh — the
// stable Name + DevicePath stay the same.
func TestDiscoveryDiffUpdateOnCurrentDevPath(t *testing.T) {
	t.Parallel()

	prev := apiv1.PhysicalDevice{Name: "n1-sda", CurrentDevPath: "/dev/sda"}
	cur := apiv1.PhysicalDevice{Name: "n1-sda", CurrentDevPath: "/dev/sdc"}

	actions := satellite.DiscoveryDiff([]apiv1.PhysicalDevice{cur}, []apiv1.PhysicalDevice{prev})
	if len(actions) != 1 {
		t.Fatalf("got %d actions, want 1", len(actions))
	}

	if actions[0].Op != satellite.OpUpdate {
		t.Errorf("Op: got %v, want OpUpdate (sdN re-lettered)", actions[0].Op)
	}
}

// TestDiscoveryDiffUpdateOnRotationalFlip catches the HDD-to-SSD
// hot-swap case: same drive bay, but the new drive is non-
// rotational where the old one was rotational. Autoplacer uses
// the field for placement bias, so the change must propagate.
func TestDiscoveryDiffUpdateOnRotationalFlip(t *testing.T) {
	t.Parallel()

	rota := true
	notRota := false

	prev := apiv1.PhysicalDevice{Name: "n1-sda", Rotational: &rota}
	cur := apiv1.PhysicalDevice{Name: "n1-sda", Rotational: &notRota}

	actions := satellite.DiscoveryDiff([]apiv1.PhysicalDevice{cur}, []apiv1.PhysicalDevice{prev})
	if len(actions) != 1 || actions[0].Op != satellite.OpUpdate {
		t.Errorf("got %v, want exactly one OpUpdate", actions)
	}
}
