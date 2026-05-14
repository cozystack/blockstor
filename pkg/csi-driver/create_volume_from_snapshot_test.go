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

package csidriver

// Scenario 11.W09 — CSI `VolumeSnapshot` restore into new PVC.
//
// tests/scenarios/wave2-11-kubernetes.md §11.W09 pins the contract:
//
//   `Cross-listed with wave1 4.14. PVC dataSource referencing
//    VolumeSnapshot → CSI calls blockstor's "snapshot resource
//    restore" (wave2-08 8.W03) under the hood. New PVC must be ≥
//    snapshot size. Snapshot must already be readyToUse=true.`
//
// At the CSI driver layer (this package) that boils down to:
//
//   csi.CreateVolume {
//     Name=new-pvc,
//     CapacityRange.Required>=snap.size,
//     VolumeContentSource.Snapshot.SnapshotId="<rd>/<snap>",
//   }
//     → POST /v1/resource-definitions/{src-rd}/snapshot-restore-resource/{snap}
//        body: { "to_resource": "new-pvc" }
//     → 201 + a fresh ResourceDefinition row stamped with the
//        BlockstorRestoreFromSnapshot prop so the satellite knows
//        to clone (zfs send|recv / lvcreate -s) rather than
//        provision blank.
//
// These tests pin that mapping AND the CSI-spec-mandated guard
// rails (size validation, ContentSource propagation back to the
// caller) so a refactor in pkg/rest cannot silently break
// linstor-csi's PVC-from-snapshot flow.

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestCSICreateVolumeFromSnapshotHappyPath: the canonical
// "restore VolumeSnapshot into a new PVC" path. A new RD must
// land in the store, sized from the snapshot, and stamped with
// the BlockstorRestoreFromSnapshot prop so the satellite clones
// rather than provisions blank.
func TestCSICreateVolumeFromSnapshotHappyPath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Source RD with a 1 GiB volume.
	const sourceSizeKib = 1024 * 1024 // 1 GiB

	seedRDWithVolume(t, st, "pvc-src", sourceSizeKib)
	seedSnapshot(t, st, "pvc-src", "snap-1", sourceSizeKib)

	base, stop := startRESTServer(t, st)
	defer stop()

	d := &Driver{Client: newLAPIClient(t, base)}

	resp, err := d.CreateVolume(ctx, &CreateVolumeRequest{
		Name:             "pvc-restored",
		CapacityRangeMin: int64(sourceSizeKib) * 1024, // KiB → bytes
		ContentSource: &VolumeContentSourceSnapshot{
			SourceRD:     "pvc-src",
			SnapshotName: "snap-1",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if resp.VolumeID != "pvc-restored" {
		t.Errorf("VolumeID: got %q, want %q", resp.VolumeID, "pvc-restored")
	}

	// CSI spec requires the response to carry the ContentSource back
	// so external-provisioner can stamp it on the resulting PV. A
	// nil ContentSource here would leak the lineage and break
	// VolumeSnapshot/PVC accounting at the storage class level.
	if resp.ContentSource == nil {
		t.Fatal("ContentSource: got nil, want the snapshot lineage echoed back")
	}

	if resp.ContentSource.SourceRD != "pvc-src" || resp.ContentSource.SnapshotName != "snap-1" {
		t.Errorf("ContentSource: got %+v, want {pvc-src snap-1}", resp.ContentSource)
	}

	// The new RD must exist in the store.
	got, err := st.ResourceDefinitions().Get(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("expected pvc-restored to exist after restore: %v", err)
	}

	if got.Name != "pvc-restored" {
		t.Errorf("new RD name: got %q, want %q", got.Name, "pvc-restored")
	}

	// The satellite branch depends on this prop — without it,
	// CreateVolume on the destination node would provision a blank
	// device instead of cloning from the source snapshot. See
	// pkg/rest/autoplace.go (BlockstorRestoreFromSnapshot lookup).
	if got.Props == nil {
		t.Fatal("Props nil — BlockstorRestoreFromSnapshot must be stamped on the restored RD")
	}

	if want := "pvc-src:snap-1"; got.Props["BlockstorRestoreFromSnapshot"] != want {
		t.Errorf(
			"BlockstorRestoreFromSnapshot prop: got %q, want %q",
			got.Props["BlockstorRestoreFromSnapshot"], want)
	}
}

// TestCSICreateVolumeFromSnapshotRequestedTooSmall: CSI spec
// requires the new volume to be at least the snapshot size. If
// external-provisioner passes a CapacityRange whose `required`
// is smaller than the source snapshot, the driver MUST refuse
// rather than truncate at clone time.
func TestCSICreateVolumeFromSnapshotRequestedTooSmall(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const sourceSizeKib = 2 * 1024 * 1024 // 2 GiB

	seedRDWithVolume(t, st, "pvc-src", sourceSizeKib)
	seedSnapshot(t, st, "pvc-src", "snap-1", sourceSizeKib)

	base, stop := startRESTServer(t, st)
	defer stop()

	d := &Driver{Client: newLAPIClient(t, base)}

	_, err := d.CreateVolume(ctx, &CreateVolumeRequest{
		Name:             "pvc-restored",
		CapacityRangeMin: 512 * 1024 * 1024, // 512 MiB — smaller than 2 GiB
		ContentSource: &VolumeContentSourceSnapshot{
			SourceRD:     "pvc-src",
			SnapshotName: "snap-1",
		},
	})
	if err == nil {
		t.Fatal("CreateVolume with too-small CapacityRange: got nil error, want CSI spec violation")
	}

	// The destination RD must NOT have been created — the size
	// guard is a precondition, not a post-condition.
	if _, getErr := st.ResourceDefinitions().Get(ctx, "pvc-restored"); getErr == nil {
		t.Error("destination RD was created despite size validation failure")
	}
}

// TestCSICreateVolumeFromSnapshotMissingContentSource: this shim
// only models the snapshot-restore branch — a request without a
// ContentSource must be rejected so unrelated regressions in the
// blank-create path don't accidentally land in this test.
func TestCSICreateVolumeFromSnapshotMissingContentSource(t *testing.T) {
	d := &Driver{Client: newLAPIClient(t, "http://127.0.0.1:1")} // unreachable on purpose

	_, err := d.CreateVolume(t.Context(), &CreateVolumeRequest{
		Name:             "pvc-restored",
		CapacityRangeMin: 1 << 30,
		ContentSource:    nil,
	})
	if err == nil {
		t.Fatal("CreateVolume without ContentSource: got nil error, want validation failure")
	}
}

// TestCSICreateVolumeFromSnapshotUnknownSnapshot: external-provisioner
// can race ahead of the VolumeSnapshot's readyToUse=true watch in
// edge cases; the driver MUST surface the upstream 404 so the
// PVC enters Pending with a clear reason rather than spinning.
func TestCSICreateVolumeFromSnapshotUnknownSnapshot(t *testing.T) {
	st := store.NewInMemory()

	seedRDWithVolume(t, st, "pvc-src", 1024*1024)
	// No snapshot seeded.

	base, stop := startRESTServer(t, st)
	defer stop()

	d := &Driver{Client: newLAPIClient(t, base)}

	_, err := d.CreateVolume(t.Context(), &CreateVolumeRequest{
		Name:             "pvc-restored",
		CapacityRangeMin: 1 << 30,
		ContentSource: &VolumeContentSourceSnapshot{
			SourceRD:     "pvc-src",
			SnapshotName: "ghost",
		},
	})
	if err == nil {
		t.Fatal("CreateVolume against missing snapshot: got nil error, want 404 propagation")
	}
}

// TestCSICreateVolumeFromSnapshotEqualSize: CapacityRange ==
// snapshot size must be accepted (the inequality in the spec is
// `>=`, not strictly `>`). Pinned because a `<` off-by-one in
// the size guard would silently break VolumeSnapshot restore for
// the default csi-snapshotter sizing policy, which propagates
// the source PVC's exact size.
func TestCSICreateVolumeFromSnapshotEqualSize(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const sourceSizeKib = 1024 * 1024

	seedRDWithVolume(t, st, "pvc-src", sourceSizeKib)
	seedSnapshot(t, st, "pvc-src", "snap-1", sourceSizeKib)

	base, stop := startRESTServer(t, st)
	defer stop()

	d := &Driver{Client: newLAPIClient(t, base)}

	resp, err := d.CreateVolume(ctx, &CreateVolumeRequest{
		Name:             "pvc-restored",
		CapacityRangeMin: int64(sourceSizeKib) * 1024, // exactly the snapshot size
		ContentSource: &VolumeContentSourceSnapshot{
			SourceRD:     "pvc-src",
			SnapshotName: "snap-1",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume with equal CapacityRange: %v", err)
	}

	if resp.CapacityBytes != int64(sourceSizeKib)*1024 {
		t.Errorf("CapacityBytes: got %d, want %d", resp.CapacityBytes, int64(sourceSizeKib)*1024)
	}
}

// --- helpers specific to W09 ---

// seedSnapshot stamps a Snapshot row directly into the store
// with the matching SnapshotVolumeDef.SizeKib so the driver's
// size guard has something to read. Bypasses the REST handler
// because W08 already covers that path.
func seedSnapshot(t *testing.T, st store.Store, rd, name string, sizeKib int64) {
	t.Helper()

	err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name:         name,
		ResourceName: rd,
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: sizeKib},
		},
	})
	if err != nil {
		t.Fatalf("seed snapshot %s/%s: %v", rd, name, err)
	}
}
