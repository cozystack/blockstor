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

// Scenario 11.W08 — CSI `VolumeSnapshot` create.
//
// tests/scenarios/wave2-11-kubernetes.md §11.W08 pins the contract:
//
//   `VolumeSnapshotClass driver=linstor.csi.linbit.com + VolumeSnapshot
//    referencing PVC. csi-snapshotter calls blockstor's CreateSnapshot
//    (wave1 1.13). Wait for readyToUse=true.`
//
// At the CSI driver layer (this package) that boils down to:
//
//   csi.CreateSnapshot {SourceVolumeID=pvc-name, Name=snap-name}
//     → POST /v1/resource-definitions/{pvc-name}/snapshots
//        body: { "name": "snap-name" }
//     → 201 ApiCallRc + the snapshot row landing in `s.Store.Snapshots()`
//
// These tests pin that exact mapping so a refactor in pkg/rest cannot
// silently break linstor-csi's CreateSnapshot flow. They do NOT cover
// the readyToUse=true polling — that's the satellite reconcile path
// pinned by scenario 8.W03 (and the satellite snapshot controller's
// own unit tests).

import (
	"context"
	"net/url"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestCSICreateSnapshotHappyPath: csi.CreateSnapshot lands as a
// blockstor Snapshot row addressable by (rd, snap_name), and the
// returned CSI snapshot id encodes both pieces so the eventual
// DeleteSnapshot / CreateVolume-from-snapshot calls can find it.
func TestCSICreateSnapshotHappyPath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seedRDWithVolume(t, st, "pvc-1", 4*1024*1024) // 4 GiB
	base, stop := startRESTServer(t, st)
	defer stop()

	d := &Driver{Client: newLAPIClient(t, base)}

	resp, err := d.CreateSnapshot(ctx, &CreateSnapshotRequest{
		SourceVolumeID: "pvc-1",
		Name:           "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// CSI snapshot id encoding: linstor-csi uses `<rd>/<snap>` so
	// DeleteSnapshot and CreateVolume.ContentSource can decode it.
	if want := "pvc-1/snap-1"; resp.SnapshotID != want {
		t.Errorf("SnapshotID: got %q, want %q", resp.SnapshotID, want)
	}

	if resp.SourceVolumeID != "pvc-1" {
		t.Errorf("SourceVolumeID: got %q, want %q", resp.SourceVolumeID, "pvc-1")
	}

	// blockstor's REST shim returns synchronously before the satellite
	// runs. ReadyToUse must NOT be reported true here — csi-snapshotter
	// would mark the VolumeSnapshot ready while the satellite is still
	// taking the actual zfs/lvm snapshot. Promotion to true happens in
	// the ListSnapshots/GetSnapshot polling loop driven by the
	// satellite's snapshot controller (scenario 8.W03).
	if resp.ReadyToUse {
		t.Errorf("ReadyToUse: got true, want false (satellite reconcile is async)")
	}

	// Verify the row landed in the store with the right (rd, name).
	got, err := st.Snapshots().Get(ctx, "pvc-1", "snap-1")
	if err != nil {
		t.Fatalf("Store.Snapshots().Get: %v", err)
	}

	if got.ResourceName != "pvc-1" {
		t.Errorf("Snapshot.ResourceName: got %q, want %q", got.ResourceName, "pvc-1")
	}

	if got.Name != "snap-1" {
		t.Errorf("Snapshot.Name: got %q, want %q", got.Name, "snap-1")
	}
}

// TestCSICreateSnapshotIdempotent: csi-snapshotter retries
// CreateSnapshot on the same (SourceVolumeID, Name) until success.
// blockstor MUST return success on the second call rather than 409;
// otherwise a retry storm flips the VolumeSnapshot into Error.
func TestCSICreateSnapshotIdempotent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seedRDWithVolume(t, st, "pvc-1", 4*1024*1024)
	base, stop := startRESTServer(t, st)
	defer stop()

	d := &Driver{Client: newLAPIClient(t, base)}

	for i := range 3 {
		resp, err := d.CreateSnapshot(ctx, &CreateSnapshotRequest{
			SourceVolumeID: "pvc-1",
			Name:           "snap-1",
		})
		if err != nil {
			t.Fatalf("attempt %d: CreateSnapshot: %v", i, err)
		}

		if resp.SnapshotID != "pvc-1/snap-1" {
			t.Errorf("attempt %d: SnapshotID: got %q, want %q", i, resp.SnapshotID, "pvc-1/snap-1")
		}
	}

	// Only one snapshot row regardless of retry count.
	snaps, err := st.Snapshots().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("ListByDefinition: %v", err)
	}

	if len(snaps) != 1 {
		t.Errorf("idempotent retries created %d rows, want 1", len(snaps))
	}
}

// TestCSICreateSnapshotMissingSourceVolume: csi-sanity's
// "CreateSnapshot should fail when the source volume is not specified"
// case. The driver MUST reject locally rather than POSTing a request
// the REST surface would silently slug into an empty {rd} segment.
func TestCSICreateSnapshotMissingSourceVolume(t *testing.T) {
	d := &Driver{Client: newLAPIClient(t, "http://127.0.0.1:1")} // unreachable on purpose

	_, err := d.CreateSnapshot(t.Context(), &CreateSnapshotRequest{
		SourceVolumeID: "",
		Name:           "snap-1",
	})
	if err == nil {
		t.Fatal("CreateSnapshot with empty SourceVolumeID: got nil error, want validation failure")
	}
}

// TestCSICreateSnapshotMissingName: csi-sanity's
// "CreateSnapshot should fail when the name field is missing" case.
func TestCSICreateSnapshotMissingName(t *testing.T) {
	d := &Driver{Client: newLAPIClient(t, "http://127.0.0.1:1")} // unreachable on purpose

	_, err := d.CreateSnapshot(t.Context(), &CreateSnapshotRequest{
		SourceVolumeID: "pvc-1",
		Name:           "",
	})
	if err == nil {
		t.Fatal("CreateSnapshot with empty Name: got nil error, want validation failure")
	}
}

// TestCSICreateSnapshotUnknownSourceVolume: source RD missing in
// blockstor → REST returns 404 → driver propagates the error so
// csi-snapshotter marks the VolumeSnapshot Error rather than
// retrying indefinitely.
func TestCSICreateSnapshotUnknownSourceVolume(t *testing.T) {
	st := store.NewInMemory()
	base, stop := startRESTServer(t, st)
	defer stop()

	d := &Driver{Client: newLAPIClient(t, base)}

	_, err := d.CreateSnapshot(t.Context(), &CreateSnapshotRequest{
		SourceVolumeID: "ghost",
		Name:           "snap-1",
	})
	if err == nil {
		t.Fatal("CreateSnapshot against unknown RD: got nil error, want 404 propagation")
	}
}

// --- helpers shared between 11.W08 and 11.W09 ---

// seedRDWithVolume registers an RD with one VolumeDefinition so the
// REST snapshot handler can hydrate SnapshotVolumeDef.SizeKib for
// the eventual size-validation step on the restore path.
func seedRDWithVolume(t *testing.T, st store.Store, rd string, sizeKib int64) {
	t.Helper()

	ctx := context.Background()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD %q: %v", rd, err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rd, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      sizeKib,
	}); err != nil {
		t.Fatalf("seed VD on RD %q: %v", rd, err)
	}
}

// newLAPIClient builds a real golinstor REST client pointed at the
// in-memory REST server. This is the same client linstor-csi uses,
// so the wire calls under test exercise the exact serialisation
// the production driver does.
func newLAPIClient(t *testing.T, base string) *lapi.Client {
	t.Helper()

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse base url %q: %v", base, err)
	}

	c, err := lapi.NewClient(lapi.BaseURL(u))
	if err != nil {
		t.Fatalf("lapi.NewClient: %v", err)
	}

	return c
}
