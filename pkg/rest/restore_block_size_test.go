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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedBlockSizeSnapshot stores a source definition and a snapshot of it
// whose volume carries vdProps.
func seedBlockSizeSnapshot(t *testing.T, vdProps map[string]string) *store.InMemory {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err = st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1"},
		Snapshots:    []apiv1.SnapshotPerNode{{NodeName: "n1", StoragePool: "pool"}},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, VolumeDefinitionProps: vdProps},
		},
	})
	if err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	return st
}

func bsRestoreInto(t *testing.T, base, target string, nodes ...string) {
	t.Helper()

	body, _ := json.Marshal(map[string]any{"to_resource": target, "from_snapshot": "snap-1", "node_names": nodes})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("restore status: got %d, want 201", resp.StatusCode)
	}
}

func bsVolumeProps(t *testing.T, st *store.InMemory, rd string) map[string]string {
	t.Helper()

	vd, err := st.VolumeDefinitions().Get(t.Context(), rd, 0)
	if err != nil {
		t.Fatalf("restored volume definition: %v", err)
	}

	return vd.Props
}

// TestRestoreKeepsSourceBlockSize pins the restore half of the issue: the
// restored volume definition gets the snapshot's copy of its source's
// props, block size and freeze included, as LINSTOR's restore copies
// them. Before, it got none, and the restored device came up with
// 512-byte sectors under data written at 4096.
func TestRestoreKeepsSourceBlockSize(t *testing.T) {
	t.Parallel()

	st := seedBlockSizeSnapshot(t, map[string]string{
		blocksize.PropBlockSize: "4096",
		blocksize.PropFreeze:    "True",
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	bsRestoreInto(t, base, "pvc-dst")

	props := bsVolumeProps(t, st, "pvc-dst")
	if props[blocksize.PropBlockSize] != "4096" || !blocksize.Frozen(props) {
		t.Fatalf("restored volume props %v, want block-size 4096 frozen", props)
	}
}

// TestRestoreOfSourceWithoutBlockSizeKeepsDefault: a source that carried
// no size ran at DRBD's default, and the restored definition says so, so
// the controller does not give its data the pool's size.
func TestRestoreOfSourceWithoutBlockSizeKeepsDefault(t *testing.T) {
	t.Parallel()

	st := seedBlockSizeSnapshot(t, nil)

	base, stop := startServerWithStore(t, st)
	defer stop()

	bsRestoreInto(t, base, "pvc-dst")

	if got := bsVolumeProps(t, st, "pvc-dst")[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("restored block-size %q, want 512", got)
	}
}

// TestCSIRestoreOrderKeepsSourceBlockSize follows linstor-csi: it creates
// the target definition, restores the volume definitions, then restores
// the resource. The volume-definition restore copies no props, as in
// LINSTOR; the resource restore lays the snapshot's over them because the
// target has no replica yet.
func TestCSIRestoreOrderKeepsSourceBlockSize(t *testing.T) {
	t.Parallel()

	st := seedBlockSizeSnapshot(t, map[string]string{blocksize.PropBlockSize: "4096"})

	err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-dst"})
	if err != nil {
		t.Fatalf("seed target: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"to_resource": "pvc-dst"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-volume-definition/snap-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("volume-definition restore status: got %d, want 200", resp.StatusCode)
	}

	if got := bsVolumeProps(t, st, "pvc-dst")[blocksize.PropBlockSize]; got != "" {
		t.Fatalf("volume-definition restore copied block-size %q; LINSTOR's copies none", got)
	}

	// linstor-csi restores onto the one node it picked.
	bsRestoreInto(t, base, "pvc-dst", "n1")

	if got := bsVolumeProps(t, st, "pvc-dst")[blocksize.PropBlockSize]; got != "4096" {
		t.Fatalf("restored block-size %q, want the source's 4096", got)
	}
}

// TestRestoreWithAutoOffAboveWritesNoSize: with Linstor/Drbd/auto-block-size
// false on the controller, or on the target definition linstor-csi made,
// a source without a size restores without one, as LINSTOR's restore
// writes none. Only the snapshot's own props used to be read for the
// switch, so the restore wrote 512 anyway.
func TestRestoreWithAutoOffAboveWritesNoSize(t *testing.T) {
	t.Parallel()

	off := map[string]string{blocksize.PropAuto: "false"}

	t.Run("controller", func(t *testing.T) {
		t.Parallel()

		st := seedBlockSizeSnapshot(t, nil)
		if err := st.ControllerProps().Set(t.Context(), off); err != nil {
			t.Fatalf("seed controller props: %v", err)
		}

		base, stop := startServerWithStore(t, st)
		defer stop()

		bsRestoreInto(t, base, "pvc-dst")

		if got, ok := bsVolumeProps(t, st, "pvc-dst")[blocksize.PropBlockSize]; ok {
			t.Fatalf("restored block-size %q with the automatic size off", got)
		}
	})

	t.Run("target-definition", func(t *testing.T) {
		t.Parallel()

		st := seedBlockSizeSnapshot(t, nil)
		if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-dst", Props: off}); err != nil {
			t.Fatalf("seed target: %v", err)
		}

		base, stop := startServerWithStore(t, st)
		defer stop()

		body, _ := json.Marshal(map[string]string{"to_resource": "pvc-dst"})

		resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-volume-definition/snap-1", body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("volume-definition restore status: got %d, want 200", resp.StatusCode)
		}

		bsRestoreInto(t, base, "pvc-dst", "n1")

		if got, ok := bsVolumeProps(t, st, "pvc-dst")[blocksize.PropBlockSize]; ok {
			t.Fatalf("restored block-size %q with the automatic size off", got)
		}
	})
}
