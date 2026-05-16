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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 151 — `snapshot resource restore --to-resource <new>` creates an
// empty-shell target RD: the new RD has no resource_group_name
// reference, and the operator-poke-v4 repro reports a blank
// `resource_group_name` (and an unhelpful `r l` view that shows
// nothing placed). The CSI workflow papered over the gap by calling
// `rd ap` after the restore; the operator-driven `linstor s
// resource restore` CLI workflow was left with a non-functional
// target.
//
// The contract this REST surface honours after the fix:
//
//   1. Source RD with VolumeDefinitions + ResourceGroupName: the
//      restore copies VDs (existing behaviour, Bug 24) AND the
//      ResourceGroupName so `linstor rd l <new>` shows the parent
//      RG in the resource_group_name column. The new RD is in
//      "ready-to-place" shape — operator runs `rd ap` to materialise
//      replicas (deferred-place semantics; same as the CSI path).
//
//   2. Source RD with NO VolumeDefinitions is refused with HTTP 400
//      + a LINSTOR `[]ApiCallRc` envelope explaining the gap and
//      pointing at the snapshot-create + restore sequence. Without
//      this gate, the legacy handler created an empty-shell target
//      indistinguishable from a botched restore.

// TestBug151ResourceRestoreCreatesProperRD pins the
// resource_group_name carry-over: a source RD bound to a
// ResourceGroup must produce a target RD that carries the same
// ResourceGroupName. Pre-fix the field was silently dropped.
func TestBug151ResourceRestoreCreatesProperRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-bug151",
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "src-bug151",
		ResourceGroupName: "rg-bug151",
		LayerStack:        []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
		Props: map[string]string{
			"DrbdOptions/auto-quorum": "majority",
		},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "src-bug151", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      32 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-bug151",
		ResourceName: "src-bug151",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 32 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "dst-bug151",
		FromSnapshot: "snap-bug151",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/src-bug151/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("restore status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "dst-bug151")
	if err != nil {
		t.Fatalf("expected dst-bug151 to exist: %v", err)
	}

	// Bug 151's main complaint: the new RD's resource_group_name
	// was blank. The restore must carry over the source's RG
	// binding so `linstor rd l dst-bug151` shows the parent RG.
	if got.ResourceGroupName != "rg-bug151" {
		t.Errorf("Bug 151: ResourceGroupName: got %q, want %q "+
			"(restore must carry over source RG)",
			got.ResourceGroupName, "rg-bug151")
	}

	// VolumeDefinitions must also be hydrated — pre-Bug 24 the new
	// RD had zero VDs; the existing fix copies them. Re-pin here so
	// a refactor that broke either Bug 24 OR Bug 151 surfaces in one
	// place.
	vds, err := st.VolumeDefinitions().List(ctx, "dst-bug151")
	if err != nil {
		t.Fatalf("list dst VDs: %v", err)
	}

	if len(vds) != 1 {
		t.Errorf("expected 1 hydrated VD, got %d", len(vds))
	}

	if len(vds) > 0 && vds[0].SizeKib != 32*1024 {
		t.Errorf("VD size: got %d, want %d", vds[0].SizeKib, 32*1024)
	}

	// The restore-from-snapshot prop must still be stamped so the
	// satellite's RestoreVolumeFromSnapshot path picks the work up.
	if got.Props["BlockstorRestoreFromSnapshot"] != "src-bug151:snap-bug151" {
		t.Errorf("BlockstorRestoreFromSnapshot: got %q, want %q",
			got.Props["BlockstorRestoreFromSnapshot"], "src-bug151:snap-bug151")
	}
}

// TestBug151ResourceRestoreEmptySourceRefuses pins the "no-VDs source"
// refuse path. A snapshot taken of a vol-less RD shell is
// structurally meaningless to restore — the new RD would have no
// volumes to clone, and any follow-up autoplace would create empty
// Resources that never reach UpToDate.
//
// The handler must refuse with HTTP 400 + a `[]ApiCallRc` envelope
// (the upstream LINSTOR error shape for snapshot-restore) carrying
// `cause` + `correction` so python-linstor's CLI surfaces an
// actionable error.
//
// Rollback: no half-baked target RD persists; a follow-up GET on
// `dst-empty-bug151` returns NotFound.
func TestBug151ResourceRestoreEmptySourceRefuses(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Source RD with NO VolumeDefinitions.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "src-empty-bug151",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-empty-bug151",
		ResourceName: "src-empty-bug151",
		Nodes:        []string{"n1", "n2"},
		// No VolumeDefinitions on the snapshot either — matches the
		// vol-less-source repro.
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "dst-empty-bug151",
		FromSnapshot: "snap-empty-bug151",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/src-empty-bug151/snapshot-restore-resource", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (vol-less source must be refused)",
			resp.StatusCode)
	}

	// Envelope shape: upstream LINSTOR returns []ApiCallRc on the
	// snapshot-restore endpoint; the refuse must keep the same
	// wire shape so python-linstor's CLI prints the message line
	// rather than crashing in its decoder.
	var envelope []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(envelope) == 0 {
		t.Fatalf("expected at least one APICallRc entry")
	}

	first := envelope[0]
	if !strings.Contains(strings.ToLower(first.Message), "volume") {
		t.Errorf("message should mention missing volumes; got %q", first.Message)
	}

	// Rollback: no half-baked target RD must persist.
	if _, err := st.ResourceDefinitions().Get(ctx, "dst-empty-bug151"); err == nil {
		t.Errorf("dst-empty-bug151 was persisted on the refuse path; "+
			"expected NotFound rollback (got RD=%v)", err)
	}

	// No VDs leaked onto the would-be target either.
	leftover, _ := st.VolumeDefinitions().List(ctx, "dst-empty-bug151")
	if len(leftover) != 0 {
		t.Errorf("VDs leaked under refused restore target: %d entries", len(leftover))
	}
}
