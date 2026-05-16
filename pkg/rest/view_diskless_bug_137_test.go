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
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 137: `/v1/view/resources` omitted the `volumes` array entirely
// on diskless / tie-breaker replicas because the Resource CRD's
// `Status.Volumes` is empty for replicas with `Flags:[DISKLESS]` (and
// `[DISKLESS, TIE_BREAKER]`) — the satellite never writes per-volume
// usage rows for a replica that doesn't own any backing storage. The
// wire field then carried `,omitempty` on a nil slice, so the JSON
// encoder dropped the key. python-linstor's responses.py reads
// `rsc._rest_data['volumes']` and walks it unconditionally; absence
// returns `None` and the next attribute access (`vol.allocated_size`,
// `vol.state.disk_state`) crashes the CLI with
// `AttributeError: 'NoneType' object has no attribute ...`.
//
// Wire contract pinned by these tests: every replica in
// `/v1/view/resources` MUST surface a `volumes` array (possibly empty
// for a diskless witness, or populated with one placeholder per
// VolumeDefinition on the parent RD). The array is never absent,
// never null, regardless of replica Flags.

// TestBug137ViewResourcesDisklessHasVolumesArray pins the contract
// for a permanent diskless client (`Flags:[DISKLESS]`, NO
// TIE_BREAKER): the wire view must emit `volumes` as an array, not
// drop the key. Before the fix, python-linstor's `linstor r l` would
// crash on the diskless row in `_get_resource_volumes_state` because
// `rsc._rest_data['volumes']` returned `None`.
func TestBug137ViewResourcesDisklessHasVolumesArray(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-diskless",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// VD on the parent RD: a diskless replica conceptually still
	// participates in this volume slot (it ACK-s peer writes, just
	// has no local storage). The fix synthesises a placeholder
	// Volume per VD so python-linstor's
	// `rsc._rest_data['volumes'][0]` walk lands on a real entry.
	if err := st.VolumeDefinitions().Create(ctx, "pvc-diskless", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Permanent DRBD-diskless client. The satellite never writes
	// per-volume usage to Status.Volumes for this replica, so the
	// wire Volumes slice arrives as nil at the response builder.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-diskless",
		NodeName: "n1",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Loose-map decode: a strongly-typed `[]ResourceWithVolumes`
	// decode would silently swallow a missing-key regression into
	// the Go zero (nil slice), hiding exactly the contract this
	// test pins.
	var wire []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(wire) != 1 {
		t.Fatalf("len: got %d, want 1 (one diskless replica)", len(wire))
	}

	rsc := wire[0]

	raw, present := rsc["volumes"]
	if !present {
		t.Fatalf("Bug 137: diskless replica missing `volumes` key — python-linstor will crash on KeyError/None")
	}

	if raw == nil {
		t.Fatalf("Bug 137: diskless replica `volumes` is null — python-linstor will crash on NoneType")
	}

	vols, ok := raw.([]any)
	if !ok {
		t.Fatalf("Bug 137: diskless replica `volumes` is not a list: %T %v", raw, raw)
	}

	// One placeholder Volume per parent-RD VD. python-linstor walks
	// `vol.allocated_size`, `vol.state.disk_state` on every entry, so
	// each placeholder must carry the wire keys the strongly-typed
	// `Volume` shape always emits.
	if len(vols) != 1 {
		t.Fatalf("len(volumes): got %d, want 1 (one placeholder per VD)", len(vols))
	}

	vmap, ok := vols[0].(map[string]any)
	if !ok {
		t.Fatalf("volume[0] not an object: %T %v", vols[0], vols[0])
	}

	if got, _ := vmap["volume_number"].(float64); int32(got) != 0 {
		t.Errorf("volume_number: got %v, want 0", vmap["volume_number"])
	}

	// allocated_size_kib MUST always be present (Bug 112 contract);
	// for a diskless placeholder it stays at 0.
	if _, present := vmap["allocated_size_kib"]; !present {
		t.Errorf("volume[0] missing allocated_size_kib — Bug 112 regression on diskless placeholder")
	}
}

// TestBug137ViewResourcesTieBreakerHasVolumesArray is the
// TIE_BREAKER-flag variant of the diskless contract. Auto-witnesses
// stamped by `DrbdOptions/AutoAddQuorumTiebreaker` carry both
// DISKLESS and TIE_BREAKER; their on-wire shape must match a
// permanent diskless client (volumes array present, placeholder
// rows from VDs) so python-linstor's tiebreaker rendering doesn't
// crash on `None`.
func TestBug137ViewResourcesTieBreakerHasVolumesArray(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "witness"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-tb",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-tb", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-tb",
		NodeName: "witness",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed tiebreaker: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var wire []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(wire) != 1 {
		t.Fatalf("len: got %d, want 1", len(wire))
	}

	rsc := wire[0]

	raw, present := rsc["volumes"]
	if !present {
		t.Fatalf("Bug 137: TIE_BREAKER replica missing `volumes` key — python-linstor will crash on KeyError/None")
	}

	if raw == nil {
		t.Fatalf("Bug 137: TIE_BREAKER replica `volumes` is null — python-linstor will crash on NoneType")
	}

	vols, ok := raw.([]any)
	if !ok {
		t.Fatalf("Bug 137: TIE_BREAKER replica `volumes` is not a list: %T %v", raw, raw)
	}

	if len(vols) != 1 {
		t.Fatalf("len(volumes): got %d, want 1 (one placeholder per VD on tiebreaker)", len(vols))
	}

	vmap, ok := vols[0].(map[string]any)
	if !ok {
		t.Fatalf("volume[0] not an object: %T %v", vols[0], vols[0])
	}

	if _, present := vmap["allocated_size_kib"]; !present {
		t.Errorf("TIE_BREAKER volume[0] missing allocated_size_kib — Bug 112 regression on placeholder")
	}
}

// TestBug137RegularReplicaVolumesUnchanged is the regression guard
// for diskful replicas: the synthetic-placeholder path for diskless
// replicas must NOT alter the existing volumes wire shape of a
// satellite-reported diskful replica. The volumes the satellite
// observed (with real AllocatedKib, real DiskState) are passed
// through verbatim, never overwritten or shadowed by VD-derived
// placeholders.
func TestBug137RegularReplicaVolumesUnchanged(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-diskful",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-diskful", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Diskful replica with REAL satellite-reported volume data —
	// AllocatedKib + DiskState are the values python-linstor renders
	// for `Allocated` / `State` columns and the placeholder path
	// must not touch them.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-diskful",
		NodeName: "n1",
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			StoragePool:  "stand",
			DevicePath:   "/dev/drbd1000",
			AllocatedKib: 524288,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}},
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var wire []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(wire) != 1 {
		t.Fatalf("len: got %d, want 1", len(wire))
	}

	got := wire[0]

	if len(got.Volumes) != 1 {
		t.Fatalf("len(volumes): got %d, want 1", len(got.Volumes))
	}

	v := got.Volumes[0]

	if v.AllocatedKib != 524288 {
		t.Errorf("AllocatedKib: got %d, want 524288 (satellite-observed value must survive)", v.AllocatedKib)
	}

	if v.State.DiskState != "UpToDate" {
		t.Errorf("DiskState: got %q, want %q", v.State.DiskState, "UpToDate")
	}

	if v.DevicePath != "/dev/drbd1000" {
		t.Errorf("DevicePath: got %q, want %q", v.DevicePath, "/dev/drbd1000")
	}

	if v.StoragePool != "stand" {
		t.Errorf("StoragePool: got %q, want %q", v.StoragePool, "stand")
	}
}
