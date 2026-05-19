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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 136 / 139 / 140 cover the VolumeDefinition lifecycle gaps that
// drop operator-visible signal between the REST layer and the
// satellite reconciler:
//
//   - Bug 136 (P2): `linstor vd m X 0 64M` returns SUCCESS but the
//     satellite never re-applies storage — the underlying ZVOL/LV
//     stays at the old size. Operator sees a green response but the
//     filesystem can't grow into the new spec.
//
//   - Bug 139 (P2): `linstor vd d X 0` removes the spec entry, but
//     `view/resources` still surfaces the dropped volume in
//     Resource.Volumes until the satellite re-observes. The CRD
//     spec and the wire view disagree.
//
//   - Bug 140 (P2): `linstor vd c X 32M` twice in a row returns the
//     conflict on the second call wrapped as an info-band envelope
//     entry — looks like success in scripts that filter on `ret_code
//     >= 0`. The duplicate-create needs the MASK_ERROR flip + a
//     proper cause/correction so the upstream CLI surfaces the
//     refusal as an ERROR line.
//
// The bugs are bundled because they all live in / adjacent to
// pkg/rest/volume_definitions.go and the fixes share the cache /
// envelope plumbing from Bug 124 + Bug 92 / 118.

// resizePendingAnnotationKey matches the production constant in
// volume_definitions.go. Re-declared here (rather than imported) so
// a rename on the production side trips this test — the wire/CRD
// shape is the contract; the constant name is implementation detail.
//
// Per-volume to keep multi-volume RDs (rare today but on the
// roadmap) distinguishable when several volumes grow at once.
const bug136ResizePendingAnnotationPrefix = "bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-"

// TestBug136VDSetSizeTriggersSatelliteResize pins the REST→satellite
// signal: a PUT that grows a VolumeDefinition stamps a per-resource
// annotation naming the new size, so the satellite reconciler (and
// any operator running `kubectl get resource -o yaml`) can see that
// a resize is pending. Without the stamp, the apiserver-side spec
// changes but the satellite has no signal that there's per-replica
// work to drain. Bug 136 had this surface go silent.
func TestBug136VDSetSizeTriggersSatelliteResize(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName   = "pvc-bug136"
		nodeA    = "node-a"
		nodeB    = "node-b"
		volNum   = int32(0)
		oldSize  = int64(32 * 1024) // 32 MiB
		newSize  = int64(64 * 1024) // 64 MiB
		expected = "65536"          // newSize in KiB, decimal
	)

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err = st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: oldSize})
	if err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for _, node := range []string{nodeA, nodeB} {
		err := st.Resources().Create(ctx, &apiv1.Resource{Name: rdName, NodeName: node})
		if err != nil {
			t.Fatalf("seed Resource %s/%s: %v", rdName, node, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: newSize})

	resp := httpPut(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d", base, rdName, volNum),
		body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	// Annotation MUST land on every Resource of the affected RD.
	// The per-volume key suffix distinguishes a multi-volume grow
	// where some volumes shrink-rejected and some grew.
	wantKey := bug136ResizePendingAnnotationPrefix + fmt.Sprint(volNum)

	for _, node := range []string{nodeA, nodeB} {
		got, err := st.Resources().Get(ctx, rdName, node)
		if err != nil {
			t.Fatalf("re-get Resource %s/%s: %v", rdName, node, err)
		}

		if got.Annotations == nil {
			t.Errorf("Resource %s/%s: annotations nil after VD grow", rdName, node)

			continue
		}

		if got.Annotations[wantKey] != expected {
			t.Errorf("Resource %s/%s: annotation %q = %q, want %q",
				rdName, node, wantKey, got.Annotations[wantKey], expected)
		}
	}
}

// recordingProvider is a minimal storage.Provider that records every
// ResizeVolume call. The reconciler's existing applyStorage path is
// the production target — we wire a STORAGE-only DesiredResource
// through it and assert the resize landed. A regression that broke
// the `vol.GetSizeKib() > status.UsableKib` grow branch (or skipped
// the per-volume loop on a VD-only spec change) would show up as
// zero recorded calls here.
type recordingProvider struct {
	mu sync.Mutex

	resizes []storage.Volume

	// usable is what VolumeStatus returns for UsableKib. Set it
	// below the test's target size so the reconciler's grow gate
	// fires; set it above to assert the no-op path.
	usable int64
}

func (p *recordingProvider) Kind() string { return "FAKE" }

func (p *recordingProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{}, nil
}

func (p *recordingProvider) CreateVolume(_ context.Context, _ storage.Volume) error { return nil }

func (p *recordingProvider) DeleteVolume(_ context.Context, _ storage.Volume) error { return nil }

func (p *recordingProvider) ResizeVolume(_ context.Context, vol storage.Volume) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.resizes = append(p.resizes, vol)

	return nil
}

func (p *recordingProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{
		DevicePath:   "/dev/fake/" + vol.ResourceName,
		AllocatedKib: p.usable,
		UsableKib:    p.usable,
		State:        "PROVISIONED",
	}, nil
}

func (p *recordingProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (p *recordingProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (p *recordingProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}

func (p *recordingProvider) resizeSnapshot() []storage.Volume {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]storage.Volume, len(p.resizes))
	copy(out, p.resizes)

	return out
}

// TestBug136VDSetSizeStorageProviderResizeCalled pins the production
// satellite-side contract: when a DesiredResource lands with a
// SizeKib larger than the provider's reported UsableKib,
// `applyStorage` MUST call provider.ResizeVolume with the new size.
// This is the path the controller-runtime ResourceReconciler drives
// after a VD-grow REST PUT (see `enqueueResourcesForRD` in
// satellite/controllers/resource.go) — pinning it here keeps a
// regression that re-introduced Bug 136 from sneaking back via the
// satellite reconciler.
func TestBug136VDSetSizeStorageProviderResizeCalled(t *testing.T) {
	const (
		rdName  = "pvc-bug136-prov"
		node    = "n1"
		pool    = "fake1"
		oldSize = int64(32 * 1024)
		newSize = int64(64 * 1024)
	)

	prov := &recordingProvider{usable: oldSize}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: prov},
		StateDir:  t.TempDir(),
		NodeName:  node,
	})

	// STORAGE-only layer stack so the test stays off DRBD's
	// per-replica .res + drbdadm dependency tree — the resize
	// path under test is provider-level, not DRBD-level.
	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:       rdName,
			NodeName:   node,
			LayerStack: []string{"STORAGE"},
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: newSize, StoragePool: pool},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	resizes := prov.resizeSnapshot()
	if len(resizes) != 1 {
		t.Fatalf("ResizeVolume calls: got %d, want 1; details=%+v", len(resizes), resizes)
	}

	if resizes[0].ResourceName != rdName {
		t.Errorf("Resize ResourceName: got %q, want %q", resizes[0].ResourceName, rdName)
	}

	if resizes[0].VolumeNumber != 0 {
		t.Errorf("Resize VolumeNumber: got %d, want 0", resizes[0].VolumeNumber)
	}

	if resizes[0].SizeKib != newSize {
		t.Errorf("Resize SizeKib: got %d, want %d", resizes[0].SizeKib, newSize)
	}
}

// TestBug139VDDeleteRemovesVolumeFromViewProjection pins the wire-
// level invariant that motivated Bug 139: after DELETE
// /v1/resource-definitions/<rd>/volume-definitions/<vn> returns 200,
// the very next GET /v1/view/resources MUST NOT carry the dropped
// volume on the per-resource Volumes slice. Without the fix the
// projection trails the spec until the satellite re-observes — the
// CRD spec and the wire view disagree.
//
// The fix is dual: (a) drop the Volume entry from each Resource at
// view-render time when its VolumeNumber no longer matches any VD
// in the parent RD, AND (b) wait for the store's read path to
// observe the VD delete before responding so a re-issued GET on a
// laggy cache doesn't catch the pre-delete picture.
func TestBug139VDDeleteRemovesVolumeFromViewProjection(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName  = "pvc-bug139"
		node    = "node-a"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err = st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib})
	if err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Resource carries Status.Volumes with the volume entry — the
	// satellite would have stamped this on a previous reconcile.
	err = st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: node,
		Volumes: []apiv1.Volume{
			{VolumeNumber: volNum, DevicePath: "/dev/fake/" + rdName + "_00000"},
		},
	})
	if err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Sanity: the seeded volume is observable before the delete.
	pre := getViewResources(t, base)
	if len(pre) != 1 || len(pre[0].Volumes) != 1 {
		t.Fatalf("pre-delete view: got %+v, want 1 resource with 1 volume", pre)
	}

	// Bug 355: the pre-Delete walk now refuses only on
	// `state.in_use == true` (DRBD Primary with mounted consumer).
	// The seeded Resource is Secondary (state.in_use unset) so the
	// cascade path runs normally — no `?force=true` needed. Bug 139's
	// invariant is about what happens AFTER the delete is permitted,
	// so the cascade path is the right shape to keep the prune-on-
	// success path covered.
	delResp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", delResp.StatusCode)
	}

	// Wire-level invariant: the GET that runs *immediately* after
	// DELETE returns MUST NOT include the dropped volume on the
	// per-resource Volumes slice. Bug 139's symptom was the volume
	// surviving for tens of seconds (or indefinitely on a stuck
	// satellite reconciler).
	post := getViewResources(t, base)
	if len(post) != 1 {
		t.Fatalf("post-delete view: got %d resources, want 1; rows=%+v",
			len(post), post)
	}

	if len(post[0].Volumes) != 0 {
		t.Errorf("post-delete view: resource still carries %d volume(s) after VD delete: %+v",
			len(post[0].Volumes), post[0].Volumes)
	}
}

// TestBug140VDCreateConflictReturnsErrorMask pins the wire shape on a
// duplicate-VD create: the envelope's ret_code MUST have the
// MASK_ERROR bit (negative int64) set, NOT MASK_INFO. Scripts that
// filter on `ret_code >= 0` to detect success would otherwise treat
// the conflict as a no-op and skip the corrective action.
func TestBug140VDCreateConflictReturnsErrorMask(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug140"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32 * 1024},
	})

	// First create succeeds.
	first := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusOK {
		t.Fatalf("first POST status: got %d, want 200", first.StatusCode)
	}

	// Second create conflicts.
	second := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	defer func() { _ = second.Body.Close() }()

	if second.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate POST status: got %d, want 409", second.StatusCode)
	}

	var rcs []apiv1.APICallRc

	err = json.NewDecoder(second.Body).Decode(&rcs)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty on conflict; want at least one entry")
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set (apiCallRcError=%#x)",
			rcs[0].RetCode, apiCallRcError)
	}

	// Sub-code must be FAIL_EXISTS_VLM_DFN (upstream 502 | MASK_ERROR).
	// Audit-log greppers that route on the upstream catalogue need
	// the same sub-code, not a generic "high-bit set" error.
	if rcs[0].RetCode&apiCallRcFailExistsVlmDfn == 0 {
		t.Errorf("ret_code: got %#x, want FAIL_EXISTS_VLM_DFN (%#x) sub-code set",
			rcs[0].RetCode, apiCallRcFailExistsVlmDfn)
	}

	// Envelope MUST NOT carry the MASK_INFO bit on the conflict
	// entry — that was Bug 140's misclassification.
	if rcs[0].RetCode&maskInfo != 0 {
		t.Errorf("ret_code: got %#x, MASK_INFO bit set on a conflict envelope", rcs[0].RetCode)
	}
}

// TestBug140VDCreateConflictHasCorrection pins the operator-actionable
// payload on the duplicate-VD conflict envelope: cause names the
// conflict (the duplicate VolumeNumber + parent RD), correction
// points at the right remedial command. Without these the upstream
// Python CLI prints a bare "object already exists" line that doesn't
// tell the operator which knob to twist.
func TestBug140VDCreateConflictHasCorrection(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug140-cause"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err = st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32 * 1024})
	if err != nil {
		t.Fatalf("seed first VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32 * 1024},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc

	err = json.NewDecoder(resp.Body).Decode(&rcs)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty; want at least one entry")
	}

	if rcs[0].Cause == "" {
		t.Errorf("envelope missing cause field: %+v", rcs[0])
	}

	if rcs[0].Correc == "" {
		t.Errorf("envelope missing correction field: %+v", rcs[0])
	}

	// The cause must name the conflict so the operator knows what
	// was rejected. We don't lock the exact text (so the message
	// can evolve), but it must reference the parent RD and the
	// duplicate VolumeNumber so a `grep` on an audit log catches
	// the operator-relevant identifiers.
	for _, want := range []string{rdName, "volume"} {
		if !strings.Contains(strings.ToLower(rcs[0].Cause+" "+rcs[0].Message), strings.ToLower(want)) {
			t.Errorf("cause/message missing marker %q: cause=%q message=%q",
				want, rcs[0].Cause, rcs[0].Message)
		}
	}

	// ObjRefs let CLI tooling route the entry to the right object
	// when rendering — without RscDfn the Python CLI's group-by
	// renders the error against an empty resource label.
	if rcs[0].ObjRefs[objRefRscDfn] != rdName {
		t.Errorf("ObjRefs[RscDfn]: got %q, want %q",
			rcs[0].ObjRefs[objRefRscDfn], rdName)
	}
}
