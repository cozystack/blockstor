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
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
)

// TestSnapshotsViewEmpty: aggregate is empty until something gets created.
func TestSnapshotsViewEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = resp.Body.Close() }()

	var got []apiv1.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestSnapshotsCreateRoundTrip: create through REST, see it via Get/View/List.
func TestSnapshotsCreateRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{
		Name:  "snap-1",
		Nodes: []string{"n1", "n2"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	// POST now returns an `[]ApiCallRc` envelope (upstream LINSTOR
	// convention; the Python CLI parses replies[0].ret_code). The
	// snapshot itself comes back via the View endpoint a few lines
	// below — assert its observable state, not the create echo.
	var rc []apiv1.APICallRc
	if jErr := json.NewDecoder(resp.Body).Decode(&rc); jErr != nil {
		t.Fatalf("decode: %v", jErr)
	}

	if len(rc) == 0 || rc[0].RetCode <= 0 {
		t.Errorf("ApiCallRc envelope: got %+v", rc)
	}

	// View aggregate must contain it.
	viewResp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = viewResp.Body.Close() }()

	var view []apiv1.Snapshot
	if jErr := json.NewDecoder(viewResp.Body).Decode(&view); jErr != nil {
		t.Fatalf("decode view: %v", jErr)
	}

	if len(view) != 1 {
		t.Errorf("view len: got %d, want 1", len(view))
	}
}

// TestSnapshotsListMissingRD: 404 on missing RD.
func TestSnapshotsListMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost/snapshots")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestSnapshotsListPerRD pins the per-RD list happy path: a GET on
// /v1/resource-definitions/{rd}/snapshots returns 200 + a bare JSON
// slice scoped to that RD only. linstor-csi calls this endpoint when
// reconciling a tenant's VolumeSnapshot lifecycle; if the handler ever
// regressed to leak snapshots from sibling RDs, two unrelated PVCs
// would see each other's snapshot metadata in the CSI driver — a
// silent multi-tenant boundary break.
func TestSnapshotsListPerRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD pvc-1: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-2"}); err != nil {
		t.Fatalf("seed RD pvc-2: %v", err)
	}

	for _, sn := range []apiv1.Snapshot{
		{Name: "s1", ResourceName: "pvc-1"},
		{Name: "s2", ResourceName: "pvc-1"},
		{Name: "s1", ResourceName: "pvc-2"},
	} {
		if err := st.Snapshots().Create(ctx, &sn); err != nil {
			t.Fatalf("seed snap: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-1/snapshots")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2 (only pvc-1's snapshots); %+v", len(got), got)
	}

	for _, s := range got {
		if s.ResourceName != "pvc-1" {
			t.Errorf("leak: got snapshot %s/%s in pvc-1 list", s.ResourceName, s.Name)
		}
	}
}

// TestSnapshotsDeleteThenGet: delete then 404.
func TestSnapshotsDeleteThenGet(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{Name: "s1", ResourceName: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	delResp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/snapshots/s1")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete: got %d, want 200", delResp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/snapshots/s1")
	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("post-delete: got %d, want 404", getResp.StatusCode)
	}
}

// TestSnapshotsViewFilters pins ?resources= and ?snapshots= on the
// cross-RD aggregate. linstor-csi's snapshot-existence poll arrives
// scoped to one RD + name; without filtering we'd return the whole
// cluster's snapshot list and force the client to scan.
func TestSnapshotsViewFilters(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, sn := range []apiv1.Snapshot{
		{Name: "s1", ResourceName: "pvc-1"},
		{Name: "s2", ResourceName: "pvc-1"},
		{Name: "s1", ResourceName: "pvc-2"},
	} {
		if err := st.Snapshots().Create(ctx, &sn); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	cases := []struct {
		query string
		want  int
	}{
		{"resources=pvc-1", 2},
		{"snapshots=s1", 2},
		{"resources=pvc-1&snapshots=S2", 1},
	}

	for _, tc := range cases {
		resp := httpGet(t, base+"/v1/view/snapshots?"+tc.query)

		var got []apiv1.Snapshot

		err := json.NewDecoder(resp.Body).Decode(&got)
		_ = resp.Body.Close()

		if err != nil {
			t.Errorf("%s decode: %v", tc.query, err)

			continue
		}

		if len(got) != tc.want {
			t.Errorf("%s: got %d entries, want %d (%v)", tc.query, len(got), tc.want, got)
		}
	}
}

// TestSnapshotsCreateBadJSON: malformed body → 400.
func TestSnapshotsCreateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSnapshotsCreateMissingName: empty snap name → 400 + body marker.
// linstor-csi derives the snap name from a VolumeSnapshot UID — a
// regression that allowed nameless snapshots would orphan rows that
// no later reconcile can address. csi-sanity's "CreateSnapshot should
// fail when the name field is missing" feeds in an empty CSI
// snapshot-name; linstor-csi forwards that as `{"name": ""}` and the
// handler must surface the marker text in the response so the driver
// can echo it into the CSI gRPC error.
func TestSnapshotsCreateMissingName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Nodes: []string{"n1"}}) // Name omitted
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	bodyBytes := make([]byte, 1024)
	n, _ := resp.Body.Read(bodyBytes)
	got := string(bodyBytes[:n])

	if !strings.Contains(got, "snapshot name is required") {
		t.Errorf("body: got %q, want it to contain 'snapshot name is required'", got)
	}
}

// TestSnapshotsCreateWhitespaceOnlyName pins TrimSpace on snap.Name.
// A `"   "` payload previously slipped past the bare `== ""` guard
// and persisted an unaddressable snapshot row — zfs barfs on the
// whitespace snap name later in the satellite reconcile, but
// linstor-csi has already seen the 201 response and never retries.
// csi-sanity's "CreateSnapshot empty-name" parametrisation includes
// the whitespace shape, so this nails the (c) wire-shape gap end-to-end.
func TestSnapshotsCreateWhitespaceOnlyName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "   ", Nodes: []string{"n1"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	bodyBytes := make([]byte, 1024)
	n, _ := resp.Body.Read(bodyBytes)
	got := string(bodyBytes[:n])

	if !strings.Contains(got, "snapshot name is required") {
		t.Errorf("body: got %q, want it to contain 'snapshot name is required'", got)
	}
}

// TestSnapshotsCreateValidNameReturns201 pins the happy path after
// the empty-name guard: a well-formed payload still gets 201 +
// ApiCallRc envelope. Without this, an over-zealous trim regression
// (e.g. trimming the JSON-decoded name in-place and emptying valid
// content) would slip through CI silently.
func TestSnapshotsCreateValidNameReturns201(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "foo", Nodes: []string{"n1"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].RetCode <= 0 {
		t.Errorf("ApiCallRc envelope: got %+v, want non-empty with positive ret_code", rc)
	}
}

// TestSnapshotRollback501WithActionableText pins the deliberate 501
// response for `POST .../snapshots/{snap}/rollback`. blockstor refuses
// to expose `zfs rollback` (which destroys every snapshot newer than
// the rollback target — silent data loss), so the route exists only
// to return a structured ApiCallRc error pointing the operator at the
// non-destructive `snapshot-restore-resource` path. A regression to
// 404 would make upstream `linstor snapshot rollback` print
// "unable to parse server response" and confuse the operator into
// thinking the controller crashed.
func TestSnapshotRollback501WithActionableText(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "s1", ResourceName: "pvc-1"}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots/s1/rollback", []byte("{}"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want 501", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("empty ApiCallRc envelope")
	}

	// Hard error, NOT maskInfo — `linstor` CLI prints the message
	// verbatim and treats this as failure. A non-negative ret_code
	// would have it cheerfully report "success" for a no-op.
	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code: got %#x, want negative (error)", rc[0].RetCode)
	}

	if !strings.Contains(rc[0].Message, "snapshot-restore-resource") {
		t.Errorf("message missing actionable pointer to snapshot-restore-resource: %q", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "non-destructive") {
		t.Errorf("message missing 'non-destructive' rationale: %q", rc[0].Message)
	}
}

// TestSnapshotRollbackUnknownSnap404 pins the input-validation order:
// an unknown (rd, snap) returns 404 (not 501) so the operator learns
// about the typo BEFORE they learn rollback isn't supported. Mirrors
// upstream LINSTOR's "validate the snapshot reference first, then run
// the strategy" sequencing.
func TestSnapshotRollbackUnknownSnap404(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/snapshots/nope/rollback", []byte("{}"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestSnapshotsDeleteUnknownRD pins one half of the CSI idempotence
// contract: DELETE on an unknown {rd} path segment returns 200 +
// ApiCallRc("snapshot already absent: ..."), NOT 404. csi-sanity's
// "DeleteSnapshot should succeed when an invalid snapshot id is used"
// feeds in a (volume-id, snap-id) pair that decomposes to (rd, snap)
// where the rd never existed; a 404 on that path breaks both the
// spec contract and the second-delete-after-success retry loop the
// CSI driver runs. The "already absent" message lets operators
// distinguish a real drop from a no-op replay in API logs.
func TestSnapshotsDeleteUnknownRD(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/ghost/snapshots/s1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].RetCode <= 0 {
		t.Errorf("ApiCallRc envelope: got %+v, want non-empty with positive ret_code", rc)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}
}

// TestSnapshotsDeleteKnownRDUnknownSnap pins the other half of the
// idempotence contract: once the RD exists the handler must still
// fold a missing per-snap row into success rather than 404,
// otherwise the CSI retry loop after a partial DeleteSnapshot
// success stalls. Same "already absent" message as the unknown-RD
// branch so operators reading the API log get one consistent
// no-op marker.
func TestSnapshotsDeleteKnownRDUnknownSnap(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/snapshots/ghost-snap")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].RetCode <= 0 {
		t.Errorf("ApiCallRc envelope: got %+v, want non-empty with positive ret_code", rc)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "ghost-snap") {
		t.Errorf("message: got %q, want it to name the missing snapshot", rc[0].Message)
	}
}

// TestSnapshotDeleteUsesWarnMaskOnAbsent pins cli-parity-audit row #33
// (also alignment with the Bug 56/66 family for resource/RD delete on
// missing objects): the "snapshot already absent" envelope must carry
// the warn-mask bit so it normalises to the <warn> bucket, not <info>.
//
// Upstream LINSTOR emits `WARNING: Snapshot definition <snap> of
// resource <rd> not found.` exit 0 on this input. blockstor was
// returning the right idempotent-200 (CSI is happy) but with maskInfo
// — so tools that classify replies by mask put a no-op replay into
// the SUCCESS bucket. Tooling like the contract normaliser at
// tests/contract/normalize.go then misclassified the row vs upstream,
// surfacing as a parity miss in the wire-shape audit.
//
// The earlier maskInfo-on-absent fix is preserved here as a regression
// guard — flipping the mask back would surface both a maskInfo-bit
// drop and a maskWarn-bit miss in this assertion pair.
func TestSnapshotDeleteUsesWarnMaskOnAbsent(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/ghost-rd/snapshots/ghost-snap")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (idempotent)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) != 1 {
		t.Fatalf("envelope entries: got %d, want 1; got=%+v", len(rc), rc)
	}

	// WARN bit MUST be set. Without it, the contract normaliser
	// (tests/contract/normalize.go) classifies the row as <info>
	// instead of <warn> and the parity audit re-opens.
	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code = %#x, want maskWarn (%#x) bit set", rc[0].RetCode, maskWarn)
	}

	// ret_code must still be non-negative — golinstor's "is this a
	// fatal error?" check is `ret_code < 0` (MASK_ERROR sign bit).
	// Flipping the sign would turn idempotent "already absent" into
	// a hard CSI failure.
	if rc[0].RetCode < 0 {
		t.Errorf("ret_code = %#x, leaked sign bit (became error)", rc[0].RetCode)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "ghost-snap") {
		t.Errorf("message: got %q, want it to name the missing snapshot", rc[0].Message)
	}

	// ObjRefs should carry the RD and Snapshot definition names —
	// tooling that does cross-correlation against `linstor err l`
	// uses these to link an ApiCallRc reply to the affected object.
	if rc[0].ObjRefs["RscDfn"] != "ghost-rd" {
		t.Errorf("ObjRefs[RscDfn]: got %q, want 'ghost-rd'", rc[0].ObjRefs["RscDfn"])
	}

	if rc[0].ObjRefs["SnapshotDfn"] != "ghost-snap" {
		t.Errorf("ObjRefs[SnapshotDfn]: got %q, want 'ghost-snap'", rc[0].ObjRefs["SnapshotDfn"])
	}
}

// TestSnapshotsDeleteExistingReturnsEnvelopeAndDrops pins the happy
// path: DELETE on a really-present snapshot returns 200 +
// ApiCallRc("snapshot deleted: ...") AND the row actually leaves the
// store. Without the storage probe a 200 envelope that quietly left
// the snapshot in place would silently leak entries on every CSI
// retry.
func TestSnapshotsDeleteExistingReturnsEnvelopeAndDrops(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{Name: "s1", ResourceName: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/snapshots/s1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].RetCode <= 0 {
		t.Errorf("ApiCallRc envelope: got %+v, want non-empty with positive ret_code", rc)
	}

	if !strings.Contains(rc[0].Message, "snapshot deleted") || !strings.Contains(rc[0].Message, "s1") {
		t.Errorf("message: got %q, want 'snapshot deleted: s1'", rc[0].Message)
	}

	if _, err := st.Snapshots().Get(t.Context(), "pvc-1", "s1"); err == nil {
		t.Errorf("snapshot still in store after delete")
	}
}

// TestSnapshotsViewEmptyRendersBracketsNotNull pins the empty-store
// wire envelope: GET /v1/view/snapshots on an empty cluster must
// serialise as the literal byte sequence `[]`, never `null`.
// linstor-csi's ListSnapshots decoder treats a `null` body as
// malformed and surfaces it as `Internal` — csi-sanity's
// "should return empty when the specified snapshot id does not
// exist" assertion fires on the malformed-envelope path, not on
// the snapshot-id mismatch. The fix lives at the wire edge in
// `handleSnapshotsView`/`paginateSnapshots`, but the contract worth
// pinning here is the byte-level shape, not just the decoded-slice
// emptiness — a future regression that flips back to nil-slice
// would deserialise to len==0 in the helper but emit `null` on the
// wire, which is the actual failure mode.
func TestSnapshotsViewEmptyRendersBracketsNotNull(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	got := strings.TrimSpace(string(body))
	if got != "[]" {
		t.Errorf("body: got %q, want literal `[]` (not `null`)", got)
	}
}

// TestSnapshotsViewPaginationMaxEntriesPartial pins csi-sanity's
// "should return snapshots when given a max entries" contract for
// the four-of-two case: 4 snapshots in the store, max_entries=2
// (→ ?limit=2). First page returns 2 entries; CSI then issues the
// next call with starting_token=2 (→ ?offset=2&limit=2), which
// must return the remaining 2 entries; a follow-up at offset=4
// must return `[]` (the wire signal for "no more pages" — there is
// no next_token envelope on this REST surface, the CSI client
// keys end-of-data off the empty array).
func TestSnapshotsViewPaginationMaxEntriesPartial(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, name := range []string{"s1", "s2", "s3", "s4"} {
		if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: name, ResourceName: "pvc-1",
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Page 1: offset=0, limit=2 → 2 entries, more remain.
	page1 := decodeSnapshotPage(t, base+"/v1/view/snapshots?limit=2")
	if len(page1) != 2 {
		t.Fatalf("page 1 len: got %d, want 2 (%+v)", len(page1), page1)
	}

	// Page 2: offset=2, limit=2 → 2 entries, exact-fit end.
	page2 := decodeSnapshotPage(t, base+"/v1/view/snapshots?offset=2&limit=2")
	if len(page2) != 2 {
		t.Fatalf("page 2 len: got %d, want 2 (%+v)", len(page2), page2)
	}

	// Page 3 (the "no more pages" probe CSI emits after exact-fit):
	// offset=4 → `[]` literal, signalling end-of-data.
	resp := httpGet(t, base+"/v1/view/snapshots?offset=4&limit=2")
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if got := strings.TrimSpace(string(body)); got != "[]" {
		t.Errorf("page 3 body: got %q, want literal `[]`", got)
	}
}

// TestSnapshotsViewPaginationMaxEntriesExactFit pins the
// "max_entries exactly equals the store size" edge — csi-sanity's
// pagination-edge test. 2 snapshots in the store, max_entries=2
// (→ ?limit=2): the response carries both entries, and the CSI
// client's follow-up call at starting_token=2 (→ ?offset=2)
// must return `[]` so the sidecar sees the empty wire envelope
// as "no next page". Before this fix, paginateSnapshots could
// silently emit `null` on that follow-up, which linstor-csi
// rejects as a malformed body.
func TestSnapshotsViewPaginationMaxEntriesExactFit(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, name := range []string{"s1", "s2"} {
		if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: name, ResourceName: "pvc-1",
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	page1 := decodeSnapshotPage(t, base+"/v1/view/snapshots?limit=2")
	if len(page1) != 2 {
		t.Fatalf("page 1 len: got %d, want 2 (%+v)", len(page1), page1)
	}

	// Follow-up at offset=2 must serialise as `[]`, never `null`.
	resp := httpGet(t, base+"/v1/view/snapshots?offset=2&limit=2")
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if got := strings.TrimSpace(string(body)); got != "[]" {
		t.Errorf("end-of-data body: got %q, want literal `[]`", got)
	}
}

// TestSnapshotListSurfacesFailedFlag pins the F18 cli-parity
// fix's wire-shape half: a Snapshot CRD whose Status.Flags
// carries "FAILED" (stamped by the satellite reconciler on a
// terminal CreateSnapshot error) MUST surface that flag on the
// /v1/view/snapshots wire response, so the Python CLI's
// `linstor s l` State column can render "Failed" instead of
// "Incomplete".
//
// A regression that dropped Status.Flags in crdToWireSnapshot
// would leave the CLI showing "Incomplete" forever for a
// dead-letter snapshot, which the operator has no way to
// distinguish from a still-in-progress one — and CSI's
// CreateSnapshot success-poll loop would never surface the
// failure either.
//
// Uses the CRD-backed store (not the in-memory one) because
// Status.Flags is a CRD-only field — the in-memory store
// already carries Flags on the wire shape directly, so it
// can't exercise the crdToWireSnapshot translation that is
// the actual subject under test.
func TestSnapshotListSurfacesFailedFlag(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor: %v", err)
	}

	// Seed a CRD directly with Status.Flags=["FAILED"] —
	// emulates what the satellite reconciler stamps on the
	// terminal-error path. WithStatusSubresource so the fake
	// client honours the Status subresource semantic (without
	// it, the seeded Status.Flags would get dropped on the
	// initial Create the fake performs internally).
	crd := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			Flags: []string{blockstoriov1alpha1.SnapshotStatusFlagFailed},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(crd).
		WithStatusSubresource(crd).
		Build()

	st := k8sstore.New(cli)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1 (%+v)", len(got), got)
	}

	if !slices.Contains(got[0].Flags, "FAILED") {
		t.Errorf("Flags: got %v, want it to contain FAILED", got[0].Flags)
	}
}

// decodeSnapshotPage is a tiny helper used by the pagination tests
// to keep the page-decode boilerplate out of the assertion sites.
func decodeSnapshotPage(t *testing.T, url string) []apiv1.Snapshot {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %s: got %d, want 200", url, resp.StatusCode)
	}

	var got []apiv1.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}

	return got
}

// TestSnapshotListIncludesSnapshotVolumes pins F20's CLI-parity DTO
// shape: every per-node `snapshots[]` entry must carry one
// `snapshot_volumes[]` slot per VolumeDefinition the snapshot
// captured. `linstor backup` and the snapshot-shipping tooling read
// `vlm_nr` from this list to address per-volume satellite RPCs;
// without it the CLI's per-node table loses the volume column.
func TestSnapshotListIncludesSnapshotVolumes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-1",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "snap-1", Nodes: []string{"n1"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Snapshots().Get(ctx, "pvc-1", "snap-1")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	if len(got.Snapshots) != 1 {
		t.Fatalf("Snapshots[] len: got %d, want 1; %+v", len(got.Snapshots), got.Snapshots)
	}

	if len(got.Snapshots[0].SnapshotVolumes) != 1 {
		t.Fatalf("snapshot_volumes[] len: got %d, want 1; %+v",
			len(got.Snapshots[0].SnapshotVolumes), got.Snapshots[0].SnapshotVolumes)
	}

	if got.Snapshots[0].SnapshotVolumes[0].VolumeNumber != 0 {
		t.Errorf("snapshot_volumes[0].vlm_nr: got %d, want 0",
			got.Snapshots[0].SnapshotVolumes[0].VolumeNumber)
	}
}

// TestSnapshotListIncludesVolumeDefinitionProps pins F20: each
// `volume_definitions[]` slot on the snapshot DTO carries the parent
// RD's per-volume props bag (`volume_definition_props`). The upstream
// CLI reads inherited keys (e.g. `Aux/PvcName`) through the snapshot
// view rather than re-fetching the parent RD per-snapshot.
func TestSnapshotListIncludesVolumeDefinitionProps(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-1",
		&apiv1.VolumeDefinition{
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
			Props:        map[string]string{"Aux/source": "pvc-orig"},
		}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "snap-1", Nodes: []string{"n1"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", resp.StatusCode)
	}

	got := decodeSnapshotPage(t, base+"/v1/view/snapshots")
	if len(got) != 1 {
		t.Fatalf("view len: got %d, want 1", len(got))
	}

	if len(got[0].VolumeDefinitions) != 1 {
		t.Fatalf("volume_definitions len: got %d, want 1; %+v",
			len(got[0].VolumeDefinitions), got[0].VolumeDefinitions)
	}

	if got[0].VolumeDefinitions[0].VolumeDefinitionProps["Aux/source"] != "pvc-orig" {
		t.Errorf("volume_definition_props: got %v, want Aux/source=pvc-orig",
			got[0].VolumeDefinitions[0].VolumeDefinitionProps)
	}
}

// TestSnapshotListIncludesRDProps pins F20: the snapshot DTO carries
// a snapshot-time copy of the parent RD's `props` bag as
// `resource_definition_props`. `linstor backup` reads inherited keys
// (e.g. `DrbdOptions/auto-resync-after`) through the snapshot view —
// re-fetching the RD per-snapshot would be racy if the RD's props
// mutate between snapshot-take and the read.
func TestSnapshotListIncludesRDProps(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-1",
		Props: map[string]string{"Aux/owner": "team-a"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "snap-1", Nodes: []string{"n1"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", resp.StatusCode)
	}

	got := decodeSnapshotPage(t, base+"/v1/view/snapshots")
	if len(got) != 1 {
		t.Fatalf("view len: got %d, want 1", len(got))
	}

	if got[0].ResourceDefinitionProps["Aux/owner"] != "team-a" {
		t.Errorf("resource_definition_props: got %v, want Aux/owner=team-a",
			got[0].ResourceDefinitionProps)
	}
}

// TestSnapshotListEmptyResourceDoesntPanic pins the orphan-snapshot
// path: a Snapshot row whose parent RD has been deleted out from
// under it (e.g. an in-flight delete reordering) must still render
// via the view endpoint without nil-dereferencing on the missing
// parent. The new F20 derived fields (`resource_definition_props`,
// per-VD `volume_definition_props`) gracefully degrade to empty
// when the parent is gone.
func TestSnapshotListEmptyResourceDoesntPanic(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed a snapshot WITHOUT seeding the parent RD — simulates the
	// orphan path. The in-memory store accepts a snapshot whose
	// resource_name does not resolve to an RD; the view layer must
	// handle the dangling reference rather than panicking.
	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-orphan",
		ResourceName: "ghost-rd",
	}); err != nil {
		t.Fatalf("seed orphan snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := decodeSnapshotPage(t, base+"/v1/view/snapshots")
	if len(got) != 1 {
		t.Fatalf("view len: got %d, want 1; %+v", len(got), got)
	}

	// New F20 derived fields must be empty (zero map / nil slice),
	// never carry stale values from a previous render.
	if len(got[0].ResourceDefinitionProps) != 0 {
		t.Errorf("ResourceDefinitionProps on orphan: got %v, want empty",
			got[0].ResourceDefinitionProps)
	}

	for i, vd := range got[0].VolumeDefinitions {
		if len(vd.VolumeDefinitionProps) != 0 {
			t.Errorf("VolumeDefinitions[%d].VolumeDefinitionProps on orphan: got %v, want empty",
				i, vd.VolumeDefinitionProps)
		}
	}
}
