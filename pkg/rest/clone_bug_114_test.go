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

// Bug 114: `rd clone` returned HTTP 201 + a `ResourceDefinitionCloneStarted`
// envelope even when the resulting clone RD contained zero
// VolumeDefinitions — operators thought they had a usable clone but
// were left with an empty shell. The bug surfaced in operator-poke v2:
//
//   linstor rd c srcrd112
//   linstor vd c srcrd112 32M
//   linstor rd ap --place-count 2 -s stand -- srcrd112
//   linstor rd clone srcrd112 clone112
//   curl /v1/resource-definitions/clone112  # → no volume_definitions, no resources
//
// The previous handler shallow-copied only the RD's mutable spec
// fields (Props, RG ref) and never touched the inline VD store or
// triggered any satellite-side data copy. golinstor's CloneStatus
// poll then answered COMPLETE as soon as the empty RD existed,
// which lied to the operator about clone progress.
//
// The fix below pins the contract this REST surface honours today:
//
//   1. Source with no VolumeDefinitions clones to a target with no
//      VolumeDefinitions (matches Group D's seed-then-clone smoke
//      test) and clone-status answers COMPLETE — the operator
//      explicitly asked to clone a vol-less RD shell.
//   2. Source with at least one VolumeDefinition surfaces the
//      satellite-side data-plane gap: the apiserver refuses with
//      HTTP 501 + a LINSTOR `[]ApiCallRc` envelope carrying
//      `cause` + `correction`. We deliberately leave no half-baked
//      target RD behind, so `kubectl get rd <clone>` is `NotFound`
//      after the refuse.
//   3. clone-status reflects reality: COMPLETE only when the target
//      RD survived the POST and its VD count matches the source's.
//      A leftover empty target (legacy data, or a partial roll-back
//      that didn't reach the Delete) surfaces FAILED so linstor-csi
//      can stop polling.

// TestBug114CloneEmptySourceRefusedOrCreated covers the empty-source
// edge case used by Group D's smoke test: a freshly-created RD with
// no VolumeDefinitions clones successfully to a structurally
// equivalent empty RD. We must not break the existing copy-the-shell
// contract — the bug is specifically about the silently-empty target
// when the source had volumes.
func TestBug114CloneEmptySourceRefusedOrCreated(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "src-empty",
		Props: map[string]string{"Aux/cozystack.io/origin": "src"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "dst-empty"})

	resp := httpPost(t, base+"/v1/resource-definitions/src-empty/clone", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("empty-source clone: status got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "dst-empty")
	if err != nil {
		t.Fatalf("expected dst-empty to exist: %v", err)
	}

	if got.Props["Aux/cozystack.io/origin"] != "src" {
		t.Errorf("Props not copied: %v", got.Props)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst-empty")
	if err != nil {
		t.Fatalf("list dst VDs: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("expected zero VDs on cloned empty shell, got %d", len(vds))
	}
}

// TestBug114CloneNonEmptySourceRefusesWithEnvelope is the core
// regression guard. Source has 1 VolumeDefinition. The old handler
// answered 201 + a `Completed cloning clone112.` message while
// leaving the target VD-less. The new contract is:
//
//   - status 501 (Not Implemented)
//   - body is the upstream `ResourceDefinitionCloneStarted` object
//     shape — same envelope as the success path — but with the
//     `messages[]` field carrying the error, `cause`, and
//     `correction`. Mirroring upstream LINSTOR's
//     `mapToCloneStarted` keeps python-linstor's CLI from
//     crashing in `_rest_request_raw` when it decodes the body
//     straight into `CloneStarted`.
//   - no half-baked target RD persists; a follow-up GET on the
//     target name 404s
//
// If a future commit wires satellite-side data copy, flip this guard
// to assert the materialised state instead.
func TestBug114CloneNonEmptySourceRefusesWithEnvelope(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "srcrd114",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "srcrd114", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      32 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "clone114"})

	resp := httpPost(t, base+"/v1/resource-definitions/srcrd114/clone", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("non-empty-source clone: status got %d, want 501 (Not Implemented)",
			resp.StatusCode)
	}

	// Upstream LINSTOR returns the `ResourceDefinitionCloneStarted`
	// object shape on errors too — the error lives inside
	// `messages[]`. python-linstor's clone path decodes into
	// CloneStarted unconditionally; an array body would crash the
	// CLI before the error line reaches the operator.
	var started struct {
		Location   string            `json:"location"`
		SourceName string            `json:"source_name"`
		CloneName  string            `json:"clone_name"`
		Messages   []apiv1.APICallRc `json:"messages"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&started); err != nil {
		t.Fatalf("decode CloneStarted envelope: %v", err)
	}

	if started.SourceName != "srcrd114" {
		t.Errorf("source_name: got %q, want %q", started.SourceName, "srcrd114")
	}

	if started.CloneName != "clone114" {
		t.Errorf("clone_name: got %q, want %q", started.CloneName, "clone114")
	}

	if len(started.Messages) == 0 {
		t.Fatalf("expected at least one entry in messages[]")
	}

	first := started.Messages[0]

	if !strings.Contains(strings.ToLower(first.Message), "clone") ||
		!strings.Contains(strings.ToLower(first.Message), "not") {
		t.Errorf("message should explain clone is not implemented; got %q",
			first.Message)
	}

	if first.Cause == "" {
		t.Errorf("envelope missing cause field: %+v", first)
	}

	if first.Correc == "" {
		t.Errorf("envelope missing correction field: %+v", first)
	}

	// The half-implemented apiserver used to leave behind an empty
	// clone RD even on the bug path. The refuse must roll back any
	// partial state — operator's `kubectl get rd clone114` must
	// return NotFound.
	if _, err := st.ResourceDefinitions().Get(ctx, "clone114"); err == nil {
		t.Errorf("clone114 was persisted on the refuse path; expected NotFound rollback")
	}

	// And no VDs leaked onto it either.
	leftoverVDs, _ := st.VolumeDefinitions().List(ctx, "clone114")
	if len(leftoverVDs) != 0 {
		t.Errorf("VDs leaked under refused clone target: %d entries", len(leftoverVDs))
	}
}

// TestBug114CloneReportsAccurateStatus pins the clone-status surface
// against reality. golinstor's `CloneStatus` poll trusts the
// `status` enum verbatim — answering COMPLETE on an empty clone
// shell stops linstor-csi's wait loop while the data plane has not
// even started copying. The new contract:
//
//   - target RD missing → 404 (existing behaviour, unchanged)
//   - target RD exists but is structurally inconsistent with the
//     source (VD count mismatch) → FAILED so the caller surfaces
//     a concrete error rather than spinning forever
//   - target RD exists and VD counts match → COMPLETE
//
// We exercise the FAILED branch by directly seeding an empty
// target alongside a non-empty source — this is exactly the
// post-Bug-114 legacy state for any clone that completed under
// the old apiserver.
func TestBug114CloneReportsAccurateStatus(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src-status"}); err != nil {
		t.Fatalf("seed src: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "src-status", &apiv1.VolumeDefinition{
		VolumeNumber: 0, SizeKib: 1024,
	}); err != nil {
		t.Fatalf("seed src VD: %v", err)
	}

	// Empty target — represents a legacy half-cloned RD from the
	// pre-Bug-114 apiserver, or any future async-clone that has
	// not yet copied VDs.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "dst-empty-clone"}); err != nil {
		t.Fatalf("seed dst: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/src-status/clone/dst-empty-clone")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clone-status: got %d, want 200", resp.StatusCode)
	}

	var got struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode clone-status: %v", err)
	}

	if got.Status != "FAILED" {
		t.Errorf("clone-status of empty target with non-empty source: got %q, want FAILED",
			got.Status)
	}
}
