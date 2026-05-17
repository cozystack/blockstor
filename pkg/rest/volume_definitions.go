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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// volumeDefinitionModifyBody is the shape upstream golinstor sends on
// `PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}` — driven
// by `linstor vd set-size`, `linstor vd set-property`, and the CSI
// ControllerExpandVolume path. Top-level fields are a modify delta,
// not the full VD spec.
//
// SizeKib is a pointer so we can distinguish "client omitted size_kib"
// (preserve existing) from "client sent size_kib=0" (explicit zero).
// Wholesale Decode(&VolumeDefinition) would conflate the two and the
// satellite reconciler's `vol.GetSizeKib() > status.UsableKib` grow
// branch would never fire after a no-op props-only modify because
// SizeKib was silently zeroed. See Bug 36 (4.6 audit).
type volumeDefinitionModifyBody struct {
	OverrideProps    map[string]string `json:"override_props,omitempty"`
	DeleteProps      []string          `json:"delete_props,omitempty"`
	DeleteNamespaces []string          `json:"delete_namespaces,omitempty"`
	SizeKib          *int64            `json:"size_kib,omitempty"`
	Flags            []string          `json:"flags,omitempty"`

	// Props mirrors the legacy callers that PUT the full VolumeDefinition
	// shape (matches the read-side wire field). Treated as an override
	// overlay on the existing Props map — equivalent to OverrideProps.
	Props map[string]string `json:"props,omitempty"`

	// Force is the wave2 4.W13 escape hatch for a spec-shrink: when the
	// operator has already shrunk the backing filesystem out-of-band
	// (`resize2fs -s <new-size>`, etc.) they need a way to bring the
	// LINSTOR spec back into sync with the now-smaller FS. Upstream
	// LINSTOR rejects all shrinks unconditionally; blockstor matches
	// that default but accepts force=true as an opt-in. Also honoured
	// via the `?force=true` query parameter so ad-hoc `curl` scripts
	// don't have to re-shape a golinstor payload.
	Force bool `json:"force,omitempty"`

	// VolumeNumber + UUID round-trip the read-side `apiv1.VolumeDefinition`
	// shape that legacy callers PUT verbatim — the path's `{vn}` segment
	// remains authoritative, but accepting the body-side field keeps
	// Bug 161's DisallowUnknownFields gate from breaking
	// `json.Marshal(apiv1.VolumeDefinition{...})` callers that send the
	// full read-side object. The handler reads VolumeNumber from the path
	// and ignores the body value (see TestVDSetSizeUsesPathVolumeNumber
	// in volume_definitions_test.go); UUID is similarly informational.
	VolumeNumber int32  `json:"volume_number,omitempty"`
	UUID         string `json:"uuid,omitempty"`
}

// registerVolumeDefinitions wires
// /v1/resource-definitions/{rd}/volume-definitions[/{vn}] CRUD.
func (s *Server) registerVolumeDefinitions(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/volume-definitions",
		s.requireStore(s.handleVDView))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/volume-definitions",
		s.requireStore(s.handleVDList))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/volume-definitions",
		s.requireStore(s.handleVDCreate))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDGet))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDUpdate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDDelete))
	// Bug 233 (P3): per-VD LUKS passphrase rotation. Upstream Java
	// `VolumeDefinitions.java:modifyVolumeDefinitionPassphrase`
	// (line 278); body shape is `VolumeDefinitionModifyPassphrase`
	// (`{"new_passphrase":"…"}`). We also accept the bare-string
	// `PassPhraseEnter` shape symmetric with the Bug 173 dual-form
	// cluster-passphrase PATCH. Pre-fix this 404'd, breaking
	// `linstor vd set-passphrase` entirely. Path uses `{vlmNr}` to
	// match the upstream OpenAPI spec.
	mux.HandleFunc(
		"PUT /v1/resource-definitions/{rd}/volume-definitions/{vlmNr}/encryption-passphrase",
		s.requireStore(s.handleVDPassphraseRotate))
}

// handleVDView is the cluster-wide aggregate for
// `linstor vd l` / golinstor's VolumeDefinitions.GetAll(). Returns
// upstream LINSTOR's shape: an array of ResourceDefinitionWithVolumeDefinition
// (each RD wrapping its inline volume_definitions array). The Python
// linstor CLI iterates `lstmsg.resource_definitions` → for each rd:
// `rsc_dfn.volume_definitions` — a flat per-VD entry would render
// the table empty because the attribute path doesn't match.
//
// Empty-VD RDs are dropped from the response so the CLI's
// per-row groupby doesn't show RDs without any defined volumes.
func (s *Server) handleVDView(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	type rdWithVDs struct {
		Name              string                   `json:"name"`
		ExternalName      string                   `json:"external_name,omitempty"`
		ResourceGroupName string                   `json:"resource_group_name,omitempty"`
		Flags             []string                 `json:"flags,omitempty"`
		Props             map[string]string        `json:"props,omitempty"`
		VolumeDefinitions []apiv1.VolumeDefinition `json:"volume_definitions"`
	}

	out := make([]rdWithVDs, 0, len(rds))

	for i := range rds {
		vds, listErr := s.Store.VolumeDefinitions().List(r.Context(), rds[i].Name)
		if listErr != nil {
			writeError(w, http.StatusInternalServerError, listErr.Error())

			return
		}

		if len(vds) == 0 {
			continue
		}

		// Bug 185: scrub sensitive keys from every VD's Props bag
		// before bundling into the aggregate view. Mirrors Bug 115's
		// RD-side redaction — `linstor vd l` would otherwise surface
		// the LUKS passphrase verbatim under each volume's `props`.
		// The parent RD's Props bag is ALSO redacted here for parity
		// with /v1/resource-definitions which has Bug 115's
		// stampRDEffectiveProps redaction on the same key set; the
		// VD-view emits a bare per-RD Props map that bypasses that
		// path entirely.
		rdProps := rds[i].Props
		redactSensitiveProps(rdProps)
		redactVolumeDefinitionsInPlace(vds)

		out = append(out, rdWithVDs{
			Name:              rds[i].Name,
			ExternalName:      rds[i].ExternalName,
			ResourceGroupName: rds[i].ResourceGroupName,
			Flags:             rds[i].Flags,
			Props:             rdProps,
			VolumeDefinitions: vds,
		})
	}

	writeJSON(w, http.StatusOK, out)
}

// redactVolumeDefinitionsInPlace walks every VD's Props bag and
// scrubs deny-listed keys. Centralised so the per-RD list + cluster-
// wide view + per-VD GET paths share the wire-edge invariant.
// Idempotent.
func redactVolumeDefinitionsInPlace(vds []apiv1.VolumeDefinition) {
	for i := range vds {
		redactSensitiveProps(vds[i].Props)
	}
}

func (s *Server) handleVDList(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	// Verify the parent RD exists so a missing RD is 404, not 200 with [].
	// k8s store does this internally; in-memory does not, so we do it here.
	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	vds, err := s.Store.VolumeDefinitions().List(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Defensive non-nil: linstor-csi's VD-list decoder treats a `null`
	// body as malformed. Both store backends `make()` their result,
	// but pin the invariant at the wire edge.
	if vds == nil {
		vds = []apiv1.VolumeDefinition{}
	}

	// Bug 185: scrub sensitive Props on every VD before emit.
	redactVolumeDefinitionsInPlace(vds)

	writeJSON(w, http.StatusOK, vds)
}

func (s *Server) handleVDGet(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	vd, err := s.Store.VolumeDefinitions().Get(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 185: redact the per-VD Props bag at the REST boundary.
	// Get() returns a value copy, so the in-place mutation is local
	// to this response — the store cache stays un-redacted.
	redactSensitiveProps(vd.Props)

	writeJSON(w, http.StatusOK, vd)
}

// handleVDCreate accepts either the upstream `VolumeDefinitionCreate`
// envelope (`{"volume_definition": {...}}`) or a bare VolumeDefinition body —
// both shapes appear in the wild.
//
// Bug 191 (P2 SPEC): upstream LINSTOR documents `volume_number` as
// optional — when absent the controller auto-assigns the smallest free
// VlmNr. The pre-fix handler decoded an absent field to Go's int32
// zero value and silently forwarded VlmNr=0; the second `linstor vd c
// X 32M` invocation then collided with Bug 140's FAIL_EXISTS_VLM_DFN
// refusal. The fix probes the raw JSON for the literal
// `volume_number` key inside the `volume_definition` object (mirrors
// Bug 156's `disklessOnRemainingExplicitlyFalse` pattern); when
// absent/null it walks the parent RD's existing VDs and assigns the
// smallest free non-negative integer.
func (s *Server) handleVDCreate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)

		return
	}

	var envelope apiv1.VolumeDefinitionCreate

	dec := json.NewDecoder(bytes.NewReader(rawBody))
	dec.DisallowUnknownFields()

	if decErr := dec.Decode(&envelope); decErr != nil {
		writeDecodeError(w, decErr)

		return
	}

	vd := envelope.VolumeDefinition

	// Bug 191: distinguish "client omitted volume_number" (auto-assign)
	// from "client sent volume_number=0" (explicit zero). The typed
	// decode above can't tell them apart because the wire field is a
	// plain int32 — both shapes deserialise to VolumeNumber=0.
	if !vdCreateVolumeNumberExplicit(rawBody) {
		assigned, assignErr := s.autoAssignVolumeNumber(r.Context(), rd)
		if assignErr != nil {
			writeStoreError(w, assignErr)

			return
		}

		vd.VolumeNumber = assigned
	}

	// Bug 155: refuse out-of-bounds sizes at the REST boundary so the
	// satellite reconciler doesn't hot-loop on `drbdadm create-md`
	// failures. See validateVDSize for the bounds rationale.
	if sizeErr := validateVDSize(vd.SizeKib); sizeErr != nil {
		writeVDSizeRejection(w, rd, vd.VolumeNumber, vd.SizeKib, sizeErr)

		return
	}

	err = s.Store.VolumeDefinitions().Create(r.Context(), rd, &vd)
	if err != nil {
		// Bug 140: duplicate-VD conflict gets a typed envelope with
		// the upstream FAIL_EXISTS_VLM_DFN sub-code plus actionable
		// cause/correction so scripts and audit-log greppers route
		// the same way they do for upstream's `linstor vd c` reply.
		// The bare writeStoreError fallback emitted apiCallRcError
		// alone — high-bit error, no sub-code, no cause/correction
		// — which the Python CLI rendered as a generic "object
		// already exists" line that didn't tell the operator which
		// VlmNr to twist.
		if errors.Is(err, store.ErrAlreadyExists) {
			writeVDExistsConflict(w, rd, vd.VolumeNumber)

			return
		}

		writeStoreError(w, err)

		return
	}

	// Matches upstream LINSTOR: POST /v1/resource-definitions/<n>/
	// volume-definitions returns 200 OK (not 201 Created). Java
	// LINSTOR is consistent about this — only top-level entity
	// creates return 201, child-volume creates stay 200 because
	// the parent already exists.
	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition created",
	}})
}

// vdCreateVolumeNumberExplicit reports whether the raw POST body
// carries an explicit `volume_number` key inside the
// `volume_definition` object. Bug 191: a typed decode into
// `apiv1.VolumeDefinitionCreate` flattens an absent/null
// `volume_number` to Go's int32 zero — indistinguishable from an
// explicit `"volume_number": 0`. The handler walks the wire shape
// directly so the auto-assign branch only fires when the operator
// actually omitted the field (`linstor vd c X 32M` without
// --vlmnr).
//
// Two wire shapes are supported, matching `handleVDCreate`'s decode:
//
//  1. Envelope: `{"volume_definition": {"size_kib": ..., ...}}` —
//     upstream golinstor's `VolumeDefinitionCreate`. Walk into the
//     inner object.
//  2. Bare: `{"size_kib": ..., ...}` — some legacy callers POST the
//     bare VolumeDefinition shape. Walk the top level directly.
//
// Returns false on malformed JSON, missing key, or explicit JSON
// `null` (treated as "absent" per the Bug 156 idiom). Treats an
// explicit `"volume_number": 0` as present so an operator who
// genuinely wants VlmNr=0 (e.g. seeding the first VD on a fresh RD
// via a script that always sends 0) keeps that behaviour.
func vdCreateVolumeNumberExplicit(raw []byte) bool {
	if len(bytes.TrimSpace(raw)) == 0 {
		return false
	}

	var envelope map[string]json.RawMessage

	if err := json.Unmarshal(raw, &envelope); err != nil {
		return false
	}

	// Shape 1: `{"volume_definition": {...}}` envelope.
	if inner, ok := envelope["volume_definition"]; ok {
		var innerObj map[string]json.RawMessage

		if err := json.Unmarshal(inner, &innerObj); err != nil {
			return false
		}

		return rawHasNonNullKey(innerObj, "volume_number")
	}

	// Shape 2: bare VolumeDefinition at the top level.
	return rawHasNonNullKey(envelope, "volume_number")
}

// rawHasNonNullKey returns true when the key is present in the
// decoded JSON object AND its value is not the literal `null`.
// Matches Bug 156's "absent or null both mean unset" rule.
func rawHasNonNullKey(obj map[string]json.RawMessage, key string) bool {
	raw, ok := obj[key]
	if !ok {
		return false
	}

	return !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// autoAssignVolumeNumber returns the smallest free non-negative
// VolumeNumber under the given RD. Mirrors upstream LINSTOR's
// CtrlVlmDfnCrtApiCallHandler smallest-hole rule: with VDs 0 and 2
// present, an auto-assign POST lands at 1 — not 3.
//
// A missing RD surfaces here as the underlying store's ErrNotFound;
// the caller routes it through writeStoreError so the wire shape
// matches the pre-fix 404 path for an unknown parent RD.
func (s *Server) autoAssignVolumeNumber(ctx context.Context, rd string) (int32, error) {
	vds, err := s.Store.VolumeDefinitions().List(ctx, rd)
	if err != nil {
		return 0, err //nolint:wrapcheck // surfaced to writeStoreError
	}

	used := make(map[int32]bool, len(vds))
	for i := range vds {
		used[vds[i].VolumeNumber] = true
	}

	for candidate := int32(0); candidate >= 0; candidate++ {
		if !used[candidate] {
			return candidate, nil
		}
	}

	// Unreachable for any sane RD (the smallest-free walk terminates
	// at the first gap; an RD with 2^31 VDs is impossible on real
	// storage). Pin the contract anyway.
	return 0, errors.New("auto-assign: VolumeNumber space exhausted")
}

// minVolumeDefinitionSizeKib is the smallest accepted size_kib on
// `POST /v1/resource-definitions/{rd}/volume-definitions` (Bug 155).
// DRBD reserves ~32 KiB of metadata per peer; backing-storage layers
// (LVM-thin, ZFS, LUKS) layer additional alignment on top. Picking
// 4 MiB as the floor keeps every layered composition viable without
// having to chase the exact ceiling for each provider.
const minVolumeDefinitionSizeKib int64 = 4 * 1024

// maxVolumeDefinitionSizeKib is the largest accepted size_kib (Bug
// 155). 16 TiB is DRBD's hard per-device ceiling — the on-disk
// activity-log encoding can't address more than 16 TiB of net data.
// Requests above that bound will fail at `drbdadm create-md` time
// regardless of backing storage capacity, so refusing here gets the
// operator a typed error envelope instead of an opaque satellite
// retry loop.
const maxVolumeDefinitionSizeKib int64 = 16 * 1024 * 1024 * 1024

// validateVDSize returns nil when the requested size_kib is within
// the accepted bounds [minVolumeDefinitionSizeKib,
// maxVolumeDefinitionSizeKib] (Bug 155). Otherwise it returns a
// human-readable rejection reason the caller formats into the
// LINSTOR envelope.
func validateVDSize(sizeKib int64) error {
	if sizeKib < minVolumeDefinitionSizeKib {
		return fmt.Errorf(
			"size_kib=%d below minimum %d KiB (DRBD reserves ~32 KiB of "+
				"metadata per peer; backing layers add alignment on top)",
			sizeKib, minVolumeDefinitionSizeKib,
		)
	}

	if sizeKib > maxVolumeDefinitionSizeKib {
		return fmt.Errorf(
			"size_kib=%d above maximum %d KiB (DRBD's per-device hard ceiling)",
			sizeKib, maxVolumeDefinitionSizeKib,
		)
	}

	return nil
}

// writeVDSizeRejection emits the Bug 155 size-out-of-bounds refusal
// envelope. 400 + FAIL_INVLD_VLM_SIZE keeps the wire shape byte-
// identical to upstream LINSTOR's `linstor vd c` reply on the same
// input (the shrink branch in handleVDUpdate uses the same sub-code).
func writeVDSizeRejection(w http.ResponseWriter, rd string, vn int32, sizeKib int64, reason error) {
	writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInvldVlmSize,
		Message: fmt.Sprintf("invalid volume definition size for %q vlm=%d: %s",
			rd, vn, reason.Error()),
		Cause: fmt.Sprintf(
			"size_kib must be in [%d, %d]; the satellite reconciler "+
				"would loop on drbdadm create-md otherwise",
			minVolumeDefinitionSizeKib, maxVolumeDefinitionSizeKib,
		),
		Correc: fmt.Sprintf(
			"pick a size between %d KiB (~4 MiB) and %d KiB (~16 TiB) and re-issue `linstor vd c`",
			minVolumeDefinitionSizeKib, maxVolumeDefinitionSizeKib,
		),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	}})
	_ = sizeKib // retained for future audit-log fields
}

// writeVDExistsConflict emits the Bug 140 typed conflict envelope on
// a duplicate `POST /v1/resource-definitions/{rd}/volume-definitions`.
// Wire shape matches upstream LINSTOR's `linstor vd c` reply on the
// same input: 409 Conflict + ApiCallRc with apiCallRcError |
// FAIL_EXISTS_VLM_DFN sub-code, an operator-actionable message
// naming the parent RD and the duplicate VlmNr, and a non-empty
// cause/correction so the Python CLI surfaces the refusal as an
// ERROR line (not a generic "object already exists").
//
// Per cli-parity-audit alignment, the correction names the two
// remedial commands: PUT to modify the existing VD (`vd m`) or
// POST with an explicit, free VolumeNumber (`vd c --vlmnr`).
func writeVDExistsConflict(w http.ResponseWriter, rd string, vn int32) {
	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailExistsVlmDfn,
		Message: fmt.Sprintf(
			"volume definition %d already exists on resource definition %q",
			vn, rd),
		Cause: fmt.Sprintf(
			"a volume definition with VlmNr=%d is already registered under %q; "+
				"`linstor vd c` without --vlmnr defaults to 0 and the second invocation "+
				"collides with the first",
			vn, rd),
		Correc: fmt.Sprintf(
			"to modify the existing volume use `linstor vd m %s %d <new-size>`; "+
				"to add a second volume pick a free VlmNr explicitly "+
				"(`linstor vd c --vlmnr=<N> %s <size>`)",
			rd, vn, rd),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	}})
}

// handleVDUpdate applies a modify delta to an existing VolumeDefinition.
// PUT semantics for upstream LINSTOR's `vd set-size` / `vd set-property`
// are MERGE, not REPLACE — golinstor sends only the fields that changed
// (size_kib alone for CSI grow, override_props/delete_props alone for
// property modifies) and expects the rest of the VD spec to be
// preserved. A naive Decode(&fullVD) + Update silently zeroes SizeKib
// whenever the body omits it (see audit-4.6 finding). Fetch + merge.
func (s *Server) handleVDUpdate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	var patch volumeDefinitionModifyBody

	if !decodeJSON(w, r, &patch) {
		return
	}

	// Pre-merge fetch needed only for the shrink-refusal precheck —
	// PatchVolumeDefinitionSpec performs the real fetch+merge+write
	// loop. Doing the precheck against this snapshot is sound: shrink
	// rejection is invariant under concurrent prop edits (only the
	// SizeKib comparison matters, and the only writer that touches
	// SizeKib is the resize CSI path itself, which is serialised at
	// the linstor-csi caller).
	existing, err := s.Store.VolumeDefinitions().Get(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	previousSizeKib := existing.SizeKib

	// Scenario 4.W13: reject any shrink (`new < previous`) unless the
	// operator opted in via `force=true`. Runs BEFORE the merge + store
	// write so a rejected shrink leaves the stored spec untouched — a
	// partial update would desync the controller spec from the
	// satellite reality.
	if rejectShrinkWithoutForce(w, r, &patch, rd, vn, previousSizeKib) {
		return
	}

	// Bug 204b: route the merge-write through PatchVolumeDefinitionSpec
	// so the modify delta is re-applied to the freshly-fetched VD on
	// every retry. The previous `Get → mutate → Update` path's retry
	// loop replayed the caller's stale wire snapshot and silently lost
	// concurrent prop edits on the same VD.
	err = s.Store.VolumeDefinitions().PatchVolumeDefinitionSpec(r.Context(), rd, vn, func(vd *apiv1.VolumeDefinition) error {
		mergeVolumeDefinitionPatch(vd, &patch)

		// Path-derived VolumeNumber wins — never trust the body's
		// vol_num.
		vd.VolumeNumber = vn

		return nil
	})
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 136: on a grow, stamp the per-resource resize-pending
	// annotation. See stampResizePendingOnResources for rationale.
	if patch.SizeKib != nil && *patch.SizeKib > previousSizeKib {
		s.stampResizePendingOnResources(r.Context(), rd, vn, *patch.SizeKib)
	}

	envelope := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition modified",
	}}

	envelope = appendForceShrinkAdvisory(envelope, &patch, rd, vn, previousSizeKib)

	writeJSON(w, http.StatusOK, envelope)
}

// appendForceShrinkAdvisory appends the force-shrink warning entry
// to the success envelope when the patch reduced SizeKib. Only
// reachable when force=true (the strict-reject branch in
// rejectShrinkWithoutForce otherwise short-circuits with 400).
// Matches upstream's ApiCallRcImpl order where the "operation
// succeeded" entry leads and per-resource warnings tail. Bug 38 /
// scenario 4.W13.
func appendForceShrinkAdvisory(envelope []apiv1.APICallRc, patch *volumeDefinitionModifyBody, rd string, vn int32, previousSizeKib int64) []apiv1.APICallRc {
	if patch.SizeKib == nil || *patch.SizeKib >= previousSizeKib {
		return envelope
	}

	return append(envelope, apiv1.APICallRc{
		RetCode: warnVlmDfnResizeShrink,
		Message: fmt.Sprintf(
			"shrinking volume %d from %d KiB to %d KiB (force=true; DATA LOSS RISK — caller intent assumed)",
			vn, previousSizeKib, *patch.SizeKib,
		),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	})
}

// rejectShrinkWithoutForce writes a 400 + FAIL_INVLD_VLM_SIZE
// envelope when the patch reduces SizeKib without `force=true` and
// returns true to signal the caller to short-circuit. The error path
// is split out of handleVDUpdate to keep the HTTP handler under the
// funlen budget; the formatted message stays inline here so a single
// grep against the binary finds the operator-actionable text.
//
// LINSTOR does NOT auto-shrink the backing FS — `lvreduce` after a
// spec-shrink without an in-FS `resize2fs -s` first would truncate
// live data. Upstream LINSTOR's
// CtrlVlmDfnModifyApiCallHandler.ensureShrinkingIsSupported raises
// FAIL_INVLD_VLM_SIZE (206 | MASK_ERROR) on the same input; mirror
// the wire code and 400 Bad Request HTTP status so golinstor's
// `client.ApiCallError` surfaces the message in `linstor`'s exit-1
// path.
func rejectShrinkWithoutForce(
	w http.ResponseWriter, r *http.Request, patch *volumeDefinitionModifyBody,
	rd string, vn int32, previousSizeKib int64,
) bool {
	if patch.SizeKib == nil || *patch.SizeKib >= previousSizeKib {
		return false
	}

	if shrinkForceRequested(r, patch) {
		return false
	}

	writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInvldVlmSize,
		Message: fmt.Sprintf(
			"cannot shrink volume %d from %d KiB to %d KiB: "+
				"filesystem shrink-then-resize required; LINSTOR does NOT auto-shrink. "+
				"Operator action: (1) `resize2fs -s <new>` or `xfs` dump+restore on the consumer, "+
				"(2) unmount or detach the volume, "+
				"(3) re-issue this PUT with `force=true` (body field) or `?force=true` (query).",
			vn, previousSizeKib, *patch.SizeKib,
		),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			"VlmNr":      strconv.FormatInt(int64(vn), 10),
		},
	}})

	return true
}

// shrinkForceRequested returns true when the caller opted into the
// shrink escape hatch via either the JSON body's `force` field or the
// `?force=true` query parameter. The query parameter exists so ad-hoc
// `curl -X PUT … ?force=true` scripts work without re-shaping the
// JSON body around a golinstor-shaped payload. Both knobs must accept
// the literal string "true" (case-insensitive) — Go's
// `strconv.ParseBool` covers "1"/"t"/"true"/"True"/"TRUE" which is a
// strict superset of the documented form.
func shrinkForceRequested(r *http.Request, patch *volumeDefinitionModifyBody) bool {
	if patch.Force {
		return true
	}

	raw := r.URL.Query().Get("force")
	if raw == "" {
		return false
	}

	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}

	return v
}

// mergeVolumeDefinitionPatch overlays the modify delta onto an existing
// VolumeDefinition in place. Split out of handleVDUpdate to keep the
// HTTP handler under the gocyclo budget; the merge rules are unit-
// tested through the handler.
func mergeVolumeDefinitionPatch(existing *apiv1.VolumeDefinition, patch *volumeDefinitionModifyBody) {
	if patch.SizeKib != nil {
		existing.SizeKib = *patch.SizeKib
	}

	// Props: overlay override_props (and the legacy `props` field —
	// some callers PUT the full VD shape) on top of existing, then
	// drop anything in delete_props. delete_namespaces is the upstream
	// "delete every key under prefix" knob.
	if len(patch.OverrideProps) > 0 || len(patch.Props) > 0 {
		if existing.Props == nil {
			existing.Props = map[string]string{}
		}

		maps.Copy(existing.Props, patch.OverrideProps)
		maps.Copy(existing.Props, patch.Props)
	}

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}

	for _, ns := range patch.DeleteNamespaces {
		for k := range existing.Props {
			if k == ns || (len(k) > len(ns) && k[:len(ns)] == ns && k[len(ns)] == '/') {
				delete(existing.Props, k)
			}
		}
	}
}

// handleVDDelete drops a VolumeDefinition under an RD.
//
// Idempotent on NotFound (Bug 66): both NotFound shapes — the parent
// RD missing AND the (rd, vn) pair missing inside an extant RD — fold
// into a 200 + warn-mask envelope. linstor-csi's ControllerExpand /
// shrink paths re-issue `vd d` on retry; the bare 404 used to crash
// the Python CLI on its XML decoder fallback (see Bug 56 commentary).
//
// Bug 186 (P2): refuses with 409 + FAIL_IN_USE | MASK_ERROR when at
// least one Resource on the parent RD still carries a Volume row for
// the dropped VolumeNumber. Mirrors upstream LINSTOR's
// CtrlVlmDfnDeleteApiCallHandler refusal pattern (Bug 92 /
// Bug 174 envelope shape) — the previous behaviour silently dropped
// the spec and pruned satellite-observed Volume rows off the
// Resource CRDs, leaving no operator-visible signal that the delete
// was unsafe. `?force=true` (and the body's `force` field for parity
// with Bug 92 / W13) bypasses the refusal so the operator can drop
// the spec out from under a stuck satellite.
func (s *Server) handleVDDelete(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	force := isForce(r)

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Bug 186: pre-Delete walk of referencing Resources. Runs BEFORE
	// the store-level Delete so a refused call leaves the VD spec and
	// every dependent Resource.Volumes row untouched — partial-state
	// after a rejected DELETE would be a worse failure mode than the
	// bug itself.
	if !force && s.refuseVDDeleteIfReferenced(w, r, rd, vn) {
		return
	}

	// Bug 202: capture the VD pre-Delete so the post-Delete re-walk
	// has something to restore if a racing `r c` slipped between the
	// pre-walk and the store-level Delete. Capture-after-refuse
	// matches the Bug 174 ordering (deleteWithRollback in
	// pkg/rest/delete_toctou.go) — a refused call discards the
	// snapshot anyway, so we don't waste the Get on the refusal path.
	captured, capturedOK := s.captureVolumeDefinition(r.Context(), rd, vn)

	err = s.Store.VolumeDefinitions().Delete(r.Context(), rd, vn)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	if err != nil {
		// Bug 139: even on the idempotent no-op branch, drain the
		// local cache so a re-issued DELETE during a real delete-in-
		// flight is still read-your-writes on the follow-up view.
		s.waitForVDDeletionVisible(r.Context(), rd, vn)

		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnVDNotFound,
			Message: fmt.Sprintf("volume definition already absent: %s/%d", rd, vn),
		}})

		return
	}

	// Bug 202: post-Delete re-walk. A racing `r c` may have slipped
	// between the Bug 186 pre-walk and the Delete above: the pre-walk
	// saw an empty reference set, then the racing create persisted a
	// Resource on the parent RD (implicit reference per the spec
	// contract, see resourceReferencesVolume), then we dropped the
	// VD spec out from under it. The post-walk catches that
	// ordering, restores the captured VD via store Create, and
	// surfaces the same 409 envelope the pre-walk would have
	// emitted. Skipped on the explicit `?force=true` bypass (the
	// operator opted in to the cascade) and on the capture-miss
	// path (idempotent-delete replay — nothing to roll back to).
	if !force && capturedOK && s.rollbackVDDeleteIfRaced(w, r, rd, vn, &captured) {
		return
	}

	// Bug 139: prune the deleted VolumeNumber off each child
	// Resource's Status.Volumes, then wait for the VD delete to be
	// observable on the local store. The satellite reconciler
	// eventually re-stamps Status.Volumes when it re-applies after
	// the RD spec change, but the gap surfaces the dropped volume
	// on `view/resources` for tens of seconds. Pre-stamping the
	// Status.Volumes update here closes the gap synchronously.
	s.pruneVolumesFromResources(r.Context(), rd, vn)
	s.waitForVDDeletionVisible(r.Context(), rd, vn)

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("volume definition deleted: %s/%d", rd, vn),
	}})
}

// captureVolumeDefinition grabs a snapshot of the VD spec so the
// Bug 202 post-Delete re-walk has something to restore when a racing
// `r c <rd>.<node>` slipped past the Bug 186 pre-walk. The second
// return is false when the VD no longer exists at capture time
// (benign idempotent-delete replay) — the rollback path is skipped
// in that case.
func (s *Server) captureVolumeDefinition(ctx context.Context, rd string, vn int32) (apiv1.VolumeDefinition, bool) {
	vd, err := s.Store.VolumeDefinitions().Get(ctx, rd, vn)
	if err != nil {
		return apiv1.VolumeDefinition{}, false
	}

	return vd, true
}

// rollbackVDDeleteIfRaced runs the Bug 202 post-Delete re-walk.
// If a Resource reference appeared between the pre-walk and the
// Delete, restore the captured VD via store Create and write the
// 409 envelope the pre-walk would have written. Returns true when
// the rollback fired (HTTP error already written, caller must stop)
// and false when the delete is safe to commit. Mirrors Bug 174's
// `rollbackRGDeleteIfRaced` shape — same Bug 178 5xx envelope when
// the restore Create itself fails so the operator gets an actionable
// signal that the deleted primary may need manual restoration.
func (s *Server) rollbackVDDeleteIfRaced(
	w http.ResponseWriter,
	r *http.Request,
	rd string,
	vn int32,
	captured *apiv1.VolumeDefinition,
) bool {
	refs, err := s.resourcesReferencingVolume(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if len(refs) == 0 {
		return false
	}

	// Bug 178: a Create error here used to be silently swallowed in
	// sibling rollback paths, so the cluster ended up with the VD
	// deleted, a racing Resource still on the parent RD, and the
	// operator handed a 409 "still referenced" envelope while
	// staring at a cluster whose VD row no longer exists. Surface
	// a 5xx envelope that names the rollback failure explicitly.
	createErr := s.Store.VolumeDefinitions().Create(r.Context(), rd, captured)
	if createErr != nil {
		writeRollbackRestoreFailure(r.Context(), w, createErr,
			"volume definition", fmt.Sprintf("%s/%d", rd, vn),
			"linstor vd l "+rd)

		return true
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: fmt.Sprintf(
			"Volume definition %d on resource definition %q cannot be "+
				"deleted because resource replicas still reference it.",
			vn, rd),
		Cause: fmt.Sprintf(
			"%d resource replica(s) reference VolumeNumber %d on %q: %s",
			len(refs), vn, rd, strings.Join(refs, ", ")),
		Correc: "Delete the listed resource replicas first " +
			"(`linstor r d <node> " + rd + "`), or pass `?force=true` " +
			"to drop the volume definition anyway and accept the " +
			"orphan replicas.",
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	}})

	return true
}

// refuseVDDeleteIfReferenced runs the Bug 186 pre-Delete walk: any
// Resource of the parent RD whose Volumes carry a row for the
// dropped VolumeNumber is a live reference and must block the
// delete. Returns true when the HTTP error has already been written
// (the caller must stop processing) and false when the delete may
// proceed.
//
// Cause line names the referencing Resources sorted by NodeName so
// the surfaced text is deterministic across cache iteration orders.
// Wire shape mirrors Bug 92 (node delete in-use) and Bug 152 (sp
// delete in-use) — 409 + FAIL_IN_USE | MASK_ERROR, Cause/Correction
// pointing at the remedial commands.
func (s *Server) refuseVDDeleteIfReferenced(w http.ResponseWriter, r *http.Request, rd string, vn int32) bool {
	refs, err := s.resourcesReferencingVolume(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if len(refs) == 0 {
		return false
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: fmt.Sprintf(
			"Volume definition %d on resource definition %q cannot be "+
				"deleted because resource replicas still reference it.",
			vn, rd),
		Cause: fmt.Sprintf(
			"%d resource replica(s) reference VolumeNumber %d on %q: %s",
			len(refs), vn, rd, strings.Join(refs, ", ")),
		Correc: "Delete the listed resource replicas first " +
			"(`linstor r d <node> " + rd + "`), or pass `?force=true` " +
			"to drop the volume definition anyway and accept the " +
			"orphan replicas.",
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	}})

	return true
}

// resourcesReferencingVolume returns the sorted-by-NodeName list of
// Resources on the parent RD that reference the given VolumeNumber.
// Used by Bug 186's pre-Delete walk; sort order pinned so the
// surfaced 409 envelope's Cause line is byte-identical across cache
// iteration orders (the same trick node_lifecycle.go uses for the
// in-use evacuate refusal).
//
// Two reference shapes count as live:
//
//  1. The Resource carries an explicit Volumes[] entry whose
//     VolumeNumber matches `vn` — this is what the unit tests seed
//     directly and what fully-converged Resources carry on the wire
//     once the satellite has stamped Status.Volumes.
//  2. The Resource has no Volumes[] rows yet — common in production
//     while the satellite is mid-reconcile, on freshly-created
//     diskless / TIE_BREAKER replicas, and any time DRBD hasn't
//     advanced past `Unknown`. Upstream LINSTOR's
//     CtrlVlmDfnDeleteApiCallHandler treats every Resource on the
//     RD as an implicit reference to every VD on that RD — the
//     spec contract says "Resource has one Volume per VD"; a missing
//     Status.Volumes row means "not yet stamped", not "the spec
//     doesn't reference it".
//
// The blanket shape (2) closes the live-cluster reproducer where
// `vd d` slipped past the prune-only check because the satellite
// hadn't filled in Status.Volumes yet — exactly the Bug 186 symptom
// upstream LINSTOR's FAIL_IN_USE refusal exists to prevent.
func (s *Server) resourcesReferencingVolume(ctx context.Context, rd string, vn int32) ([]string, error) {
	if s == nil || s.Store == nil {
		return nil, nil
	}

	resources, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return nil, err //nolint:wrapcheck // surfaced to writeStoreError
	}

	var refs []string

	for i := range resources {
		if resourceReferencesVolume(&resources[i], vn) {
			refs = append(refs, resources[i].NodeName)
		}
	}

	sort.Strings(refs)

	return refs, nil
}

// resourceReferencesVolume returns true when the Resource is a live
// reference to the (RD, VolumeNumber) pair under Bug 186's refusal
// semantics. See resourcesReferencingVolume for the two reference
// shapes (explicit Volume row OR implicit "spec contract" reference
// while the satellite has not yet stamped Status.Volumes).
func resourceReferencesVolume(rsc *apiv1.Resource, vn int32) bool {
	if len(rsc.Volumes) == 0 {
		return true
	}

	for i := range rsc.Volumes {
		if rsc.Volumes[i].VolumeNumber == vn {
			return true
		}
	}

	return false
}

// pruneVolumesFromResources walks every Resource of the named RD
// and drops the deleted VolumeNumber from its Volumes slice. Bug
// 139: the satellite eventually re-stamps Status.Volumes after the
// RD-watch fires, but `view/resources` reads in the gap surface
// the phantom volume — pre-stamp here so the gap is zero.
//
// Best-effort: a single Resource failing to re-Update doesn't roll
// back the others nor the VD delete itself.
func (s *Server) pruneVolumesFromResources(ctx context.Context, rd string, vn int32) {
	if s == nil || s.Store == nil {
		return
	}

	resources, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return
	}

	for i := range resources {
		rsc := &resources[i]
		if len(rsc.Volumes) == 0 {
			continue
		}

		// Bug 205: typed-Patch via PatchResourceSpec — the closure
		// re-runs against the live Resource on every conflict, so a
		// racing satellite SetState (Status subresource, different
		// field owner) doesn't race the prune. NotFound and Patch
		// errors are swallowed identical to the wholesale `Update`
		// this replaces — the prune is best-effort, the eventual
		// satellite re-stamp closes the gap regardless.
		_ = s.Store.Resources().PatchResourceSpec(ctx, rsc.Name, rsc.NodeName, func(live *apiv1.Resource) error {
			if len(live.Volumes) == 0 {
				return nil
			}

			out := make([]apiv1.Volume, 0, len(live.Volumes))

			dropped := false

			for j := range live.Volumes {
				if live.Volumes[j].VolumeNumber == vn {
					dropped = true

					continue
				}

				out = append(out, live.Volumes[j])
			}

			if !dropped {
				return nil
			}

			live.Volumes = out

			return nil
		})
	}
}

func parseVolNum(raw string) (int32, error) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err //nolint:wrapcheck // returned to handler that wraps it
	}

	return int32(v), nil
}

// resizePendingAnnotationPrefix is the per-volume annotation key
// prefix the REST VD-grow handler stamps on each affected Resource
// (Bug 136). The full key is
// `<prefix><VolumeNumber>` and the value is the new SizeKib (decimal
// string, KiB). Per-volume so multi-volume RDs (rare today but on
// the roadmap) keep the grow signal distinguishable when several
// volumes resize at once.
//
// Operators read this via `kubectl get resource -o yaml`; the
// satellite reconciler doesn't strictly require it (the RD-watch
// in `enqueueResourcesForRD` already re-applies on any RD-spec
// change), but it gives a steady-state breadcrumb that explains
// why the satellite re-rendered and what the target size is.
const resizePendingAnnotationPrefix = "bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-"

// stampResizePendingOnResources walks every Resource of the named
// RD and stamps the per-volume "resize pending" annotation with the
// new size. Best-effort by design: a single Resource failing to
// re-Update doesn't roll back the others nor the VD spec change
// itself. Bug 136.
func (s *Server) stampResizePendingOnResources(ctx context.Context, rd string, vn int32, sizeKib int64) {
	if s == nil || s.Store == nil {
		return
	}

	resources, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return
	}

	key := resizePendingAnnotationPrefix + strconv.FormatInt(int64(vn), 10)
	value := strconv.FormatInt(sizeKib, 10)

	for i := range resources {
		rsc := &resources[i]

		// Bug 205: typed-Patch via PatchResourceSpec — the closure
		// re-runs against the live Resource on every conflict so a
		// racing satellite SetState (Status subresource, different
		// field owner) or peer-modify doesn't race the resize-pending
		// stamp. Patch errors are swallowed identical to the wholesale
		// `Update` it replaces — the stamp is best-effort breadcrumb,
		// not a correctness signal.
		_ = s.Store.Resources().PatchResourceSpec(ctx, rsc.Name, rsc.NodeName, func(live *apiv1.Resource) error {
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}

			live.Annotations[key] = value

			return nil
		})
	}
}
