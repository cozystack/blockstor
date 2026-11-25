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
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/propwhitelist"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"
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
	// `linstor vd modify-passphrase` entirely. Path uses `{vlmNr}` to
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
		s.reportAppliedSizes(r.Context(), rds[i].Name, vds)

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
	//
	// CreateVolume hot path: `vd l` / golinstor VD reads land right
	// after the RD create while the local informer cache may still
	// trail the write — see pkg/rest/cache_retry.go.
	_, err := getRDWithCacheRetry(r.Context(), s.Store, rd)
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
	s.reportAppliedSizes(r.Context(), rd, vds)

	writeJSON(w, http.StatusOK, vds)
}

func (s *Server) handleVDGet(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Bug 365: reject out-of-range volume_number at the GET wire
	// boundary too. Symmetric to Bug 363 (create) and the
	// vd-delete gate below — keeps the entire VD CRUD surface
	// consistent on the addressable [0, 65535] DRBD-9 range.
	vnErr := validateVolumeNumber(vn)
	if vnErr != nil {
		writeVDNumberRejection(w, rd, vn, vnErr)

		return
	}

	// VD-create hot path: VolumeDefinitions live inline on the parent
	// RD, so a `vd c` immediately followed by this GET can be served
	// from a cache that still holds the pre-create RD revision —
	// retry the NotFound under the standard budget. See
	// pkg/rest/cache_retry.go.
	vd, err := getVDWithCacheRetry(r.Context(), s.Store, rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 185: redact the per-VD Props bag at the REST boundary.
	// Get() returns a value copy, so the in-place mutation is local
	// to this response — the store cache stays un-redacted.
	redactSensitiveProps(vd.Props)

	one := []apiv1.VolumeDefinition{vd}
	s.reportAppliedSizes(r.Context(), rd, one)

	writeJSON(w, http.StatusOK, one[0])
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

	// CreateVolume hot path: linstor-csi POSTs the RD and this VD
	// create back-to-back (sub-ms gap); the auto-assign walk and the
	// store-level Create both read the parent RD through the informer
	// cache, which may not have observed the RD write yet. Probe with
	// the standard retry budget so the whole CreateVolume doesn't fail
	// on a spurious "resource definition not found" — see
	// pkg/rest/cache_retry.go.
	_, err := getRDWithCacheRetry(r.Context(), s.Store, rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)

		return
	}

	var envelope apiv1.VolumeDefinitionCreate

	dec := json.NewDecoder(bytes.NewReader(rawBody))
	dec.DisallowUnknownFields()

	decErr := dec.Decode(&envelope)
	if decErr != nil {
		writeDecodeError(w, decErr)

		return
	}

	vd := envelope.VolumeDefinition

	autoNumber := !vdCreateVolumeNumberExplicit(rawBody)

	// Validate the explicit number (Bug 363) before the size gate so the
	// operator-facing rejection order is stable. The auto-number path
	// owns the number itself (the store allocates it), so it skips this.
	if !autoNumber {
		nrErr := validateVolumeNumber(vd.VolumeNumber)
		if nrErr != nil {
			writeVDNumberRejection(w, rd, vd.VolumeNumber, nrErr)

			return
		}
	}

	// Bug 155: refuse out-of-bounds sizes at the REST boundary so the
	// satellite reconciler doesn't hot-loop on `drbdadm create-md`
	// failures. See validateVDSize for the bounds rationale.
	sizeErr := validateVDSize(vd.SizeKib)
	if sizeErr != nil {
		writeVDSizeRejection(w, rd, vd.VolumeNumber, vd.SizeKib, sizeErr)

		return
	}

	if refuseUnwhitelistedProps(w, propwhitelist.VolumeDefinition, vd.Props) {
		return
	}

	if !s.persistNewVolumeDefinition(w, r, rd, &vd, autoNumber) {
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

// persistNewVolumeDefinition writes the new VD to the store and, on
// failure, emits the right typed envelope. Returns true on success and
// false when an HTTP error envelope was already written (the caller
// must return immediately). Split out of handleVDCreate so the handler
// stays under the funlen budget.
//
// BUG-048 (P1, availability): when the operator did NOT name a
// VolumeNumber, the allocation MUST happen atomically with the write —
// route through CreateAutoNumbered, which picks the smallest free hole
// INSIDE the store's conflict-retry loop. The previous flow picked the
// number in a separate REST-side List (autoAssignVolumeNumber) BEFORE
// the Create, so two concurrent `linstor vd c <rd>` calls both read
// `[vol-0]`, both chose VlmNr=1, the loser was rejected FAIL_EXISTS_
// VLM_DFN, and the operator's second intended volume silently vanished.
// The explicit-number path keeps the plain Create (its retry loop
// already converges correctly and a genuine duplicate must still 409).
func (s *Server) persistNewVolumeDefinition(
	w http.ResponseWriter,
	r *http.Request,
	rd string,
	vd *apiv1.VolumeDefinition,
	autoNumber bool,
) bool {
	if autoNumber {
		_, err := s.Store.VolumeDefinitions().CreateAutoNumbered(r.Context(), rd, vd)
		if err != nil {
			writeStoreError(w, err)

			return false
		}

		return true
	}

	err := s.Store.VolumeDefinitions().Create(r.Context(), rd, vd)
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

			return false
		}

		writeStoreError(w, err)

		return false
	}

	return true
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

	err := json.Unmarshal(raw, &envelope)
	if err != nil {
		return false
	}

	// Shape 1: `{"volume_definition": {...}}` envelope.
	if inner, ok := envelope["volume_definition"]; ok {
		var innerObj map[string]json.RawMessage

		err := json.Unmarshal(inner, &innerObj)
		if err != nil {
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

// Auto-assign of the smallest free VolumeNumber now lives in the store
// layer (VolumeDefinitionStore.CreateAutoNumbered) so the read of the
// existing set and the write of the new entry are atomic — see BUG-048.
// The former REST-side autoAssignVolumeNumber helper (a separate List
// before the Create) was removed because its TOCTOU window dropped the
// second of two concurrent `linstor vd c` calls.

// minVolumeDefinitionSizeKib is the smallest accepted size_kib on
// `POST /v1/resource-definitions/{rd}/volume-definitions` (Bug 155).
// DRBD reserves ~32 KiB of metadata per peer; backing-storage layers
// (LVM-thin, ZFS, LUKS) layer additional alignment on top. Picking
// 4 MiB as the floor keeps every layered composition viable without
// having to chase the exact ceiling for each provider.
const minVolumeDefinitionSizeKib int64 = validate.MinVolumeSizeKib

// maxVolumeDefinitionSizeKib is the largest accepted size_kib (Bug
// 155). The ceiling is DRBD 9's documented per-device limit, 1 PiB. It
// was 16 TiB, which is below what DRBD 9 and upstream LINSTOR handle:
// linstormigrate copies sizes verbatim, so a cluster holding a larger
// volume failed part-way through its own migration.
// Requests above that bound will fail at `drbdadm create-md` time
// regardless of backing storage capacity, so refusing here gets the
// operator a typed error envelope instead of an opaque satellite
// retry loop.
const maxVolumeDefinitionSizeKib int64 = validate.MaxVolumeSizeKib

// minVolumeNumber is the smallest accepted explicit volume_number on
// `POST /v1/resource-definitions/{rd}/volume-definitions` (Bug 363).
// DRBD volume numbers are unsigned; the wire-side int32 was leaking
// negative values straight through, leaving the satellite stuck in
// "waiting for DRBD-ID allocation" because no positive minor can be
// derived from a negative VlmNr.
const minVolumeNumber int32 = 0

// maxVolumeNumber is the largest accepted explicit volume_number (Bug
// 363). DRBD-9 caps the per-resource volume namespace at 16 bits
// (0..65535) — values above that fail at `drbdadm adjust` time with
// "vol-XXXXX not addressable", which manifests as the same stuck
// satellite "waiting for DRBD-ID allocation" symptom.
const maxVolumeNumber int32 = 65535

// ErrVolumeNumberBelowMinimum is the sentinel for the negative-VlmNr
// rejection (volume_number < 0). Wrapped with %w + bound detail by
// validateVolumeNumber; static-error requirement is err113.
var ErrVolumeNumberBelowMinimum = errors.New("volume_number below minimum")

// ErrVolumeNumberAboveMaximum is the sentinel for the out-of-range-
// VlmNr rejection (volume_number > 65535). See
// ErrVolumeNumberBelowMinimum for the rationale.
var ErrVolumeNumberAboveMaximum = errors.New("volume_number above maximum")

// validateVolumeNumber returns nil when the requested explicit
// VolumeNumber is within DRBD-9's addressable range
// [minVolumeNumber, maxVolumeNumber] (Bug 363). Otherwise it returns a
// human-readable rejection reason the caller formats into the LINSTOR
// envelope. The auto-assign path (no explicit volume_number in the
// POST body) is always valid by construction and bypasses this gate.
func validateVolumeNumber(vn int32) error {
	if vn < minVolumeNumber {
		return fmt.Errorf(
			"%w: volume_number=%d below minimum %d (DRBD volume numbers are unsigned)",
			ErrVolumeNumberBelowMinimum, vn, minVolumeNumber,
		)
	}

	if vn > maxVolumeNumber {
		return fmt.Errorf(
			"%w: volume_number=%d above maximum %d (DRBD-9 caps the per-resource "+
				"volume namespace at 16 bits)",
			ErrVolumeNumberAboveMaximum, vn, maxVolumeNumber,
		)
	}

	return nil
}

// writeVDNumberRejection emits the Bug 363 volume-number-out-of-bounds
// refusal envelope. 400 + FAIL_INVLD_VLM_NR keeps the wire shape
// consistent with the Bug 155 size-rejection path; the inner
// cause/correction names the addressable range so the operator can
// pick a fresh VlmNr without re-reading the spec.
func writeVDNumberRejection(w http.ResponseWriter, rd string, vn int32, reason error) {
	writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInvldVlmNr,
		Message: fmt.Sprintf("invalid volume definition number for %q vlm=%d: %s",
			rd, vn, reason.Error()),
		Cause: fmt.Sprintf(
			"volume_number must be in [%d, %d]; the satellite reconciler "+
				"would hang in 'waiting for DRBD-ID allocation' otherwise",
			minVolumeNumber, maxVolumeNumber,
		),
		Correc: fmt.Sprintf(
			"pick a volume_number between %d and %d and re-issue `linstor vd c`",
			minVolumeNumber, maxVolumeNumber,
		),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	}})
}

// ErrVolumeSizeBelowMinimum is the sentinel for Bug 155's lower-bound
// rejection (size_kib < minVolumeDefinitionSizeKib). Wrapped with
// %w + a sizeKib / bound detail by validateVDSize; static-error
// requirement is err113.
var ErrVolumeSizeBelowMinimum = errors.New("size_kib below minimum")

// ErrVolumeSizeAboveMaximum is the sentinel for Bug 155's upper-bound
// rejection (size_kib > maxVolumeDefinitionSizeKib). See
// ErrVolumeSizeBelowMinimum for the rationale.
var ErrVolumeSizeAboveMaximum = errors.New("size_kib above maximum")

// validateVDSize returns nil when the requested size_kib is within
// the accepted bounds [minVolumeDefinitionSizeKib,
// maxVolumeDefinitionSizeKib] (Bug 155). Otherwise it returns a
// human-readable rejection reason the caller formats into the
// LINSTOR envelope.
func validateVDSize(sizeKib int64) error {
	if sizeKib < minVolumeDefinitionSizeKib {
		return fmt.Errorf(
			"%w: size_kib=%d below minimum %d KiB (DRBD reserves ~32 KiB of "+
				"metadata per peer; backing layers add alignment on top)",
			ErrVolumeSizeBelowMinimum, sizeKib, minVolumeDefinitionSizeKib,
		)
	}

	if sizeKib > maxVolumeDefinitionSizeKib {
		return fmt.Errorf(
			"%w: size_kib=%d above maximum %d KiB (DRBD's per-device hard ceiling)",
			ErrVolumeSizeAboveMaximum, sizeKib, maxVolumeDefinitionSizeKib,
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
			"pick a size between %d KiB (~4 MiB) and %d KiB (~1 PiB) and re-issue `linstor vd c`",
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

	vn, ok := parseAndValidateVDPathVolNum(w, rd, r.PathValue("vn"))
	if !ok {
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

	if rejectVDPatchSize(w, r, &patch, rd, vn, previousSizeKib) {
		return
	}

	if refuseUnwhitelistedProps(w, propwhitelist.VolumeDefinition, patch.OverrideProps) ||
		refuseUnwhitelistedProps(w, propwhitelist.VolumeDefinition, patch.Props) {
		return
	}

	// Bug 204b: route the merge-write through PatchVolumeDefinitionSpec
	// so the modify delta is re-applied to the freshly-fetched VD on
	// every retry. The previous `Get → mutate → Update` path's retry
	// loop replayed the caller's stale wire snapshot and silently lost
	// concurrent prop edits on the same VD. The answer waits until the
	// cache shows the write, so a read after it does too.
	err = s.patchVolumeDefinition(r.Context(), rd, vn, &patch)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 136: on a grow, stamp the per-resource resize-pending
	// annotation. See stampResizePendingOnResources for rationale.
	if patch.SizeKib != nil && *patch.SizeKib > previousSizeKib {
		s.stampResizePendingOnResources(r.Context(), rd, vn, *patch.SizeKib)
	}

	entries, outcome := s.waitForVDPatchGrown(r.Context(), rd, vn, patch.SizeKib, previousSizeKib)

	writeVDUpdateAnswer(w, &patch, rd, vn, previousSizeKib, entries, outcome)
}

// writeVDUpdateAnswer answers a volume definition modify after its grow
// wait, by how that wait ended.
func writeVDUpdateAnswer(
	w http.ResponseWriter, patch *volumeDefinitionModifyBody, rd string, vn int32, previousSizeKib int64,
	entries []apiv1.APICallRc, outcome growOutcome,
) {
	envelope := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition modified",
	}}

	switch outcome {
	case outcomeCancelled:
		// Answer even though the caller may be gone: when it is this
		// server shutting down, the connection is still open, and an
		// empty 200 reads as a finished grow. A gone caller drops it.
		writeJSON(w, http.StatusServiceUnavailable, shutdownDuringResize(rd, vn))
	case outcomeFailed:
		writeJSON(w, http.StatusInternalServerError, append(envelope, entries...))
	case outcomeGrown:
		envelope = appendForceShrinkAdvisory(envelope, patch, rd, vn, previousSizeKib)
		envelope = append(envelope, entries...)

		writeJSON(w, http.StatusOK, envelope)
	}
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

// parseAndValidateVDPathVolNum decodes `{vn}` from the URL path and
// runs the Bug 365 range check, writing the typed envelope and
// returning ok=false on either failure. Extracted out of
// handleVDUpdate to keep the HTTP handler under the funlen budget.
func parseAndValidateVDPathVolNum(w http.ResponseWriter, rd, rawVN string) (int32, bool) {
	vn, err := parseVolNum(rawVN)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return 0, false
	}

	// Bug 365: reject out-of-range volume_number at the PUT/PATCH
	// wire boundary too. Symmetric to Bug 363 (create) and the
	// vd-get / vd-delete gates — keeps the entire VD CRUD surface
	// consistent on the addressable [0, 65535] DRBD-9 range.
	vnErr := validateVolumeNumber(vn)
	if vnErr != nil {
		writeVDNumberRejection(w, rd, vn, vnErr)

		return 0, false
	}

	return vn, true
}

// rejectVDPatchSize runs the two size-related preflight checks on a
// VD PUT patch: the Bug 383 non-positive floor and the scenario 4.W13
// shrink-refusal. Returns true when either rejection fired (the HTTP
// envelope is already written) so the caller short-circuits. Split
// out of handleVDUpdate to keep the handler under the funlen budget;
// preserves the documented evaluation order (floor first, then shrink-
// vs-force) so the operator-facing error message stays accurate.
func rejectVDPatchSize(
	w http.ResponseWriter, r *http.Request, patch *volumeDefinitionModifyBody,
	rd string, vn int32, previousSizeKib int64,
) bool {
	// Bug 383 (P3, hunt-caught 2026-06-02): reject `size_kib <= 0` on
	// PUT regardless of `force=true`. See rejectVDNonPositiveSize.
	if rejectVDNonPositiveSize(w, patch, rd, vn) {
		return true
	}

	// Adversarial round 4 (2026-07-03): mirror the CREATE path's Bug 155
	// bounds gate on the RESIZE path. The create path refuses size_kib
	// outside [4 MiB, 1 PiB] via validateVDSize so the satellite never
	// hot-loops on `drbdadm create-md`; `linstor vd set-size` previously
	// skipped that check, so a below-floor force-shrink or an over-ceiling
	// grow was stored verbatim and reproduced the Bug 155 hot-loop through
	// the resize verb. Runs — like the Bug 383 non-positive floor above —
	// BEFORE the shrink-vs-force gate: `force` waives the shrink-direction
	// opt-in, never the physical floor/ceiling, and checking bounds first
	// gives the operator the accurate "invalid size" envelope instead of an
	// "add --force" hint on a size that would be refused even with force.
	if rejectVDPatchOutOfBounds(w, patch, rd, vn) {
		return true
	}

	// Scenario 4.W13: reject any shrink (`new < previous`) unless the
	// operator opted in via `force=true`. Runs BEFORE the merge + store
	// write so a rejected shrink leaves the stored spec untouched — a
	// partial update would desync the controller spec from the
	// satellite reality.
	return rejectShrinkWithoutForce(w, r, patch, rd, vn, previousSizeKib)
}

// rejectVDPatchOutOfBounds writes a 400 + FAIL_INVLD_VLM_SIZE envelope
// when the patch carries a `size_kib` outside the accepted
// [minVolumeDefinitionSizeKib, maxVolumeDefinitionSizeKib] range and
// returns true to signal the caller to short-circuit. Reuses the create
// path's validateVDSize + writeVDSizeRejection so the wire shape is
// byte-identical across the create and resize verbs (Bug 155 parity).
// A patch that does not touch size (`SizeKib == nil`) is left alone.
func rejectVDPatchOutOfBounds(
	w http.ResponseWriter, patch *volumeDefinitionModifyBody, rd string, vn int32,
) bool {
	if patch.SizeKib == nil {
		return false
	}

	sizeErr := validateVDSize(*patch.SizeKib)
	if sizeErr == nil {
		return false
	}

	writeVDSizeRejection(w, rd, vn, *patch.SizeKib, sizeErr)

	return true
}

// rejectVDNonPositiveSize writes a 400 + FAIL_INVLD_VLM_SIZE envelope
// when the patch carries a non-positive `size_kib` and returns true
// to signal the caller to short-circuit.
//
// Bug 383 (P3, hunt-caught 2026-06-02): force=true MUST NOT relax the
// absolute non-positive floor on PUT. The shrink path's force escape
// hatch was scoped only at "no auto-shrink" (callers that already ran
// `resize2fs -s` know the new size is below the live one); it was
// never intended to let a caller persist `size_kib=0` or a negative
// value into the spec. The satellite reconciler then looped on
// `drbdadm create-md` indefinitely (DRBD's per-device minimum is
// ~4 MiB once metadata is reserved), identical to the Bug 381 spawn-
// fast-path footgun but reached through the PUT update path. Rejected
// before the shrink-check so the operator-facing message is the right
// one (invalid size, not "filesystem shrink-then-resize").
func rejectVDNonPositiveSize(
	w http.ResponseWriter, patch *volumeDefinitionModifyBody, rd string, vn int32,
) bool {
	if patch.SizeKib == nil || *patch.SizeKib > 0 {
		return false
	}

	writeVDSizeRejection(w, rd, vn, *patch.SizeKib,
		fmt.Errorf("%w: size_kib=%d must be > 0 (force=true does NOT relax this floor)",
			ErrVolumeSizeBelowMinimum, *patch.SizeKib))

	return true
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

		// I1: override_props is the set-property surface where an
		// empty value deletes the key (set-property KEY ""). The
		// legacy `props` full-shape overlay keeps plain copy semantics.
		applyPropsModify(existing.Props, patch.OverrideProps, nil)
		maps.Copy(existing.Props, patch.Props)
	}

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}

	deletePropNamespaces(existing.Props, patch.DeleteNamespaces)
}

// handleVDDelete deletes a VolumeDefinition the way LINSTOR's
// CtrlVlmDfnDeleteApiCallHandler does: refused while a replica is in use,
// and under `StorDriver/Zfs/DeleteStrategy` DELETE while the definition has
// snapshots; otherwise the definition is flagged DELETE, every replica's
// satellite deletes its own volume of it (ZFS renaming a zvol that still has
// snapshots, as a replica delete does), and the definition leaves the
// resource definition once every replica has confirmed. The answer waits
// for that the way LINSTOR's waits for its satellites, up to the same budget
// a replica delete waits, and says which nodes it is still waiting on when
// the budget runs out. The definition stays, flagged DELETE, until they
// confirm or `node lost` takes their replicas away; the controller finishes
// the delete then. A definition with no replicas goes at once.
//
// Idempotent on NotFound (Bug 66): both NotFound shapes, the parent RD
// missing and the (rd, vn) pair missing inside an extant RD, fold into a
// 200 + warn-mask envelope. linstor-csi's ControllerExpand / shrink paths
// re-issue `vd d` on retry. A repeated delete of a definition still
// flagged joins the delete in flight.
//
// Bug 355 (P2): refuses with 409 + FAIL_IN_USE | MASK_ERROR only when at
// least one Resource on the parent RD is observed in-use (DRBD Primary with
// a mounted consumer), LINSTOR's `anyResourceInUsePrivileged`. Secondary
// replicas are no refusal cause. `?force=true` is a transport-level escape
// from that refusal for curl scripts; linstor-client has no flag for it.
// Nothing escapes the ZFS strategy refusal, as in LINSTOR.
func (s *Server) handleVDDelete(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	force := isForce(r)

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Bug 365 (P2, hunt-caught 2026-06-02): reject out-of-range
	// volume_number before the store masks it as "already absent". Bug 363
	// pins the [0, 65535] range at create-time; this gate mirrors it at
	// the DELETE wire boundary so the contract is symmetric.
	vnErr := validateVolumeNumber(vn)
	if vnErr != nil {
		writeVDNumberRejection(w, rd, vn, vnErr)

		return
	}

	// Both refusals run before anything is written: a refused delete
	// leaves the definition exactly as it was.
	if !force && s.refuseVDDeleteIfReferenced(w, r, rd, vn) {
		return
	}

	if s.refuseZFSDeleteStrategy(w, r, rd) {
		return
	}

	_, _, err = store.MarkVolumeDefinitionDeleted(r.Context(), s.Store, rd, vn)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			writeStoreError(w, err)

			return
		}

		// Bug 139: even on the idempotent no-op branch, drain the local
		// cache so a re-issued DELETE during a real delete in flight is
		// still read-your-writes on the follow-up view.
		s.waitForVDDeletionVisible(r.Context(), rd, vn)

		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnVDNotFound,
			Message: "Resource definition: " + rd + ", Volume number: " + strconv.FormatInt(int64(vn), 10) + " not found.",
			ObjRefs: vdObjRefs(rd, vn),
		}})

		return
	}

	progress, err := s.waitForVDDeleteFinished(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if progress.Removed {
		// Bug 139: prune the deleted VolumeNumber off each child
		// Resource's Volumes and wait for the removal to be observable on
		// the local store, so the next `v l` on this replica does not
		// still show the volume.
		s.pruneVolumesFromResources(r.Context(), rd, vn)
		s.waitForVDDeletionVisible(r.Context(), rd, vn)
	}

	writeJSON(w, http.StatusOK, vdDeleteAnswer(rd, vn, progress))
}

// vdDescription is LINSTOR's inline name of a volume definition, capitalised
// as its answers start with it.
func vdDescription(rd string, vn int32) string {
	return "Volume definition with number '" + strconv.FormatInt(int64(vn), 10) +
		"' of resource definition '" + rd + "'"
}

func vdObjRefs(rd string, vn int32) map[string]string {
	return map[string]string{
		objRefRscDfn: rd,
		objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
	}
}

// vdDeleteAnswer is the delete's answer in LINSTOR's order: the definition
// marked for deletion, each replica that deleted its volume, and then either
// the definition deleted or the nodes it still waits on.
func vdDeleteAnswer(rd string, vn int32, progress store.VolumeDefinitionDeleteProgress) []apiv1.APICallRc {
	answer := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: vdDescription(rd, vn) + " marked for deletion.",
		ObjRefs: vdObjRefs(rd, vn),
	}}

	for _, node := range progress.Confirmed {
		answer = append(answer, apiv1.APICallRc{
			RetCode: maskInfo,
			Message: "Deleted volume " + strconv.FormatInt(int64(vn), 10) + " of '" + rd + "' on '" + node + "'",
			ObjRefs: vdObjRefs(rd, vn),
		})
	}

	if progress.Removed {
		return append(answer, apiv1.APICallRc{
			RetCode: maskInfo,
			Message: vdDescription(rd, vn) + " deleted.",
			ObjRefs: vdObjRefs(rd, vn),
		})
	}

	return append(answer, apiv1.APICallRc{
		RetCode: warnDeleteInProgress,
		Message: "deletion of volume " + strconv.FormatInt(int64(vn), 10) + " of '" + rd + "' is in progress on " +
			strings.Join(progress.Pending, ", ") + ": the satellite has not confirmed yet that the volume's storage is gone",
		Details: "The volume definition stays listed with the DELETE flag until every replica's satellite has deleted its volume. " +
			"A satellite that is not running holds it until it starts again; for a node that is gone for good, use `node lost`.",
		ObjRefs: vdObjRefs(rd, vn),
	})
}

// vdDeleteWaitBudget bounds how long `vd d` waits for the replicas to
// confirm before it answers "in progress". One confirmation is a chain: the
// definition update reaches the satellite, its reconcile runs `drbdadm
// adjust`, deletes the storage and writes the status, and the controller
// wakes on it. That often takes longer than a cache convergence, and an
// answer given early sends an immediate `vd c` of the same number into a
// refusal. LINSTOR answers once its satellites have.
const vdDeleteWaitBudget = 30 * time.Second

// vdDeleteFirstPoll and vdDeleteMaxPoll bound the back-off between the
// attempts to finish the delete. Each attempt reads the definition, its
// spellings and its replicas uncached, so a fast confirmation is seen
// quickly while a slow one costs a few dozen calls rather than hundreds.
const (
	vdDeleteFirstPoll = 100 * time.Millisecond
	vdDeleteMaxPoll   = time.Second
)

// waitForVDDeleteFinished tries to finish the delete until it holds, the
// budget runs out, or every replica it still waits for is on a node whose
// satellite is OFFLINE, which will not confirm before it is back. The
// controller keeps trying after that; a later answer is not needed for the
// delete to complete.
func (s *Server) waitForVDDeleteFinished(ctx context.Context, rd string, vn int32) (store.VolumeDefinitionDeleteProgress, error) {
	deadline := time.Now().Add(vdDeleteWaitBudget)
	poll := vdDeleteFirstPoll

	for {
		progress, err := store.FinishVolumeDefinitionDelete(ctx, s.Store, rd, vn)
		if err != nil {
			return progress, fmt.Errorf("finish the delete of volume definition %s/%d: %w", rd, vn, err)
		}

		if progress.Removed || time.Now().After(deadline) || s.allOffline(ctx, progress.Pending) {
			return progress, nil
		}

		select {
		case <-ctx.Done():
			return progress, nil
		case <-time.After(poll):
		}

		poll = min(2*poll, vdDeleteMaxPoll)
	}
}

// allOffline reports whether every named node's satellite is explicitly
// OFFLINE. A node it cannot read counts as online, so the wait is never cut
// short on a guess.
func (s *Server) allOffline(ctx context.Context, nodes []string) bool {
	if len(nodes) == 0 {
		return false
	}

	for _, node := range nodes {
		if isNodeOnline(ctx, s.Store.Nodes(), node) {
			return false
		}
	}

	return true
}

// refuseVDDeleteIfReferenced runs the Bug 355 pre-Delete walk:
// refuses with 409 + FAIL_IN_USE only when at least one Resource on
// the parent RD has its satellite-observed state reporting in_use=true
// (DRBD Primary with a mounted consumer). Returns true when the HTTP
// error has already been written (the caller must stop processing)
// and false when the delete may proceed.
//
// Earlier Bug 186 hardening (task #329) refused on ANY Resource that
// referenced the dropped VolumeNumber — stricter than upstream and
// broke `linstor vd d <rd> 0` on every multi-replica RD because each
// Secondary replica also carries a Volumes row for the dropped
// VolumeNumber. Bug 355 narrows the gate to "in-use only" so the
// cascade matches upstream's anyResourceInUsePrivileged refusal in
// CtrlVlmDfnDeleteApiCallHandler; Secondaries (and unobserved
// replicas with state.in_use == nil) are part of the upstream
// cascade path and MUST NOT be a refusal cause.
//
// Cause line names the in-use Resources sorted by NodeName so the
// surfaced text is deterministic across cache iteration orders. Wire
// shape mirrors Bug 92 (node delete in-use) and Bug 152 (sp delete
// in-use) — 409 + FAIL_IN_USE | MASK_ERROR. Correction points at the
// operator-actionable remedies (demote the Primary, unmount the
// consumer); the misleading `?force=true` suggestion the prior
// envelope carried is intentionally absent — linstor-client has no
// `--force` flag for `vd d`, so surfacing it gave the operator a
// dead-end remedy.
//
// Name retained from the prior Bug 186 shape (the function is the
// pre-Delete walk; "referenced" now means "in-use" specifically).
func (s *Server) refuseVDDeleteIfReferenced(w http.ResponseWriter, r *http.Request, rd string, vn int32) bool {
	inUse, err := s.resourcesInUseOnDefinition(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if len(inUse) == 0 {
		return false
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: fmt.Sprintf(
			"Volume definition %d on resource definition %q cannot be "+
				"deleted because the resource is in use on %s.",
			vn, rd, strings.Join(inUse, ", ")),
		Cause: fmt.Sprintf(
			"%d resource replica(s) on %q report in_use=true (DRBD "+
				"Primary with a mounted consumer): %s",
			len(inUse), rd, strings.Join(inUse, ", ")),
		Correc: "Demote the Primary on the listed node(s) first " +
			"(`linstor r role-demote " + rd + " <node>`) or unmount " +
			"the consumer of the PVC backed by " + rd + ", then retry.",
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	}})

	return true
}

// resourcesInUseOnDefinition returns the sorted-by-NodeName list of
// Resources on the parent RD whose satellite-observed state has
// `in_use == true` (DRBD Primary with a mounted consumer). Used by
// Bug 355's narrowed VD-delete refusal gate. Mirrors upstream
// LINSTOR's `anyResourceInUsePrivileged` shape: only an active
// Primary counts as in-use; Secondary replicas and unobserved
// replicas (state.in_use == nil) do NOT.
//
// Sort order pinned so the surfaced 409 envelope's Cause line is
// byte-identical across cache iteration orders (the same trick
// node_lifecycle.go uses for the in-use evacuate refusal).
func (s *Server) resourcesInUseOnDefinition(ctx context.Context, rd string) ([]string, error) {
	if s == nil || s.Store == nil {
		return nil, nil
	}

	resources, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return nil, err //nolint:wrapcheck // surfaced to writeStoreError
	}

	var inUse []string

	for i := range resources {
		res := &resources[i]
		if res.State.InUse != nil && *res.State.InUse {
			inUse = append(inUse, res.NodeName)
		}
	}

	sort.Strings(inUse)

	return inUse, nil
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

			for idx := range live.Volumes {
				if live.Volumes[idx].VolumeNumber == vn {
					dropped = true

					continue
				}

				out = append(out, live.Volumes[idx])
			}

			if !dropped {
				return nil
			}

			live.Volumes = out

			return nil
		})
	}
}

// parseVolNum decodes the {vn} / {vlmNr} URL pathvar into the int32
// volume-number DRBD expects. Bug 380 (P3, bughunt round 11): pre-fix
// the wire surfaced a raw `strconv.ParseInt: parsing "abc": invalid
// syntax` to the caller — an internal Go error string that leaks the
// stdlib func name and gives the operator zero hint about what the
// allowed range actually is. Symmetric with the Bug 365 / Bug 363
// envelope on the POST/DELETE body branches: an out-of-range or
// non-numeric `volume_number` returns the same `[0, 65535]` correction
// regardless of whether it arrived via path or body. parseVolNum
// itself returns the wire-ready error string so every caller
// (handleVDGet / handleVDDelete / handleVDPut / handleVolumeGet ...)
// stays a one-line `writeError(w, 400, err.Error())` — no new
// per-handler envelope drift.
func parseVolNum(raw string) (int32, error) {
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		// Same correction hint Bug 365 surfaces on the body branch —
		// volume_number is a DRBD wire field, [0, 65535].
		return 0, errors.Newf( //nolint:wrapcheck // wire-ready string, returned straight to writeError
			"invalid volume number %q in URL path: "+
				"volume_number must be a decimal integer in [0, 65535]; "+
				"check the LINSTOR REST API documentation",
			raw)
	}

	return int32(parsed), nil
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
const resizePendingAnnotationPrefix = "bug136.blockstor.org/resize-pending-size-kib-vol-"

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
