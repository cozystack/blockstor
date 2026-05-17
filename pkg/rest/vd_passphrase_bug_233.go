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
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// vdPassphrasePropKey is the upstream-compatible per-VD LUKS
// passphrase property name. Stored on the VD's props bag so the
// satellite-side reconciler (once the cluster-side `cryptsetup
// luksChangeKey` orchestration lands in Phase 12) can pick it up
// via the same drbd_options channel ApplyResources already
// serialises. Mirrors upstream LINSTOR's
// `DrbdOptions/Encrypt/Passphrase` namespace so existing tooling and
// golinstor clients can read it back without translation.
//
//nolint:gosec // this is a property-name constant, not the secret value itself
const vdPassphrasePropKey = "DrbdOptions/Encrypt/Passphrase"

// vdPassphraseRotateBody mirrors upstream Java's
// `JsonGenTypes.VolumeDefinitionModifyPassphrase` — a single
// `new_passphrase` field carrying the new per-VD LUKS key. We also
// accept the bare-string `PassPhraseEnter` body shape (Bug 173) so
// strict-OpenAPI clients posting `"…"` directly land here cleanly.
type vdPassphraseRotateBody struct {
	NewPassphrase string `json:"new_passphrase,omitempty"`

	// Passphrase is the Bug 165 / 173 alias for the same field —
	// `--curl` callers and W13-shape clients send this name. The
	// canonical `new_passphrase` wins when both are populated.
	Passphrase string `json:"passphrase,omitempty"`
}

// proofOfKnowledge returns the operator-supplied passphrase honouring
// the dual-key wire surface. Canonical `new_passphrase` wins when
// both are set so a typo-defensive caller (sending both for
// belt-and-braces) lands on the upstream-canonical value.
func (b vdPassphraseRotateBody) proofOfKnowledge() string {
	if b.NewPassphrase != "" {
		return b.NewPassphrase
	}

	return b.Passphrase
}

// handleVDPassphraseRotate serves Bug 233. Validates the parent RD +
// VD exist (404 otherwise), then accepts a `{"new_passphrase":"…"}`
// wrapped object, a `{"passphrase":"…"}` alias, OR a bare JSON
// string `"…"` (the upstream `PassPhraseEnter` spec shape) and
// stamps the value onto the VD's props bag under the
// upstream-compatible `DrbdOptions/Encrypt/Passphrase` key.
//
// The actual satellite-side LUKS-header re-encryption (the
// `cryptsetup luksChangeKey` orchestration) is pending Phase 12; the
// wire-shape registration here is what unblocks
// `linstor vd set-passphrase`. Once the cluster-side rotation
// reconciler lands, this handler will additionally enqueue a
// rotation task — for now the persisted prop is the source-of-truth
// for the next reconcile pass.
//
// Status surface:
//   - missing parent RD or VD → 404 (writeStoreError)
//   - empty `new_passphrase` (any shape) → 400 + envelope (data-loss
//     guard mirroring the Bug 172 cluster-passphrase contract — an
//     empty rotation would erase the VD's LUKS key)
//   - malformed body → 400 + envelope (Bug 158/161 typed-error path)
//   - happy path → 200 + MASK_INFO envelope ("VD passphrase queued
//     for rotation, cluster-side orchestration pending Phase 12"),
//     so python-linstor's success line renders without confusion.
func (s *Server) handleVDPassphraseRotate(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	vlmNrRaw := r.PathValue("vlmNr")

	vlmNr, err := strconv.ParseInt(vlmNrRaw, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest,
			"invalid vlmNr path segment: "+vlmNrRaw+" is not an integer")

		return
	}

	// Verify the parent RD exists first so a missing RD surfaces 404,
	// not 500 from the VD store's downstream chain.
	_, err = s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	vd, err := s.Store.VolumeDefinitions().Get(r.Context(), rdName, int32(vlmNr))
	if err != nil {
		writeStoreError(w, err)

		return
	}

	body, ok := decodeVDPassphraseBody(w, r)
	if !ok {
		return
	}

	want := body.proofOfKnowledge()
	if want == "" {
		// Bug 172-class data-loss guard: an empty rotation would
		// stamp `""` into the VD's LUKS passphrase prop, and the
		// next reconcile pass would re-encrypt the LUKS header with
		// an empty key — silently erasing the operator-supplied
		// per-VD secret while returning 200. Refuse loudly.
		writeError(w, http.StatusBadRequest,
			"new_passphrase is required: rotation must specify a non-empty value")

		return
	}

	if vd.Props == nil {
		vd.Props = map[string]string{}
	}

	vd.Props[vdPassphrasePropKey] = want

	err = s.Store.VolumeDefinitions().Update(r.Context(), rdName, &vd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "VD passphrase stored; cluster-side LUKS rotation pending Phase 12",
	}})
}

// decodeVDPassphraseBody accepts BOTH the wrapped object shape
// (`{"new_passphrase":"…"}` / `{"passphrase":"…"}`) AND the bare
// JSON string shape `"…"` (upstream `PassPhraseEnter` spec).
// Mirrors the dual-shape decoder in
// `decodePassphraseEnterBody` (Bug 173) so the per-VD route shares
// one wire contract with the cluster-passphrase PATCH and the
// strict-spec golinstor client doesn't need to know which endpoint
// expects which envelope.
//
// Empty / truncated / malformed bodies fall through to the standard
// envelope via `decodeJSON` (wrapped path) or `writeDecodeError`
// (bare-string path).
func decodeVDPassphraseBody(w http.ResponseWriter, r *http.Request) (vdPassphraseRotateBody, bool) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)

		return vdPassphraseRotateBody{}, false
	}

	// Re-wrap so the wrapped-object branch can keep using decodeJSON
	// (Bug 158/161 envelope + DisallowUnknownFields).
	r.Body = io.NopCloser(bytes.NewReader(raw))

	first, ok := firstJSONToken(raw)
	if !ok {
		writeDecodeError(w, io.EOF)

		return vdPassphraseRotateBody{}, false
	}

	if first == '"' {
		var pass string

		dec := json.NewDecoder(bytes.NewReader(raw))

		err = dec.Decode(&pass)
		if err != nil {
			writeDecodeError(w, err)

			return vdPassphraseRotateBody{}, false
		}

		// Bug 203 parity: refuse residual bytes after the closing
		// quote so a body of `"valid-pass"trailing` doesn't decode
		// the partial value silently.
		if dec.More() {
			writeDecodeError(w, errTrailingJSONData)

			return vdPassphraseRotateBody{}, false
		}

		return vdPassphraseRotateBody{NewPassphrase: pass}, true
	}

	var body vdPassphraseRotateBody

	if !decodeJSON(w, r, &body) {
		return vdPassphraseRotateBody{}, false
	}

	return body, true
}
