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
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

// Historical note — the per-VD passphrase property.
//
// This handler used to stamp the operator-supplied key onto the
// volume definition's props bag under upstream LINSTOR's
// `DrbdOptions/Encrypt/Passphrase` name and answer `200 VD passphrase
// stored; cluster-side LUKS rotation pending Phase 12`. Two things
// were wrong with that, and neither was visible to the caller:
//
//  1. Nothing ever read the key back. The dispatcher lifts exactly
//     two spellings onto the `LuksPassphrase` wire prop —
//     `DrbdOptions/EncryptPassphrase` and
//     `DrbdOptions/Encryption/passphrase` (pkg/dispatcher) — and this
//     was neither. The volume kept the cluster passphrase while the
//     CLI reported success.
//  2. Volume definitions are inline in
//     `ResourceDefinition.spec.volumeDefinitions`, so the key landed
//     in cleartext in etcd, reachable by anyone holding `get
//     resourcedefinitions`. That is a materially wider RBAC surface
//     than Secret access, and in blockstor the CRDs ARE the store —
//     `kubectl get -o yaml` reads it straight out. The REST read path
//     does redact it (the `encrypt` needle in
//     sensitivePropSubstrings covers the key, and VD props are
//     scrubbed in volume_definitions.go), so this was an at-rest
//     disclosure through the Kubernetes door rather than a REST leak.
//
// Storing a secret in cleartext to power a feature that does not
// exist is all cost. The handler now refuses with a structured 501
// and persists nothing. Restoring the verb is part of the per-volume
// data-key work in docs/byok-design.md §4, which is also what makes
// the value meaningful.

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

// handleVDPassphraseRotate serves Bug 233's wire shape for
// `linstor vd set-passphrase`. It validates the parent RD + VD exist
// and that the body is well-formed, then refuses: blockstor has no
// per-volume key to set (see the historical note above).
//
// The route stays registered and the body decoding stays intact so
// golinstor and python-linstor keep parsing the exchange normally and
// the operator gets a specific ERROR line rather than a 404 that
// reads like a version mismatch.
//
// Status surface:
//   - missing parent RD or VD → 404 (writeStoreError)
//   - empty `new_passphrase` (any shape) → 400 + envelope. Retained
//     ahead of the 501 so the precedence callers already observe
//     (missing object → malformed request → unimplemented feature)
//     does not shift.
//   - malformed body → 400 + envelope (Bug 158/161 typed-error path)
//   - well-formed request → 501 + envelope naming the gap and
//     stating explicitly that the supplied key was not stored.
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

	// Probe for existence up-front so a missing VD surfaces 404
	// before the body validation's 400 — preserves the pre-Patch
	// error-precedence contract.
	_, err = s.Store.VolumeDefinitions().Get(r.Context(), rdName, int32(vlmNr))
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

	// The value is deliberately NOT persisted. See the historical note
	// at the top of this file: storing it put the operator's key in
	// cleartext into the RD CRD while no code path ever read it back,
	// so the volume kept the cluster key and the operator was told
	// otherwise. Refuse the verb until per-volume keys exist.
	//
	// `want` is validated above and then deliberately discarded — the
	// empty-body 400 stays ahead of this 501 so the wire contract
	// (404 → 400 → 501) does not shift under callers that already
	// distinguish "no such volume" from "malformed request".
	writeError(w, http.StatusNotImplemented,
		"per-volume LUKS passphrases are not implemented: blockstor derives every LUKS key "+
			"from the single cluster passphrase, so a per-volume key cannot take effect. "+
			"The supplied value was NOT stored — persisting it would have put your key in "+
			"cleartext on the ResourceDefinition while the volume kept the cluster key. "+
			"Keys written by earlier releases are still on disk and this refusal does not "+
			"remove them; docs/layer-stack.md has the purge recipe. See docs/byok-design.md.")
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
