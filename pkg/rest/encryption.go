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
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/keyring"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// passphraseOpTimeout bounds every Secret access from an encryption
// handler. Bug 110 root cause: without `secrets` RBAC the controller-
// runtime cache informer stalls on a never-syncing Forbidden list/
// watch, and a cached Get blocks until the upstream client hangs up
// (~10s default). Capping keeps the worst case bounded. Argon2id runs
// once per slot on a verification, well inside the bound.
const passphraseOpTimeout = 10 * time.Second

// maxPassphraseAttempts is how many passphrase operations run at once;
// the rest wait for a turn until their request times out.
const maxPassphraseAttempts = 2

// encryptionStateTTL is how long the encryption state the resource view
// stamps `suspended` from is reused: the view walks every replica, and
// each fresh look is three API reads.
const encryptionStateTTL = 5 * time.Second

// passphraseRequest is the body upstream linstor expects on the
// encryption/passphrase endpoints.
//
// Two field names accepted on ALL three verbs (POST/PATCH/PUT) of
// `/v1/encryption/passphrase`:
//   - `new_passphrase` — the historical upstream-LINSTOR shape that
//     `linstor encryption create-passphrase` / `modify-passphrase`
//     and golinstor's `Passphrase.Create` / `Passphrase.Enter` have
//     always posted. Canonical.
//   - `passphrase` — the W13 bare-key shape `linstor encryption
//     enter-passphrase` documents post-Phase-10, also what
//     operator-facing `--curl` scripts and hand-rolled wire-shape
//     clients tend to send. Alias.
//
// Bug 165: pre-fix, POST (create) only honoured `new_passphrase`
// while PATCH (enter) honoured both via proofOfKnowledge — operators
// hitting the same wire surface with the same body got an asymmetric
// 400-then-200. The fix routes every handler through
// proofOfKnowledge so the create/enter/modify trio accepts the same
// dual-key body. `new_passphrase` wins when both are set so a
// typo-defensive caller (sending both for belt-and-braces) lands on
// the canonical upstream value rather than the alias.
type passphraseRequest struct {
	NewPassphrase string `json:"new_passphrase,omitempty"`
	OldPassphrase string `json:"old_passphrase,omitempty"`
	Passphrase    string `json:"passphrase,omitempty"`
}

// proofOfKnowledge returns the operator-supplied passphrase from the
// request body, honouring the dual-key wire surface above. Used by
// POST/PATCH/PUT alike so every encryption verb sees the same field
// resolution (Bug 165). Canonical `new_passphrase` wins when both
// fields are populated.
func (r passphraseRequest) proofOfKnowledge() string {
	if r.NewPassphrase != "" {
		return r.NewPassphrase
	}

	return r.Passphrase
}

// errNoEncryptionClient is a server built without an API server client:
// the keyring lives in a Secret.
var errNoEncryptionClient = errors.New("cluster passphrase requires an apiserver client")

// registerEncryption wires `linstor encryption *` endpoints, and the
// blockstor-only slot verbs under /passphrases.
//
// The master key that wraps every LUKS volume password lives in the
// keyring Secret, wrapped under one slot per passphrase; the passphrase
// the cluster unlocks from lives in the passphrase Secret the
// ControllerConfig names, or in the unlock Secret blockstor generates.
// Passphrases given through these endpoints live only in slots.
//
// The Secrets are read and written under the hand-written,
// resourceNames-limited Role in config/rbac/secrets_role.yaml, not a
// kubebuilder marker: a marker cannot name the Secrets, so it would grant
// every Secret in the namespace.
//
// +kubebuilder:rbac:groups=blockstor.org,resources=controllerconfigs,verbs=get;list;watch
func (s *Server) registerEncryption(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/encryption/passphrase",
		s.requireStore(s.handlePassphraseStatus))
	mux.HandleFunc("POST /v1/encryption/passphrase",
		s.requireStore(s.handlePassphraseCreate))
	mux.HandleFunc("PATCH /v1/encryption/passphrase",
		s.requireStore(s.handlePassphraseEnter))
	mux.HandleFunc("PUT /v1/encryption/passphrase",
		s.requireStore(s.handlePassphraseModify))
	mux.HandleFunc("GET /v1/encryption/passphrases",
		s.requireStore(s.handlePassphraseList))
	mux.HandleFunc("POST /v1/encryption/passphrases",
		s.requireStore(s.handlePassphraseAdd))
	mux.HandleFunc("DELETE /v1/encryption/passphrases/{slot}",
		s.requireStore(s.handlePassphraseRemove))
}

// passphraseStatus is the upstream `PassphraseStatus` schema: a single
// tri-state status field. It goes out as a bare object, as LINSTOR's
// Encryption.passphraseStatus writes it, even though rest_v1_openapi.yaml
// declares an array: python-linstor reads the reply as one object and
// fails on a list.
type passphraseStatus struct {
	Status string `json:"status"`
}

// encryption holds the server's unlocker, so a status or a view does not
// pay an Argon2id run per request, and the last state the view stamped.
type encryption struct {
	once     sync.Once
	unlocker *passphrase.Unlocker

	// viewOnce and viewUnlocker derive the view's state through
	// Server.StateReader.
	viewOnce     sync.Once
	viewUnlocker *passphrase.Unlocker

	// attempts bounds the passphrase operations in flight: each tries a
	// passphrase against up to eight slots at 64 MiB of Argon2id apiece.
	attemptsOnce sync.Once
	attempts     chan struct{}

	mu      sync.Mutex
	state   passphrase.State
	stateAt time.Time
}

// passphraseTurn waits for one of the maxPassphraseAttempts turns and
// returns its release, or answers 503 when ctx ends first.
//
// The turns are not handed out in arrival order: a client flooding wrong
// passphrases can keep a legitimate enter waiting into its 503. Only a
// client with a certificate reaches this, and the satellites and the
// controller unlock without it, so volumes keep opening; a fair queue is
// not worth its code here.
func (s *Server) passphraseTurn(ctx context.Context, w http.ResponseWriter) (func(), bool) {
	s.encryption.attemptsOnce.Do(func() {
		s.encryption.attempts = make(chan struct{}, maxPassphraseAttempts)
	})

	select {
	case s.encryption.attempts <- struct{}{}:
		return func() { <-s.encryption.attempts }, true
	case <-ctx.Done():
		writeError(w, http.StatusServiceUnavailable, "too many passphrase operations in flight; retry")

		return nil, false
	}
}

func (s *Server) passphraseOps() (*passphrase.Ops, error) {
	if s.Client == nil {
		return nil, errNoEncryptionClient
	}

	return &passphrase.Ops{Client: s.Client, Secrets: s.passphraseSecrets(), Now: nil}, nil
}

func (s *Server) passphraseSecrets() passphrase.Secrets {
	reader := s.SecretReader
	if reader == nil {
		reader = s.Client
	}

	return passphrase.Secrets{Reader: reader, Namespace: s.Namespace}
}

func (s *Server) encryptionStatus(ctx context.Context) (passphrase.Status, error) {
	if s.Client == nil {
		return passphrase.Status{}, errNoEncryptionClient
	}

	s.encryption.once.Do(func() {
		s.encryption.unlocker = &passphrase.Unlocker{Secrets: s.passphraseSecrets()}
	})

	return s.encryption.unlocker.Unlock(ctx) //nolint:wrapcheck // pkg/passphrase names the Secret it failed on
}

// viewEncryptionStatus is encryptionStatus read through StateReader.
func (s *Server) viewEncryptionStatus(ctx context.Context) (passphrase.Status, error) {
	if s.Client == nil {
		return passphrase.Status{}, errNoEncryptionClient
	}

	s.encryption.viewOnce.Do(func() {
		secrets := s.passphraseSecrets()
		if s.StateReader != nil {
			secrets.Reader = s.StateReader
		}

		s.encryption.viewUnlocker = &passphrase.Unlocker{Secrets: secrets}
	})

	return s.encryption.viewUnlocker.Unlock(ctx) //nolint:wrapcheck // pkg/passphrase names the Secret it failed on
}

// encryptionState is the cluster's encryption state for the resource
// view, reused for encryptionStateTTL. A state that cannot be read counts
// as locked: the view then says `Suspended`, which is what an operator
// should look into.
func (s *Server) encryptionState(ctx context.Context) passphrase.State {
	s.encryption.mu.Lock()
	defer s.encryption.mu.Unlock()

	if !s.encryption.stateAt.IsZero() && time.Since(s.encryption.stateAt) < encryptionStateTTL {
		return s.encryption.state
	}

	status, err := s.viewEncryptionStatus(ctx)
	if err != nil {
		status.State = passphrase.StateLocked
	}

	s.encryption.state, s.encryption.stateAt = status.State, time.Now()

	return status.State
}

// forgetEncryptionState drops the view's cached state after this server
// changed the keyring or the unlock Secret, so the next view reads them.
func (s *Server) forgetEncryptionState() {
	s.encryption.mu.Lock()
	defer s.encryption.mu.Unlock()

	s.encryption.stateAt = time.Time{}
}

// handlePassphraseStatus reports `unset` (no keyring), `locked` (the
// passphrase Secret opens no slot) or `unlocked`, derived from the two
// Secrets, so every apiserver replica answers the same.
func (s *Server) handlePassphraseStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	status, err := s.encryptionStatus(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, passphraseStatus{Status: string(status.State)})
}

func (s *Server) handlePassphraseCreate(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	want := req.proofOfKnowledge()
	if want == "" {
		writeError(w, http.StatusBadRequest, "new_passphrase is required")

		return
	}

	ops, err := s.passphraseOps()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	release, ok := s.passphraseTurn(ctx, w)
	if !ok {
		return
	}

	defer release()

	result, err := ops.Create(ctx, []byte(want))

	s.forgetEncryptionState()

	switch {
	case errors.Is(err, passphrase.ErrKeyringExists):
		writeError(w, http.StatusConflict, "cluster passphrase already set; PUT to modify with old passphrase")
	case errors.Is(err, passphrase.ErrSecretManaged), errors.Is(err, passphrase.ErrKeyringLost):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
	case result.AlreadySet:
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{RetCode: maskInfo, Message: "Master passphrase already set"}})
	default:
		writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{RetCode: maskInfo, Message: "Master passphrase created"}})
	}
}

func (s *Server) handlePassphraseEnter(w http.ResponseWriter, r *http.Request) {
	req, ok := decodePassphraseEnterBody(w, r)
	if !ok {
		return
	}

	ops, err := s.passphraseOps()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	release, ok := s.passphraseTurn(ctx, w)
	if !ok {
		return
	}

	defer release()

	result, err := ops.Enter(ctx, []byte(req.proofOfKnowledge()))

	s.forgetEncryptionState()

	if writePassphraseError(w, err) {
		return
	}

	if result.Status.State == passphrase.StateLocked {
		// The operator asked for an unlocked cluster and did not get one;
		// the client must say so, not print a success line.
		writeError(w, http.StatusConflict, fmt.Sprintf("the passphrase is right, but the cluster stays locked: "+
			"the passphrase Secret %s does not open the keyring; put a passphrase that does into it", result.Status.Ref.Name))

		return
	}

	message := "Master passphrase entered"
	if result.Repaired {
		message = "Master passphrase entered; the unlock Secret " + passphrase.UnlockSecretName + " was re-created"
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{RetCode: maskInfo, Message: message}})
}

func (s *Server) handlePassphraseModify(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	want := req.proofOfKnowledge()
	if want == "" {
		writeError(w, http.StatusBadRequest, "new_passphrase is required: modify must specify a non-empty new value")

		return
	}

	ops, err := s.passphraseOps()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	release, ok := s.passphraseTurn(ctx, w)
	if !ok {
		return
	}

	defer release()

	result, err := ops.Modify(ctx, []byte(req.OldPassphrase), []byte(want))

	s.forgetEncryptionState()

	if writePassphraseError(w, err) {
		return
	}

	switch {
	case result.Unchanged:
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{RetCode: maskInfo, Message: "Master passphrase unchanged (idempotent no-op)"}})
	case result.SecretToUpdate != "":
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{
			{RetCode: maskInfo, Message: "Master passphrase modified"},
			{RetCode: maskWarn, Message: fmt.Sprintf("the cluster still unlocks with the old passphrase from Secret %s; "+
				"put the new one there, and the old one's slot is removed once the Secret opens the new one", result.SecretToUpdate)},
		})
	default:
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{RetCode: maskInfo, Message: "Master passphrase modified"}})
	}
}

// passphraseSlot is one slot of `GET /v1/encryption/passphrases`.
type passphraseSlot struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Origin     string    `json:"origin"`
	CreatedAt  time.Time `json:"created_at"`
	Superseded bool      `json:"superseded,omitempty"`
	KDF        string    `json:"kdf"`
	Unlocks    bool      `json:"unlocks"`
}

func (s *Server) handlePassphraseList(w http.ResponseWriter, r *http.Request) {
	ops, err := s.passphraseOps()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	release, ok := s.passphraseTurn(ctx, w)
	if !ok {
		return
	}

	defer release()

	slots, _, err := ops.List(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	out := make([]passphraseSlot, 0, len(slots))
	for _, slot := range slots {
		out = append(out, passphraseSlot(slot))
	}

	writeJSON(w, http.StatusOK, out)
}

// passphraseAddRequest is the body of `POST /v1/encryption/passphrases`:
// a passphrase that opens an existing slot, the new one, and its name.
type passphraseAddRequest struct {
	Passphrase    string `json:"passphrase"`
	NewPassphrase string `json:"new_passphrase"`
	Name          string `json:"name"`
}

func (s *Server) handlePassphraseAdd(w http.ResponseWriter, r *http.Request) {
	var req passphraseAddRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	if req.Passphrase == "" || req.NewPassphrase == "" || req.Name == "" {
		writeError(w, http.StatusBadRequest, "passphrase, new_passphrase and name are required")

		return
	}

	ops, err := s.passphraseOps()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	release, ok := s.passphraseTurn(ctx, w)
	if !ok {
		return
	}

	defer release()

	slotID, err := ops.Add(ctx, []byte(req.Passphrase), []byte(req.NewPassphrase), req.Name)
	if writePassphraseError(w, err) {
		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo, Message: fmt.Sprintf("Passphrase %s added as slot %s", req.Name, slotID),
	}})
}

// passphraseRemoveRequest is the body of `DELETE
// /v1/encryption/passphrases/{slot}`: a passphrase of another slot.
type passphraseRemoveRequest struct {
	Passphrase string `json:"passphrase"`
}

func (s *Server) handlePassphraseRemove(w http.ResponseWriter, r *http.Request) {
	ops, err := s.passphraseOps()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	release, ok := s.passphraseTurn(ctx, w)
	if !ok {
		return
	}

	defer release()

	var req passphraseRemoveRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	if req.Passphrase == "" {
		writeError(w, http.StatusBadRequest, "passphrase is required: a passphrase that stays authorises the removal")

		return
	}

	slot := r.PathValue("slot")

	err = ops.Remove(ctx, []byte(req.Passphrase), slot)
	if writePassphraseError(w, err) {
		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{RetCode: maskInfo, Message: "Passphrase slot " + slot + " removed"}})
}

// writePassphraseError answers err, if there is one, with the status its
// kind calls for, and reports whether it wrote.
func writePassphraseError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}

	status := http.StatusInternalServerError

	switch {
	case errors.Is(err, keyring.ErrWrongPassphrase):
		writeError(w, http.StatusUnauthorized, "passphrase mismatch")

		return true
	case errors.Is(err, passphrase.ErrNoKeyring):
		status = http.StatusPreconditionFailed
	case errors.Is(err, keyring.ErrSlotNotFound):
		status = http.StatusNotFound
	case errors.Is(err, keyring.ErrInvalidName):
		status = http.StatusBadRequest
	case errors.Is(err, passphrase.ErrUnlockSlot), errors.Is(err, keyring.ErrLastSlot),
		errors.Is(err, passphrase.ErrLocked), errors.Is(err, passphrase.ErrLastHumanSlot), errors.Is(err, passphrase.ErrRemoveSelf),
		errors.Is(err, keyring.ErrDuplicateName), errors.Is(err, keyring.ErrTooManySlots):
		status = http.StatusConflict
	}

	writeError(w, status, err.Error())

	return true
}

// decodePassphraseEnterBody implements the Bug 173 dual-shape decode
// for PATCH `/v1/encryption/passphrase`. The upstream OpenAPI spec
// pins `PassPhraseEnter: type: string` (bare JSON string body) so
// golinstor and strict-OpenAPI clients send `"…"` verbatim, while
// the historical Bug 165 dual-key wrapped object form
// (`{"passphrase":"…"}` / `{"new_passphrase":"…"}`) is still on the
// wire from every `--curl` and pre-strict client out there.
//
// The decoder picks between the two by peeking at the first
// non-whitespace byte:
//
//   - `"` → bare-string branch. Decoded into a plain `string` and
//     stuffed into NewPassphrase so the downstream
//     proofOfKnowledge() resolution stays dual-key-agnostic. An
//     empty `""` body proceeds (the auth path will surface 401).
//   - `{` → wrapped-object branch. Routed through the standard
//     decodeJSON path so the Bug 158/161 envelope semantics
//     (DisallowUnknownFields, typed error envelope) still apply.
//   - anything else → 400 + standard envelope, same as a wrapped
//     object with the wrong top-level shape.
//
// Empty / truncated / oversize bodies go through `decodeJSON`'s
// existing branches so error-envelope shape stays uniform across
// the encryption endpoints.
func decodePassphraseEnterBody(w http.ResponseWriter, r *http.Request) (passphraseRequest, bool) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)

		return passphraseRequest{}, false
	}

	// Re-wrap the body so the wrapped-object branch can keep using
	// `decodeJSON` (Bug 158/161 envelope + DisallowUnknownFields).
	r.Body = io.NopCloser(bytes.NewReader(body))

	// First non-whitespace byte tells us whether the caller sent the
	// bare-string form (`"…"`) or the wrapped-object form (`{…}`).
	// Anything else is malformed and falls through to the standard
	// envelope.
	first, ok := firstJSONToken(body)
	if !ok {
		// Empty / whitespace-only body. Mirror `decodeJSON`'s
		// io.EOF branch by re-running it; we already consumed the
		// body so the Reader is empty and the std-lib returns EOF.
		writeDecodeError(w, io.EOF)

		return passphraseRequest{}, false
	}

	if first == '"' {
		var pass string

		dec := json.NewDecoder(bytes.NewReader(body))

		err = dec.Decode(&pass)
		if err != nil {
			writeDecodeError(w, err)

			return passphraseRequest{}, false
		}

		// Bug 203: refuse residual bytes after the bare-string value.
		// The std-lib decoder is happy after the closing quote, so a
		// body of `"valid-pass"trailing` decoded cleanly and stuffed
		// "valid-pass" into NewPassphrase — the downstream auth path
		// then ran on the partial value. Same dec.More() guard the
		// wrapped-object branch picks up via decodeJSON, applied here
		// directly because this branch decodes into a `string` rather
		// than through decodeJSON.
		if dec.More() {
			writeDecodeError(w, errTrailingJSONData)

			return passphraseRequest{}, false
		}

		// Stuff the bare-string value into NewPassphrase so the
		// downstream proofOfKnowledge() lookup keeps working
		// unchanged. NewPassphrase wins over Passphrase in
		// proofOfKnowledge so a future caller mixing forms is
		// impossible here (only one field is populated).
		return passphraseRequest{NewPassphrase: pass}, true
	}

	var req passphraseRequest

	if !decodeJSON(w, r, &req) {
		return passphraseRequest{}, false
	}

	return req, true
}

// firstJSONToken returns the first non-whitespace byte of a JSON
// payload and whether one was found. Whitespace per RFC 8259
// (space, tab, LF, CR). Used by decodePassphraseEnterBody to tell
// the bare-string and wrapped-object body shapes apart without
// double-decoding the payload.
func firstJSONToken(body []byte) (byte, bool) {
	for _, b := range body {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return b, true
		}
	}

	return 0, false
}
