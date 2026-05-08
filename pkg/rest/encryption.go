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
	"net/http"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// passphraseRequest is the body upstream linstor expects on the
// encryption/passphrase endpoints.
type passphraseRequest struct {
	NewPassphrase string `json:"new_passphrase"`
	OldPassphrase string `json:"old_passphrase,omitempty"`
}

// registerEncryption wires `linstor encryption *` endpoints. The
// cluster passphrase is the master key used to encrypt LUKS volume
// keys at rest in the controller's KV store.
//
// Storage scaffolding only for now: the passphrase is held verbatim
// under the ControllerProps KV instance. Real KDF + at-rest encryption
// of the per-volume keys is Phase 6 work.
func (s *Server) registerEncryption(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/encryption/passphrase",
		s.requireStore(s.handlePassphraseCreate))
	mux.HandleFunc("PATCH /v1/encryption/passphrase",
		s.requireStore(s.handlePassphraseEnter))
	mux.HandleFunc("PUT /v1/encryption/passphrase",
		s.requireStore(s.handlePassphraseModify))
}

// handlePassphraseCreate sets the passphrase the first time. 409 if
// one already exists; the caller is supposed to PATCH to change.
func (s *Server) handlePassphraseCreate(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if req.NewPassphrase == "" {
		writeError(w, http.StatusBadRequest, "new_passphrase is required")

		return
	}

	have, err := getPassphrase(r.Context(), s.Store)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if have != "" {
		writeError(w, http.StatusConflict, "cluster passphrase already set; PATCH to modify")

		return
	}

	err = setPassphrase(r.Context(), s.Store, req.NewPassphrase)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	w.WriteHeader(http.StatusCreated)
}

// handlePassphraseEnter unlocks the in-memory crypto context for this
// controller process. We treat the request body's `new_passphrase` as
// the proof-of-knowledge — matches upstream's PATCH semantics.
func (s *Server) handlePassphraseEnter(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	have, err := getPassphrase(r.Context(), s.Store)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if have == "" {
		writeError(w, http.StatusPreconditionFailed, "no cluster passphrase set; POST to create")

		return
	}

	if have != req.NewPassphrase {
		writeError(w, http.StatusForbidden, "passphrase mismatch")

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handlePassphraseModify rotates the passphrase. Old must verify, new
// replaces it.
func (s *Server) handlePassphraseModify(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	have, err := getPassphrase(r.Context(), s.Store)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if have == "" {
		writeError(w, http.StatusPreconditionFailed, "no cluster passphrase set")

		return
	}

	if have != req.OldPassphrase {
		writeError(w, http.StatusForbidden, "old passphrase mismatch")

		return
	}

	err = setPassphrase(r.Context(), s.Store, req.NewPassphrase)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	w.WriteHeader(http.StatusOK)
}

// getPassphrase reads the current cluster passphrase. Empty string
// (not error) for "not yet set".
func getPassphrase(ctx context.Context, st store.Store) (string, error) {
	props, err := st.KeyValueStore().GetInstance(ctx, controllerPropsInstance)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", nil
		}

		return "", errors.Wrap(err, "read passphrase")
	}

	return props[passphraseKey], nil
}

// setPassphrase writes the cluster passphrase via SetKeys (so the
// merge semantic preserves any sibling controller props).
func setPassphrase(ctx context.Context, st store.Store, value string) error {
	err := st.KeyValueStore().SetKeys(ctx, controllerPropsInstance, apiv1.GenericPropsModify{
		OverrideProps: map[string]string{passphraseKey: value},
	})
	if err != nil {
		return errors.Wrap(err, "write passphrase")
	}

	return nil
}

// passphraseKey is the property key under ControllerProps where the
// cluster passphrase lives. The upstream-compatible name keeps
// satellites that look it up by string identifier working.
//
//nolint:gosec // this is the storage key name, not the secret value itself
const passphraseKey = "Cluster/EncryptionPassphrase"
