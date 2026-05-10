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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// passphraseRequest is the body upstream linstor expects on the
// encryption/passphrase endpoints.
type passphraseRequest struct {
	NewPassphrase string `json:"new_passphrase"`
	OldPassphrase string `json:"old_passphrase,omitempty"`
}

// passphraseKey is the legacy property key under ControllerProps
// where the cluster passphrase lives in pre-Phase-10.4 clusters.
// Kept for the KV fallback path (in-memory tests, pre-migration
// data); production reads/writes go through the Secret instead.
//
//nolint:gosec // this is the storage key name, not the secret value itself
const passphraseKey = "Cluster/EncryptionPassphrase"

// defaultPassphraseSecretName is the Secret the controller falls
// back to when ControllerConfig.Spec.PassphraseSecretRef is unset.
// Operators override via the ControllerConfig CRD.
const defaultPassphraseSecretName = "blockstor-cluster-passphrase"

// passphraseSecretKey is the data key inside the Secret carrying
// the cluster passphrase. Matches the upstream-LINSTOR-on-k8s
// convention so existing Secret YAML manifests continue to work.
const passphraseSecretKey = "passphrase"

// registerEncryption wires `linstor encryption *` endpoints. The
// cluster passphrase is the master key used to encrypt LUKS volume
// keys at rest.
//
// Phase 10.4: when the Server is wired with a controller-runtime
// `Client` + `Namespace`, the passphrase lives in a native Secret
// (default `blockstor-cluster-passphrase`, key `passphrase`).
// Without a Client we fall back to the legacy KV path so
// in-memory-store tests and pre-migration clusters keep working.
//
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=controllerconfigs,verbs=get;list;watch
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

	have, err := s.readPassphrase(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if have != "" {
		writeError(w, http.StatusConflict, "cluster passphrase already set; PATCH to modify")

		return
	}

	err = s.writePassphrase(r.Context(), req.NewPassphrase)
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

	have, err := s.readPassphrase(r.Context())
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

	have, err := s.readPassphrase(r.Context())
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

	err = s.writePassphrase(r.Context(), req.NewPassphrase)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	w.WriteHeader(http.StatusOK)
}

// readPassphrase reads the current cluster passphrase. Empty string
// (not error) means "not yet set". Picks the Secret path when the
// Server is wired with a controller-runtime Client, falls back to
// the legacy KV path otherwise.
func (s *Server) readPassphrase(ctx context.Context) (string, error) {
	if s.Client != nil {
		return s.readPassphraseSecret(ctx)
	}

	return getPassphraseKV(ctx, s.Store)
}

// writePassphrase persists the cluster passphrase. Same path
// selection as readPassphrase.
func (s *Server) writePassphrase(ctx context.Context, value string) error {
	if s.Client != nil {
		return s.writePassphraseSecret(ctx, value)
	}

	return setPassphraseKV(ctx, s.Store, value)
}

// readPassphraseSecret reads the passphrase from a native Secret.
// Empty string for "Secret missing" is the explicit signal upstream
// uses for "no passphrase set yet" so the POST→PATCH handshake
// works the same as on the KV path.
func (s *Server) readPassphraseSecret(ctx context.Context) (string, error) {
	name, err := s.resolvePassphraseSecretName(ctx)
	if err != nil {
		return "", err
	}

	var sec corev1.Secret

	err = s.Client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: name}, &sec)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}

		return "", errors.Wrap(err, "get passphrase Secret")
	}

	return string(sec.Data[passphraseSecretKey]), nil
}

// writePassphraseSecret create-or-updates the Secret. The Secret is
// namespaced; pre-existing Secrets without our managed key get the
// key added rather than the whole data map replaced so operators
// can carry extra annotations / data fields out-of-band.
func (s *Server) writePassphraseSecret(ctx context.Context, value string) error {
	name, err := s.resolvePassphraseSecretName(ctx)
	if err != nil {
		return err
	}

	var sec corev1.Secret

	err = s.Client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: name}, &sec)
	if apierrors.IsNotFound(err) {
		sec = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{passphraseSecretKey: []byte(value)},
		}

		createErr := s.Client.Create(ctx, &sec)
		if createErr != nil {
			return errors.Wrap(createErr, "create passphrase Secret")
		}

		return nil
	}

	if err != nil {
		return errors.Wrap(err, "get passphrase Secret")
	}

	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}

	sec.Data[passphraseSecretKey] = []byte(value)

	err = s.Client.Update(ctx, &sec)
	if err != nil {
		return errors.Wrap(err, "update passphrase Secret")
	}

	return nil
}

// resolvePassphraseSecretName reads the singleton ControllerConfig
// and returns the configured Secret name, falling back to the
// default when the ControllerConfig is absent or doesn't pin a
// reference. The fallback lets operators get a working cluster
// without first applying a ControllerConfig CRD.
func (s *Server) resolvePassphraseSecretName(ctx context.Context) (string, error) {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := s.Client.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return defaultPassphraseSecretName, nil
		}

		return "", errors.Wrap(err, "get ControllerConfig")
	}

	if ctrlConfig.Spec.PassphraseSecretRef != nil && ctrlConfig.Spec.PassphraseSecretRef.Name != "" {
		return ctrlConfig.Spec.PassphraseSecretRef.Name, nil
	}

	return defaultPassphraseSecretName, nil
}

// getPassphraseKV reads the current cluster passphrase from the
// legacy KV-store path. Kept for in-memory-store tests and
// pre-migration clusters.
func getPassphraseKV(ctx context.Context, st store.Store) (string, error) {
	props, err := st.KeyValueStore().GetInstance(ctx, controllerPropsInstance)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", nil
		}

		return "", errors.Wrap(err, "read passphrase")
	}

	return props[passphraseKey], nil
}

// setPassphraseKV writes the cluster passphrase via SetKeys (so the
// merge semantic preserves any sibling controller props). Legacy
// KV-path counterpart to writePassphraseSecret.
func setPassphraseKV(ctx context.Context, st store.Store, value string) error {
	err := st.KeyValueStore().SetKeys(ctx, controllerPropsInstance, apiv1.GenericPropsModify{
		OverrideProps: map[string]string{passphraseKey: value},
	})
	if err != nil {
		return errors.Wrap(err, "write passphrase")
	}

	return nil
}
