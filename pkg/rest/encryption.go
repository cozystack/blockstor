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
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// passphraseOpTimeout bounds every Secret access from an encryption
// handler. Bug 110 root cause: without `secrets` RBAC the controller-
// runtime cache informer stalls on a never-syncing Forbidden list/
// watch, and a cached Get blocks until the upstream client hangs up
// (~10s default). Even with RBAC fixed, an apiserver-side outage
// would otherwise let a single bad request tie up a worker for the
// process-default request timeout. Capping at 5s keeps the worst
// case bounded and matches the operator's tolerance window — the
// CLI's surrounding retry loop kicks in well before this.
const passphraseOpTimeout = 5 * time.Second

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

// handlePassphraseCreate stamps the cluster-wide master passphrase
// into the sealed K8s Secret on first call (→ 201 Created). If a
// passphrase already exists, the body must match it byte-for-byte:
// same value → 200 OK (idempotent retry, scenario 6.W12), different
// value → 409 Conflict (rotations must go through PUT/modify with
// proof-of-knowledge of the old passphrase).
func (s *Server) handlePassphraseCreate(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	// Bug 158/161: typed-envelope decode + DisallowUnknownFields.
	if !decodeJSON(w, r, &req) {
		return
	}

	// Bug 165: accept BOTH `new_passphrase` (canonical upstream) and
	// `passphrase` (alias / W13 CLI shape) here so POST is symmetric
	// with the PATCH/PUT siblings. Pre-fix, POST checked only
	// req.NewPassphrase, so `--curl` callers sending `{"passphrase":…}`
	// got 400 here even though the very same body unlocked on PATCH.
	want := req.proofOfKnowledge()
	if want == "" {
		writeError(w, http.StatusBadRequest, "new_passphrase is required")

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	have, err := s.readPassphrase(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if have != "" {
		if have == want {
			// Idempotent re-create from a caller that
			// demonstrably knows the value — flip the
			// in-memory unlock too so a crash-loop-during-
			// bootstrap retry leaves the controller in the
			// same usable state as the original create.
			s.passphraseUnlocked.Store(true)

			w.WriteHeader(http.StatusOK)

			return
		}

		writeError(w, http.StatusConflict, "cluster passphrase already set; PUT to modify with old passphrase")

		return
	}

	err = s.writePassphrase(ctx, want)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Fresh-cluster create-passphrase: the operator just stamped
	// the value, so they implicitly hold the proof-of-knowledge.
	// Marking the controller unlocked here saves them an
	// immediately-following PATCH to start provisioning LUKS.
	s.passphraseUnlocked.Store(true)

	// Bug 129: python-linstor 1.27.1 unconditionally json-decodes
	// every non-204 2xx response, so a bare `WriteHeader(201)`
	// with no body crashes the CLI with "Unable to parse REST
	// json data: Expecting value: line 1 column 1 (char 0)".
	// Match the MASK_INFO envelope shape every sibling write-side
	// endpoint (handlePassphraseEnter / handlePassphraseModify,
	// controller_props create, autoplace create, …) emits so
	// python-linstor renders a clean operator-visible success line.
	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "Master passphrase created",
	}})
}

// handlePassphraseEnter unlocks the in-memory crypto context for
// this controller process. Scenario 6.W13: a controller restart
// loses the unlock flag, leaving the sealed-Secret passphrase
// dormant on the apiserver. The operator calls
// `linstor encryption enter-passphrase`, which PATCHes this endpoint
// with the master passphrase as proof-of-knowledge:
//
//   - missing prior passphrase → 412 (must POST/create first)
//   - wrong proof → 401 Unauthorized + descriptive error; the unlock
//     flag MUST stay false, otherwise a single wrong-passphrase
//     guess would silently grant LUKS provisioning.
//   - right proof → 200 OK + s.passphraseUnlocked flipped to true,
//     so the very next /v1/view/resources GET reports every LUKS
//     replica as Suspended=false (Available).
//
// Body shape accepts BOTH `{"new_passphrase":"..."}` (historical,
// upstream-LINSTOR-compatible) AND `{"passphrase":"..."}` (the W13
// CLI shape). passphraseRequest.proofOfKnowledge centralises the
// choice so the rest of the handler is dual-key-agnostic.
func (s *Server) handlePassphraseEnter(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	have, err := s.readPassphrase(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if have == "" {
		writeError(w, http.StatusPreconditionFailed, "no cluster passphrase set; POST to create")

		return
	}

	if have != req.proofOfKnowledge() {
		writeError(w, http.StatusUnauthorized, "passphrase mismatch")

		return
	}

	s.passphraseUnlocked.Store(true)

	w.WriteHeader(http.StatusOK)
}

// handlePassphraseModify rotates the cluster master passphrase
// (scenario 6.W14). PUT `/v1/encryption/passphrase` with body
// `{"old_passphrase":"…","new_passphrase":"…"}` swaps the sealed
// Secret's stored value: old must verify, new replaces it, atomic.
//
// Status surface:
//   - no prior passphrase → 412 Precondition Failed (must POST first)
//   - wrong old → 401 Unauthorized. Scenario 6.W14 narrows upstream's
//     historical 403 down to 401 — symmetric with the W13 PATCH
//     mismatch path so `linstor encryption modify-passphrase` and
//     `linstor encryption enter-passphrase` share one "wrong
//     passphrase" CLI string keyed off the status code. The unlock
//     flag MUST stay at its prior value; a regression that flipped
//     it on mismatch would silently grant LUKS provisioning to any
//     caller after a single wrong-old guess.
//   - new == old (and old verified) → 200 OK + MASK_INFO envelope,
//     no Secret write. Idempotent no-op for a retried CLI call.
//   - happy rotation → 200 OK + MASK_INFO envelope with an
//     operator-facing "modified" line, sealed Secret updated, and
//     s.passphraseUnlocked flipped to true atomic with the
//     create-passphrase + enter-passphrase paths (W12/W13). Without
//     the flip, a post-rotation /v1/view/resources would still
//     report Suspended on every LUKS replica until the operator
//     did a follow-up PATCH.
//
// The envelope shape (`[]ApiCallRc` with MASK_INFO + message)
// matches every other write-side endpoint in the apiserver so
// python-linstor's CLI loop renders the success line uniformly.
func (s *Server) handlePassphraseModify(w http.ResponseWriter, r *http.Request) {
	var req passphraseRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	// Bug 165: the NEW value accepts both `new_passphrase` (canonical)
	// and `passphrase` (alias) symmetric with POST/PATCH. The OLD
	// value stays on its own `old_passphrase` field — that one has no
	// alias in either upstream LINSTOR or the W13 CLI shape.
	want := req.proofOfKnowledge()

	ctx, cancel := context.WithTimeout(r.Context(), passphraseOpTimeout)
	defer cancel()

	have, err := s.readPassphrase(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if have == "" {
		writeError(w, http.StatusPreconditionFailed, "no cluster passphrase set")

		return
	}

	if have != req.OldPassphrase {
		writeError(w, http.StatusUnauthorized, "old passphrase mismatch")

		return
	}

	// Idempotent no-op: caller proved knowledge of the old
	// passphrase AND asked us to "rotate" to the same value.
	// Skip the Secret write entirely so the resourceVersion
	// stays put (no needless apiserver churn) but still flip
	// the unlock flag — the caller demonstrably knows the
	// value, exactly like the W12 same-value POST path.
	if want == have {
		s.passphraseUnlocked.Store(true)

		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "Master passphrase unchanged (idempotent no-op)",
		}})

		return
	}

	err = s.writePassphrase(ctx, want)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Successful rotation implies the caller knew the old
	// passphrase and just stamped the new one — they're unlocked
	// by definition. Without this flip, a post-rotation
	// /v1/view/resources would still report Suspended on every
	// LUKS replica until a follow-up PATCH, which surprises any
	// operator who thought modify-passphrase was enough.
	s.passphraseUnlocked.Store(true)

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "Master passphrase modified",
	}})
}

// readPassphrase reads the current cluster passphrase. Empty
// string (not error) means "not yet set". Secret-backed only —
// the legacy KVEntry fallback was retired in Phase 10.6.
func (s *Server) readPassphrase(ctx context.Context) (string, error) {
	if s.Client == nil {
		return "", errors.New("cluster passphrase requires an apiserver client")
	}

	return s.readPassphraseSecret(ctx)
}

// writePassphrase persists the cluster passphrase. Same path
// selection as readPassphrase.
func (s *Server) writePassphrase(ctx context.Context, value string) error {
	if s.Client == nil {
		return errors.New("cluster passphrase requires an apiserver client")
	}

	return s.writePassphraseSecret(ctx, value)
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
