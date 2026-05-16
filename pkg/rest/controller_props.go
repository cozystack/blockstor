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
	"maps"
	"net/http"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerControllerProperties wires the cluster-wide controller
// property bag. linstor CLI's `controller list-properties` calls
// /v1/controller/properties; `controller set-property` calls POST.
// Backed by the singleton `ControllerConfig` CRD's
// `Spec.ExtraProps` (Phase 10.4 — replaces the legacy KVEntry
// "ControllerProps" instance).
func (s *Server) registerControllerProperties(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/controller/properties", s.requireStore(s.handleControllerPropsGet))
	mux.HandleFunc("POST /v1/controller/properties", s.requireStore(s.handleControllerPropsModify))
	// LINSTOR property keys embed slashes (e.g. "Aux/trace-recorder-stamp"),
	// so the per-key DELETE route uses Go 1.22's `{key...}` wildcard
	// matcher to consume the remaining path. Without `...` the
	// default `{key}` only matches a single non-slash segment and
	// every Aux/Foo-style key would 404.
	mux.HandleFunc("DELETE /v1/controller/properties/{key...}", s.requireStore(s.handleControllerPropDelete))
	// GET /v1/controller/config is golinstor's `Controller.GetConfig()`.
	// Upstream returns a deep ControllerConfig tree (db / http / log /
	// debug / ...); blockstor doesn't run the JVM-based config layer so
	// every field would be zero. Return an empty object — every field
	// is `omitempty` so the wire shape is `{}` which deserializes into
	// a zero-value ControllerConfig without error.
	mux.HandleFunc("GET /v1/controller/config", handleControllerConfig)
	// Bug 159: `linstor c set-log-level <LVL>` routes through PUT on
	// the same path (python-linstor 1.27.1, linstorapi.py:3146-3173).
	// Before this wire-up the apiserver only registered GET so the CLI
	// got 405 + the Bug 109 typed envelope — clean error, but the
	// operator could not change the log level at all. handlePutControllerConfig
	// translates the upstream nested shape `{"log":{"level":"<LVL>"}}`
	// (and a flat operator-friendly `{"log_level":"<LVL>"}` alias)
	// onto the same runtimeLogLevel LevelVar applyRuntimeLogLevel
	// already mutates on the property-bag path.
	mux.HandleFunc("PUT /v1/controller/config", handlePutControllerConfig)
}

// handleControllerConfig returns an empty ControllerConfig object.
// blockstor's config lives in k8s (Deployment env, ConfigMaps, the
// ControllerConfig CRD's typed fields) — there's no JVM-style flat
// config file to expose. The empty `{}` satisfies golinstor's decoder
// without leaking anything implementation-specific.
func handleControllerConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, struct{}{})
}

// handleControllerPropsGet returns ControllerConfig.Spec.ExtraProps
// as a flat map. A missing ControllerConfig CRD returns an empty
// map (LINSTOR CLI happily renders zero properties).
func (s *Server) handleControllerPropsGet(w http.ResponseWriter, r *http.Request) {
	if s.Client == nil {
		writeJSON(w, http.StatusOK, map[string]string{})

		return
	}

	props, err := readControllerProps(r.Context(), s.Client)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Bug 115: never let `DrbdOptions/EncryptPassphrase` (or any
	// deny-listed sensitive key) leak through the read-only `c lp`
	// surface. readControllerProps returns the live backing map —
	// copy before mutating so a future caller (or a sibling handler
	// in the same request) that re-reads gets the un-redacted view.
	wire := maps.Clone(props)
	redactSensitiveProps(wire)

	writeJSON(w, http.StatusOK, wire)
}

// handleControllerPropsModify applies an OverrideProps /
// DeleteProps batch onto ControllerConfig.Spec.ExtraProps. The
// CRD is auto-created on first write (canonical name `default`).
func (s *Server) handleControllerPropsModify(w http.ResponseWriter, r *http.Request) {
	if s.Client == nil {
		writeError(w, http.StatusServiceUnavailable, "controller properties require an apiserver client")

		return
	}

	var modify apiv1.GenericPropsModify

	err := json.NewDecoder(r.Body).Decode(&modify)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	err = applyControllerProps(r.Context(), s.Client, &modify)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Scenario 7.W06: `controller set-log-level DEBUG` lands as a
	// LogLevel property write on the controller. Apply the runtime
	// flip AFTER the CRD write so a persistence failure doesn't
	// silently change the slog level — operators expect the
	// list-properties output and the live log stream to agree.
	applyRuntimeLogLevel(&modify)

	// Java LINSTOR returns 201 Created for a property-bag mutation
	// (one ApiCallRc per override key plus one "Controller properties
	// applied" entry per peer). The contract test collapses that array
	// to a single semantic class — return one info entry so the
	// collapsed shape matches.
	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "controller properties applied",
	}})
}

// handleControllerPropDelete removes one property from
// ControllerConfig.Spec.ExtraProps. The key is captured by the
// `{key...}` wildcard so slash-bearing keys like
// "Aux/trace-recorder-stamp" round-trip intact. Missing CRD /
// missing key are folded into success: LINSTOR treats
// "delete a property that wasn't set" as a no-op, not a 404.
func (s *Server) handleControllerPropDelete(w http.ResponseWriter, r *http.Request) {
	if s.Client == nil {
		writeError(w, http.StatusServiceUnavailable, "controller properties require an apiserver client")

		return
	}

	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing property key")

		return
	}

	err := deleteControllerProp(r.Context(), s.Client, key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "property deleted",
	}})
}

// deleteControllerProp removes a single key from ExtraProps. A
// missing ControllerConfig or absent key returns nil — LINSTOR's
// `controller drop-property` is idempotent in the same way.
func deleteControllerProp(ctx context.Context, c client.Client, key string) error {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return errors.Wrap(err, "get ControllerConfig")
	}

	if ctrlConfig.Spec.ExtraProps == nil {
		return nil
	}

	if _, present := ctrlConfig.Spec.ExtraProps[key]; !present {
		return nil
	}

	delete(ctrlConfig.Spec.ExtraProps, key)

	err = c.Update(ctx, &ctrlConfig)
	if err != nil {
		return errors.Wrap(err, "update ControllerConfig")
	}

	return nil
}

// readControllerProps fetches the singleton ControllerConfig and
// returns its ExtraProps bag. NotFound is folded into an empty
// map so the LINSTOR CLI never sees a 500 on a fresh cluster.
func readControllerProps(ctx context.Context, c client.Client) (map[string]string, error) {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return map[string]string{}, nil
		}

		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	if ctrlConfig.Spec.ExtraProps == nil {
		return map[string]string{}, nil
	}

	return ctrlConfig.Spec.ExtraProps, nil
}

// applyControllerProps merges an OverrideProps / DeleteProps
// batch into ControllerConfig.Spec.ExtraProps. Creates the CRD
// on first write so a fresh cluster doesn't need an explicit
// `kubectl apply` of the ControllerConfig manifest before
// `linstor controller set-property` works.
func applyControllerProps(ctx context.Context, c client.Client, modify *apiv1.GenericPropsModify) error {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if apierrors.IsNotFound(err) {
		ctrlConfig = blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec:       blockstoriov1alpha1.ControllerConfigSpec{ExtraProps: map[string]string{}},
		}

		mergeControllerProps(&ctrlConfig, modify)

		err = c.Create(ctx, &ctrlConfig)
		if err != nil {
			return errors.Wrap(err, "create ControllerConfig")
		}

		return nil
	}

	if err != nil {
		return errors.Wrap(err, "get ControllerConfig")
	}

	mergeControllerProps(&ctrlConfig, modify)

	err = c.Update(ctx, &ctrlConfig)
	if err != nil {
		return errors.Wrap(err, "update ControllerConfig")
	}

	return nil
}

// mergeControllerProps applies the OverrideProps / DeleteProps
// merge semantic LINSTOR uses: set keys land first, then delete
// keys remove their entries. Same precedence as the upstream
// "merge into existing" rule.
func mergeControllerProps(ctrlConfig *blockstoriov1alpha1.ControllerConfig, modify *apiv1.GenericPropsModify) {
	if ctrlConfig.Spec.ExtraProps == nil {
		ctrlConfig.Spec.ExtraProps = map[string]string{}
	}

	maps.Copy(ctrlConfig.Spec.ExtraProps, modify.OverrideProps)

	for _, k := range modify.DeleteProps {
		delete(ctrlConfig.Spec.ExtraProps, k)
	}
}
