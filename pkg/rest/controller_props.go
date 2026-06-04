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
	"context"
	"log/slog"
	"maps"
	"net/http"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
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

// handleControllerConfig returns the populated ControllerConfig
// subset blockstor supports. Bug-hunt v0.1.3 Finding 15: pre-fix
// this returned bare `{}`, breaking `linstor c v` config display and
// any client that does `cfg.log.level` to read the current logger
// state without a PUT round-trip.
//
// Populated sub-objects:
//
//   - `log.level`: derived from the runtime slog level (PUT
//     /v1/controller/config flips this same LevelVar — Bug 159).
//   - `http.enabled`: literal true; the apiserver only exists when
//     HTTP is wired, so reaching this handler proves it.
//
// Other ControllerConfig sub-objects (debug, db, https, ldap) stay
// empty in this PR — blockstor doesn't run the JVM-style flat
// config layer, so there's nothing meaningful to expose there.
// Wider population lands when individual surfaces (e.g. mTLS for
// https) become user-tunable.
func handleControllerConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, controllerConfigEnvelope{
		Log: controllerConfigLog{
			Level: currentRuntimeLogLevelName(),
		},
		HTTP: controllerConfigHTTP{
			Enabled: true,
		},
	})
}

// controllerConfigEnvelope mirrors the upstream ControllerConfig
// JSON shape (subset). Bug-hunt v0.1.3 Finding 15. Only the
// sub-objects blockstor can meaningfully populate are non-empty;
// the rest stay zero-valued and serialise as `omitempty`-absent so
// the wire shape stays compact.
type controllerConfigEnvelope struct {
	Log  controllerConfigLog  `json:"log"`
	HTTP controllerConfigHTTP `json:"http"`
}

// controllerConfigLog mirrors the upstream log sub-object. We expose
// the runtime level only; the upstream `directory` / `rest_access_log`
// / `rest_access_log_mode` fields are JVM-specific and stay absent.
type controllerConfigLog struct {
	Level string `json:"level"`
}

// controllerConfigHTTP mirrors the upstream http sub-object. We
// expose `enabled` only — the apiserver's listen-address / port
// come from the K8s Service surface, not from the JVM-config file
// upstream is mirroring.
type controllerConfigHTTP struct {
	Enabled bool `json:"enabled"`
}

// currentRuntimeLogLevelName reverse-maps the runtimeLogLevel
// LevelVar to the upstream-CLI level vocabulary used by
// parseLogLevel (TRACE / DEBUG / INFO / WARN / ERROR). Mirrors the
// `set-log-level` PUT path so a GET → PUT round-trip is the
// identity for any value the operator can set.
//
// The mapping is exact at the parseLogLevel boundaries (LevelDebug
// → DEBUG, LevelInfo → INFO, ...) and uses the closest-bucket rule
// for intermediate values — the LevelVar's Level() always returns
// the value we Set() on the PUT path, so the only off-mapping case
// is the initial default LevelInfo (slog's zero value), which maps
// cleanly to INFO.
func currentRuntimeLogLevelName() string {
	level := runtimeLogLevel.Level()
	switch {
	case level <= slog.LevelDebug-traceBelowDebug:
		return logLevelTrace
	case level <= slog.LevelDebug:
		return logLevelDebug
	case level <= slog.LevelInfo:
		return logLevelInfo
	case level <= slog.LevelWarn:
		return logLevelWarn
	default:
		return logLevelError
	}
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

	if !decodeJSON(w, r, &modify) {
		return
	}

	err := applyControllerProps(r.Context(), s.Client, &modify)
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
//
// Routed through `k8sstore.PatchControllerExtraProps` so concurrent
// drop-property / set-property mutations against the singleton
// (Bug 204a) don't lose updates to one another. The NotFound fold
// happens at the helper boundary: the helper auto-creates an empty
// CRD if the operator hasn't applied one yet, which we then no-op
// against by skipping the delete when the key is absent.
func deleteControllerProp(ctx context.Context, c client.Client, key string) error {
	err := k8sstore.PatchControllerExtraProps(ctx, c, func(props map[string]string) error {
		delete(props, key)

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "patch ControllerConfig")
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
// batch into ControllerConfig.Spec.ExtraProps. Routed through
// `k8sstore.PatchControllerExtraProps` (Bug 204a): the helper
// auto-creates the singleton on NotFound, and retries the
// Get to mutate to Patch cycle under optimistic concurrency, so
// concurrent set-property / drop-property calls converge instead
// of clobbering one another via stale wire snapshots.
func applyControllerProps(ctx context.Context, c client.Client, modify *apiv1.GenericPropsModify) error {
	err := k8sstore.PatchControllerExtraProps(ctx, c, func(props map[string]string) error {
		applyControllerPropsModify(props, modify)

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "patch ControllerConfig")
	}

	return nil
}

// applyControllerPropsModify is the in-place OverrideProps / DeleteProps
// merge step extracted so PatchControllerExtraProps can re-run it on
// each retry against the freshly-fetched props bag.
func applyControllerPropsModify(props map[string]string, modify *apiv1.GenericPropsModify) {
	// I1: empty override value deletes the key (set-property KEY "").
	applyPropsModify(props, modify.OverrideProps, modify.DeleteProps)
}
