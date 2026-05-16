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
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// runtimeLogLevel is the process-wide slog.LevelVar the REST layer
// flips when an operator runs `linstor controller set-log-level
// DEBUG`. UG9 §"Logging" (lines 3926+) says the change must take
// effect immediately — no pod restart — so a LevelVar is the right
// fit: every call to slog.Default().Handler() reads it on every
// emit, so a single Set() reroutes every subsequent log line at
// runtime. Scenario 7.W06.
//
// We use a package-level var (not a Server field) because:
//   - the apiserver may have N replicas behind a Service; an operator
//     PUT lands on exactly one of them and that replica flips its OWN
//     level. (The Java controller has a single in-memory instance so
//     "the controller's level" is unambiguous; in K8s the closest
//     equivalent is "all replicas converge eventually" — the apiserver
//     write also persists the LogLevel prop to ControllerConfig.ExtraProps,
//     and on next pod start the controller will re-apply it. The PUT
//     itself only mutates the receiving replica's runtime level — a
//     deliberate trade-off versus pushing a Reconcile-driven fan-out
//     for a debug-only knob.)
//   - slog.SetDefault() is itself process-global, so wrapping the
//     LevelVar in a struct adds no value.
var runtimeLogLevel = &slog.LevelVar{} //nolint:gochecknoglobals // runtime-tunable global log level

// PropLogLevel is the controller-scope property key that flips the
// runtime slog level. Upstream LINSTOR exposes the same lever via
// `PUT /v1/controller/config` with `{"log":{"level":"DEBUG"}}`; we
// surface it as a regular controller property so the existing
// ControllerPropsModify route handles persistence + RBAC + audit
// without a second endpoint. The CLI command
// `controller set-log-level DEBUG` ultimately writes this prop.
const PropLogLevel = "LogLevel"

// LINSTOR-CLI level vocabulary, normalised to upper-case. UG9
// §"Logging" (lines 3926+) names exactly this set; aliases ("WARN"
// vs "WARNING") match what the python-linstor-client accepts so an
// operator who types `set-log-level warning` doesn't surprise us.
const (
	logLevelTrace   = "TRACE"
	logLevelDebug   = "DEBUG"
	logLevelInfo    = "INFO"
	logLevelWarn    = "WARN"
	logLevelWarning = "WARNING"
	logLevelError   = "ERROR"
)

// traceBelowDebug is the offset we use to map LINSTOR's TRACE level
// onto slog (which has no native TRACE). slog.LevelDebug is -4 and
// slog.LevelInfo is 0 — the four-unit step is the conventional
// "next slog level down" gap, so TRACE = LevelDebug - 4 sits one
// notch below DEBUG just like LINSTOR's hierarchy says.
const traceBelowDebug slog.Level = 4

// parseLogLevel maps the LINSTOR-CLI level vocabulary
// (TRACE / DEBUG / INFO / WARN / ERROR) onto slog levels. TRACE has
// no slog equivalent — we map it to slog.LevelDebug-traceBelowDebug
// so a TRACE flip is strictly noisier than DEBUG (matches the
// upstream "TRACE < DEBUG" ordering). Returns (level, true) on a
// known value; (_, false) on garbage so the caller can reject the
// PUT without mutating runtime state.
func parseLogLevel(s string) (slog.Level, bool) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case logLevelTrace:
		return slog.LevelDebug - traceBelowDebug, true
	case logLevelDebug:
		return slog.LevelDebug, true
	case logLevelInfo:
		return slog.LevelInfo, true
	case logLevelWarn, logLevelWarning:
		return slog.LevelWarn, true
	case logLevelError:
		return slog.LevelError, true
	default:
		return 0, false
	}
}

// applyRuntimeLogLevel flips the process-wide slog level if `modify`
// carries a LogLevel prop. The actual ControllerConfig CRD write
// still happens via the regular applyControllerProps path — this
// function is the runtime "echo" side that makes the flip take
// effect without a pod restart. No-ops cleanly when LogLevel is
// absent (the common case for non-debug property writes).
//
// Unknown values are silently ignored on the runtime side: the
// property still persists in ExtraProps (so `controller
// list-properties` shows what the operator typed), but the slog
// level keeps its previous value. Strict validation lives in the
// CLI / linter — the REST layer treats unknown levels the same way
// LINSTOR treats unknown DrbdOptions/* keys: stored, not enforced.
func applyRuntimeLogLevel(modify *apiv1.GenericPropsModify) {
	if modify == nil {
		return
	}

	raw, present := modify.OverrideProps[PropLogLevel]
	if !present {
		return
	}

	level, ok := parseLogLevel(raw)
	if !ok {
		return
	}

	runtimeLogLevel.Set(level)
}

// RuntimeLogLevel returns the current runtime log level. Exported
// so cmd/apiserver and integration tests can bind their slog
// handler to the same LevelVar — without that binding the
// `set-log-level` PUT would mutate an orphan variable that no
// handler reads. cmd/apiserver wires this on startup so the
// scenario 7.W06 invariant ("flip without restart") holds.
func RuntimeLogLevel() *slog.LevelVar { return runtimeLogLevel }

// putControllerConfigBody mirrors the subset of upstream's
// ControllerConfig wire shape that blockstor honours on
// PUT /v1/controller/config. python-linstor 1.27.1
// (linstorapi.py:3146-3173) builds the nested form
// `{"log":{"level"|"level_linstor"|"level_global"|"level_linstor_global":"<LVL>"}}`
// — the *_global variants ask the controller to fan the flip out to
// every satellite, *_linstor restricts it to LINSTOR's own logger
// (not its dependency libraries). blockstor doesn't run a JVM-style
// dependency-library logger and the fan-out half is a
// controller-runtime concern, so every variant collapses to "flip
// the receiving replica's slog level". We also accept the flat
// `{"log_level":"<LVL>"}` shape so an operator running `curl` (or a
// hand-rolled client) doesn't need to know the nested wrapper —
// matches the operator-friendly shape the Bug 159 tests pin.
type putControllerConfigBody struct {
	LogLevel string                             `json:"log_level"`
	Log      *putControllerConfigBodyLogSubtree `json:"log"`
}

// putControllerConfigBodyLogSubtree is the nested `log` object the
// upstream wire shape uses. All four `Level*` fields fall back to
// each other in order so the operator's choice of (glob, library)
// flag still flips the receiving replica's level.
type putControllerConfigBodyLogSubtree struct {
	Level              string `json:"level"`
	LevelLinstor       string `json:"level_linstor"`
	LevelGlobal        string `json:"level_global"`
	LevelLinstorGlobal string `json:"level_linstor_global"`
}

// pickLevel returns the first non-empty level string from the
// nested `log` object, mirroring the python-linstor precedence
// (level_linstor_global → level_global → level_linstor → level).
// The CLI only ever sets one of them — but a hand-rolled client
// might set several, so we prefer the most-specific one.
func (b *putControllerConfigBodyLogSubtree) pickLevel() string {
	if b == nil {
		return ""
	}

	switch {
	case b.LevelLinstorGlobal != "":
		return b.LevelLinstorGlobal
	case b.LevelGlobal != "":
		return b.LevelGlobal
	case b.LevelLinstor != "":
		return b.LevelLinstor
	default:
		return b.Level
	}
}

// acceptedLogLevels is the upstream-approved level vocabulary,
// surfaced verbatim in the rejection envelope so the operator can
// self-correct after typing `linstor c set-log-level <typo>`.
// Matches the LogLevel enum in pkg/api/openapi/types.gen.go.
var acceptedLogLevels = []string{logLevelTrace, logLevelDebug, logLevelInfo, logLevelWarn, logLevelError} //nolint:gochecknoglobals // canonical enum

// handlePutControllerConfig wires the upstream-shaped
// PUT /v1/controller/config. Bug 159: before this handler the
// apiserver only wired GET on the path, so `linstor c set-log-level
// DEBUG` (which routes through PUT per python-linstor 1.27.1)
// returned 405 + the Bug 109 typed envelope — clean error, but the
// operator couldn't change the level at all. The fix is a thin
// translator from the nested upstream wire shape onto the same
// runtime LevelVar that applyRuntimeLogLevel already mutates for
// the property-bag path.
//
// The handler is intentionally minimal: blockstor's ControllerConfig
// CRD doesn't carry a `log.level` typed field (the upstream JVM
// config is not mirrored — see handleControllerConfig's empty `{}`
// reply), so the only persisted side-effect is the receiving
// replica's slog level. A future cross-replica fan-out would need
// to land the level in ControllerConfig.Spec.ExtraProps (under
// PropLogLevel) so a restart re-applies it — for now operators who
// want persistence should `linstor c sp LogLevel <LVL>` instead,
// which goes through the controller-properties path.
func handlePutControllerConfig(w http.ResponseWriter, r *http.Request) {
	var body putControllerConfigBody

	// Bug 197: route the decode through the canonical decodeJSON
	// helper so empty / malformed / unknown-field bodies hit the
	// Bug 158/161 typed-envelope path (no raw `EOF` / `invalid
	// character ...` text on the wire, unknown top-level fields
	// refused with 400 + envelope citing the offending field).
	// Pre-fix this handler used a bare `json.NewDecoder` + raw
	// `writeError(400, err.Error())`, the one decode site Bug 161's
	// sweep missed because Bug 159 landed two days after Bug 161.
	if !decodeJSON(w, r, &body) {
		return
	}

	raw := body.LogLevel
	if raw == "" {
		raw = body.Log.pickLevel()
	}

	if raw == "" {
		writeError(w, http.StatusBadRequest,
			`missing log level: send {"log_level":"<LVL>"} or {"log":{"level":"<LVL>"}}; accepted levels: `+
				strings.Join(acceptedLogLevels, ", "),
		)

		return
	}

	level, ok := parseLogLevel(raw)
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Sprintf(
			"unknown log level %q; accepted levels: %s",
			raw, strings.Join(acceptedLogLevels, ", "),
		))

		return
	}

	runtimeLogLevel.Set(level)

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "log level set to " + strings.ToUpper(strings.TrimSpace(raw)),
	}})
}
