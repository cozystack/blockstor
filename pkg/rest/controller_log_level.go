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
	"log/slog"
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
