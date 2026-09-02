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

// Package color classifies blockstor/DRBD state strings into semantic
// classes and paints them with ANSI escapes.
//
// The CLI keeps the upstream client's at-a-glance semantics — healthy
// green, transitional yellow, broken red — because operators read these
// tables under incident pressure and the colour carries the signal. The
// mapping below is derived from the state vocabulary blockstor itself
// reports on CRD Status (pkg/satellite observer, DRBD-9 disk /
// connection / replication states) and from OBSERVED upstream client
// output. No upstream client source was consulted.
package color

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrBadMode is returned by ParseMode for an unrecognised --color value.
var ErrBadMode = errors.New("invalid color mode")

// Class is the semantic bucket a state string falls into.
type Class int

const (
	// Neutral is a real, unremarkable state (or an unrecognised one).
	// Unknown states land here on purpose: a state this CLI has never
	// seen must never be painted green and read as healthy.
	Neutral Class = iota
	// Healthy is a fully-converged, serving state.
	Healthy
	// Transitional is work in flight — resync, connecting, outdated
	// data that is being caught up. Not an error, not yet done.
	Transitional
	// Broken is a state an operator has to act on.
	Broken
)

// Mode is the --color flag's value.
type Mode int

const (
	// Auto paints only when stdout is an interactive terminal.
	Auto Mode = iota
	// Always paints regardless of the output being a pipe or a file.
	Always
	// Never disables painting entirely.
	Never
)

// ANSI SGR sequences. Kept as raw strings rather than pulling in a
// colour dependency: the palette is three colours wide and the
// classification rules — not the escapes — are the part worth testing.
const (
	ansiReset  = "\x1b[0m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiRed    = "\x1b[31m"
)

// healthyStates are the fully-converged states.
var healthyStates = map[string]struct{}{ //nolint:gochecknoglobals // static classification table
	"uptodate":    {},
	"online":      {},
	"connected":   {},
	"established": {},
	"ok":          {},
	"inuse":       {},
	"primary":     {},
}

// transitionalStates are in-flight, self-resolving states.
var transitionalStates = map[string]struct{}{ //nolint:gochecknoglobals // static classification table
	"synctarget":    {},
	"syncsource":    {},
	"pausedsyncs":   {},
	"pausedsynct":   {},
	"outdated":      {},
	"connecting":    {},
	"negotiating":   {},
	"consistent":    {},
	"wfbitmaps":     {},
	"wfbitmapt":     {},
	"wfsyncuuid":    {},
	"startingsyncs": {},
	"startingsynct": {},
	"ahead":         {},
	"behind":        {},
	"verifys":       {},
	"verifyt":       {},
	"attaching":     {},
	"detaching":     {},
}

// brokenStates are the states that need an operator.
var brokenStates = map[string]struct{}{ //nolint:gochecknoglobals // static classification table
	"inconsistent":   {},
	"failed":         {},
	"dunknown":       {},
	"unknown":        {},
	"offline":        {},
	"standalone":     {},
	"brokenpipe":     {},
	"networkfailure": {},
	"timeout":        {},
	"evicted":        {},
	"deleting":       {},
	"diskfailed":     {},
	"unconnected":    {},
	"faulty":         {},
	"error":          {},
}

// ClassifyState buckets a state string. Matching is case-insensitive
// and ignores a trailing sync percentage (`SyncTarget(45%)`), which the
// view layer appends for progress reporting.
func ClassifyState(state string) Class {
	key := normalise(state)
	if key == "" {
		return Neutral
	}

	if _, ok := healthyStates[key]; ok {
		return Healthy
	}

	if _, ok := transitionalStates[key]; ok {
		return Transitional
	}

	if _, ok := brokenStates[key]; ok {
		return Broken
	}

	return Neutral
}

// normalise lowercases, trims spaces and strips a `(NN%)` suffix.
func normalise(state string) string {
	key := strings.ToLower(strings.TrimSpace(state))

	if idx := strings.IndexByte(key, '('); idx > 0 {
		key = strings.TrimSpace(key[:idx])
	}

	return key
}

// ParseMode maps the --color flag value; empty means auto.
func ParseMode(in string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(in)) {
	case "", "auto":
		return Auto, nil
	case "always", "force", "yes":
		return Always, nil
	case "never", "none", "no":
		return Never, nil
	default:
		return Auto, fmt.Errorf("%w: %q (want auto, always or never)", ErrBadMode, in)
	}
}

// EnabledFor decides whether to paint. `always` wins over every
// heuristic; otherwise painting needs an interactive terminal, no
// NO_COLOR in the environment (the https://no-color.org convention) and
// a TERM that can render escapes.
func EnabledFor(mode Mode, isTTY bool) bool {
	switch mode {
	case Never:
		return false
	case Always:
		return true
	case Auto:
	}

	if !isTTY {
		return false
	}

	if _, noColor := os.LookupEnv("NO_COLOR"); noColor {
		return false
	}

	if term := os.Getenv("TERM"); term == "dumb" {
		return false
	}

	return true
}

// Writer paints strings, or does not.
type Writer struct {
	enabled bool
}

// New returns a Writer that paints only when enabled.
func New(enabled bool) Writer {
	return Writer{enabled: enabled}
}

// Enabled reports whether this Writer paints.
func (w Writer) Enabled() bool {
	return w.enabled
}

// Paint wraps s in the class's colour. Neutral is never painted, so
// ordinary cells stay free of escape noise, and a disabled Writer
// returns s byte-for-byte — the shell harnesses that grep our tables
// depend on that.
func (w Writer) Paint(s string, class Class) string {
	if !w.enabled || s == "" {
		return s
	}

	var code string

	switch class {
	case Healthy:
		code = ansiGreen
	case Transitional:
		code = ansiYellow
	case Broken:
		code = ansiRed
	case Neutral:
		return s
	}

	return code + s + ansiReset
}

// PaintState is the common case: classify a state string, then paint it.
func (w Writer) PaintState(state string) string {
	return w.Paint(state, ClassifyState(state))
}
