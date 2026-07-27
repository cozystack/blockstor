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

package color_test

import (
	"os"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/internal/cli/color"
)

// The CLI reproduces the upstream client's semantic colouring — healthy
// green, transitional yellow, broken red — classified from the state
// strings blockstor already reports on CRD Status. The mapping is
// derived from OBSERVED client output, never from its source.
func TestClassifyState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state string
		want  color.Class
	}{
		// Healthy.
		{"UpToDate", color.Healthy},
		{"Online", color.Healthy},
		{"Connected", color.Healthy},
		{"Established", color.Healthy},
		{"Ok", color.Healthy},
		// Transitional — work in flight, not an error.
		{"SyncTarget", color.Transitional},
		{"SyncSource", color.Transitional},
		{"SyncTarget(45%)", color.Transitional},
		{"Outdated", color.Transitional},
		{"Connecting", color.Transitional},
		{"Negotiating", color.Transitional},
		{"Consistent", color.Transitional},
		// Broken.
		{"Inconsistent", color.Broken},
		{"Failed", color.Broken},
		{"DUnknown", color.Broken},
		{"Unknown", color.Broken},
		{"Offline", color.Broken},
		{"StandAlone", color.Broken},
		{"BrokenPipe", color.Broken},
		{"NetworkFailure", color.Broken},
		{"Timeout", color.Broken},
		{"EVICTED", color.Broken},
		{"DELETING", color.Broken},
		// Neutral — a real state that is neither good nor bad.
		{"Diskless", color.Neutral},
		{"TieBreaker", color.Neutral},
		{"Unused", color.Neutral},
		{"", color.Neutral},
	}

	for _, tc := range cases {
		got := color.ClassifyState(tc.state)
		if got != tc.want {
			t.Errorf("ClassifyState(%q) = %v, want %v", tc.state, got, tc.want)
		}
	}
}

// Case-insensitivity matters: the DRBD kernel, the REST view layer and
// the CRD status use different casings for the same state.
func TestClassifyStateIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"uptodate", "UPTODATE", "UpToDate"} {
		if got := color.ClassifyState(s); got != color.Healthy {
			t.Errorf("ClassifyState(%q) = %v, want Healthy", s, got)
		}
	}
}

// A sync percentage must not change the classification, and an unknown
// state must never be silently painted green — unknown reads as neutral
// so a new DRBD state can never masquerade as healthy.
func TestUnknownStateIsNeutralNotHealthy(t *testing.T) {
	t.Parallel()

	if got := color.ClassifyState("SomeFutureDRBDState"); got != color.Neutral {
		t.Errorf("unknown state classified %v, want Neutral", got)
	}
}

// Writer is the colouring gate. Disabled writers must emit the payload
// byte-for-byte unchanged so piped/redirected output stays parseable by
// the shell harnesses that grep our tables.
func TestDisabledWriterEmitsPlainText(t *testing.T) {
	t.Parallel()

	w := color.New(false)

	got := w.Paint("UpToDate", color.Healthy)
	if got != "UpToDate" {
		t.Errorf("disabled Paint = %q, want plain %q", got, "UpToDate")
	}

	if strings.Contains(got, "\x1b[") {
		t.Errorf("disabled Paint leaked an ANSI escape: %q", got)
	}
}

func TestEnabledWriterWrapsInANSI(t *testing.T) {
	t.Parallel()

	w := color.New(true)

	got := w.Paint("UpToDate", color.Healthy)
	if !strings.HasPrefix(got, "\x1b[32m") || !strings.HasSuffix(got, "\x1b[0m") {
		t.Errorf("enabled Paint = %q, want green-wrapped", got)
	}

	// Neutral is not painted at all — no escape noise for ordinary cells.
	if plain := w.Paint("Diskless", color.Neutral); plain != "Diskless" {
		t.Errorf("neutral Paint = %q, want unpainted", plain)
	}
}

// Colour is enabled only for an interactive terminal, and every standard
// opt-out is honoured. Piping into `grep`/`awk` (the shape our e2e
// harnesses use) must never receive escapes.
// Not parallel: the subtests mutate the environment via t.Setenv,
// which the runtime forbids in parallel tests.
func TestEnabledForDecision(t *testing.T) {
	cases := []struct {
		name string
		tty  bool
		flag color.Mode
		env  map[string]string
		want bool
	}{
		{name: "tty, auto", tty: true, flag: color.Auto, want: true},
		{name: "no tty, auto", tty: false, flag: color.Auto, want: false},
		{name: "no tty, always", tty: false, flag: color.Always, want: true},
		{name: "tty, never", tty: true, flag: color.Never, want: false},
		{name: "tty, auto, NO_COLOR set", tty: true, flag: color.Auto, env: map[string]string{"NO_COLOR": "1"}, want: false},
		{name: "tty, auto, TERM=dumb", tty: true, flag: color.Auto, env: map[string]string{"TERM": "dumb"}, want: false},
		{name: "no tty, always, NO_COLOR", tty: false, flag: color.Always, env: map[string]string{"NO_COLOR": "1"}, want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			if _, ok := tc.env["NO_COLOR"]; !ok {
				t.Setenv("NO_COLOR", "")
				_ = os.Unsetenv("NO_COLOR")
			}

			got := color.EnabledFor(tc.flag, tc.tty)
			if got != tc.want {
				t.Errorf("EnabledFor(%v, tty=%v) = %v, want %v", tc.flag, tc.tty, got, tc.want)
			}
		})
	}
}

// ParseMode backs the --color=auto|always|never flag.
func TestParseMode(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]color.Mode{
		"auto":   color.Auto,
		"always": color.Always,
		"never":  color.Never,
		"":       color.Auto,
	} {
		got, err := color.ParseMode(in)
		if err != nil {
			t.Errorf("ParseMode(%q): %v", in, err)

			continue
		}

		if got != want {
			t.Errorf("ParseMode(%q) = %v, want %v", in, got, want)
		}
	}

	if _, err := color.ParseMode("technicolor"); err == nil {
		t.Error("ParseMode(technicolor): want error, got nil")
	}
}
