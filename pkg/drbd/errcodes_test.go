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

package drbd_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// Static error fixtures for the predicate tables. err113 rejects
// dynamic `errors.New` inside subtests, so we pin the canonical
// drbdadm-9 stderr shapes (and a few near-miss variants) at
// package scope and reference them from the table rows. The
// strings here intentionally mirror the verbatim drbdadm-9
// stderr the satellite reconciler observes in production —
// including the `Failure:` capitalisation drbd-utils emits.
var (
	//nolint:staticcheck // verbatim drbdadm-9 stderr fixture
	errIs158Canonical = errors.New("Failure: (158) Unknown resource")
	//nolint:staticcheck // verbatim drbdadm-9 stderr fixture
	errIs10Errno = errors.New("Failure: (10) Unknown resource")

	errIsNotDefined = errors.New("'pvc-x' not defined in your config (for this host)")

	// errSyntheticFailure is the static wrap target used by the
	// round-trip test to satisfy err113 — the `(NN)` numeric
	// prefix is formatted with fmt.Errorf, but the underlying
	// chain wraps this single sentinel.
	errSyntheticFailure = errors.New("synthetic drbdsetup failure")
)

// TestExtractDrbdsetupErrCode pins the numeric-extraction contract:
// the first `(NN)` token in the input wins, and inputs with no
// parenthesised integer return (0, false). This is the building
// block IsErrCode delegates to, so its edge cases must be
// table-driven and exhaustively asserted.
func TestExtractDrbdsetupErrCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantCode  drbd.DrbdsetupErrCode
		wantFound bool
	}{
		{
			name:      "empty string has no errno",
			input:     "",
			wantCode:  0,
			wantFound: false,
		},
		{
			name:      "verbatim drbdadm-9 158 stderr",
			input:     "Failure: (158) Unknown resource",
			wantCode:  drbd.ErrMinorNotKnownOnPeer,
			wantFound: true,
		},
		{
			name:      "wrapped 158 through cockroachdb errors.Wrap",
			input:     "adjust pvc-x: drbdadm: Failure: (158) Unknown resource: exit status 1",
			wantCode:  drbd.ErrMinorNotKnownOnPeer,
			wantFound: true,
		},
		{
			name:      "trailing kernel info noise after canonical line",
			input:     "pvc-x: Failure: (158) Unknown resource\nadditional info from kernel:\nunknown resource\n",
			wantCode:  drbd.ErrMinorNotKnownOnPeer,
			wantFound: true,
		},
		{
			name:      "drbdadm-9 (10) Unknown resource — different failure mode",
			input:     "Failure: (10) Unknown resource: exit status 1",
			wantCode:  drbd.ErrNoResources,
			wantFound: true,
		},
		{
			name:      "drbdsetup 151 res-not-known",
			input:     "drbdsetup down pvc-x: Failure: (151) Resource not known: exit status 1",
			wantCode:  drbd.ErrResNotKnown,
			wantFound: true,
		},
		{
			name:      "drbdsetup 150 local==peer addr",
			input:     "drbdsetup new-path: Failure: (150) local and peer address are equal",
			wantCode:  drbd.ErrLocalAndPeerAddr,
			wantFound: true,
		},
		{
			name:      "drbdsetup 167 need apply-al",
			input:     "drbdsetup: Failure: (167) need apply-al first",
			wantCode:  drbd.ErrNeedApplyAL,
			wantFound: true,
		},
		{
			name:      "generic 'unknown resource' substring without errno — not matched",
			input:     "drbdsetup detach pvc-x: kernel reports: unknown resource",
			wantCode:  0,
			wantFound: false,
		},
		{
			name:      "LINSTOR ApiCallRc without parenthesised errno",
			input:     "ApiCallRc: -4611686018427387904, unknown resource pvc-x",
			wantCode:  0,
			wantFound: false,
		},
		{
			name:      "drbdadm 'not defined in your config' — no errno prefix",
			input:     "'pvc-x' not defined in your config (for this host).",
			wantCode:  0,
			wantFound: false,
		},
		{
			name:      "first parenthesised numeric wins — exit-code suffix ignored",
			input:     "Failure: (158) Unknown resource\nCommand 'drbdsetup new-minor pvc-x 1000 0' terminated with exit code 10",
			wantCode:  drbd.ErrMinorNotKnownOnPeer,
			wantFound: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotCode, gotFound := drbd.ExtractDrbdsetupErrCode(tc.input)
			if gotFound != tc.wantFound {
				t.Errorf("ExtractDrbdsetupErrCode(%q) found = %v, want %v",
					tc.input, gotFound, tc.wantFound)
			}

			if gotCode != tc.wantCode {
				t.Errorf("ExtractDrbdsetupErrCode(%q) code = %d, want %d",
					tc.input, gotCode, tc.wantCode)
			}
		})
	}
}

// TestIsErrCodeRoundTrip asserts that for every code the package
// publicly enumerates, fabricating a synthetic stderr of the shape
// `(NN) some message` produces an IsErrCode positive — and that
// IsErrCode against a different code returns false. This is the
// round-trip contract callers depend on.
func TestIsErrCodeRoundTrip(t *testing.T) {
	t.Parallel()

	codes := []drbd.DrbdsetupErrCode{
		drbd.ErrCmdFailed,
		drbd.ErrNoResources,
		drbd.ErrMinorInvalid,
		drbd.ErrVolumeInvalid,
		drbd.ErrLocalAndPeerAddr,
		drbd.ErrResNotKnown,
		drbd.ErrMinorNotKnownOnPeer,
		drbd.ErrNeedApplyAL,
	}

	for _, code := range codes {
		t.Run(fmt.Sprintf("code=%d", code), func(t *testing.T) {
			t.Parallel()

			// Wrap the static sentinel so the formatted
			// `(NN)` token lands in err.Error() while err113
			// still sees a wrapped static error.
			err := fmt.Errorf("Failure: (%d) some human-readable message: %w",
				int(code), errSyntheticFailure)
			if !drbd.IsErrCode(err, code) {
				t.Errorf("IsErrCode(%q, %d) = false, want true", err.Error(), code)
			}

			// IsErrCode against a different known code must
			// reject the same input — i.e., the predicate
			// discriminates rather than accepting any errno.
			other := drbd.ErrCmdFailed
			if code == other {
				other = drbd.ErrMinorNotKnownOnPeer
			}

			if drbd.IsErrCode(err, other) {
				t.Errorf("IsErrCode(%q, %d) = true, want false (cross-code leak)",
					err.Error(), other)
			}
		})
	}
}

// TestIsErrCodeNil pins the nil-safety contract: IsErrCode must
// never panic and must return false for a nil error.
func TestIsErrCodeNil(t *testing.T) {
	t.Parallel()

	if drbd.IsErrCode(nil, drbd.ErrMinorNotKnownOnPeer) {
		t.Errorf("IsErrCode(nil, 158) = true, want false")
	}
}

// TestExtractIgnoresTrailingNoise verifies the canonical Bug 287 /
// scenario 5.32 race stderr shape parses correctly even when
// drbdadm appends its "additional info from kernel" diagnostic.
// This is the exact stderr the satellite reconciler's adjust path
// sees in production.
func TestExtractIgnoresTrailingNoise(t *testing.T) {
	t.Parallel()

	input := "pvc-x: Failure: (158) Unknown resource\n" +
		"additional info from kernel:\nunknown resource\n" +
		"Command 'drbdsetup new-minor pvc-x 1000 0' terminated with exit code 10"

	code, found := drbd.ExtractDrbdsetupErrCode(input)
	if !found {
		t.Fatalf("ExtractDrbdsetupErrCode(%q) found = false, want true", input)
	}

	if code != drbd.ErrMinorNotKnownOnPeer {
		t.Errorf("ExtractDrbdsetupErrCode(%q) = %d, want %d (ErrMinorNotKnownOnPeer)",
			input, code, drbd.ErrMinorNotKnownOnPeer)
	}
}

// TestIsUnknownResourceErr pins the named-predicate contract: it
// is a thin alias for IsErrCode(err, ErrMinorNotKnownOnPeer), used
// by satellite reconciler call sites for self-documenting intent.
func TestIsUnknownResourceErr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{
			name: "canonical 158 stderr",
			err:  errIs158Canonical,
			want: true,
		},
		{
			name: "errno 10 (not 158) MUST NOT match",
			err:  errIs10Errno,
			want: false,
		},
		{
			name: "no errno prefix MUST NOT match",
			err:  errIsNotDefined,
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := drbd.IsUnknownResourceErr(tc.err)
			if got != tc.want {
				t.Errorf("IsUnknownResourceErr(%v) = %v, want %v",
					tc.err, got, tc.want)
			}
		})
	}
}
