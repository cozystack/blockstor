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

package satellite

import (
	"errors"
	"testing"
)

// Static error fixtures for the predicate table-test (err113
// rejects errors.New inside a t.Run body). These mirror the
// verbatim stderr drbdadm-9 and unrelated callers emit; the
// predicate's contract is to ONLY accept the canonical
// `(158) Unknown resource` shape and reject every other variant.
var (
	// Canonical drbdadm-9 stderr emitted when adjust's
	// `drbdsetup new-minor` lands on a slot that was torn down
	// between probe and exec (Bug 287 / scenario 5.32 race). The
	// up-fallback MUST fire on this and only this shape.
	errFixt158Canonical = errors.New("pvc-x: Failure: (158) Unknown resource\n" +
		"additional info from kernel:\nunknown resource\n" +
		"Command 'drbdsetup new-minor pvc-x 1000 0' terminated with exit code 10")

	// Same 158 wrapped through cockroachdb errors.Wrap as the
	// runAdjust path would surface it. Predicate must still
	// recognise it through the wrap chain.
	errFixt158Wrapped = errors.New("adjust pvc-x: drbdadm: " +
		"Failure: (158) Unknown resource: exit status 1")

	// drbdsetup (NOT drbdadm) detach emits "unknown resource"
	// lowercase without the (158) errno. Pre-Bug-291 the bare
	// substring match accepted this and fired up-fallback for
	// an unrelated detach failure — which then left state
	// half-up.
	errFixtDetachLower = errors.New("drbdsetup detach pvc-x: kernel reports: unknown resource")

	// LINSTOR's REST adapter surfaces not-found errors through
	// the same wrap chain. The bare substring match also
	// accepted these.
	errFixtLINSTORNotFound = errors.New("ApiCallRc: -4611686018427387904, unknown resource pvc-x")

	// drbdsetup new-path with capitalised "Unknown resource"
	// but no (158) errno — also a false-positive trigger pre-fix.
	errFixtNewPathNoErrno = errors.New("drbdsetup new-path pvc-x: Unknown resource")

	// drbdadm's `'<rsc>' not defined in your config` — the .res-less
	// failure mode; distinct from 158, must NOT fire up.
	errFixtNotDefined = errors.New("'pvc-x' not defined in your config (for this host).")

	// "no resources defined!" from drbdadm when /etc/drbd.d is
	// empty — also NOT 158.
	errFixtNoResources = errors.New("no resources defined!")

	// errno other than 158 paired with "Unknown resource" — e.g.
	// drbdadm-9's (10) Unknown resource for a different failure
	// mode. The anchored regex MUST reject this.
	errFixtErrno10 = errors.New("Failure: (10) Unknown resource: exit status 1")
)

// TestIsUnknownResourceErrMatchesOnly158 pins the tightened
// predicate (Bug 291): only the canonical drbdadm-9
// `(158) Unknown resource` stderr should fire the up-fallback.
// The previous predicate also accepted the bare substring
// "unknown resource" (case-sensitive but unanchored), which
// caused false-positive fallback fires on unrelated drbdsetup
// diagnostics and on LINSTOR's not-found error chain — those
// false positives cascaded into the e2e regressions on
// lifecycle-toggle-migrate / observability-linstor-node-bridge /
// recovery-inconsistent-blocking / rolling-upgrade.
func TestIsUnknownResourceErrMatchesOnly158(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error is never a 158", err: nil, want: false},
		{name: "canonical drbdadm-9 158 stderr matches", err: errFixt158Canonical, want: true},
		{name: "wrapped 158 (cockroachdb errors.Wrap chain) still matches", err: errFixt158Wrapped, want: true},
		{name: "bare 'unknown resource' substring (lowercase) MUST NOT match — issue 291 false positive", err: errFixtDetachLower, want: false},
		{name: "LINSTOR ApiCallRc 'unknown resource' MUST NOT match", err: errFixtLINSTORNotFound, want: false},
		{name: "DRBD new-path 'Unknown resource' (capitalised) without 158 MUST NOT match", err: errFixtNewPathNoErrno, want: false},
		{name: "drbdadm 'no resources defined' MUST NOT match (different failure mode)", err: errFixtNoResources, want: false},
		{name: "drbdadm 'not defined in your config' MUST NOT match", err: errFixtNotDefined, want: false},
		{name: "errno other than 158 with 'Unknown resource' MUST NOT match", err: errFixtErrno10, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := isUnknownResourceErr(tc.err)
			if got != tc.want {
				t.Errorf("isUnknownResourceErr(%q) = %v, want %v",
					errOrNil(tc.err), got, tc.want)
			}
		})
	}
}

// errOrNil renders nil errors as "<nil>" so the table-driven
// test output stays readable without panicking on err.Error().
func errOrNil(err error) string {
	if err == nil {
		return "<nil>"
	}

	return err.Error()
}
