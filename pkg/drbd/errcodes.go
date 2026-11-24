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

package drbd

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// DrbdsetupErrCode is a numeric DRBD-utils error code surfaced in
// stderr as `(NN)` followed by a human message. Codes are stable
// across drbd-utils versions per the published drbdsetup.xml docs
// and the drbd-utils error-code table — drbdadm/drbdsetup always
// prefix their failure-mode stderr with the parenthesised numeric
// code so external tooling can switch on it without grepping the
// human message (which drifts between locales and versions).
//
// Phase 11.4.b P1 replaces fragile verbatim string regexes
// (`(158) Unknown resource`) with this numeric type so future
// callers can express predicates as `IsErrCode(err, ErrXxx)`
// instead of duplicating ad-hoc patterns.
type DrbdsetupErrCode int

// Known DRBD-utils numeric error codes. Only codes the reconciler
// currently switches on (directly or via substring matching) are
// enumerated here; extend as new code paths need numeric matching.
//
// Values are stable across drbd-utils 9.x per the public
// drbdsetup.xml docs and `drbd_strings.[ch]` in the upstream
// drbd-utils tree.
const (
	// ErrCmdFailed (2) is the generic catch-all "command failed"
	// drbdsetup returns when no more specific code applies.
	ErrCmdFailed DrbdsetupErrCode = 2

	// ErrNoResources (10) — `drbdadm` could not find any resources
	// (no .res files in /etc/drbd.d, or the named resource has no
	// volumes). Distinct from ErrResNotKnown (kernel-side).
	ErrNoResources DrbdsetupErrCode = 10

	// ErrMinorInvalid (11) — the minor number passed to drbdsetup
	// is malformed or outside the allowed range.
	ErrMinorInvalid DrbdsetupErrCode = 11

	// ErrVolumeInvalid (12) — the volume number is malformed.
	ErrVolumeInvalid DrbdsetupErrCode = 12

	// ErrLocalAndPeerAddr (150) — `drbdsetup new-path` rejected
	// the path because its local and peer addresses are equal.
	ErrLocalAndPeerAddr DrbdsetupErrCode = 150

	// ErrResNotKnown (151) — the resource referenced does not
	// exist in the kernel side. Different from the adjust-time
	// `(158) Unknown resource` (which is a per-minor lookup).
	ErrResNotKnown DrbdsetupErrCode = 151

	// ErrMinorNotKnownOnPeer (158) — drbdadm-9's verbatim
	// `Failure: (158) Unknown resource` stderr emitted when the
	// kernel slot vanished between probe and exec. This is the
	// scenario 5.32 probe-vs-adjust race the reconciler's adjust
	// path recovers from via `drbdadm up`.
	ErrMinorNotKnownOnPeer DrbdsetupErrCode = 158

	// ErrInvalidConfigRequest (162) — drbdsetup's generic
	// "kernel rejected the request" envelope, with the specific
	// failure mode carried in the `additional info from kernel:`
	// trailer. The reconciler matches on the (162) prefix when
	// pairing with a known kernel trailer signature; see
	// IsBitmapDropBothDisksErr for the tiebreaker-relocate case.
	ErrInvalidConfigRequest DrbdsetupErrCode = 162

	// ErrNeedApplyAL (167) — drbdsetup needs an activity-log
	// apply pass before the requested verb can proceed.
	ErrNeedApplyAL DrbdsetupErrCode = 167
)

// drbdErrCodeRegex captures the first `(NN)` parenthesised numeric
// token in drbdadm/drbdsetup stderr. The match is intentionally
// permissive on surrounding text so it works through cockroachdb
// errors.Wrap chains, exit-status suffixes, and locale-translated
// kernel info lines. Compiled once at package init via MustCompile
// because a misspelled pattern is a build-time bug.
var drbdErrCodeRegex = regexp.MustCompile(`\((\d+)\)`)

// ExtractDrbdsetupErrCode parses stderr/output for the first
// `(NN)` token and returns the code if present.
//
// Returns (0, false) when no parenthesised numeric token is found
// in the input. The first match wins — drbdadm always emits the
// errno prefix before any wrapped/translated diagnostic, and the
// reconciler's error-wrap chain (cockroachdb errors.Wrap →
// pkg/storage/exec.go) preserves the prefix verbatim.
func ExtractDrbdsetupErrCode(output string) (DrbdsetupErrCode, bool) {
	m := drbdErrCodeRegex.FindStringSubmatch(output)
	if m == nil {
		return 0, false
	}

	n, err := strconv.Atoi(m[1])
	if err != nil {
		// regex captured only \d+, Atoi can only fail on overflow;
		// treat the impossibly-large code as "not a known DRBD
		// errno" rather than panicking.
		return 0, false
	}

	return DrbdsetupErrCode(n), true
}

// IsErrCode reports whether err's output contains the given
// drbdsetup numeric error code. Returns false for nil errors and
// for errors whose stringified form has no `(NN)` token.
//
// Use this in place of ad-hoc substring matches against the
// human-readable portion of drbdadm/drbdsetup stderr — those
// drift across drbd-utils releases and locales, but the numeric
// code is stable.
func IsErrCode(err error, code DrbdsetupErrCode) bool {
	if err == nil {
		return false
	}

	got, ok := ExtractDrbdsetupErrCode(err.Error())

	return ok && got == code
}

// IsUnknownResourceErr is a named predicate for the Bug 287 /
// scenario 5.32 race: drbdadm-9's `(158) Unknown resource` emitted
// when the kernel slot vanished between the satellite's probe and
// adjust's own `drbdsetup new-minor` shell-out. Reconciler call
// sites should prefer this named helper over a raw IsErrCode
// against ErrMinorNotKnownOnPeer so the intent stays self-
// documenting.
func IsUnknownResourceErr(err error) bool {
	return IsErrCode(err, ErrMinorNotKnownOnPeer)
}

// IsBitmapDropBothDisksErr reports whether `err` matches the
// tiebreaker-relocate-StandAlone-wedge signature: the kernel
// refused to drop the per-peer bitmap because its on-disk
// metadata still reads the peer as diskful for that node-id
// slot. The verbatim signature is `(162) Invalid configuration
// request` + kernel info `Can not drop the bitmap when both
// sides have a disk` emitted by `drbdsetup peer-device-options
// <res> <peer-node-id> <vol> --set-defaults --bitmap=no` — the
// declarative form `drbdadm adjust` runs for any `disk none`
// peer in the .res.
//
// Triggered when a diskless replica (a TIE_BREAKER, typically)
// comes up under a node-id whose metadata slot still holds a
// diskful predecessor that was never forgotten: new-peer reads
// the slot's peer-outdated flag and starts the peer device as
// `peer-disk:Outdated`, so the kernel will not drop its bitmap.
// BitmapDropPeerNodeIDs names the node-ids the refusal was for.
//
// The first failed adjust leaves a StandAlone slot with
// peer-device entries registered (kernel allocated them via
// new-peer before peer-device-options fired), at which point
// shouldSkipNetOnAdjust permanently misfires for that slot —
// surface the predicate here so the adjust-error handler can
// take the recovery path before the gate latches.
func IsBitmapDropBothDisksErr(err error) bool {
	if !IsErrCode(err, ErrInvalidConfigRequest) {
		return false
	}

	// Other (162) failure modes exist (e.g. ineligible flag
	// combinations on `new-peer`); the kernel-info trailer is
	// the disambiguator. Match on the stable "Can not drop the
	// bitmap" phrase emitted from drbd-utils `wire.c` so locale
	// translations of surrounding kernel diagnostics don't
	// false-match.
	return strings.Contains(err.Error(), "Can not drop the bitmap")
}

// bitmapDropCmdRegex picks the peer node-id out of the command line
// drbdadm quotes for a failed `drbdsetup peer-device-options <res>
// <peer-node-id> <vol> ... --bitmap=no`.
var bitmapDropCmdRegex = regexp.MustCompile(`peer-device-options \S+ (\d+) \d+ [^'\n]*--bitmap=no`)

// BitmapDropPeerNodeIDs returns the node-ids of the peers whose bitmap
// the kernel refused to drop in err, in the order drbdadm reported
// them and without repeats. Empty when err is not that refusal or does
// not quote the command.
func BitmapDropPeerNodeIDs(err error) []int32 {
	if !IsBitmapDropBothDisksErr(err) {
		return nil
	}

	var ids []int32

	for _, m := range bitmapDropCmdRegex.FindAllStringSubmatch(err.Error(), -1) {
		id, convErr := strconv.ParseInt(m[1], 10, 32)
		if convErr != nil || slices.Contains(ids, int32(id)) {
			continue
		}

		ids = append(ids, int32(id))
	}

	return ids
}
