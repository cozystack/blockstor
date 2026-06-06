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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
)

// ErrGIUpToDateNoCurrent rejects a GI seed that flags a slot
// up-to-date while leaving the current-UUID empty — an illegal
// combination DRBD's handshake cannot interpret (an up-to-date slot
// must have a generation to be authoritative about).
var ErrGIUpToDateNoCurrent = errors.New("invalid GI seed: up-to-date set with empty current-UUID")

// ErrGIUpToDateNotConsistent rejects a GI seed flagged up-to-date but
// not consistent; up-to-date data is by definition internally
// consistent.
var ErrGIUpToDateNotConsistent = errors.New("invalid GI seed: up-to-date set without consistent")

// GISeed describes the generation-identifier (GI) state to stamp into
// ONE DRBD-9 v09 per-node-id metadata slot via `drbdmeta set-gi`. It
// is the typed, validated counterpart of the positional colon-
// separated GI string drbdmeta consumes.
//
// The fields map onto the v09 GI string positions:
//
//		<current-uuid>:<bitmap-base-uuid>:<history-uuid-0>:<history-uuid-1>:<flags...>
//
//	  - Current is this slot's current data generation (position 1).
//	    Empty means "no current-UUID" (a fresh peer-bitmap slot).
//	  - BitmapBase is the base generation the out-of-sync bitmap is
//	    reckoned against (position 2) — the per-peer "where my data was
//	    last in sync with this node-id" anchor.
//	  - Consistent / UpToDate are the only two boolean flags blockstor
//	    ever seeds. History UUIDs are always left empty.
//
// Why a typed seed (not a bare string): reaching the initial UpToDate
// state by writing GI metadata directly (instead of force-promoting)
// needs two shapes that differ only in the flags — the elected winner
// (current = day0, bitmap-base EMPTY, + Consistent + UpToDate, so it is
// UpToDate from metadata alone) and the skip-init-sync replica
// (current = day0, bitmap-base EMPTY, no flags, reaches UpToDate at the
// peer handshake). Encoding each as a GISeed keeps the field semantics
// and the "uptodate requires a current-UUID" invariant in one place.
//
// The decisive field is BitmapBase: it is left EMPTY (drbdmeta writes
// bitmap-uuid 0x0) in both seed shapes. This was verified by capturing
// a working upstream LINSTOR (piraeus) thin resource's live
// `drbdmeta dump-md`: every per-peer slot carried bitmap-uuid 0x0, and
// the resource reached UpToDate with ZERO resync. A non-zero
// bitmap-base (e.g. day0) is read by DRBD's handshake as a live
// out-of-sync bitmap anchor and triggers a full SyncTarget — the bug a
// previous iteration shipped. The struct still supports a distinct
// non-empty BitmapBase for callers that need it, but the day0 seed
// paths deliberately leave it empty.
type GISeed struct {
	// Current is the current-UUID hex token. Empty = field omitted.
	Current string
	// BitmapBase is the bitmap/data base-UUID hex token. Empty =
	// field omitted.
	BitmapBase string
	// Consistent sets the "data is internally consistent" flag.
	Consistent bool
	// UpToDate sets the "data is currently up to date" flag. Per the
	// DRBD invariant it is illegal to mark a slot up-to-date with an
	// empty current-UUID; String() enforces this by also requiring
	// Consistent and refusing to emit otherwise (see Validate).
	UpToDate bool
	// WasUpToDate stamps MDF_WAS_UP_TO_DATE (GI string index 5) WITHOUT
	// MDF_CONSISTENT — the non-winner day0 skip-init-sync shape.
	//
	// Why this combination exists: at attach, the DRBD kernel decides
	// whether a freshly-grown region (a brand-new replica grows from
	// metadata la_size 0 to the full device size) may be assumed zeroed
	// — and therefore left CLEAN in the bitmap — via two paths
	// (drbd_nl.c attach, "new region assumed zeroed"): MDF_WAS_UP_TO_DATE
	// ("was UpToDate"), or all-zero bitmap/history UUIDs gated on a
	// non-zero rs-discard-granularity ("day0 volume"). A loop-backed
	// FILE_THIN volume deliberately renders NO rs-discard-granularity
	// (the mkfs-discard wedge), so the second path is unavailable and a
	// flagless day0 seed attaches with the FULL device marked
	// out-of-sync — turning the skip into a full byte-copy initial sync.
	// Stamping WasUpToDate (without Consistent) routes the non-winner
	// through the first path.
	//
	// Data-safe: disk_state_from_md checks MDF_CONSISTENT FIRST — a
	// !Consistent device attaches Inconsistent regardless of this flag,
	// so the non-winner still cannot be promoted or treated as a data
	// authority; it only flips UpToDate via the (now 0-bit) handshake
	// with the elected winner. The kernel rewrites the flag from live
	// state at the first real disk-state transition.
	WasUpToDate bool
}

// Validate enforces the DRBD GI invariants blockstor relies on:
//
//   - up-to-date is never set with an empty current-UUID. DRBD treats
//     a slot flagged up-to-date as the authoritative source for the
//     metadata negotiation; a slot with no current-UUID has no
//     generation to be authoritative about, so the combination is
//     nonsensical and `drbdmeta set-gi` would seed a slot the kernel
//     later rejects.
//   - up-to-date implies consistent (up-to-date data is by definition
//     internally consistent).
func (s GISeed) Validate() error {
	if s.UpToDate && s.Current == "" {
		return ErrGIUpToDateNoCurrent
	}

	if s.UpToDate && !s.Consistent {
		return ErrGIUpToDateNotConsistent
	}

	// WasUpToDate anchors the kernel's "new region assumed zeroed"
	// attach decision to a generation; with no current-UUID there is
	// nothing to anchor to (same rationale as the UpToDate invariant).
	if s.WasUpToDate && s.Current == "" {
		return ErrGIUpToDateNoCurrent
	}

	return nil
}

// String renders the GI seed as the positional, colon-separated GI
// string `drbdmeta <minor> v09 <dev> <internal|flex-external> set-gi`
// consumes. The layout is FULLY POSITIONAL (drbd-utils'
// m_set_v9_uuid): every field — including the flags — is a
// colon-separated token at a fixed index, parsed left-to-right:
//
//	index 0: current-UUID            (u64 hex)
//	index 1: bitmap-base/data UUID   (u64 hex)
//	index 2: history-UUID 0          (u64 hex)
//	index 3: history-UUID 1          (u64 hex)
//	index 4: MDF_CONSISTENT          (0 or 1)
//	index 5: MDF_WAS_UP_TO_DATE      (0 or 1)
//	index 6+: primary / crashed-primary / AL-clean / AL-disabled /
//	          lost-quorum / per-peer connected / outdated / fencing /
//	          full-sync / device-seen — all left clear here.
//
// CRUCIAL: the flags are positional 0/1 integers, NOT `name:value`
// pairs. drbdmeta parses each with m_strsep_bit, which exits(10) on
// any value other than 0 or 1 — so a token like "consistent" would
// hard-error. To set up-to-date (index 5) the consistent field
// (index 4) MUST be emitted ahead of it; blockstor never sets
// up-to-date without consistent (Validate enforces this), so the
// pair is always emitted together.
//
// Examples (the day0 seed paths leave bitmap-base EMPTY → index 1 is
// the literal "0" = bitmap-uuid 0x0, matching upstream's working-skip
// metadata):
//
//   - skip-init-sync slot:           "<day0>:0:0:0"
//   - winner slot (UpToDate):        "<day0>:0:0:0:1:1"
//   - empty-current slot (unused):   "0:<base>:0:0"
//
// The empty-current/empty-base case emits a literal "0" (drbdmeta
// reads it as "no UUID") rather than a bare empty token, which the
// positional parser would mis-align. A zero bitmap-base (index 1) is
// what DRBD's handshake reads as "no out-of-sync bits relative to this
// peer", so a fresh thin replica reaches UpToDate with no resync.
//
// String assumes Validate has already passed; callers in the seed
// path Validate first and surface the error.
func (s GISeed) String() string {
	current := s.Current
	if current == "" {
		current = "0"
	}

	base := s.BitmapBase
	if base == "" {
		base = "0"
	}

	giStr := fmt.Sprintf("%s:%s:0:0", current, base)

	// Flags are positional and parsed in order, so we can only append
	// the consistent (index 4) and was-up-to-date (index 5) fields,
	// never index 5 alone — an explicit "0" must hold index 4 open.
	// Emit nothing when no flag is set (the legacy flagless shape) so
	// the slot keeps drbdmeta's default-clear flags.
	if s.UpToDate {
		return giStr + ":1:1" // consistent=1, was-up-to-date=1
	}

	if s.Consistent {
		return giStr + ":1" // consistent=1, was-up-to-date defaults clear
	}

	if s.WasUpToDate {
		// Non-winner day0 skip shape: NOT consistent (attaches
		// Inconsistent, cannot be promoted) but was-up-to-date set so
		// the kernel's attach-time "new region assumed zeroed" path
		// keeps the bitmap clean (see the field doc).
		return giStr + ":0:1" // consistent=0, was-up-to-date=1
	}

	return giStr
}

// Day0GIFor derives the deterministic per-RD, per-volume "day 0" DRBD
// generation identifier. Same RD name + volume number always yields
// the same value on every node and across time, so every replica
// converges on an identical CURRENT-UUID without needing a shared
// random seed — DRBD's GI handshake then sees both replicas at the
// same generation and, with a clean (zero) per-peer bitmap, skips the
// full initial sync. (The per-peer bitmap-base is left EMPTY, not set
// to day0 — see GISeed; upstream LINSTOR's working-skip metadata
// carries a shared current-UUID with bitmap-uuid 0x0 in every slot.)
//
// Single source of truth: the satellite stamps this value into fresh
// metadata, and BOTH the dispatcher and controller seed-safety gates
// compare a peer's observed CurrentGI against it to tell a fresh,
// never-written "day0 sibling" (skip-safe) apart from a real data-
// bearing relocate survivor (must SyncTarget). A real survivor mints
// a runtime current-UUID that cannot equal this deterministic value
// (2^-64 collision), so the discriminator is exact.
//
// Format: 16 upper-case hex chars (the first 8 bytes of
// sha256("blockstor-day0:<rd>/<vol>")), low bit cleared so the GID is
// even — DRBD's low bit is the "primary writes happened" marker, and
// a synthetic day0 represents "consistent / no primary writes yet".
func Day0GIFor(resourceName string, volumeNumber int32) string {
	h := sha256.Sum256(fmt.Appendf(nil, "blockstor-day0:%s/%d", resourceName, volumeNumber))
	h[7] &^= 0x01 // force low bit to 0 (even)

	return strings.ToUpper(hex.EncodeToString(h[:8]))
}
