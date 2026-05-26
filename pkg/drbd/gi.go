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
// (current = bitmap = day0 + Consistent + UpToDate, so it is UpToDate
// from metadata alone) and the all-day0 skip-init-sync replica
// (current = bitmap = day0, no flags, reaches UpToDate at the peer
// handshake). Encoding each as a GISeed keeps the field semantics and
// the "uptodate requires a current-UUID" invariant in one place. The
// struct still supports a distinct current vs bitmap-base (e.g. for a
// controller-supplied SeedFromGI) even though the day0 paths set them
// equal.
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
// Examples:
//
//   - all-day0 skip-init-sync slot:  "<day0>:<day0>:0:0"
//   - winner slot (UpToDate):        "<day0>:<day0>:0:0:1:1"
//   - empty-current slot (unused):   "0:<base>:0:0"
//
// The empty-current/empty-base case emits a literal "0" (drbdmeta
// reads it as "no UUID") rather than a bare empty token, which the
// positional parser would mis-align. The day0 base in index 1 is
// what carries the lineage for the winner's peer slots.
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
	// the consistent (index 4) and up-to-date (index 5) fields, never
	// up-to-date alone. Emit nothing when neither is set (the all-day0
	// skip-init-sync shape) so the slot keeps drbdmeta's default-clear
	// flags.
	if s.UpToDate {
		return giStr + ":1:1" // consistent=1, up-to-date=1
	}

	if s.Consistent {
		return giStr + ":1" // consistent=1, up-to-date defaults clear
	}

	return giStr
}

// Day0GIFor derives the deterministic per-RD, per-volume "day 0" DRBD
// generation identifier. Same RD name + volume number always yields
// the same value on every node and across time, so every replica
// converges on identical bitmap-base (and, in the skip-init-sync
// case, current) UUIDs without a shared random seed — DRBD's GI
// handshake then matches and skips the full initial sync.
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
