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

package drbd_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestGISeedStringWinnerSlot pins the elected winner's GI shape as it
// is actually seeded: a current-UUID (day0 lineage anchor), bitmap-base
// EMPTY (→ literal "0" = bitmap-uuid 0x0), and BOTH the consistent +
// up-to-date flags. The empty bitmap-base is the load-bearing field —
// it matches upstream LINSTOR's working-skip metadata (every per-peer
// bitmap-uuid 0x0) so the source reaches UpToDate from metadata alone
// AND the peers skip the resync. A non-zero bitmap-base triggered a
// full SyncTarget (the bug).
func TestGISeedStringWinnerSlot(t *testing.T) {
	seed := drbd.GISeed{
		Current:    "AABBCCDDEEFF0010",
		Consistent: true,
		UpToDate:   true,
	}

	got := seed.String()
	want := "AABBCCDDEEFF0010:0:0:0:1:1"
	if got != want {
		t.Errorf("winner slot GI: got %q want %q", got, want)
	}
}

// TestGISeedStringWinnerPeerSlot pins the winner's peer-slot GI shape:
// day0 bitmap-base only, NO current-UUID (emitted as "0"), NO flags.
// Peers seeded this way recognise the winner (same day0 bitmap-base)
// as their lineage source and skip the initial sync.
func TestGISeedStringWinnerPeerSlot(t *testing.T) {
	seed := drbd.GISeed{BitmapBase: "1122334455667780"}

	got := seed.String()
	want := "0:1122334455667780:0:0"
	if got != want {
		t.Errorf("winner peer slot GI: got %q want %q", got, want)
	}
}

// TestGISeedStringSkipInitSync pins the skip-init-sync shape (case A):
// current = day0, bitmap-base EMPTY (→ "0" = bitmap-uuid 0x0), no
// flags. Both peers present equal current-UUIDs with a clean (zero)
// bitmap → no sync. The empty bitmap-base matches upstream's
// working-skip metadata; a day0 (non-zero) bitmap-base triggered a
// full resync.
func TestGISeedStringSkipInitSync(t *testing.T) {
	day0 := "1122334455667780"
	seed := drbd.GISeed{Current: day0}

	got := seed.String()
	want := "1122334455667780:0:0:0"
	if got != want {
		t.Errorf("skip-init-sync GI: got %q want %q", got, want)
	}
}

// TestGISeedConsistentWithoutUpToDate confirms a Consistent-only seed
// emits the consistent flag but not up-to-date.
func TestGISeedConsistentWithoutUpToDate(t *testing.T) {
	seed := drbd.GISeed{Current: "AABBCCDDEEFF0010", BitmapBase: "1122334455667780", Consistent: true}

	got := seed.String()
	want := "AABBCCDDEEFF0010:1122334455667780:0:0:1"
	if got != want {
		t.Errorf("consistent-only GI: got %q want %q", got, want)
	}
}

// TestGISeedValidateUpToDateRequiresCurrent pins the load-bearing
// invariant: a slot can never be flagged up-to-date with an empty
// current-UUID. DRBD treats an up-to-date slot as the authoritative
// source for the metadata negotiation; a slot with no current-UUID
// has no generation to be authoritative about.
func TestGISeedValidateUpToDateRequiresCurrent(t *testing.T) {
	seed := drbd.GISeed{BitmapBase: "1122334455667780", Consistent: true, UpToDate: true}

	err := seed.Validate()
	if !errors.Is(err, drbd.ErrGIUpToDateNoCurrent) {
		t.Errorf("up-to-date with empty current must be rejected; got %v", err)
	}
}

// TestGISeedValidateUpToDateRequiresConsistent confirms up-to-date
// implies consistent.
func TestGISeedValidateUpToDateRequiresConsistent(t *testing.T) {
	seed := drbd.GISeed{Current: "AABBCCDDEEFF0010", BitmapBase: "1122334455667780", UpToDate: true}

	err := seed.Validate()
	if !errors.Is(err, drbd.ErrGIUpToDateNotConsistent) {
		t.Errorf("up-to-date without consistent must be rejected; got %v", err)
	}
}

// TestGISeedValidateAcceptsWinnerAndPeer confirms the two shapes the
// winner path actually emits pass Validate.
func TestGISeedValidateAcceptsWinnerAndPeer(t *testing.T) {
	local := drbd.GISeed{Current: "AABBCCDDEEFF0010", BitmapBase: "1122334455667780", Consistent: true, UpToDate: true}
	if err := local.Validate(); err != nil {
		t.Errorf("winner local slot must validate; got %v", err)
	}

	peer := drbd.GISeed{BitmapBase: "1122334455667780"}
	if err := peer.Validate(); err != nil {
		t.Errorf("winner peer slot must validate; got %v", err)
	}
}

// TestDay0GIForDeterministicAndEven pins the day0 GID derivation: the
// same RD name + volume number always yields the same value (so every
// node and the seed-safety gates agree), distinct (RD, vol) pairs
// differ, and the value follows DRBD's conventions — 16 upper-case
// hex chars with the low bit clear (even).
func TestDay0GIForDeterministicAndEven(t *testing.T) {
	a := drbd.Day0GIFor("pvc-x", 0)
	b := drbd.Day0GIFor("pvc-x", 0)
	if a != b {
		t.Errorf("Day0GIFor must be deterministic; got %q then %q", a, b)
	}

	if a == drbd.Day0GIFor("pvc-x", 1) {
		t.Errorf("Day0GIFor must differ per volume; vol0==vol1 (%q)", a)
	}

	if a == drbd.Day0GIFor("pvc-y", 0) {
		t.Errorf("Day0GIFor must differ per RD; pvc-x==pvc-y (%q)", a)
	}

	if len(a) != 16 || a != strings.ToUpper(a) {
		t.Errorf("Day0GIFor must be 16 upper-case hex chars; got %q", a)
	}

	v, err := strconv.ParseUint(a, 16, 64)
	if err != nil {
		t.Fatalf("Day0GIFor must be hex; got %q: %v", a, err)
	}

	if v&1 != 0 {
		t.Errorf("Day0GIFor must be even (low bit clear); got %q", a)
	}
}
