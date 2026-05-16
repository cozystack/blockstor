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
	"net/http"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 145 + Bug 174 — shared close for the
// pre-condition-then-delete TOCTOU window.
//
// Three sibling handlers — `sp d` (Bug 145), `n d` (Bug 174),
// `rg d` (Bug 174) — share the same shape:
//
//  1. Pre-walk the dependents (`refs == 0 ? proceed : 409`).
//  2. Delete the primary object via the store.
//  3. (PRE-FIX) trust the pre-walk and write 200.
//
// Step 1 and step 2 are not atomic. A concurrent
// `r c` / `rd c --resource-group <rg>` can slip between them,
// the pre-walk sees an empty set, and step 2 drops the primary
// object out from under the just-persisted dependent. The
// dependent is then orphaned: its NodeName / ResourceGroupName /
// Props["StorPoolName"] points at a row that no longer exists.
//
// The fix folds a post-Delete re-walk into the same handler:
// capture the pre-Delete object, run Delete, re-walk the
// dependents, and if a reference appeared during the window
// restore the captured object via Create and write the same 409
// envelope the pre-walk would have emitted. Mirrors Bug 145's
// SP-delete close exactly — this helper lifts the pattern out
// so the three handlers converge on one machinery rather than
// drifting copies.

// deleteWithRollback is the wire-side spec for one
// pre-condition-then-delete handler. The handler fills in each
// callback; deleteRunner.run drives the sequence.
//
// The generic parameter `T` is the primary-object type captured
// from the store (Node / ResourceGroup / StoragePool). The
// captured value is round-tripped through the rollback path on
// the race-loser side; on the happy path it's never read.
type deleteWithRollback[T any] struct {
	// capture grabs the pre-Delete snapshot of the primary
	// object. The second return is false when the row is already
	// absent at capture time — the rollback path is skipped in
	// that case (idempotent-delete replay). Any other error from
	// the store is surfaced via writeStoreError and treated as a
	// capture miss; the Delete still runs (matches the
	// pre-existing per-handler behaviour, which never aborted on
	// a Get failure).
	capture func() (T, bool)

	// refuseIfReferenced runs the pre-Delete dependent walk. It
	// returns true when the walk surfaced a refusal envelope
	// (the handler has already written the HTTP error and must
	// return); false when the Delete may proceed. Implementations
	// reuse the Bug 92 / Bug 152 / Bug 11 refusal logic that
	// existed before the rollback close — we don't deduplicate
	// the envelope text since each handler's wording / RetCode
	// pair is canonical for its object type.
	refuseIfReferenced func() bool

	// remove invokes the store-level Delete. The helper folds
	// store.ErrNotFound into the warn-band path (writeWarn) and
	// any other error into writeStoreError. The returned error is
	// the raw store error so the caller's branching on NotFound
	// stays in the helper rather than spreading into every callback.
	remove func() error

	// rolledBackIfRaced runs the post-Delete dependent walk + the
	// captured-object restore. It returns true when the rollback
	// fired (the handler has already written the 409 and must
	// return); false when the Delete is safe to commit. The
	// captured object is passed in via closure on the caller side
	// so the helper stays object-type-agnostic. Implementations
	// MUST best-effort restore via store Create — a Create error
	// here means another goroutine already recreated the primary
	// object, which still satisfies the invariant we care about
	// (the primary object exists, the operator can retry).
	rolledBackIfRaced func(captured T, capturedOK bool) bool

	// writeWarn emits the "already absent" warn-band envelope on
	// the idempotent-delete replay path (Bug 66 alignment). Each
	// handler keeps its own per-object warn code and message —
	// `warnNodeNotFound`, `warnRGNotFound`,
	// `warnStoragePoolNotFound` — so the operator-side
	// `grep WARN` lookup stays object-typed.
	writeWarn func()

	// writeSuccess emits the happy-path 200 + maskInfo envelope.
	// Each handler keeps its own per-object message so audit logs
	// disambiguate "node deleted" from "resource group deleted".
	writeSuccess func()
}

// run drives the full Bug 174 close: pre-walk gate → capture →
// store Delete → post-walk gate → success/warn envelope. Caller
// passes a writer so the helper can route store errors uniformly.
//
// The capture deliberately runs AFTER the pre-walk so the
// happy-path orderings stays identical to Bug 145's:
//
//	refuseSPDeleteIfReferenced → captureStoragePool → Delete →
//	rollbackSPDeleteIfRaced
//
// (Capture-after-refuse keeps the helper from issuing a wasted
// Get on the refused path — the refusal envelope is the only
// observable on that branch, and the captured snapshot would
// have been discarded anyway. The post-walk + restore order
// remains the same: a racing dependent only matters once the
// primary has been removed.)
func (d *deleteWithRollback[T]) run(w http.ResponseWriter) {
	if d.refuseIfReferenced() {
		return
	}

	captured, capturedOK := d.capture()

	err := d.remove()
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	// Bug 174 post-delete re-scan. A concurrent dependent create
	// may have slipped between the pre-walk and the Delete: the
	// pre-walk saw an empty reference set, then the racing create
	// persisted its row, then we just dropped the primary. The
	// post-walk catches that ordering, restores the captured
	// primary, and surfaces the 409 the operator should have seen
	// pre-race. Skipped when capture missed (idempotent-delete
	// replay path — there's nothing to roll back to).
	if capturedOK && d.rolledBackIfRaced(captured, capturedOK) {
		return
	}

	if err != nil {
		d.writeWarn()

		return
	}

	d.writeSuccess()
}
