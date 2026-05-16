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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Bug-167 allow-lists: every wire path that accepts a `flags[]` slice
// must reject strings outside the documented upstream LINSTOR enum.
// Pre-fix, RD-create + RD-modify + Resource-create all accepted
// arbitrary strings (`linstor rd c X --flags YOLOFLAG`) and persisted
// the phantom flag — silently ignored at reconcile time (best case),
// or branched on a typo of a real flag (worst case: a misspelled
// `DISKLESS` could diverge placement semantics across releases).
//
// The allow-lists below mirror upstream LINSTOR's `Flags` enums
// verbatim (server/src/main/java/com/linbit/linstor/core/objects/
// ResourceDefinition.java::Flags + Resource.java::Flags). Deprecated
// entries are kept on the wire so legacy clients that still carry
// them don't break — the satellite reconciler is responsible for
// treating deprecated tokens as no-ops, not the wire layer.

// Resource-definition flag enum literals — see upstream
// `ResourceDefinition.java::Flags`. Hoisted as named constants so
// goconst doesn't flag the repeated string literals and so the
// allow-list, the error sentinel, and the test suite share a single
// source-of-truth spelling.
const (
	rdFlagDelete        = "DELETE"
	rdFlagRestoreTarget = "RESTORE_TARGET"
	rdFlagCloning       = "CLONING"
	rdFlagFailed        = legacySnapshotFlagFailed // "FAILED" — reuse existing const
)

// Resource flag enum literals — see upstream `Resource.java::Flags`.
// Same hoisting rationale as the rd-flag block above. `DISKLESS`
// re-uses the providerKindDiskless constant defined in
// physical_storage.go (same literal, same upstream meaning).
const (
	rscFlagClean                  = "CLEAN"
	rscFlagDelete                 = "DELETE"
	rscFlagDiskless               = providerKindDiskless // "DISKLESS" — reuse
	rscFlagDiskAddRequested       = "DISK_ADD_REQUESTED"
	rscFlagDiskAdding             = "DISK_ADDING"
	rscFlagDiskRemoveRequested    = "DISK_REMOVE_REQUESTED"
	rscFlagDiskRemoving           = "DISK_REMOVING"
	rscFlagDrbdDiskless           = "DRBD_DISKLESS"
	rscFlagTieBreaker             = "TIE_BREAKER"
	rscFlagNVMeInitiator          = "NVME_INITIATOR"
	rscFlagInactive               = "INACTIVE"
	rscFlagReactivate             = "REACTIVATE"
	rscFlagInactivePermanently    = "INACTIVE_PERMANENTLY"
	rscFlagBackupRestore          = "BACKUP_RESTORE"
	rscFlagEvicted                = "EVICTED"
	rscFlagInactiveBeforeEviction = "INACTIVE_BEFORE_EVICTION"
	rscFlagRestoreFromSnapshot    = "RESTORE_FROM_SNAPSHOT"
	rscFlagEvacuate               = "EVACUATE"
	rscFlagDrbdDelete             = "DRBD_DELETE"
	rscFlagInactivating           = "INACTIVATING"
	rscFlagEBSInitiator           = "EBS_INITIATOR"
	rscFlagAutoDiskful            = "AUTO_DISKFUL"
)

// allowedResourceDefinitionFlags is the upstream-canonical RD flag
// enum. Operators set these via golinstor / linstor-csi during
// snapshot restore (RESTORE_TARGET), clone (CLONING), and the
// internal delete-cascade (DELETE / FAILED).
//
//nolint:gochecknoglobals // immutable allow-list — `const` can't hold a map
var allowedResourceDefinitionFlags = map[string]struct{}{
	rdFlagDelete:        {},
	rdFlagRestoreTarget: {},
	rdFlagCloning:       {},
	rdFlagFailed:        {},
}

// allowedResourceFlags is the upstream-canonical Resource flag enum.
// The values the blockstor REST surface actually drives are DISKLESS,
// TIE_BREAKER, EVICTED, INACTIVE, and EVACUATE; the rest are accepted
// so callers (golinstor SDK, snapshot-restore flows, the upstream CLI)
// can post their full wire shape without the gate falsely refusing
// valid requests. Deprecated entries (DISKLESS, BACKUP_RESTORE) are
// kept because legacy clients still send them — the satellite
// reconciler decides what they mean, not the wire layer.
//
// We re-cross-check against apiv1.ResourceFlagDiskless /
// apiv1.ResourceFlagInactive / apiv1.ResourceFlagTieBreaker (the
// already-exported Go constants) by asserting equality in the
// blank-identifier compile-time check below. Drift between this
// allow-list and the public Go API surface is a build failure, not a
// wire-time surprise.
//
//nolint:gochecknoglobals // immutable allow-list — `const` can't hold a map
var allowedResourceFlags = map[string]struct{}{
	rscFlagClean:                  {},
	rscFlagDelete:                 {},
	rscFlagDiskless:               {}, // upstream marks deprecated; kept on-wire
	rscFlagDiskAddRequested:       {},
	rscFlagDiskAdding:             {},
	rscFlagDiskRemoveRequested:    {},
	rscFlagDiskRemoving:           {},
	rscFlagDrbdDiskless:           {},
	rscFlagTieBreaker:             {},
	rscFlagNVMeInitiator:          {},
	rscFlagInactive:               {},
	rscFlagReactivate:             {},
	rscFlagInactivePermanently:    {},
	rscFlagBackupRestore:          {}, // upstream marks deprecated; kept on-wire
	rscFlagEvicted:                {},
	rscFlagInactiveBeforeEviction: {},
	rscFlagRestoreFromSnapshot:    {},
	rscFlagEvacuate:               {},
	rscFlagDrbdDelete:             {},
	rscFlagInactivating:           {},
	rscFlagEBSInitiator:           {},
	rscFlagAutoDiskful:            {},
}

// Compile-time spelling guard: the local Bug-167 constants MUST
// match the already-exported apiv1 wire constants. A mismatch (e.g.
// someone renames apiv1.ResourceFlagDiskless to a different upstream
// spelling) lights up a panic on package init rather than silently
// desyncing the allow-list from the rest of the codebase.
//
//nolint:gochecknoinits // build-time invariant, not runtime config
func init() {
	pairs := []struct {
		local, exported, name string
	}{
		{rscFlagDiskless, apiv1.ResourceFlagDiskless, rscFlagDiskless},
		{rscFlagInactive, apiv1.ResourceFlagInactive, rscFlagInactive},
		{rscFlagTieBreaker, apiv1.ResourceFlagTieBreaker, rscFlagTieBreaker},
	}

	for _, p := range pairs {
		if p.local != p.exported {
			panic("Bug-167 flag-allowlist desync: local " + p.name + " != apiv1 constant")
		}
	}
}

// errUnknownFlag is the static sentinel surfaced as a 400 envelope on
// every wire path that rejects a flag string outside the upstream
// LINSTOR enum. Sentinel-shaped so callers (and the test suite) can
// `errors.Is`-detect the class without scraping the message; the
// wrapper carries the offending flag plus the allow-list.
var errUnknownFlag = errors.New("unknown flag")

// validateResourceDefinitionFlags walks the input slice and refuses
// any entry not present in allowedResourceDefinitionFlags. Empty
// input is allowed (the common case — RD create rarely carries
// flags). Returns nil for the happy path; otherwise an error wrapping
// errUnknownFlag whose message names the offending input AND the
// canonical allow-list so the operator can fix the call.
func validateResourceDefinitionFlags(flags []string) error {
	return validateFlagsAgainst("resource definition", flags, allowedResourceDefinitionFlags)
}

// validateResourceFlags is the Resource-create counterpart of
// validateResourceDefinitionFlags. Same shape and semantics; different
// allow-list.
func validateResourceFlags(flags []string) error {
	return validateFlagsAgainst("resource", flags, allowedResourceFlags)
}

// validateFlagsAgainst is the shared implementation behind
// validateResourceDefinitionFlags / validateResourceFlags. Pulled out
// so the two per-type helpers stay one-liners and the test suite has
// a single canonical envelope shape to assert on.
//
// The error message lists the canonical allow-list in lexical order
// (rather than map-iteration order) so the wire envelope is stable
// across runs — operators script against it and the test suite pins
// the literal "DELETE" / "DISKLESS" entries.
func validateFlagsAgainst(kind string, flags []string, allowed map[string]struct{}) error {
	for _, flag := range flags {
		if _, ok := allowed[flag]; ok {
			continue
		}

		return errors.Wrapf(errUnknownFlag,
			"%s flag %q is not a documented LINSTOR flag; allowed values: %s",
			kind, flag, sortedFlagList(allowed))
	}

	return nil
}

// sortedFlagList renders the allow-list in lexical order for the
// envelope message. Map iteration order is non-deterministic in Go,
// and operator-facing error messages must be stable so scripts +
// snapshot-style tests don't flake.
func sortedFlagList(allowed map[string]struct{}) string {
	out := make([]string, 0, len(allowed))
	for k := range allowed {
		out = append(out, k)
	}

	sort.Strings(out)

	return "[" + strings.Join(out, ", ") + "]"
}
