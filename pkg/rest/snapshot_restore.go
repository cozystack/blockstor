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

package rest

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"
)

// storPoolPropKey is the LINSTOR-wire property name pinning a Resource
// to a specific storage pool. Mirrors upstream LINSTOR (`StorPoolName`
// — CamelCase per the REST contract).
const storPoolPropKey = "StorPoolName"

// snapshotRestoreRequest is the JSON body upstream linstor expects on
// the restore endpoint. The snapshot name has two wire dialects:
//
//   - upstream LINSTOR CLI / golinstor: snapshot in URL path
//     (`/snapshot-restore-resource/{snap}`); body carries `nodes`,
//     `stor_pool_rename`, `to_resource` only.
//   - blockstor CSI clone shim + older callers: snapshot in body
//     under `snapshot_name`; URL is the bare path.
//   - legacy in-tree callers: snapshot in body under `from_snapshot`.
//
// Accept all three so the existing tests / linstor-csi / linstor CLI
// can all hit this endpoint without translation glue. The handler
// resolves the snapshot name in that precedence order: path > body
// `from_snapshot` > body `snapshot_name`.
type snapshotRestoreRequest struct {
	ToResource   string   `json:"to_resource"`
	FromSnapshot string   `json:"from_snapshot,omitempty"`
	SnapshotName string   `json:"snapshot_name,omitempty"`
	NodeNames    []string `json:"node_names,omitempty"`
	Nodes        []string `json:"nodes,omitempty"`
}

// registerSnapshotRestore wires the controller-side restore endpoint.
// linstor CLI's `snapshot resource restore` lands here.
//
// Bug 225 (P2): the sibling `snapshot-restore-volume-definition` route
// upstream LINSTOR exposes (Java
// `controller/.../SnapshotRestoreVolumeDefinition.java`) was missing —
// `linstor snapshot volume-definition-restore` returned 404. The
// VD-only variant copies the snapshot's recorded volume layout onto a
// (typically pre-existing) target RD without spawning replicas; the
// operator then drives placement via a separate `rd ap` call. Wire
// shape matches the resource-restore handler — same
// `{to_resource: ...}` body, snapshot name carried in the URL path.
func (s *Server) registerSnapshotRestore(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshot-restore-resource",
		s.requireStore(s.handleSnapshotRestore))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshot-restore-resource/{snap}",
		s.requireStore(s.handleSnapshotRestore))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshot-restore-volume-definition/{snap}",
		s.requireStore(s.handleSnapshotRestoreVolumeDefinition))
}

// handleSnapshotRestoreVolumeDefinition serves the Bug 225 endpoint.
// Resolves the source snapshot, validates the target RD exists, then
// hydrates the snapshot's VolumeDefinition slice onto the target. The
// target RD is NOT created here (resource-restore is the
// new-RD-spawning variant) — if the operator wants a fresh RD they
// run `rd c <new>` first.
func (s *Server) handleSnapshotRestoreVolumeDefinition(w http.ResponseWriter, r *http.Request) {
	srcRD := r.PathValue("rd")
	snapName := r.PathValue("snap")

	var req snapshotRestoreRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	// Bug C.4 (bug-hunt v3): same name-validation gate as the sibling
	// resource-restore handler — both share the ToResource field and
	// both mutate Store state keyed on its raw value.
	if !validateSnapshotRestoreRequest(w, &req) {
		return
	}

	// Cache-retry (Bug 124 class): linstor-csi restores VDs right
	// after the snapshot create POST; absorb informer-cache lag on
	// the source-snapshot read instead of 404-ing the restore.
	snap, err := getSnapshotWithCacheRetry(r.Context(), s.Store, srcRD, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	target, err := s.Store.ResourceDefinitions().Get(r.Context(), req.ToResource)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// A target carrying DELETE is refused, as on the sibling handlers:
	// hydrating volumes into it races the tear-down reaping what it writes.
	if slices.Contains(target.Flags, rdFlagDelete) {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + req.ToResource + "' is being deleted",
			Cause: "the target carries the DELETE flag; the volumes this would hydrate " +
				"are being reaped as it writes them",
			Correc: "wait for the delete to finish, then restore into a fresh definition",
		}})

		return
	}

	if !s.refuseVolumeRestoreIntoMarkedTarget(r.Context(), w, req.ToResource) {
		return
	}

	// G3b (corner-case): the VD-restore variant hydrates the snapshot's
	// recorded volume layout onto a (typically pre-existing, empty)
	// target RD. If the target RD ALREADY carries a volume-definition
	// whose number collides with one of the snapshot's, refuse up front
	// with a typed FAIL_EXISTS_VLM_DFN envelope naming the offending
	// volume number — rather than letting hydrateVolumesFromSnapshot's
	// per-VD Create surface a bare 409 only AFTER it has already
	// partially mutated the target (an earlier non-colliding VD would
	// land before the colliding one errored, leaving a half-restored RD).
	if !s.refuseRestoreOnVolumeConflict(w, r, req.ToResource, &snap) {
		return
	}

	err = hydrateVolumesFromSnapshot(r.Context(), s, req.ToResource, &snap, false)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot volume definitions restored: " +
			snapName + " → " + req.ToResource,
	}})
}

// refuseVolumeRestoreIntoMarkedTarget refuses a volume-definition restore into
// a definition another restore or clone has marked as its own, as the CLI
// does: that operation hydrates the volumes itself, and its rollback, if it
// fails, takes the definition with whatever this answered 200 for. The marker
// goes on with the definition, and is read past the cache. False means a
// refusal has been written.
func (s *Server) refuseVolumeRestoreIntoMarkedTarget(ctx context.Context, w http.ResponseWriter, toResource string) bool {
	live, err := s.Store.ResourceDefinitions().GetUncached(ctx, toResource)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	marker := live.Props[restoreFromSnapshotKey]
	if marker == "" {
		return true
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError,
		Message: "resource definition '" + toResource + "' is the target of a restore or clone",
		Cause:   "it carries the restore marker '" + marker + "', and that operation writes its volumes itself",
		Correc:  "restore it with `linstor snapshot resource restore` instead",
	}})

	return false
}

// refuseRestoreOnVolumeConflict is the G3b pre-mutation guard for the
// VD-restore handler. It compares the snapshot's recorded volume
// numbers against the target RD's existing VolumeDefinitions and
// refuses with a typed FAIL_EXISTS_VLM_DFN (502) envelope when any
// number collides. Returns true (caller may proceed) when the target
// has no conflicting VD or its VD list could not be read (best-effort:
// the downstream hydrate Create still guards). Returns false (and
// writes the 409 envelope) on a collision.
func (s *Server) refuseRestoreOnVolumeConflict(w http.ResponseWriter, r *http.Request, toResource string, snap *apiv1.Snapshot) bool {
	existing, err := s.Store.VolumeDefinitions().List(r.Context(), toResource)
	if err != nil || len(existing) == 0 {
		return true
	}

	have := make(map[int32]struct{}, len(existing))
	for i := range existing {
		have[existing[i].VolumeNumber] = struct{}{}
	}

	var clashes []int32

	for i := range snap.VolumeDefinitions {
		if _, ok := have[snap.VolumeDefinitions[i].VolumeNumber]; ok {
			clashes = append(clashes, snap.VolumeDefinitions[i].VolumeNumber)
		}
	}

	if len(clashes) == 0 {
		return true
	}

	slices.Sort(clashes)

	nums := make([]string, 0, len(clashes))
	for _, n := range clashes {
		nums = append(nums, strconv.Itoa(int(n)))
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailExistsVlmDfn,
		Message: "cannot restore snapshot volume definitions onto '" +
			toResource + "': volume number(s) " + strings.Join(nums, ", ") +
			" already exist on the target",
		Cause: "the target resource definition already carries a volume " +
			"definition with the same number as the snapshot's; restoring " +
			"would collide on the volume-number key",
		Correc: "restore into a resource definition with no conflicting " +
			"volume definitions (e.g. a freshly-created empty RD), or remove " +
			"the clashing volume definition(s) from '" + toResource + "' first",
	}})

	return false
}

// validateSnapshotRestoreRequest runs every pre-store wire-boundary
// gate the new-RD-spawning restore handler needs: ToResource is set
// (required field), and ToResource is a valid LINSTOR identifier
// (Bug C.4 / bug-hunt v3). Returns true when the caller may proceed,
// false when the HTTP error has already been written.
//
// Mirrors validateRDCreateBody's shape so every new pre-Store gate
// lives in one canonical spot rather than as another `if/return`
// inside the parent handler.
//
// Bug C.4: the target RD name flows straight into materializeRestoredRD
// → Store.ResourceDefinitions().Create(), where the k8s CRD store
// slugifies + hash-prefixes the metadata.name and the lowercased result
// no longer matches the spec.resourceDefinitionName CRD admission
// check. The store-side rejection leaves a half-built RD entry in the
// linstor view, and the operator sees a raw "metadata.name must equal …"
// leak — the same Bug-97 class the direct `rd c` path already gates
// against. We mirror that gate here at the wire boundary, BEFORE the
// Store.Create call, so the failure mode is one consistent LINSTOR
// envelope and no orphan state is left behind.
func validateSnapshotRestoreRequest(w http.ResponseWriter, req *snapshotRestoreRequest) bool {
	if req.ToResource == "" {
		writeError(w, http.StatusBadRequest, "to_resource is required")

		return false
	}

	nameErr := validateLinstorName("resource definition", req.ToResource)
	if nameErr != nil {
		writeError(w, http.StatusBadRequest, nameErr.Error())

		return false
	}

	return true
}

// handleSnapshotRestore creates a new ResourceDefinition from a
// snapshot. The data clone (zfs send|recv / lvcreate -s of a snapshot
// LV) is the satellite's job once it picks up the new RD via reconcile;
// the controller's job here is to seed the desired-state objects.
func (s *Server) handleSnapshotRestore(w http.ResponseWriter, r *http.Request) {
	srcRD := r.PathValue("rd")

	var req snapshotRestoreRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	if !validateSnapshotRestoreRequest(w, &req) {
		return
	}

	if !s.rebindRestoreTarget(r.Context(), w, &req) {
		return
	}

	snapName := resolveSnapshotName(r, &req)
	if snapName == "" {
		writeError(w, http.StatusBadRequest, "snapshot name required (URL path, from_snapshot, or snapshot_name)")

		return
	}

	// Cache-retry (Bug 124 class): linstor-csi's CreateVolume-from-
	// snapshot POSTs the restore right after the snapshot create;
	// absorb informer-cache lag on the source-snapshot read instead
	// of 404-ing the restore.
	snap, ok := s.restoreSnapshotOrReplay(r.Context(), w, srcRD, snapName, &req)
	if !ok {
		return
	}

	// Bug 151: a vol-less snapshot taken of a vol-less source RD
	// restores to a structurally meaningless target — no volumes
	// to clone, no resources to place. Operator-poke v4 reproduced
	// this by snapshotting an RD with no VDs and seeing an empty
	// shell on the other side. Refuse with 400 + a LINSTOR-shape
	// envelope explaining the gap; rollback nothing because we
	// haven't touched the Store yet.
	if len(snap.VolumeDefinitions) == 0 {
		writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "snapshot '" + snapName + "' on '" + srcRD +
				"' has no volume definitions; refusing to restore an empty shell",
			Cause: "the snapshot was taken of a resource definition with no " +
				"VolumeDefinitions, so there is nothing to restore from",
			Correc: "create the volume definitions on '" + srcRD +
				"' (`linstor vd c " + srcRD + " <size>`), retake the snapshot, " +
				"then re-run `linstor s resource restore`",
		}})

		return
	}

	// Bug 397 (P0, DATA INTEGRITY): an explicit `--node-name` restore MUST
	// NOT place a diskful replica on a node that does NOT hold the snapshot.
	// Such a replica falls back to a BLANK CreateVolume on the satellite (no
	// local snapshot, cross-node fetch may miss) and — if it then takes the
	// skip-init-sync day0 seed — latches UpToDate while EMPTY: a silent
	// data-integrity loss (an empty replica presenting as a good copy,
	// promotable on failover). The auto-place branch already constrains
	// placement to snap.Nodes via constrainAutoplaceToSnapshotNodes; mirror
	// that contract here at the API edge for the explicit-node path so the
	// bad placement is rejected BEFORE any Store mutation, matching upstream
	// LINSTOR (restoring to a snapshot-less node errors clearly rather than
	// silently placing an empty replica).
	if !validateRestoreNodesHoldSnapshot(w, srcRD, snapName, &req, &snap) {
		return
	}

	// Bare restore: eagerPlace=false. With no explicit --node-name the
	// target is left an empty shell for the operator / linstor-csi to
	// place (restore-then-scale-out); an explicit node list is still
	// stamped verbatim inside materializeRestoredRD.
	// A leftover from an earlier attempt is resumed, not reported done:
	// the marker is stamped with the definition, before its volumes and
	// replicas exist, so on its own it says a restore started, not that it
	// finished.
	prepared, stop := s.adoptPreparedRestoreTarget(r.Context(), w, &snap, req.ToResource)
	if stop {
		return
	}

	resume, stop := s.restoreReplayState(r.Context(), w, &snap, &req, prepared)
	if stop {
		return
	}

	// A target the caller prepared is this restore's first run, not a retry.
	s.materializeRestore(r.Context(), w, srcRD, &req, &snap, resume && !prepared)
}

// rebindRestoreTarget names the restore's target as it is stored; see
// storedDefinitionName. False means a refusal has been written.
func (s *Server) rebindRestoreTarget(ctx context.Context, w http.ResponseWriter, req *snapshotRestoreRequest) bool {
	stored, err := s.storedDefinitionName(ctx, req.ToResource)
	if err != nil {
		writeRestoreTargetUnreadable(w, req.ToResource, err)

		return false
	}

	req.ToResource = stored

	return true
}

// writeRestoreTargetUnreadable refuses a restore whose target could not be
// read. Read as absent, the create that follows collides with what is there
// and is adopted past every judgement of a leftover (finished, torn down,
// parented to a group that is gone), so a replay of a finished restore
// re-placed it on a node the operator had emptied; the retry this answer asks
// for gets the judgement.
func writeRestoreTargetUnreadable(w http.ResponseWriter, toResource string, err error) {
	writeJSON(w, http.StatusInternalServerError, []apiv1.APICallRc{{
		RetCode: apiCallRcError,
		Message: "restore target '" + toResource + "' could not be read: " + scrubImplDetails(err.Error()),
		Correc:  "retry the restore",
	}})
}

// adoptPreparedRestoreTarget takes a definition the caller prepared for this
// restore as its target: created empty, its volume definitions restored from
// this snapshot, no replica yet. That is how LINSTOR's own restore sequence
// runs, and how linstor-csi restores every volume from a snapshot; see
// store.PreparedRestoreTarget. The marker is stamped on it here, so from then
// on the target reads as this restore's own, its first run and every retry
// alike. It returns (adopted, stop).
func (s *Server) adoptPreparedRestoreTarget(
	ctx context.Context, w http.ResponseWriter, snap *apiv1.Snapshot, toResource string,
) (bool, bool) {
	// Past the cache, like every read this path decides on: the caller
	// created this definition one call earlier, possibly through another
	// apiserver replica, and a cache that holds an older copy under the name
	// (deleted and recreated, or patched since) answers the marker wrong.
	existing, err := s.Store.ResourceDefinitions().GetUncached(ctx, toResource)
	if errors.Is(err, store.ErrNotFound) {
		return false, false
	}

	if err != nil {
		writeRestoreTargetUnreadable(w, toResource, err)

		return false, true
	}

	prepared, err := store.PreparedRestoreTarget(ctx, s.Store, &existing, snap)
	if err != nil {
		writePreparedTargetRefusal(w, existing.Name, snap, err)

		return false, true
	}

	if !prepared {
		return false, false
	}

	// Everything that can still refuse this target runs before the marker
	// goes on it: the definition is the caller's, and a refusal after the
	// stamp would leave it looking like a restore that started.
	if !s.restoreTargetGroupSurvived(ctx, w, &existing) {
		return false, true
	}

	adopted, err := store.AdoptPreparedRestoreTarget(ctx, s.Store, existing.Name, snap)
	if errors.Is(err, store.ErrRestoreTargetTaken) {
		// Another restore marked it first; restoreTargetState refuses it
		// as somebody else's, with the wording that already says why.
		return false, false
	}

	if err != nil {
		writePreparedTargetRefusal(w, existing.Name, snap, err)

		return false, true
	}

	// Not adopted with no error: another request for this same restore
	// marked it first, so it is that request's leftover, judged by the
	// replay gate like any other.
	return adopted, false
}

// writePreparedTargetRefusal answers a target that is prepared in every
// respect but one that makes restoring into it wrong; see
// store.PreparedRestoreTarget. Anything else is the store error it is.
func writePreparedTargetRefusal(w http.ResponseWriter, rdName string, snap *apiv1.Snapshot, err error) {
	switch {
	case errors.Is(err, store.ErrRestoreTargetLayers):
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + rdName + "' cannot take the restore of '" +
				snap.Name + "': " + scrubImplDetails(err.Error()),
			Cause: "the restore puts the source's data under the target's layer stack, so a " +
				"layer the source did not have writes its own metadata across it, and one it " +
				"had is missing on read",
			Correc: "create '" + rdName + "' with the layer stack of '" + snap.ResourceName +
				"' (for linstor-csi, a StorageClass with the same layer list), or restore under " +
				"a name that does not exist yet",
		}})
	case errors.Is(err, store.ErrRestoreTargetTearingDown):
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + rdName + "' still has replicas being deleted",
			Cause:   "placing the restore over them would race the tear-down reaping them",
			Correc:  "wait for the replicas of '" + rdName + "' to go, then re-issue the restore",
		}})
	default:
		writeStoreError(w, err)
	}
}

// restoreTargetGroupSurvived refuses an existing target parented to a group
// that is gone, before the restore writes anything into it: a prepared target
// before it is marked, a leftover before it is resumed. The post-write guard
// would otherwise refuse the same target after the write, and it does not
// roll back a definition this request did not create.
func (s *Server) restoreTargetGroupSurvived(
	ctx context.Context, w http.ResponseWriter, existing *apiv1.ResourceDefinition,
) bool {
	survived, err := s.parentRGSurvived(ctx, existing.ResourceGroupName)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, []apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "resource definition '" + existing.Name + "' exists, but its resource group '" +
				existing.ResourceGroupName + "' could not be read: " + scrubImplDetails(err.Error()),
			Correc: "retry the restore",
		}})

		return false
	}

	if survived {
		return true
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{
		*adoptedOverDeletedGroupRefusal("restore", existing.Name, existing.ResourceGroupName,
			correcRecreateGroupThenRestore),
	})

	return false
}

// materializeRestore restores the target, guards the group it was written
// with and answers. resume says whether this request is finishing a leftover,
// which only the wording sees.
func (s *Server) materializeRestore(
	ctx context.Context, w http.ResponseWriter, srcRD string, req *snapshotRestoreRequest,
	snap *apiv1.Snapshot, resume bool,
) {
	made, err := s.materializeRestoredRD(ctx, srcRD, req, snap, false, nil)
	if err != nil {
		s.writeRestoreMaterialiseFailed(ctx, w, snap.Name, req, made, err)

		return
	}

	// The group validated is the one that was WRITTEN, not the source's read
	// back a second time: re-reading answers a different question, and the
	// extra read was itself a way to fail a restore that had already worked.
	uncheckedRG, ok := s.restoreParentRGSurvived(ctx, w, made)
	if !ok {
		return
	}

	writeRestoreDone(w, restoreDoneMessage(resume, snap.Name, made.Name), uncheckedRG)
}

// writeRestoreMaterialiseFailed answers a restore whose materialisation failed.
//
// A failure after this request's own create is its own partial work, and it
// is rolled back before the answer goes out: the retry under the
// deterministic CSI target name then starts clean rather than resuming over
// half of a definition. A leftover this request adopted is never wrapped as
// that, so it stays for the replay gate to judge. Anything else is answered as
// the store error it is.
func (s *Server) writeRestoreMaterialiseFailed(
	ctx context.Context, w http.ResponseWriter, snapName string, req *snapshotRestoreRequest,
	made materialisedRD, err error,
) {
	rdName := req.ToResource

	// Spared on the terms the replay judges a finished restore by: with a
	// replica when this request placed some, by its volumes alone when not.
	wanted := canonicalRestoreNodeList(req)

	scope := rollbackUnlessHydrated
	if len(wanted) > 0 {
		scope = rollbackUnlessPlaced
	}

	if kind, refused := materialiseRefusalKind(err); refused {
		writeStoreErrorTyped(w, err, kind)

		return
	}

	writeJSON(w, http.StatusInternalServerError, []apiv1.APICallRc{*s.failedMaterialiseRefusal(ctx,
		"snapshot restore of '"+snapName+"' into '"+rdName+"' failed: "+err.Error(),
		"restore", rdName, made.Placed, scope, wanted, err)})
}

// materialiseRefusalKind sorts a failed materialisation that is not this
// request's own partial work: an answer about what was already there (a
// replica still being deleted, a creator already rolling back), which both
// doors give in the same typed shape. It reports false for partial work, which
// the caller undoes.
//
// A replica still being deleted, or one holding no data, carries no
// FAIL_EXISTS band: the replica it names is not one that exists for the
// caller to use, and the 409 is the retryable failure it is.
func materialiseRefusalKind(err error) (storeResourceKind, bool) {
	var partial *materialiseAfterCreateError
	if errors.As(err, &partial) {
		return storeKindUnknown, false
	}

	switch {
	case errors.Is(err, errReplicaStillDeleting):
		return storeKindUnknown, true
	case errors.Is(err, errAdoptedLeftoverRollingBack):
		return storeKindResourceDfn, true
	default:
		return storeKindUnknown, true
	}
}

// restoreReplayState is restoreTargetState followed, over a leftover of this
// restore, by the finished question. It returns (resume, stop): stop when an
// answer has been written, the replay of a finished restore included.
//
// prepared says the target is one the caller prepared and this request just
// marked: its first run, not a retry, which only the wording sees.
//
// The gates over an unfinished leftover run before the resume writes into it,
// in the order the clone door runs them: tear-down, then the group, then the
// abandoned-rollback mark. A resume over a definition whose group is gone
// would hydrate and place into it and only then be refused by the group guard
// after the write, which does not roll back what it adopted.
func (s *Server) restoreReplayState(
	ctx context.Context, w http.ResponseWriter, snap *apiv1.Snapshot, req *snapshotRestoreRequest, prepared bool,
) (bool, bool) {
	resume, stop := s.restoreTargetState(ctx, w, snap, req.ToResource)
	if stop || !resume {
		return resume, stop
	}

	finished, halt := s.restoreLeftoverIsFinished(ctx, w, snap, snap.Name, req)
	if halt {
		return false, true
	}

	if finished {
		s.writeFinishedRestoreReplay(ctx, w, snap.Name, req.ToResource, !prepared)

		return false, true
	}

	existing, err := s.Store.ResourceDefinitions().Get(ctx, req.ToResource)
	if err != nil {
		writeStoreError(w, err)

		return false, true
	}

	if !s.restoreTargetGroupSurvived(ctx, w, &existing) {
		return false, true
	}

	if s.restoreRollbackWasAbandoned(ctx, w, req.ToResource) {
		return false, true
	}

	return true, false
}

// writeFinishedRestoreReplay answers the replay of a restore that finished.
// The leftover is adopted, not this request's, so the group guard over it
// refuses rather than rolls back when its group is gone, the same answer the
// resume of an unfinished one gets.
func (s *Server) writeFinishedRestoreReplay(
	ctx context.Context, w http.ResponseWriter, snapName, rdName string, resumed bool,
) {
	existing, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// The pre-write gate, not the post-write one: the replay answers for a
	// definition nobody verified, so a group it cannot read refuses, as the
	// clone's replay and the CLI restore refuse it, rather than answering 201
	// with a warning that binds a PV to a definition parented to nothing.
	if !s.restoreTargetGroupSurvived(ctx, w, &existing) {
		return
	}

	if s.restoreRollbackWasAbandoned(ctx, w, existing.Name) {
		return
	}

	if status, refusal := s.finishedLeftoverRefusal(ctx, "restore", existing.Name); refusal != nil {
		if status == http.StatusConflict {
			refusal.RetCode |= apiCallRcFailExistsRscDfn
		}

		writeJSON(w, status, []apiv1.APICallRc{*refusal})

		return
	}

	writeRestoreDone(w, restoreDoneMessage(resumed, snapName, rdName), nil)
}

// restoreLeftoverIsFinished answers a retry over a leftover of this restore
// before anything is re-run over it. It returns (finished, stop).
//
// A finished restore is not placed again: re-placing it would put a replica on
// every requested node that no longer has one, including a node the operator
// emptied since, restored from the point-in-time beside a replica that has
// moved on with live writes. It is judged the way a finished clone is, by its
// volumes and, when the request placed replicas, by holding at least one;
// where they are is placement's business.
//
// snap is nil once the snapshot is gone; the leftover is then judged against
// the volumes the definition recorded when it was restored, the way a clone
// whose internal snapshot is gone is. A bare restore is finished without a
// replica only while the snapshot its later placement restores from exists:
// once that is gone too, a shell with no replica holds its data nowhere, and
// it is judged as the CLI judges it, needing a replica.
func (s *Server) restoreLeftoverIsFinished(
	ctx context.Context, w http.ResponseWriter, snap *apiv1.Snapshot, snapName string, req *snapshotRestoreRequest,
) (bool, bool) {
	vds, err := store.LiveVolumes(ctx, s.Store, req.ToResource)
	if err != nil {
		writeStoreError(w, err)

		return false, true
	}

	needReplica := len(canonicalRestoreNodeList(req)) > 0 || snap == nil

	progress, err := assessLeftover(ctx, s.Store, req.ToResource, vds, snap, needReplica)
	if err != nil {
		writeStoreError(w, err)

		return false, true
	}

	switch progress {
	case cloneFinished:
		return true, false
	case cloneForeign:
		// Once the snapshot is gone, restoring again after the delete meets
		// nothing to restore from, the tear-down's wording below.
		correc := "delete '" + req.ToResource + "' and restore again, or restore under a different name"
		if snap == nil {
			correc = "delete '" + req.ToResource + "' and restore it from another snapshot, or restore " +
				"another snapshot under a different name"
		}

		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + req.ToResource + "' holds a volume the restore of '" +
				snapName + "' would not have written",
			Cause: "the definition under that name carries this restore's marker, but its volumes " +
				"are not the ones the snapshot recorded, so resuming would report complete a " +
				"layout nothing restored",
			Correc: correc,
		}})

		return false, true
	case cloneTearingDown:
		// Once the snapshot is gone, restoring again after the tear-down
		// meets a target with nothing to restore from.
		correc := "wait until the replicas of '" + req.ToResource + "' are gone, then restore again"
		if snap == nil {
			correc = "wait until the replicas of '" + req.ToResource + "' are gone, then delete '" +
				req.ToResource + "' and restore it from another snapshot"
		}

		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + req.ToResource + "' is still being torn down",
			Cause: "the definition under that name carries this restore's marker, and every " +
				"replica of it is already accepted for deletion; resuming would race the " +
				"tear-down reaping what it writes",
			Correc: correc,
		}})

		return false, true
	case cloneUnfinished:
	}

	return false, false
}

// restoreSnapshotOrReplay reads the snapshot a restore is taken from. One
// that is gone hands the request to replayWithoutSnapshot, which answers it.
func (s *Server) restoreSnapshotOrReplay(
	ctx context.Context, w http.ResponseWriter, srcRD, snapName string, req *snapshotRestoreRequest,
) (apiv1.Snapshot, bool) {
	snap, err := getSnapshotWithCacheRetry(ctx, s.Store, srcRD, snapName)
	if errors.Is(err, store.ErrNotFound) {
		s.replayWithoutSnapshot(ctx, w, srcRD, snapName, req, err)

		return snap, false
	}

	if err != nil {
		writeStoreError(w, err)

		return snap, false
	}

	return snap, true
}

// replayWithoutSnapshot answers a restore whose snapshot is gone. Deleting
// the snapshot once the restore finished is ordinary cleanup, and a retry
// after it is still a replay: an absent snapshot cannot unmake a finished
// target, so a target carrying this restore's marker and the record of the
// volumes the snapshot held is judged against that record, the way a clone
// whose internal snapshot is gone is. A finished one answers as the replay it
// is; an unfinished one cannot be finished from a snapshot that is gone, and
// the refusal says so. Anything else, a legacy target without the record
// included, is the 404 it was.
func (s *Server) replayWithoutSnapshot(
	ctx context.Context, w http.ResponseWriter, srcRD, snapName string, req *snapshotRestoreRequest, notFound error,
) {
	existing, err := s.Store.ResourceDefinitions().GetUncached(ctx, req.ToResource)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		// Whether the target is a finished replay or an unfinished leftover
		// is not known, so the snapshot's 404 would turn a failed read into
		// a refusal that reads as permanent.
		writeJSON(w, http.StatusInternalServerError, []apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "snapshot '" + snapName + "' of '" + srcRD + "' is gone, and '" + req.ToResource +
				"' could not be read to tell whether the restore into it finished: " + scrubImplDetails(err.Error()),
			Correc: "retry the restore",
		}})

		return
	}

	if err != nil || !restoreMarkerMatches(existing.Props, srcRD, snapName) {
		writeStoreError(w, notFound)

		return
	}

	if _, recorded := store.RecordedRestoreVolumes(existing.Props); !recorded {
		writeStoreError(w, notFound)

		return
	}

	if slices.Contains(existing.Flags, rdFlagDelete) {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + req.ToResource + "' is being deleted",
			Cause: "the leftover from an earlier attempt at this restore carries the DELETE " +
				"flag; finishing it would race the tear-down reaping what it writes",
			Correc: "wait for the delete to finish, then re-issue the restore",
		}})

		return
	}

	finished, halt := s.restoreLeftoverIsFinished(ctx, w, nil, snapName, req)
	if halt {
		return
	}

	if finished {
		s.writeFinishedRestoreReplay(ctx, w, snapName, req.ToResource, true)

		return
	}

	writeJSON(w, http.StatusNotFound, []apiv1.APICallRc{{
		RetCode: apiCallRcError,
		Message: "snapshot '" + snapName + "' of '" + srcRD + "' is gone, and the restore into '" +
			req.ToResource + "' never finished",
		Cause: "the definition carries this restore's marker, but not every volume the snapshot " +
			"held, and what is missing can only come from the snapshot",
		Correc: "delete '" + req.ToResource + "' and restore it from another snapshot",
	}})
}

// restoreDoneMessage names whether the restore finished a leftover, so a
// retry is legible in the operator's own output rather than looking like a
// first run.
func restoreDoneMessage(resumed bool, snapName, rdName string) string {
	if resumed {
		return "snapshot restore completed on retry: " + snapName + " → " + rdName
	}

	return "snapshot restored: " + snapName + " → " + rdName
}

// writeRestoreDone emits the restore's success envelope, with any warning the
// post-write checks want to ride back alongside it.
func writeRestoreDone(w http.ResponseWriter, message string, warn *apiv1.APICallRc) {
	rcs := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: message,
	}}

	if warn != nil {
		rcs = append(rcs, *warn)
	}

	writeJSON(w, http.StatusCreated, rcs)
}

// restoreTargetState decides what an existing definition under the target
// name means. It returns (resume, stop): stop when an answer has already been
// written, resume when the caller should re-run the restore over the leftover.
//
// CSI requires CreateVolume to be idempotent: a repeat with the same name and
// the same parameters has to succeed and return the volume that already
// exists. external-provisioner has no other way to make progress after a
// partial failure, so a leftover of this restore is resumed rather than
// refused as already existing.
//
// The restore marker is what makes a leftover recognisable, and it is NOT
// evidence that the restore finished: materializeRestoredRD stamps it with
// the definition and hydrates the volumes and places the replicas afterwards,
// so a failure in either leaves the marker on an empty shell. That is why
// this resumes rather than reporting success — the restore steps tolerate
// objects a previous attempt already created, so re-running them completes
// what is missing and leaves what is there.
//
// A leftover mid-tear-down is refused rather than resumed: the deletion is
// reaping the very objects finishing the restore would be writing.
//
// Anything else under that name is a genuine collision and stays a refusal: a
// name holding somebody else's definition must not come back as a restore
// that never happened.
//
// The snapshot is taken as the stored object, not as the names the request
// spelled, because the marker is written off the same object: LINSTOR folds
// name case, and a retry arriving as `--from-snapshot SNAP` over a marker
// stamped `snap` must still read as this restore's own.
//
// The target is read from the API server, as is the re-read after a create
// meets AlreadyExists: both decide on the marker, which
// adoptPreparedRestoreTarget may have just stamped through the API server, and
// a cache that has not seen that write serves the definition unmarked.
func (s *Server) restoreTargetState(ctx context.Context, w http.ResponseWriter, snap *apiv1.Snapshot, toResource string) (bool, bool) {
	existing, err := s.Store.ResourceDefinitions().GetUncached(ctx, toResource)
	if errors.Is(err, store.ErrNotFound) {
		return false, false
	}

	if err != nil {
		writeRestoreTargetUnreadable(w, toResource, err)

		return false, true
	}

	if !restoreMarkerMatches(existing.Props, snap.ResourceName, snap.Name) {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + toResource + "' already exists and is not a restore of '" +
				snap.Name + "'",
			Correc: "restore under a different name, or delete the existing resource definition first",
		}})

		return false, true
	}

	if slices.Contains(existing.Flags, rdFlagDelete) {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "resource definition '" + toResource + "' is being deleted",
			Cause: "the leftover from an earlier attempt at this restore carries the DELETE " +
				"flag; finishing it would race the tear-down reaping what it writes",
			Correc: "wait for the delete to finish, then re-issue the restore",
		}})

		return false, true
	}

	return true, false
}

// restoreRollbackWasAbandoned is the abandoned-rollback gate on the restore
// door. The restore writes the mark through the same failed materialisation
// the clone does, so it reads it back the same way and in the same place: after
// the tear-down and group refusals, which are more precise about the same
// leftover. See abandonedRollbackRefusal.
func (s *Server) restoreRollbackWasAbandoned(ctx context.Context, w http.ResponseWriter, rdName string) bool {
	status, refusal := s.abandonedRollbackRefusal(ctx, "restore", rdName)
	if refusal == nil {
		return false
	}

	// The exists band says what stands under the name; a read that failed
	// says nothing about it.
	if status == http.StatusConflict {
		refusal.RetCode |= apiCallRcFailExistsRscDfn
	}

	writeJSON(w, status, []apiv1.APICallRc{*refusal})

	return true
}

// restoreFromSnapshotKey marks a definition as produced by a snapshot
// restore, encoded `<source RD>:<snapshot>`. The satellite reads it to route
// the storage provider to RestoreVolumeFromSnapshot, and the retry path above
// reads it to tell its own leftover from somebody else's definition.
const restoreFromSnapshotKey = store.RestoreFromSnapshotProp

// restoreMarker builds that value. Both halves come off the stored Snapshot
// rather than off the request, so the marker a retry compares is the marker
// the first attempt wrote whichever way the caller spelled the names.
func restoreMarker(srcRD, snapName string) string {
	return srcRD + ":" + snapName
}

// restoreMarkerMatches reports whether a definition was produced by this
// restore or clone.
//
// The comparison is case-insensitive because the two sides reach it from
// different places: the marker is written from the stored objects, and a
// caller derives the other side from names it spelled itself. LINSTOR folds
// name case and pkg/store/k8s/crdname.go lowercases every lookup key, so both
// spellings address one object — and a byte comparison here would answer that
// somebody else owns a definition this restore created, which is the
// terminal-on-first-failure behaviour the resume path exists to end.
func restoreMarkerMatches(props map[string]string, srcRD, snapName string) bool {
	return strings.EqualFold(props[restoreFromSnapshotKey], restoreMarker(srcRD, snapName))
}

// validateRestoreNodesHoldSnapshot is the Bug 397 input-validation guard
// for the explicit `--node-name` restore path. It rejects the request when
// any requested node does NOT appear in the snapshot's node set
// (snap.Nodes) — those nodes cannot clone the snapshot locally, so a
// diskful replica stamped there would fall back to a blank volume and risk
// latching UpToDate while empty.
//
// No-ops (returns true) when:
//   - the caller supplied no explicit nodes (auto-place branch handles its
//     own snap.Nodes constraint downstream);
//   - the snapshot records no nodes (snap.Nodes empty) — we cannot prove a
//     violation, so we defer to the satellite-side seed guard (defense in
//     depth) rather than reject a possibly-legitimate request.
//
// Returns false (and writes the typed error envelope) when at least one
// requested node is not in snap.Nodes.
func validateRestoreNodesHoldSnapshot(w http.ResponseWriter, srcRD, snapName string, req *snapshotRestoreRequest, snap *apiv1.Snapshot) bool {
	nodes := canonicalRestoreNodeList(req)
	if len(nodes) == 0 {
		return true
	}

	// The decision is validate.RestoreNodesMissingSnapshot, shared with the
	// CLI, which writes the same objects this handler does. Only the
	// envelope below is this door's own.
	missing := validate.RestoreNodesMissingSnapshot(nodes, snap.Nodes)
	if len(missing) == 0 {
		return true
	}

	writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailNotFoundNode,
		Message: "cannot restore snapshot '" + snapName + "' onto node(s) " +
			strings.Join(missing, ", ") + ": the snapshot does not exist there",
		Cause: "snapshot '" + snapName + "' on '" + srcRD + "' is present only on " +
			strings.Join(snap.Nodes, ", ") + "; a diskful replica on a snapshot-less " +
			"node would be created empty and could silently latch UpToDate without " +
			"the snapshot's data",
		Correc: "restore onto a node that holds the snapshot (" +
			strings.Join(snap.Nodes, ", ") + "), or omit --node-name to auto-place " +
			"onto the snapshot's nodes",
	}})

	return false
}

// resolveSnapshotName picks the snapshot name from the three accepted
// wire dialects (URL path, body from_snapshot, body snapshot_name)
// in precedence order. Empty result = caller should reject with 400.
func resolveSnapshotName(r *http.Request, req *snapshotRestoreRequest) string {
	if v := r.PathValue("snap"); v != "" {
		return v
	}

	if req.FromSnapshot != "" {
		return req.FromSnapshot
	}

	return req.SnapshotName
}

// leftoverIsThisRestore is the AlreadyExists tolerance's whole question: the
// marker says the definition is this operation's own, the DELETE flag says
// whether it is still there to finish, and the fields the caller named say
// whether it is the same operation.
func leftoverIsThisRestore(existing *apiv1.ResourceDefinition, snap *apiv1.Snapshot, overrides *rdShapeOverrides) bool {
	var namedRG string

	var namedLayers []string

	if overrides != nil {
		namedRG, namedLayers = overrides.ResourceGroupName, overrides.LayerStack
	}

	return restoreMarkerMatches(existing.Props, snap.ResourceName, snap.Name) &&
		!slices.Contains(existing.Flags, rdFlagDelete) &&
		requestedShapeDiffers(existing, namedRG, namedLayers) == ""
}

// requestedShapeDiffers names the first way a definition already under the
// target name differs from what THIS request asked for, or "" when it asked for
// nothing the leftover does not already have.
//
// A retry that resumes a leftover keeps the leftover, so a request naming a
// different resource_group or layer stack would get that shape validated and
// then dropped while the answer says the operation completed — the
// accept-and-drop this endpoint refuses external_name and volume_passphrases to
// avoid. The parent group is not cosmetic: it decides replica count and pool
// selection.
//
// Only the fields the caller NAMED are compared, and only against the
// leftover, never against the live source: the source can change between
// attempts (an ordinary `rd modify --resource-group`), and comparing against
// it would turn every later retry into a permanent 409 with a correction
// linstor-csi cannot act on, since it sends the same body every time. A retry
// that names nothing resumes what the first attempt started, whatever the
// source has become, and one that names what the first attempt stamped, which
// is what linstor-csi's does, resumes it too.
//
// The empty-name guard on the group is load-bearing: the leftover carries the
// group materializeRestoredRD stamped, the source's unless the request named
// one, so without it a request that names no group is compared as "" against
// that group and refused.
func requestedShapeDiffers(existing *apiv1.ResourceDefinition, rgName string, layers []string) string {
	if rgName != "" && !strings.EqualFold(existing.ResourceGroupName, rgName) {
		return "resource group '" + existing.ResourceGroupName + "', not '" + rgName + "'"
	}

	if len(layers) == 0 {
		return ""
	}

	// An unset stack is a definition that never said, which is what
	// materializeRestoredRD copies off a source that never said either, and
	// not a definition with no layers. Unresolved, that leftover compared
	// against a retry naming [DRBD, STORAGE], which is every linstor-csi
	// retry, reads as adding both layers and refuses the resume. A retry that
	// names no stack is not compared at all.
	have := store.EffectiveLayerStack(existing.LayerStack)

	added, dropped := layerSetDifference(have, layers)
	if len(added) > 0 || len(dropped) > 0 {
		return "layer stack " + strings.Join(have, ",") + ", not " + strings.Join(layers, ",")
	}

	return ""
}

// uncheckedRestoreGroupWarning tells the caller the restore worked and that
// the group behind it went unverified, which is the one piece of information
// that would make them look.
func uncheckedRestoreGroupWarning(rdName, rgName string, err error) *apiv1.APICallRc {
	return &apiv1.APICallRc{
		RetCode: maskWarn,
		Message: "resource group '" + rgName + "' could not be re-checked after the " +
			"restore: " + scrubImplDetails(err.Error()),
		Cause: "the restore itself succeeded; only the safety net over it could not be " +
			"inspected, so a group deleted during the restore would not have been caught",
		Correc: "confirm resource group '" + rgName + "' still exists",
		ObjRefs: map[string]string{
			objRefRscDfn: rdName,
			objRefRscGrp: rgName,
		},
	}
}

// restoreParentRGSurvived is the post-write half of the Bug 174 guard on the
// restore path. The restored definition inherits the source's resource group,
// so a `rg d` landing while it materialises leaves it parented to a group that
// is gone. False means the restore has been rolled back and a refusal written.
func (s *Server) restoreParentRGSurvived(
	ctx context.Context, w http.ResponseWriter, made materialisedRD,
) (*apiv1.APICallRc, bool) {
	newRDName, stampedRG := made.Name, made.StampedRG

	ctx, cancel := detachedCompensation(ctx)
	defer cancel()

	survived, err := s.parentRGSurvived(ctx, stampedRG)
	if err != nil {
		// The CHECK failed, which says nothing about the restore: that
		// already succeeded, and this is the safety net over it. Undoing a
		// completed restore because the net could not be inspected trades a
		// rare dangling group for a certain lost restore.
		//
		// getRGWithCacheRetry returns immediately on anything that is not
		// NotFound, so this branch is apiserver unavailability, a timeout or
		// a decode failure, none of them a statement about the group. A
		// cancelled request is not among them: the read runs on the detached
		// context above, so a caller that has gone does not get its restore
		// waved through over a group that is gone.
		log.FromContext(ctx).Info("could not re-check the restored definition's parent group",
			"resourceDefinition", newRDName, "resourceGroup", stampedRG, "reason", err.Error())

		// And say so to the caller. Proceeding is right; leaving the only
		// trace in an apiserver log is not.
		return uncheckedRestoreGroupWarning(newRDName, stampedRG, err), true
	}

	if survived {
		return nil, true
	}

	if !made.createdHere() {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{
			*adoptedOverDeletedGroupRefusal("restore", newRDName, stampedRG, correcRecreateGroupThenRestore),
		})

		return nil, false
	}

	rollbackErr := s.rollBackCompensating(ctx, newRDName, made.Placed, rollbackEvenIfFinished)
	if rollbackErr != nil {
		cause, correc := rollbackFailureAdviceOverDeletedGroup(rollbackErr, newRDName, stampedRG)

		writeJSON(w, http.StatusInternalServerError, []apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "snapshot restore: " +
				rollbackFailedMessage(newRDName, stampedRG, rollbackErr),
			Cause:  cause,
			Correc: correc,
		}})

		return nil, false
	}

	writeJSON(w, http.StatusNotFound, []apiv1.APICallRc{{
		RetCode: apiCallRcError,
		Message: "snapshot restore rolled back: " + rgDeletedRaceCorrection(stampedRG),
		Cause: "the restored definition inherits its parent group from the source, and that " +
			"group was deleted while the restore was being materialised; a definition " +
			"pointing at a group that is gone lists fine and places badly",
		Correc: correcRecreateGroupThenRestore,
	}})

	return nil, false
}

// materializeRestoredRD creates the target RD inheriting the source
// RD's LayerStack + Props (snapshot Props win when set) and hydrates
// its VolumeDefinitions from the snapshot's recorded volume layout.
// Returns the new RD's name on success.
//
// eagerPlace selects the placement policy for an EMPTY caller node list:
//
//   - true (the clone path, cloneWithData): stamp one diskful replica on
//     EVERY snapshot-holding node in the source pool. `rd clone` is a
//     one-shot CSI operation with no follow-up autoplace, so the clone
//     replicas must materialise here; keeping them on the snapshot nodes
//     in the SOURCE backend is what closes Bug 038 (no cross-backend
//     stream into `zfs recv`).
//   - false (the bare snapshot-restore handler): leave an EMPTY shell —
//     no Resources stamped — so the operator / linstor-csi drives
//     placement explicitly via `rd ap`. This preserves upstream's
//     restore-then-scale-out workflow and the legitimate STAGED
//     cross-node bring-up the e2e restore lanes rely on (place the
//     data-bearing replica on a snapshot node first, then add a
//     cross-node replica that SyncTargets it). The placer's restore-
//     source backend pin (constrainFilterToRestoreSource) keeps that
//     later autoplace same-backend, so Bug 038 stays fixed without the
//     eager all-nodes stamp.
//
// An explicit caller node list is always stamped verbatim, regardless of
// eagerPlace.
// rdShapeOverrides carries the parts of a definition's shape a caller may
// choose for itself rather than inherit from the source. Nil means "inherit
// everything", which is what a snapshot restore does.
type rdShapeOverrides struct {
	// LayerStack replaces the source's stack when non-empty.
	LayerStack []string
	// ResourceGroupName replaces the source's parent group when non-empty.
	ResourceGroupName string
	// OverrideProps, DeleteProps and DeleteNamespaces are the clone's prop
	// edits, folded in before the definition is created. Applied only once
	// the clone finished, every leftover a failed one kept carried what the
	// caller asked to remove: linstor-csi deletes the source's
	// Aux/csi-provisioning-completed-by on every clone, and its CreateVolume
	// takes a definition that carries it, with volumes of the right size,
	// as one already provisioned, so its retry never reaches the clone that
	// would resume it.
	OverrideProps    map[string]string
	DeleteProps      []string
	DeleteNamespaces []string
}

// applyTo puts a clone's own group, stack and prop edits on the definition it
// is about to create; a restore passes none and inherits the source's.
func (o *rdShapeOverrides) applyTo(rd *apiv1.ResourceDefinition) {
	if o == nil {
		return
	}

	if len(o.LayerStack) > 0 {
		rd.LayerStack = o.LayerStack
	}

	if o.ResourceGroupName != "" {
		rd.ResourceGroupName = o.ResourceGroupName
	}

	if rd.Props == nil {
		rd.Props = make(map[string]string, len(o.OverrideProps))
	}

	maps.Copy(rd.Props, o.OverrideProps)

	for _, k := range o.DeleteProps {
		delete(rd.Props, k)
	}

	deletePropNamespaces(rd.Props, o.DeleteNamespaces)
}

func (s *Server) materializeRestoredRD(ctx context.Context, srcRD string, req *snapshotRestoreRequest, snap *apiv1.Snapshot, eagerPlace bool, overrides *rdShapeOverrides) (materialisedRD, error) {
	srcRDObj, err := s.Store.ResourceDefinitions().Get(ctx, srcRD)
	if err != nil {
		return materialisedRD{}, err //nolint:wrapcheck // surfaced via writeStoreError
	}

	newRD := apiv1.ResourceDefinition{
		Name: req.ToResource,
		// Bug 151: carry over the source's ResourceGroupName so the
		// restored RD inherits the same parent RG. Pre-fix the field
		// was silently dropped — `linstor rd l <restored>` then
		// showed a blank resource_group_name column, breaking the
		// `s resource restore` → `rd l` operator workflow upstream
		// LINSTOR ties together (the parent RG drives subsequent
		// auto-placement and prop inheritance).
		ResourceGroupName: srcRDObj.ResourceGroupName,
		Props:             store.TravellingProps(snap.Props),
		LayerStack:        srcRDObj.LayerStack,
	}

	if newRD.Props == nil {
		newRD.Props = store.TravellingProps(srcRDObj.Props)
	}

	overrides.applyTo(&newRD)

	// The stack the restored bytes are brought up under is the source's as
	// the data plane resolves it. Left empty, a group named for the target
	// whose stack differs would have the control plane judge the definition
	// by that group's stack while the satellite brings it up as the source's.
	newRD.LayerStack = store.EffectiveLayerStack(newRD.LayerStack)

	// Stamp the clone-source so the dispatcher's buildVolumes (called
	// at every satellite-reconcile of placed Resources) emits
	// DesiredVolume.SourceSnapshot, which routes the storage provider
	// to RestoreVolumeFromSnapshot instead of CreateVolume.
	// `<srcRD>:<snapName>` is the agreed encoding — satellite splits
	// on the colon. We persist on the RD (not per-Resource) because
	// every replica of the new RD clones from the same source.
	newRD.Props = store.WithRestoreMarker(newRD.Props, snap)

	// The owner prop is the snapshot's, not the definition's: copied onward,
	// every snapshot later taken of this definition would inherit a claim of
	// ownership it was never given.
	delete(newRD.Props, store.CloneSnapshotOwnerProp)
	delete(newRD.Props, store.CloneSnapshotReapingProp)

	created, adoptedRG, err := s.createOrAdoptRestoredRD(ctx, &newRD, snap, overrides)
	if err != nil {
		return materialisedRD{}, err
	}

	// An adopted leftover is checked against the group it was written with,
	// which is the one it carries, not the one this request would have
	// stamped: the source may have moved since the first attempt.
	made := adoptedRD(newRD.Name, adoptedRG)
	if created {
		made = createdRD(newRD.Name, newRD.ResourceGroupName)
	}

	// The snapshot was read before the definition existed, and a reap that
	// listed dependents in between could not see this one. Read it back now
	// the definition is there; see store.ReapClonedSnapshot. The withdraw has
	// already taken back what this request created, so its error is not one
	// to roll back over.
	err = store.RestoreSourceWithdrawn(ctx, s.Store, snap.ResourceName, snap.Name)
	if err != nil {
		return materialisedRD{}, s.withdrawRestoredRD(ctx, newRD.Name, created, err)
	}

	err = hydrateVolumesFromSnapshot(ctx, s, newRD.Name, snap, true)
	if err != nil {
		return made, made.failedAfterWrite(err)
	}

	// Bug 354: stamp per-node Resource CRDs so satellites have something
	// to reconcile. Pre-fix the target RD + VDs landed in the store but
	// `Store.Resources().Create()` was never called — satellites never
	// observed a Resource for the new RD, so the BlockstorRestoreFromSnapshot
	// prop marker on the RD was dead code and the restored RD stayed an
	// empty shell. Mirrors upstream CtrlSnapshotRestoreApiCallHandler.
	made.Placed, err = s.placeRestoredResources(ctx, srcRD, &newRD, req, snap, eagerPlace)
	if err != nil {
		return made, made.failedAfterWrite(err)
	}

	return made, nil
}

// createOrAdoptRestoredRD creates the restored definition, and reports whether
// this request created it and, when it adopted one instead, the group that
// leftover carries.
//
// AlreadyExists is tolerated when the definition already there is this
// restore's own — the resume path above, or a second restore of the same
// snapshot racing this one between the state check and here. The marker is
// what tells the two apart from somebody else's definition, and re-reading is
// what makes the decision on fresh state rather than on the read that lost the
// race.
func (s *Server) createOrAdoptRestoredRD(ctx context.Context, newRD *apiv1.ResourceDefinition, snap *apiv1.Snapshot, overrides *rdShapeOverrides) (bool, string, error) {
	err := s.Store.ResourceDefinitions().Create(ctx, newRD)
	if err == nil {
		return true, "", nil
	}

	if !errors.Is(err, store.ErrAlreadyExists) {
		return false, "", err //nolint:wrapcheck // surfaced via writeStoreError
	}

	// The definition that answered AlreadyExists is read from the API server:
	// the decision below is whether it is this restore's, and a cache that
	// has not seen the marker answers that wrong.
	existing, getErr := s.Store.ResourceDefinitions().GetUncached(ctx, newRD.Name)
	if getErr != nil {
		return false, "", getErr //nolint:wrapcheck // surfaced via writeStoreError
	}

	// Re-made on fresh state, and all of it: the marker says the definition
	// is this operation's own, the DELETE flag says whether it is still there
	// to finish, and the shape says whether it is the same operation. The
	// window is narrow — another request completed and the target was deleted
	// between the state check above and this Create — but it is the exact
	// state the 409 in restoreTargetState exists to prevent, and hydrating
	// volumes into a dying definition races the tear-down reaping them.
	if !leftoverIsThisRestore(&existing, snap, overrides) {
		return false, "", err //nolint:wrapcheck // surfaced via writeStoreError
	}

	err = s.claimAdoptedLeftover(ctx, existing.Name, snap)
	if err != nil {
		return false, "", err
	}

	// Hydrated and placed under the name it is stored with: replicas are
	// selected by that name exactly, and one stamped under another spelling
	// is invisible to the definition's own cascade.
	newRD.Name = existing.Name

	return false, existing.ResourceGroupName, nil
}

// errAdoptedLeftoverRollingBack refuses to adopt a leftover whose creator has
// started rolling it back.
var errAdoptedLeftoverRollingBack = store.ErrAdoptedLeftoverRollingBack

// claimAdoptedLeftover is the adopting half of a handshake with the attempt
// that created the definition, which "may still be running".
//
// That attempt can fail after this one answered for the definition, and its
// rollback cascades a delete over everything under the name: the caller would
// hold a 201 for a volume that no longer exists. So before writing anything
// into the definition this one marks it as adopted, through the API server,
// and then reads back, past the cache, whether a rollback has already started
// on it. The creator does the mirror image: it marks its rollback in progress
// and then reads back the adoption (see rollBackCompensating). Whichever writes
// second sees the other's mark. A rollback that sees the adoption yields and
// leaves the definition; an adoption that sees the rollback refuses. When both
// marks land before either read, both sides stand down: nothing is deleted,
// and the next retry resumes the leftover.
func (s *Server) claimAdoptedLeftover(ctx context.Context, rdName string, snap *apiv1.Snapshot) error {
	err := store.ClaimAdoptedLeftover(ctx, s.Store, rdName, snap)
	if errors.Is(err, store.ErrAdoptedLeftoverRollingBack) {
		return errors.Mark(errors.Wrapf(store.ErrAlreadyExists, //nolint:wrapcheck // the mark is the wrap
			"'%s' cannot be adopted: %v", rdName, errAdoptedLeftoverRollingBack), errAdoptedLeftoverRollingBack)
	}

	return err //nolint:wrapcheck // store names the definition
}

// withdrawRestoredRD takes back a definition this request created from a
// snapshot that went away under it, before anything was hydrated into it, and
// answers as for a snapshot that does not exist.
//
// A definition an earlier attempt left is not this request's to delete: it
// may hold what that attempt hydrated. It cannot be finished without the
// snapshot either, so the answer names it and the way out, rather than a 404
// every retry repeats with nothing pointing at the leftover.
func (s *Server) withdrawRestoredRD(ctx context.Context, rdName string, created bool, cause error) error {
	if created {
		err := s.Store.ResourceDefinitions().Delete(ctx, rdName)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			log.FromContext(ctx).Info("could not withdraw a definition whose snapshot went away",
				"resourceDefinition", rdName, "error", err.Error())
		}
	}

	if !errors.Is(cause, store.ErrRestoreSourceWithdrawn) {
		return cause
	}

	if created {
		return errors.Wrap(store.ErrNotFound, cause.Error())
	}

	return errors.Wrapf(store.ErrNotFound, "%s; '%s', left by an earlier attempt at this restore, "+
		"cannot be finished without it: delete it with `linstor rd d %s`", cause.Error(), rdName, rdName)
}

// materialisedRD is what a materialisation wrote, and whether the definition
// under that name is this call's to undo.
//
// Created-or-adopted is the line every compensation on these paths draws. A
// definition this call created is its own work, and a rollback may take it with
// everything under it. A definition that was already there, adopted as the
// leftover of an earlier attempt at the same operation, is not: that attempt
// may still be running, and reaping it deletes someone else's clone.
//
// So the answer is not a field a producer can forget. It is carried in an
// unexported origin whose zero value says nothing, and the two constructors are
// the only way to state it; a literal that skips them leaves the origin
// unstated, and createdHere answers false for it, which is the safe side of the
// line. materializeRestoredRD tolerates this restore's own leftover, so both
// constructors are called there, off what createOrAdoptRestoredRD reports.
type materialisedRD struct {
	// Name is the target definition.
	Name string
	// StampedRG is the resource group the definition was written with.
	StampedRG string
	// Placed are the nodes this call stamped a replica on.
	Placed []string

	origin rdOrigin
}

// rdOrigin says who the definition under the target name belongs to.
type rdOrigin uint8

const (
	// rdOriginUnstated is the zero value, and never a claim of ownership.
	rdOriginUnstated rdOrigin = iota
	// rdOriginCreated is this call's own create of the definition.
	rdOriginCreated
	// rdOriginAdopted is a definition that was already there.
	rdOriginAdopted
)

// createdRD records a definition this call created.
func createdRD(name, stampedRG string) materialisedRD {
	return materialisedRD{Name: name, StampedRG: stampedRG, origin: rdOriginCreated}
}

// adoptedRD records a definition that was already there when this call ran.
func adoptedRD(name, stampedRG string) materialisedRD {
	return materialisedRD{Name: name, StampedRG: stampedRG, origin: rdOriginAdopted}
}

// createdHere reports whether a compensation may reap this definition.
func (m materialisedRD) createdHere() bool {
	return m.origin == rdOriginCreated
}

// materialiseAfterCreateError is a materialisation that failed after this call
// created the target definition. What stands under the name is then this
// call's own partial work, and a caller may undo it; any other failure leaves
// whatever was there before the call, which may be another attempt's.
type materialiseAfterCreateError struct {
	err error
}

// failedAfterWrite is how a materialisation reports a failure after the
// definition stood: as this call's own partial work, which a caller may undo,
// only when this call created it. A leftover it adopted belongs to the attempt
// that left it, and the failure is answered as the store error it is.
func (m materialisedRD) failedAfterWrite(err error) error {
	if m.createdHere() {
		return &materialiseAfterCreateError{err: err}
	}

	return err
}

func (e *materialiseAfterCreateError) Error() string { return e.err.Error() }

func (e *materialiseAfterCreateError) Unwrap() error { return e.err }

// placeRestoredResources stamps the Resource CRDs that materialise the
// restored RD on the cluster. Two branches mirror upstream LINSTOR's
// snapshot-restore handler:
//
//   - Explicit `--node-name` list (req.Nodes / req.NodeNames): stamp
//     one Resource per requested node.
//   - Empty node list: stamp one Resource on EVERY node that holds the
//     snapshot. Bug 038: this branch used to auto-place against the
//     parent ResourceGroup's SelectFilter, which (a) silently placed
//     ZERO replicas when the RG had no place_count (the empty-spec
//     DfltRscGrp default), and (b) left the real placement to the
//     controller-side RG reconcilers, whose unconstrained placer pass
//     could land a replica on a pool of a DIFFERENT backend — the
//     satellite then piped the source's snapshot stream into the wrong
//     receiver (`zfs recv … bad magic number` looping forever on a
//     FILE_THIN→ZFS clone). Upstream LINSTOR restores onto all
//     snapshot-holding nodes in the snapshot's own storage pool and
//     never consults the autoplacer (verified against the live
//     linstor-oracle, LINSTOR 1.33.2: restore of a 2-node FILE_THIN
//     snapshot landed on exactly those 2 nodes, same pool, despite
//     DfltRscGrp PlaceCount=2 and a third candidate node).
//
// Each stamped Resource pins Spec.Props["StorPoolName"] to the pool the
// SOURCE replica uses on that node (fallback: the source's first
// diskful pool), so the satellite materialises the clone in the same
// pool — node-local RestoreVolumeFromSnapshot, same backend by
// construction.
//
// The Nodes / NodeNames request fields are aliased — callers may use
// either; we normalise to one canonical list before iterating.
func (s *Server) placeRestoredResources(ctx context.Context, srcRDName string, newRD *apiv1.ResourceDefinition, req *snapshotRestoreRequest, snap *apiv1.Snapshot, eagerPlace bool) ([]string, error) {
	nodes := canonicalRestoreNodeList(req)

	if len(nodes) == 0 {
		if !eagerPlace {
			// Bare restore with no explicit nodes → EMPTY shell. The
			// operator / linstor-csi drives placement via `rd ap`,
			// which the placer keeps on the source backend
			// (constrainFilterToRestoreSource). This preserves the
			// upstream restore-then-scale-out workflow and the staged
			// cross-node bring-up the e2e restore lanes rely on. _ =
			// snap keeps the signature uniform with the eager branch.
			_ = snap

			return nil, nil
		}

		// Clone path (eager): stamp one replica on every snapshot node
		// in the source pool. `rd clone` is a one-shot CSI op with no
		// follow-up autoplace, so the clone replicas must materialise
		// here. An empty snap.Nodes (legacy snapshot CRD without the
		// node list) stamps nothing — the operator can still drive
		// placement explicitly via `linstor rd ap <new>`.
		nodes = snap.Nodes
	}

	// Bug 038: stamp the restore-marked replicas SYNCHRONOUSLY and
	// return promptly — the HTTP handler must never block on async
	// reconciler state. The restore data plane is a node-local
	// RestoreVolumeFromSnapshot, but the per-node `@snap` is created
	// ASYNCHRONOUSLY by the satellite SnapshotReconciler, so a replica
	// can reconcile before its co-located `@snap` exists. That race is
	// absorbed entirely on the SATELLITE side: materializeVolume REQUEUES
	// the reconcile (restoreSnapshotMissingBudget passes, ~5s backoff
	// each) on RestoreVolumeFromSnapshot ErrNotFound BEFORE conceding to
	// the terminal blank CreateVolume, giving the local snapshot time to
	// land. Blocking the POST here on a satellite-set CreateTimestamp
	// would hang the restore until the client deadline whenever the
	// snapshot is slow / never Ready (and deadlocks envtest, which has no
	// satellite to stamp the timestamp at all) — so the handler does NOT
	// wait.
	return s.stampRestoredResourcesOnNodes(ctx, srcRDName, newRD.Name, nodes)
}

// stampRestoredResourcesOnNodes iterates the node list and stamps one
// Resource CRD per node, resolving the storage pool from the source
// RD's replica on that same node (fallback: the source's first diskful
// pool — pool names are cluster-wide in LINSTOR, so the fallback only
// matters when the source replica on that node is already gone).
// Idempotent on duplicates in the list (one Create per unique node).
func (s *Server) stampRestoredResourcesOnNodes(ctx context.Context, srcRDName, newRDName string, nodes []string) ([]string, error) {
	poolByNode, fallbackPool := storPoolsByNodeFromSourceRD(ctx, s.Store, srcRDName)

	seen := make(map[string]struct{}, len(nodes))
	placed := make([]string, 0, len(nodes))

	for _, node := range nodes {
		if node == "" {
			continue
		}

		if _, dup := seen[node]; dup {
			continue
		}

		seen[node] = struct{}{}

		res := apiv1.Resource{
			Name:     newRDName,
			NodeName: node,
		}

		pool := poolByNode[node]
		if pool == "" {
			pool = fallbackPool
		}

		if pool != "" {
			res.Props = map[string]string{storPoolPropKey: pool}
		}

		created, err := s.stampRestoredReplica(ctx, &res)
		if err != nil {
			return placed, err
		}

		if !created {
			// Neither created nor promoted by this call, so not in what a
			// rollback may take.
			continue
		}

		placed = append(placed, node)
	}

	return placed, nil
}

// errReplicaStillDeleting marks a stamp refused because the replica already
// under that (definition, node) is going away. It wraps ErrAlreadyExists, but
// is answered with no FAIL_EXISTS band; see materialiseRefusalKind.
var errReplicaStillDeleting = errors.New("the replica under that name is still being deleted")

// stampRestoredReplica creates one replica of a restore, and reports whether
// this call created it, or promoted the controller's witness into it.
//
// Same reasoning as hydrateVolumesFromSnapshot: the replica is keyed
// (definition, node) and the caller stamps exactly the nodes the restore
// resolved, so an existing one is the replica a previous attempt already
// placed. Unless it is going away: a Create over a replica still held by its
// satellite finalizer answers AlreadyExists too, and counting that one as
// placed reports a definition with no live replica as done. Nor when it is
// diskless: it holds no copy of the data. A tie-breaker is the witness the
// controller places once two replicas with a disk exist, so on a node the
// restore also places onto it is promoted, the way autoplace promotes one,
// and its volumes are restored from the snapshot like any other replica of a
// marked target; an operator's diskless replica is refused.
//
// The existing replica is read from the API server, not the cache: right after
// a delete began a cache still serves the replica without its DELETE flag, and
// would have it counted as placed. A NotFound on that read is the replica
// having gone between the Create and the read, so the Create is tried again;
// one that keeps colliding with a replica the API server says is not there is
// reported, not taken as placed.
//
// That report cannot come from a store whose read trails its own create.
// Every store this runs against answers both from the same source: the servers
// build theirs with the manager's direct reader (storek8s.NewManager), the CLI
// with an uncached client, and the in-memory store has no cache. A store built
// with a cached client and no direct reader would read past nothing here, and
// is not a configuration any binary ships.
func (s *Server) stampRestoredReplica(ctx context.Context, res *apiv1.Resource) (bool, error) {
	const attempts = 3

	for attempt := 1; ; attempt++ {
		err := s.Store.Resources().Create(ctx, res)
		if err == nil {
			return true, nil
		}

		if !errors.Is(err, store.ErrAlreadyExists) {
			return false, err //nolint:wrapcheck // wrapped as materialiseAfterCreateError by the caller
		}

		existing, getErr := getResourceUncached(ctx, s.Store, res.Name, res.NodeName)
		if errors.Is(getErr, store.ErrNotFound) {
			if attempt < attempts {
				continue
			}

			return false, errors.Wrapf(store.ErrAlreadyExists,
				"the replica of %q on node %q keeps colliding with one the API server does not have",
				res.Name, res.NodeName)
		}

		if getErr != nil {
			return false, getErr
		}

		if replicaAcceptedForDeletion(&existing) {
			return false, errors.Mark(errors.Wrapf(store.ErrAlreadyExists, //nolint:wrapcheck // the mark is the wrap
				"the replica of %q on node %q is still being deleted", res.Name, res.NodeName),
				errReplicaStillDeleting)
		}

		// A promoted witness is this request's replica now, and counts as
		// placed: a rollback deletes it by name like any replica it made.
		promotedHere := false

		if slices.Contains(existing.Flags, apiv1.ResourceFlagTieBreaker) {
			promoted, promoteErr := s.promoteRestoreWitness(ctx, res)
			if promoteErr != nil {
				return false, promoteErr
			}

			existing, promotedHere = *promoted, true
		}

		if !store.HoldsData(&existing) {
			return false, errors.Wrapf(store.ErrAlreadyExists,
				"the replica of %q on node %q is diskless and holds no copy of the data; "+
					"delete it, then retry", res.Name, res.NodeName)
		}

		return promotedHere, nil
	}
}

// promoteRestoreWitness promotes the controller's witness on a node the restore
// places onto. The pool is settled before anything is written: a promotion
// that found none would strip the witness's TIE_BREAKER flag and leave it
// looking like an operator's diskless replica, refused on every retry. With no
// pool the witness is left untouched and the stamp is refused.
func (s *Server) promoteRestoreWitness(ctx context.Context, res *apiv1.Resource) (*apiv1.Resource, error) {
	target := *res
	if target.Props["StorPoolName"] == "" {
		pool, err := s.resolveTakeoverStorPool(ctx, res.Name, res.NodeName)
		if err != nil {
			return nil, err
		}

		if pool == "" {
			return nil, errors.Wrapf(store.ErrAlreadyExists,
				"the witness of %q on node %q has no storage pool to take a disk in; give it one with "+
					"`linstor resource create --storage-pool <pool> %s %s`, then retry", res.Name, res.NodeName,
				res.NodeName, res.Name)
		}

		target.Props = maps.Clone(res.Props)
		if target.Props == nil {
			target.Props = map[string]string{}
		}

		target.Props["StorPoolName"] = pool
	}

	return s.promoteDisklessReplica(ctx, &target)
}

// canonicalRestoreNodeList collapses the request's two node-list
// aliases (`nodes` and `node_names`) into a single ordered list.
// Both wire shapes appear in the wild: upstream LINSTOR CLI/golinstor
// emit `nodes`; older blockstor callers and the linstor-csi clone
// shim emit `node_names`. The handler accepts either.
func canonicalRestoreNodeList(req *snapshotRestoreRequest) []string {
	if len(req.Nodes) > 0 {
		return req.Nodes
	}

	return req.NodeNames
}

// storPoolsByNodeFromSourceRD maps each of the source RD's diskful
// replica nodes to the StorPoolName backing the replica there, plus
// the first diskful pool as a fallback for nodes without a live
// source replica (pool names are cluster-wide in LINSTOR, so the
// fallback names the same pool on the other node in the common case).
// Used to seed the restored Resources' Spec.Props["StorPoolName"] so
// satellites stage the clone in the SAME pool as the source — the
// restore data plane is a node-local RestoreVolumeFromSnapshot, never
// a cross-backend stream (Bug 038). Best-effort: an empty result
// falls through to the satellite-side pool resolution.
func storPoolsByNodeFromSourceRD(ctx context.Context, st store.Store, srcRDName string) (map[string]string, string) {
	resList, err := st.Resources().ListByDefinition(ctx, srcRDName)
	if err != nil {
		return nil, ""
	}

	byNode := make(map[string]string, len(resList))
	fallback := ""

	for i := range resList {
		if slices.Contains(resList[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		pool := resList[i].Props["StorPoolName"]
		if pool == "" {
			continue
		}

		byNode[resList[i].NodeName] = pool

		if fallback == "" {
			fallback = pool
		}
	}

	return byNode, fallback
}

// hydrateVolumesFromSnapshot copies the snapshot's recorded
// VolumeDefinitions onto the freshly-created restore-target RD.
// Without this, the new RD has zero volumes and any subsequent
// autoplace creates empty Resources that never reach UpToDate.
// linstor-csi's CreateVolume-from-source path relies on this
// hydration to surface the cloned PVC's block device.
//
// ownTarget says the definition carries this restore's or clone's own marker,
// which materializeRestoredRD has established before it calls this. Only then
// is a volume LARGER than the snapshot recorded this operation's own: it is
// the one an earlier attempt hydrated, expanded since like any volume.
// leftoverAgainstSnapshot classifies that shape as the clone's own, and the
// resume it admits ends here, so refusing it answered a bare 500 on every
// retry. The volume-definition restore hydrates into a definition that is
// not its own and accepts no existing volume at all.
func hydrateVolumesFromSnapshot(ctx context.Context, s *Server, rdName string, snap *apiv1.Snapshot, ownTarget bool) error {
	for i := range snap.VolumeDefinitions {
		svd := &snap.VolumeDefinitions[i]
		vd := apiv1.VolumeDefinition{
			VolumeNumber: svd.VolumeNumber,
			SizeKib:      svd.SizeKib,
		}

		err := s.Store.VolumeDefinitions().Create(ctx, rdName, &vd)
		if err == nil {
			continue
		}

		// The volume-definition restore writes into a definition that is not
		// its own, and every collision there is a refusal, as on main: it is
		// the second half of that restore's collision guard, and a volume
		// that appeared between the guard's list and this create was written
		// by somebody else. A concurrent CLI restore of the same snapshot can
		// fail on a later volume and unwind this one, so a 200 over it would
		// report a layout that is about to go.
		if !errors.Is(err, store.ErrAlreadyExists) || !ownTarget {
			return err //nolint:wrapcheck // wrapped as materialiseAfterCreateError by the caller
		}

		// On this operation's own target, under its marker and the adoption
		// handshake, AlreadyExists is the volume an earlier attempt hydrated,
		// and tolerating it is what lets a retry finish an incomplete restore.
		// The size still tells it apart: one at the snapshot's size, or grown
		// since, is the restore's own; a smaller one is somebody else's.
		existing, getErr := s.Store.VolumeDefinitions().Get(ctx, rdName, svd.VolumeNumber)
		if getErr != nil {
			return err //nolint:wrapcheck // the collision is the answer, not the read
		}

		if existing.SizeKib < svd.SizeKib {
			return err //nolint:wrapcheck // wrapped as materialiseAfterCreateError by the caller
		}
	}

	return nil
}
