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
	"strings"
	"time"

	"github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/clonestatus"
	"github.com/cockroachdb/errors"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// rdCloneRequest is the body for `resource-definition clone`. Only the
// new name is required.
//
// Bug 232: after Bug 222 bumped the wire-advertised rest_api_version
// from 1.23.0 to 1.27.0, python-linstor's `_require_version()` gates
// open up `override_props` / `delete_props` (gated on 1.26.0) and
// the `src_snap_name` snapshot-based clone path. Pre-fix the
// DisallowUnknownFields decoder rejects them as 400 + "unknown field"
// and the CLI crashes on the malformed envelope.
//
//   - `override_props` (map[string]string): properties to overwrite
//     on the cloned RD's prop set. Wired through:
//     handleRDClone applies these on top of the source Props after
//     the shallow-copy, so the operator's `--override-prop K=V`
//     lands on the cloned RD.
//   - `delete_props` ([]string): property keys to drop from the
//     cloned RD's prop set. Wired through alongside override_props.
//   - `src_snap_name` (string): name of the source snapshot the
//     clone should materialise from (vs. live-resource clone). Bug
//     239: a non-empty value MUST surface an explicit HTTP 501 +
//     CloneStarted-envelope refusal rather than silently dropping
//     to the live-RD shell-copy path. Pre-Bug-239 the field was
//     accepted-and-no-op (Bug 232), which gave operators a fresh
//     empty shell with no error — the "snap" intent vanished.
//     The operator should fall back to the snapshot-then-restore
//     workflow the writeSnapshotCloneNotImplemented envelope hints
//     at (the live-RD clone path below takes its own internal
//     snapshot; honouring an OPERATOR-named snapshot here is a
//     different contract and stays explicit-refusal until wired).
//   - `use_zfs_clone` (bool): Bug-020 — golinstor v0.58+ /
//     linstor-csi send this on CSI clone-from-source
//     (`CreateVolume` with `VolumeContentSource_Volume`). Upstream
//     semantics: `true` requests a `zfs clone` of an internal
//     snapshot instead of the default `zfs send | zfs recv` full
//     copy. blockstor's only clone data plane IS the snapshot-
//     restore machinery, whose ZFS provider materialises restore
//     targets with `zfs clone` (pkg/storage/zfs
//     RestoreVolumeFromSnapshot) — i.e. exactly the semantics
//     `use_zfs_clone=true` requests. The field is therefore
//     accepted and honoured by construction for the `true` case;
//     `false`/absent (upstream: full send/recv copy) currently
//     lands on the same snapshot-clone path — an accepted
//     divergence documented in docs/cli-parity-known-deltas.md.
type rdCloneRequest struct {
	Name          string            `json:"name"`
	OverrideProps map[string]string `json:"override_props,omitempty"`
	DeleteProps   []string          `json:"delete_props,omitempty"`
	SrcSnapName   string            `json:"src_snap_name,omitempty"`
	UseZfsClone   bool              `json:"use_zfs_clone,omitempty"`
}

// registerRDClone wires the /v1/resource-definitions/{rd}/clone endpoints.
//
// The GET path mirrors upstream LINSTOR exactly:
// `/v1/resource-definitions/{src}/clone/{target}` — that's what
// golinstor's `ResourceDefinitionService.CloneStatus` issues, and what
// linstor-csi polls in a loop until `status == "COMPLETE"`. A 404 here
// makes CSI clone-from-source fail with "clone status: not found".
func (s *Server) registerRDClone(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/clone",
		s.requireStore(s.handleRDClone))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/clone/{target}",
		s.requireStore(s.handleRDCloneStatus))
}

// handleRDClone clones a ResourceDefinition under a new name.
//
// Two materialisation paths, switched on the source's
// VolumeDefinition count:
//
//   - Source with no VDs: shallow metadata copy (Props, RG ref) —
//     Group D's integration smoke test pins this contract.
//   - Source with VDs (Bug-020): clone via the snapshot-restore
//     machinery — an internal snapshot of the source is taken and
//     the target RD is materialised from it exactly like
//     `linstor s resource restore` (VDs hydrated, the
//     `BlockstorRestoreFromSnapshot` marker routes the satellite's
//     storage provider to RestoreVolumeFromSnapshot — `zfs clone`
//     on ZFS — instead of a blank CreateVolume). This replaces the
//     Bug 114 explicit 501 refusal: linstor-csi's CSI
//     clone-from-source (`CreateVolume` +
//     `VolumeContentSource_Volume`) POSTs here with
//     `use_zfs_clone` and needs a real clone, not a refusal.
//
// Bug 114 history: before the 501 gate, this handler answered 201 +
// a synthetic "Completed cloning" line while producing an empty
// target shell. The 501 gate made the gap honest; the snapshot-based
// materialisation now closes it. The matching pins live in
// clone_bug_114_test.go (refusal contract for un-deployed sources)
// and clone_use_zfs_clone_bug020_test.go (materialisation contract).
func (s *Server) handleRDClone(w http.ResponseWriter, r *http.Request) {
	srcName := r.PathValue("rd")

	var req rdCloneRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")

		return
	}

	// Bug 239: clone-from-an-OPERATOR-NAMED-snapshot is not wired.
	// The Bug 232 decoder accepts `src_snap_name` so the CLI stops
	// crashing on the wire-shape mismatch, but silently dropping it
	// gave operators a fresh empty shell that lied about the
	// snapshot. Surface an explicit 501 + CloneStarted envelope so
	// the operator sees the gap (and the matching snapshot-then-
	// restore workaround). Note the LIVE-clone path below (Bug-020)
	// takes its own internal snapshot — that is a different
	// contract from honouring a caller-chosen point-in-time.
	if req.SrcSnapName != "" {
		writeSnapshotCloneNotImplemented(w, srcName, req.Name, req.SrcSnapName)

		return
	}

	src, err := s.Store.ResourceDefinitions().Get(r.Context(), srcName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// VD-bearing sources take the snapshot-based data-plane path
	// (Bug-020); vol-less sources keep the legacy shallow-copy
	// contract Group D pins.
	srcVDs, err := s.Store.VolumeDefinitions().List(r.Context(), srcName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if len(srcVDs) > 0 {
		s.cloneWithData(w, r, &src, &req)

		return
	}

	s.cloneEmptyRDShell(w, r, &src, &req)
}

// cloneWithData materialises a clone of a VD-bearing source RD by
// routing through the snapshot-restore machinery (Bug-020):
//
//  1. take (or reuse) an internal snapshot `clone-<target>` of the
//     source — same guards as the operator-facing snapshot create
//     (source deployed, targets online, snapshot-capable pools);
//  2. materialise the target RD from it via materializeRestoredRD —
//     VDs hydrated from the snapshot's recorded layout, the
//     `BlockstorRestoreFromSnapshot` marker stamped so satellites
//     route the storage provider to RestoreVolumeFromSnapshot
//     (`zfs clone` on ZFS — the `use_zfs_clone=true` semantics
//     linstor-csi requests), replicas placed via the parent RG
//     constrained to snapshot-holding nodes;
//  3. apply the operator's `override_props` / `delete_props` edits
//     on the freshly-created target (Bug 232 parity with the
//     empty-shell path).
//
// Every refusal on this path is emitted through writeCloneRefused —
// the CloneStarted OBJECT envelope — because python-linstor's
// `resource_dfn_clone` decodes the body into CloneStarted
// unconditionally and a bare `[]ApiCallRc` array crashes the CLI
// (see writeSnapshotCloneNotImplemented's wire-shape note).
func (s *Server) cloneWithData(w http.ResponseWriter, r *http.Request, src *apiv1.ResourceDefinition, req *rdCloneRequest) {
	ctx := r.Context()

	if s.cloneTargetPreexists(ctx, w, src.Name, req.Name) {
		return
	}

	// Mirror the snapshot-create Bug 180 gate: a source RD mid-tear-
	// down would reap the internal snapshot + clone marker from
	// under the satellite's restore.
	if rdHasDeleteFlag(ctx, s, src.Name) {
		writeCloneRefused(w, http.StatusConflict, src.Name, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' refused: the source is being deleted",
			Cause:   "the source RD carries the DELETE flag; its backing data is being torn down",
			Correc:  "clone before deleting the source, or restore from a snapshot taken earlier",
		})

		return
	}

	snap, ok := s.ensureCloneSnapshot(w, r, src, req.Name)
	if !ok {
		return
	}

	restoreReq := &snapshotRestoreRequest{ToResource: req.Name}

	// Clone path: eagerPlace=true. `rd clone` is a one-shot CSI
	// operation with no follow-up autoplace, so the clone replicas must
	// materialise on the snapshot-holding nodes in the source pool here
	// (same backend by construction — Bug 038).
	made, err := s.materializeRestoredRD(ctx, src.Name, restoreReq, snap, true)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, req.Name,
			s.failedMaterialiseRefusal(ctx, "clone of resource definition '"+src.Name+"' failed: "+err.Error(),
				"clone", req.Name, made.Placed, err))

		return
	}

	uncheckedRG, ok := s.cloneParentRGSurvived(ctx, w, src, req.Name, made)
	if !ok {
		return
	}

	err = s.applyClonePropEdits(ctx, req)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' created, but applying " +
				"override_props/delete_props failed: " + err.Error(),
		})

		return
	}

	writeCloneStarted(w, src.Name, req.Name, "resource definition cloned: "+req.Name, uncheckedRG)
}

// correcRecreateGroupThenClone is the one wording both rollback doors on this
// path give the operator.
const correcRecreateGroupThenClone = "re-create the resource group, then clone again"

// writeCloneStarted emits the envelope golinstor's Clone decoder expects, with
// any warning the post-write checks want to ride back alongside the result.
func writeCloneStarted(w http.ResponseWriter, srcName, cloneName, message string, warn *apiv1.APICallRc) {
	messages := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: message,
	}}

	if warn != nil {
		messages = append(messages, *warn)
	}

	writeJSON(w, http.StatusCreated, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + srcName + "/clone/" + cloneName,
		SourceName: srcName,
		CloneName:  cloneName,
		Messages:   &messages,
	})
}

// cloneParentRGSurvived is the post-write half of the Bug 174 guard on the
// clone path: the target inherits the source's resource group, and a `rg d`
// that lands between the check the create did and the definition this wrote
// leaves the clone parented to a group that is gone. False means the clone has
// been rolled back and a refusal written.
func (s *Server) cloneParentRGSurvived(
	ctx context.Context, w http.ResponseWriter,
	src *apiv1.ResourceDefinition, cloneName string, made materialisedRD,
) (*apiv1.APICallRc, bool) {
	stampedRG := made.StampedRG

	survived, err := s.parentRGSurvived(ctx, stampedRG)
	if err != nil {
		// The check failed, not the clone. See restoreParentRGSurvived for
		// why an inconclusive safety net must not undo work that succeeded —
		// and why the caller is told it went unverified rather than left to
		// find out from an apiserver log.
		log.FromContext(ctx).Info("could not re-check the clone's parent group",
			"resourceDefinition", cloneName, "resourceGroup", stampedRG, "reason", err.Error())

		return uncheckedCloneGroupWarning(cloneName, stampedRG, err), true
	}

	if survived {
		return nil, true
	}

	if !made.Created {
		writeCloneRefused(w, http.StatusConflict, src.Name, cloneName,
			adoptedOverDeletedGroupRefusal("clone", cloneName, stampedRG, correcRecreateGroupThenClone))

		return nil, false
	}

	rollbackErr := s.rollBackDetached(ctx, cloneName, made.Placed)
	if rollbackErr != nil {
		cause, correc := rollbackFailureAdvice(rollbackErr, cloneName)

		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "': " +
				rollbackFailedMessage(cloneName, stampedRG, rollbackErr),
			Cause:  cause,
			Correc: correc,
		})

		return nil, false
	}

	writeCloneRefused(w, http.StatusNotFound, src.Name, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone of resource definition '" + src.Name + "' rolled back: " +
			rgDeletedRaceCorrection(stampedRG),
		Cause: "the clone inherits its parent group from the source, and that group was " +
			"deleted while the clone was being materialised; a definition pointing at a " +
			"group that is gone lists fine and places badly",
		Correc: correcRecreateGroupThenClone,
	})

	return nil, false
}

// uncheckedCloneGroupWarning is what both halves of the clone's post-write
// group check ride back when the check itself could not be made.
func uncheckedCloneGroupWarning(cloneName, rgName string, err error) *apiv1.APICallRc {
	return &apiv1.APICallRc{
		RetCode: maskWarn,
		Message: "resource group '" + rgName + "' could not be re-checked after the " +
			"clone: " + err.Error(),
		Cause: "the clone itself succeeded; only the safety net over it could not be " +
			"inspected, so a group deleted during the clone would not have been caught",
		Correc: "confirm resource group '" + rgName + "' still exists",
		ObjRefs: map[string]string{
			objRefRscDfn: cloneName,
			objRefRscGrp: rgName,
		},
	}
}

// cloneSnapshotName derives the internal snapshot name backing a
// data-plane clone. Deterministic (`clone-<target>`) so an
// interrupted clone retried by linstor-csi reuses the same snapshot
// instead of accreting one per attempt. The snapshot is visible in
// `linstor s l` like any other — it must outlive the clone because
// `zfs clone` targets stay dependent on their origin snapshot.
func cloneSnapshotName(cloneName string) string {
	return "clone-" + cloneName
}

// cloneTargetPreexists handles the clone-target-already-exists edge
// up front (true = response already written):
//
//   - target carrying OUR restore marker for this exact source +
//     internal snapshot → idempotent retry of a clone that already
//     materialised (linstor-csi replays CreateVolume until it sees
//     success); answer 201 + the same CloneStarted envelope.
//   - any other pre-existing RD under that name → 409 refusal in
//     CloneStarted shape (a bare store AlreadyExists envelope would
//     crash python-linstor's clone decode).
func (s *Server) cloneTargetPreexists(ctx context.Context, w http.ResponseWriter, srcName, cloneName string) bool {
	existing, err := s.Store.ResourceDefinitions().Get(ctx, cloneName)
	if err != nil {
		// NotFound (or any read blip) → proceed with the create;
		// a real store outage surfaces on the next write anyway.
		return false
	}

	if existing.Props["BlockstorRestoreFromSnapshot"] == srcName+":"+cloneSnapshotName(cloneName) {
		if !s.cloneLeftoverIsUsable(ctx, w, srcName, cloneName, existing.ResourceGroupName) {
			return true
		}

		writeJSON(w, http.StatusCreated, cloneStartedResponse{
			Location:   "/v1/resource-definitions/" + srcName + "/clone/" + cloneName,
			SourceName: srcName,
			CloneName:  cloneName,
			Messages: &[]apiv1.APICallRc{{
				RetCode: maskInfo,
				Message: "resource definition already cloned: " + cloneName,
			}},
		})

		return true
	}

	writeCloneRefused(w, http.StatusConflict, srcName, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone target '" + cloneName + "' already exists and is not a clone of '" + srcName + "'",
		Correc:  "pick a different clone name, or delete the existing resource definition first",
	})

	return true
}

// cloneLeftover is what a replay found under a marker-bearing definition.
type cloneLeftover int

const (
	// cloneLeftoverWhole holds volumes and at least one replica that is not
	// being deleted: the least a replay may answer 201 over.
	cloneLeftoverWhole cloneLeftover = iota
	// cloneLeftoverTearingDown still lists replicas, but every one of them is
	// already accepted for deletion.
	cloneLeftoverTearingDown
	// cloneLeftoverEmpty has neither volumes nor replicas.
	cloneLeftoverEmpty
	// cloneLeftoverNoVolumes has replicas and no volumes.
	cloneLeftoverNoVolumes
	// cloneLeftoverNoReplicas has volumes and no replica at all.
	cloneLeftoverNoReplicas
)

// assessCloneLeftover reads a marker-bearing definition until it is whole or
// the cache-retry budget the parent-group read carries is spent.
//
// Both reads are informer-cache served, and the informer that matched the
// marker is not the one answering the replica listing, so a definition seen
// with its replicas not yet seen is the ordinary skew that budget exists for.
// Deciding on the first read told a complete clone to delete itself. A NotFound
// counts as "not seen yet" for the same reason; any other read error is
// returned at once, and the caller refuses on it.
func (s *Server) assessCloneLeftover(ctx context.Context, cloneName string) (cloneLeftover, error) {
	var state cloneLeftover

	for attempt := range cacheRetryAttempts {
		var err error

		state, err = s.readCloneLeftover(ctx, cloneName)
		if err != nil || state == cloneLeftoverWhole || attempt == cacheRetryAttempts-1 {
			return state, err
		}

		select {
		case <-ctx.Done():
			return state, errors.Wrapf(ctx.Err(), "re-read the leftover %q", cloneName)
		case <-time.After(cacheRetryDelay):
		}
	}

	return state, nil
}

// readCloneLeftover is one read of a marker-bearing definition's volumes and
// replicas. A replica stamped for deletion is not counted as holding the
// clone: its satellite finalizer may keep it listed for as long as the owning
// node is down, and a replay answered 201 over it binds a volume to a clone
// that is going away.
func (s *Server) readCloneLeftover(ctx context.Context, cloneName string) (cloneLeftover, error) {
	vds, err := s.Store.VolumeDefinitions().List(ctx, cloneName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return cloneLeftoverEmpty, errors.Wrapf(err, "list the volumes of %q", cloneName)
	}

	replicas, err := s.Store.Resources().ListByDefinition(ctx, cloneName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return cloneLeftoverEmpty, errors.Wrapf(err, "list the replicas of %q", cloneName)
	}

	live := 0

	for i := range replicas {
		if !replicaAcceptedForDeletion(&replicas[i]) {
			live++
		}
	}

	switch {
	case len(vds) > 0 && live > 0:
		return cloneLeftoverWhole, nil
	case len(replicas) > 0 && live == 0:
		return cloneLeftoverTearingDown, nil
	case len(vds) == 0 && len(replicas) == 0:
		return cloneLeftoverEmpty, nil
	case len(vds) == 0:
		return cloneLeftoverNoVolumes, nil
	default:
		return cloneLeftoverNoReplicas, nil
	}
}

// cloneShellParentRGSurvived is the vol-less half of the same guard.
//
// handleRDClone splits on the source's volume count. The branch above copies a
// bare definition, carrying the source's resource group over verbatim, and had
// no check on either side of its write — so a `rg d` landing while it runs
// leaves exactly the definition the guard exists to prevent, on the cheaper of
// the two branches.
//
// The compensation is RD-create's rather than the cascade, and deliberately:
// what this branch created is a bare definition, with no volumes hydrated and
// no replicas stamped, so a single Delete undoes all of it. Best-effort, for
// the reason refuseRDCreateOnRGDeletedRace is: the operator gets the refusal
// either way, and a definition that outlives a failed delete surfaces on its
// next access rather than stranding a replica.
func (s *Server) cloneShellParentRGSurvived(
	ctx context.Context, w http.ResponseWriter, srcName, cloneName, stampedRG string,
) (*apiv1.APICallRc, bool) {
	if stampedRG == "" {
		return nil, true
	}

	survived, err := s.parentRGSurvived(ctx, stampedRG)
	if err != nil {
		// The check failed, not the clone. Same stance as the data-plane
		// half, and the same report: an inconclusive safety net must not
		// undo work that succeeded, and the caller is told it went
		// unverified rather than left to find out from an apiserver log.
		log.FromContext(ctx).Info("could not re-check the cloned shell's parent group",
			"resourceDefinition", cloneName, "resourceGroup", stampedRG, "reason", err.Error())

		return uncheckedCloneGroupWarning(cloneName, stampedRG, err), true
	}

	if survived {
		return nil, true
	}

	// Checked, unlike refuseRDCreateOnRGDeletedRace: the 404 below tells the
	// caller the clone was rolled back, and a delete that failed leaves the
	// shell exactly where it was, parented to a group that is gone. The data
	// path refuses to make that claim over a failed compensation, and the two
	// halves of one guard should not answer the same question differently.
	err = s.Store.ResourceDefinitions().Delete(ctx, cloneName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " +
				rollbackFailedMessage(cloneName, stampedRG, err),
			Cause:  "the parent group is gone and deleting the cloned shell failed",
			Correc: "delete '" + cloneName + "' by hand",
		})

		return nil, false
	}

	writeCloneRefused(w, http.StatusNotFound, srcName, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone of resource definition '" + srcName + "' rolled back: " +
			rgDeletedRaceCorrection(stampedRG),
		Cause: "the clone inherits its parent group from the source, and that group was " +
			"deleted while the clone was being created; a definition pointing at a group " +
			"that is gone lists fine and places badly",
		Correc: correcRecreateGroupThenClone,
	})

	return nil, false
}

// cloneLeftoverIsUsable decides whether a definition that carries this clone's
// marker may be answered as an idempotent replay.
//
// The marker is stamped at RD-create, before the volumes are hydrated and
// before any replica exists, so it says "an attempt at this clone got this
// far" and nothing more. When a rollback fails, the definition is deliberately
// kept and the operator is told to delete it — but the CSI target name is
// deterministic and linstor-csi retries CreateVolume on any error, so the very
// next call meets that leftover, matches the marker and is answered 201
// "already cloned" for a definition still parented to a group that is gone.
// The advice in the 500 never reaches a human, because the machine turns the
// failure into a success first.
//
// So a replay is answered 201 only over a leftover that is whole and whose
// parent group still resolves. An inconclusive read of either refuses: unlike
// the post-write check, which guards a clone this request has just made, this
// gate would otherwise report a clone nobody verified, and the CSI retry makes
// a refusal cheap.
func (s *Server) cloneLeftoverIsUsable(
	ctx context.Context, w http.ResponseWriter, srcName, cloneName, stampedRG string,
) bool {
	// The group resolving is not enough on its own. A failed rollback may
	// have reaped every replica and still kept the definition, and both of
	// this guard's corrections tell the operator to re-create the group —
	// so the moment they do, a group-only gate answers 201 for a clone that
	// exists on no node. The leftover has to be whole.
	state, err := s.assessCloneLeftover(ctx, cloneName)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone target '" + cloneName + "' exists, but reading it back failed: " + err.Error(),
			Cause: "without its volumes and replicas the replay cannot tell a finished clone " +
				"from an unfinished one, and answering 201 over the second binds a volume to " +
				"a clone that may exist on no node",
			Correc: "retry the clone",
		})

		return false
	}

	if state != cloneLeftoverWhole {
		writeCloneRefused(w, http.StatusConflict, srcName, cloneName, cloneLeftoverRefusal(cloneName, state))

		return false
	}

	if stampedRG == "" {
		return true
	}

	// An unreadable group is refused rather than waved through. Refusing
	// costs nothing here, since the CSI retry is self-healing, while a false
	// 201 binds a PV to a definition parented to nothing.
	//
	// But it is refused as unreadable, not as deleted. The two readings send
	// the operator to opposite actions: "the group is gone, delete the clone"
	// over an apiserver blip destroys a working volume for a reason that is
	// not true, where the honest answer is to try again.
	survived, err := s.parentRGSurvived(ctx, stampedRG)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone target '" + cloneName + "' exists, but its parent resource group '" +
				stampedRG + "' could not be read: " + err.Error(),
			Cause: "the replay only answers for a clone whose parent group resolves, and this " +
				"read failed rather than saying the group is gone",
			Correc: "retry the clone",
		})

		return false
	}

	if survived {
		return true
	}

	writeCloneRefused(w, http.StatusConflict, srcName, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone target '" + cloneName + "' exists but is parented to resource group '" +
			stampedRG + "', which no longer exists",
		Cause: "an earlier attempt at this clone could not be rolled back after its parent " +
			"group was deleted, so the definition was left in place rather than orphaning " +
			"its replicas; answering this retry as an idempotent replay would report a " +
			"clone that is not usable",
		Correc: "delete '" + cloneName + "' by hand, re-create resource group '" + stampedRG +
			"', then clone again",
	})

	return false
}

// cloneLeftoverRefusal words the refusal for a leftover that is not whole,
// naming what is missing.
//
// None of these states proves the attempt behind it is dead. The marker lands
// before the volumes and the volumes before the replicas, so a first attempt
// still running looks exactly like an attempt that stopped, and the correction
// has to hold for both: an operator reading "delete it" must not be pointed at
// a clone that is about to finish.
func cloneLeftoverRefusal(cloneName string, state cloneLeftover) *apiv1.APICallRc {
	const replayWouldLie = "; answering this retry as an idempotent replay would bind a volume " +
		"to a clone that exists on no node"

	correcIfStopped := "if no clone under that name is still running, delete '" + cloneName +
		"' by hand, then clone again"

	refusal := &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone target '" + cloneName + "' exists but is not a whole clone",
		Correc:  correcIfStopped,
	}

	switch state {
	case cloneLeftoverTearingDown:
		refusal.Message = "clone target '" + cloneName + "' exists but is still being torn down"
		refusal.Cause = "the definition under that name carries this clone's marker, and every " +
			"replica of it is already accepted for deletion" + replayWouldLie
		refusal.Correc = "wait until the replicas of '" + cloneName + "' are gone, then clone again"
	case cloneLeftoverEmpty:
		refusal.Cause = "the definition under that name carries this clone's marker but has no " +
			"volumes and no replicas: an attempt at this clone stopped before creating any " +
			"volume, or has not reached that step yet" + replayWouldLie
	case cloneLeftoverNoVolumes:
		refusal.Cause = "the definition under that name carries this clone's marker and has " +
			"replicas but no volumes" + replayWouldLie
	case cloneLeftoverNoReplicas, cloneLeftoverWhole:
		refusal.Cause = "the definition under that name carries this clone's marker and has " +
			"volumes but no replicas: an attempt at this clone has not placed any yet, or a " +
			"rollback removed them and could not remove the definition" + replayWouldLie
	}

	return refusal
}

// ensureCloneSnapshot takes (or reuses) the internal snapshot backing
// a data-plane clone. Returns (snap, true) when the caller may
// proceed; (nil, false) when a refusal envelope was already written.
// Guards mirror handleSnapshotCreate's: the source must have at
// least one ACTIVE diskful replica, every replica's node must be
// online, and every backing pool must be snapshot-capable (thin LVM
// / ZFS / FILE_THIN) — the clone data plane IS a snapshot restore,
// so a source that cannot be snapshotted cannot be cloned.
func (s *Server) ensureCloneSnapshot(w http.ResponseWriter, r *http.Request, src *apiv1.ResourceDefinition, cloneName string) (*apiv1.Snapshot, bool) {
	ctx := r.Context()
	snapName := cloneSnapshotName(cloneName)

	existing, err := s.Store.Snapshots().Get(ctx, src.Name, snapName)
	if err == nil {
		// Interrupted-clone retry: the snapshot landed on a previous
		// attempt; reuse it so the restore sees the same point-in-time.
		return &existing, true
	}

	snap := apiv1.Snapshot{Name: snapName, ResourceName: src.Name}

	err = s.hydrateSnapshotFromRD(ctx, &snap, src.Name)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' failed: " + err.Error(),
		})

		return nil, false
	}

	if !s.cloneSnapshotPreconditionsHold(ctx, w, src, &snap, cloneName) {
		return nil, false
	}

	snap.Snapshots = makeSnapshotPerNode(snapName, snap.Nodes, snap.VolumeDefinitions)

	err = s.Store.Snapshots().Create(ctx, &snap)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name +
				"' failed: internal snapshot create: " + err.Error(),
		})

		return nil, false
	}

	return &snap, true
}

// cloneSnapshotPreconditionsHold runs the snapshot-feasibility guards
// for the clone path, emitting CloneStarted-shaped refusals (true =
// caller may proceed). Same checks handleSnapshotCreate applies, but
// the refusal envelopes are CloneStarted objects, not bare
// `[]ApiCallRc` arrays (python-linstor wire-shape; see cloneWithData).
// Split out of ensureCloneSnapshot for the funlen budget.
func (s *Server) cloneSnapshotPreconditionsHold(ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, snap *apiv1.Snapshot, cloneName string) bool {
	// An un-deployed source (no ACTIVE diskful replica) has no
	// backing data to snapshot — a "clone" of it would be the Bug
	// 114 empty shell all over again.
	if len(snap.Nodes) == 0 {
		writeCloneRefused(w, http.StatusConflict, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name +
				"' refused: the source has no active diskful replicas to clone from",
			Cause: "the clone data plane snapshots the source and restores the target " +
				"from it; with no deployed diskful replica there is nothing to snapshot " +
				"and the result would be an empty shell (Bug 114)",
			Correc: "deploy the source first (`linstor rd ap " + src.Name + "`), " +
				"or clone before removing its replicas",
		})

		return false
	}

	if offline := s.offlineTargetNodes(ctx, snap.Nodes); len(offline) > 0 {
		writeCloneRefused(w, http.StatusServiceUnavailable, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' refused: node(s) " +
				strings.Join(offline, ", ") + " are offline",
			Cause: "the internal clone snapshot must be taken on every diskful replica; " +
				"an offline node would leave the snapshot (and the clone) incomplete",
			Correc: "retry once the node(s) reconnect",
		})

		return false
	}

	return s.clonePoolsSupportSnapshots(ctx, w, src, snap, cloneName)
}

// clonePoolsSupportSnapshots is the G5 capability gate applied to the
// clone path: every diskful replica of the source must sit in a
// snapshot-capable pool, because the clone data plane is a snapshot
// restore. Thick LVM / plain FILE sources surface an actionable
// refusal instead of an internal snapshot that the satellite could
// never take. True = caller may proceed.
func (s *Server) clonePoolsSupportSnapshots(ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, snap *apiv1.Snapshot, cloneName string) bool {
	resList, err := s.Store.Resources().ListByDefinition(ctx, src.Name)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' failed: " + err.Error(),
		})

		return false
	}

	locs := s.nonSnapshotPoolLocations(ctx, resList, snap.Nodes)
	if len(locs) == 0 {
		return true
	}

	writeCloneRefused(w, http.StatusBadRequest, src.Name, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError | apiCallRcFailSnapshotsNotSupported,
		Message: "clone of resource definition '" + src.Name + "' refused: storage pool(s) " +
			strings.Join(locs, ", ") + " do not support snapshots",
		Cause: "the clone data plane takes an internal snapshot of the source and " +
			"restores the target from it; thick providers (thick LVM, plain FILE, " +
			"DISKLESS) cannot take copy-on-write snapshots",
		Correc: "place the source on a thin-provisioned snapshot-capable pool " +
			"(LVM_THIN / ZFS_THIN / FILE_THIN) before cloning",
	})

	return false
}

// applyClonePropEdits applies the Bug 232 `override_props` /
// `delete_props` edits onto the freshly-materialised clone target —
// parity with the empty-shell path, which folds them in during the
// shallow copy. No-op when the request carries neither.
func (s *Server) applyClonePropEdits(ctx context.Context, req *rdCloneRequest) error {
	if len(req.OverrideProps) == 0 && len(req.DeleteProps) == 0 {
		return nil
	}

	// Bug 204b shape: typed-Patch with retry-on-conflict so the
	// freshly-materialised clone target's reconciler can't race this
	// prop fold-in into a 409 (the edits re-apply to fresh state).
	//nolint:wrapcheck // message wrapped by the caller's envelope
	return s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, req.Name,
		func(rd *apiv1.ResourceDefinition) error {
			if rd.Props == nil {
				rd.Props = make(map[string]string, len(req.OverrideProps))
			}

			maps.Copy(rd.Props, req.OverrideProps)

			for _, k := range req.DeleteProps {
				delete(rd.Props, k)
			}

			return nil
		})
}

// writeCloneRefused stamps a clone refusal in the CloneStarted OBJECT
// envelope. python-linstor's `resource_dfn_clone` decodes the
// response body into CloneStarted unconditionally (success AND
// error), so every non-2xx answer on the clone POST must keep the
// object shape — a bare `[]ApiCallRc` array crashes the CLI with
// `AttributeError: 'list' object has no attribute 'get'` before the
// error line reaches the operator (see writeSnapshotCloneNotImplemented).
func writeCloneRefused(w http.ResponseWriter, status int, srcName, cloneName string, callRc *apiv1.APICallRc) {
	if callRc.ObjRefs == nil {
		callRc.ObjRefs = map[string]string{objRefRscDfn: srcName}
	}

	writeJSON(w, status, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + srcName + "/clone/" + cloneName,
		SourceName: srcName,
		CloneName:  cloneName,
		Messages:   &[]apiv1.APICallRc{*callRc},
	})
}

// cloneEmptyRDShell materialises the empty-source clone path: shallow-copy
// of the RD spec (Props, RG ref) under a new name. Group D's integration
// smoke test pins this branch — a freshly-created vol-less RD must be
// cloneable with Props and RG carried over. Extracted out of handleRDClone
// to keep its funlen under budget after the Bug 114 VD-presence gate.
//
// Bug 232: the `req.OverrideProps` map is applied on top of the
// source's Props after the shallow-copy, and `req.DeleteProps` keys
// are dropped before the Create lands. python-linstor 1.27.0 sends
// these via `linstor resource-definition clone --override-prop K=V`
// / `--delete-prop K`; wiring them through here keeps the operator's
// intent honoured for the empty-VD path. `req.SrcSnapName` is
// accepted-and-no-op (see rdCloneRequest docstring) — the snapshot-
// based clone data plane lands separately.
func (s *Server) cloneEmptyRDShell(w http.ResponseWriter, r *http.Request,
	src *apiv1.ResourceDefinition, req *rdCloneRequest,
) {
	clone := *src
	clone.Name = req.Name
	clone.UUID = ""

	if src.Props != nil || len(req.OverrideProps) > 0 {
		clone.Props = make(map[string]string, len(src.Props)+len(req.OverrideProps))
		maps.Copy(clone.Props, src.Props)
	}

	maps.Copy(clone.Props, req.OverrideProps)

	for _, k := range req.DeleteProps {
		delete(clone.Props, k)
	}

	err := s.Store.ResourceDefinitions().Create(r.Context(), &clone)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	uncheckedRG, ok := s.cloneShellParentRGSurvived(r.Context(), w, src.Name, clone.Name, clone.ResourceGroupName)
	if !ok {
		return
	}

	// golinstor's ResourceDefinitionService.Clone decodes into
	// `ResourceDefinitionCloneStarted` (an object), NOT
	// `[]ApiCallRc`. Returning the bare ApiCallRc array breaks the
	// decoder with "cannot unmarshal array into Go value of type
	// client.ResourceDefinitionCloneStarted" — surfaced as a
	// CSI CreateVolume-from-source failure in csi-sanity. Emit the
	// envelope shape upstream specifies.
	writeCloneStarted(w, src.Name, clone.Name, "resource definition cloned: "+clone.Name, uncheckedRG)
}

// handleRDCloneStatus answers golinstor's `CloneStatus` poll. The
// response is grounded in actual store state (Bug 114): we compare
// the source RD's VolumeDefinition count to the target's. Equal
// counts → COMPLETE (the clone is structurally consistent with the
// source). A non-empty source paired with an empty target → FAILED,
// so linstor-csi surfaces a concrete error rather than spinning on
// a stale COMPLETE while the data plane never copied anything.
//
// Path: GET /v1/resource-definitions/{src}/clone/{target}.
// A 404 on the target signals "clone failed mid-way" — which gives
// linstor-csi an actionable error rather than an infinite poll loop.
// A 404 on the source surfaces the same way: it would have been
// caught at clone-POST time, but a delete-source race shouldn't
// produce a phantom COMPLETE either.
func (s *Server) handleRDCloneStatus(w http.ResponseWriter, r *http.Request) {
	srcName := r.PathValue("rd")
	targetName := r.PathValue("target")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), targetName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	status := computeCloneStatus(r.Context(), s.Store, srcName, targetName)
	writeJSON(w, http.StatusOK, client.ResourceDefinitionCloneStatus{
		Status: status,
	})
}

// computeCloneStatus resolves COMPLETE vs FAILED for a clone pair by
// comparing source-vs-target VolumeDefinition counts. Bug 114: an
// empty target paired with a non-empty source is structurally
// incomplete — golinstor's poll loop must see FAILED so it stops
// waiting on data that will never arrive.
//
// If the source RD itself is gone (race with `rd d <src>` while the
// poll is in flight), we cannot prove the target is consistent —
// the safest answer is COMPLETE because the target survived and any
// further validation requires the source to compare against. This
// preserves the legacy behaviour for that edge case.
func computeCloneStatus(ctx context.Context, st store.Store, srcName, targetName string) clonestatus.CloneStatus {
	srcVDs, err := st.VolumeDefinitions().List(ctx, srcName)
	if err != nil {
		return clonestatus.Complete
	}

	targetVDs, err := st.VolumeDefinitions().List(ctx, targetName)
	if err != nil {
		return clonestatus.Complete
	}

	if len(srcVDs) > 0 && len(targetVDs) < len(srcVDs) {
		return clonestatus.Failed
	}

	return clonestatus.Complete
}

// writeSnapshotCloneNotImplemented stamps the Bug 239 refusal envelope
// for the `src_snap_name`-bearing clone path. Same wire shape as
// writeCloneRefused (CloneStarted-object on 501 so python-
// linstor's `resource_dfn_clone` can decode it without crashing), but
// the messages are scoped to the snapshot-clone gap rather than the
// VD-copy gap. The operator gets a concrete fallback that uses the
// `s create` + `s resource restore` workflow which IS wired today.
//
// Bug 232 used to accept `src_snap_name` and silently drop it,
// producing a fresh empty shell on the live-RD path with the wrong
// data shape — Bug 239 trades the silent-success for an explicit
// 501 so the operator either learns the gap immediately or scripts
// the snapshot+restore fallback.
func writeSnapshotCloneNotImplemented(w http.ResponseWriter, srcName, cloneName, srcSnapName string) {
	writeJSON(w, http.StatusNotImplemented, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + srcName + "/clone/" + cloneName,
		SourceName: srcName,
		CloneName:  cloneName,
		Messages: &[]apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "snapshot-based clone not implemented in this release (pending Phase 12)",
			Cause: "the apiserver accepts `src_snap_name` on the wire for python-linstor 1.27.0 " +
				"compatibility (Bug 232 + 237) but the satellite-side snapshot-clone data plane is " +
				"not yet wired; silently falling back to a live-RD shell copy would discard the " +
				"snapshot intent and produce a clone with the wrong contents",
			Correc: "use the snapshot-then-restore workflow which IS wired today: " +
				"`linstor s create " + srcName + " " + srcSnapName + "` (if the snapshot " +
				"doesn't already exist) then " +
				"`linstor s resource restore --from-resource " + srcName +
				" --from-snapshot " + srcSnapName + " --to-resource " + cloneName + "`",
			ObjRefs: map[string]string{
				objRefRscDfn: srcName,
				"SnapName":   srcSnapName,
			},
		}},
	})
}

// cloneStartedResponse mirrors upstream LINSTOR's
// `ResourceDefinitionCloneStarted` — the JSON object golinstor's
// Clone(...) decodes into. Defined here (not in pkg/api/v1) since
// it's an output-only response envelope; no client-side caller
// constructs it.
type cloneStartedResponse struct {
	Location   string             `json:"location"`
	SourceName string             `json:"source_name"`
	CloneName  string             `json:"clone_name"`
	Messages   *[]apiv1.APICallRc `json:"messages,omitempty"`
}
