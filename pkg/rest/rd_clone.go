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

	"github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/clonestatus"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
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

	// The remaining fields golinstor puts on the wire for this
	// endpoint. They are declared because the body is decoded with
	// DisallowUnknownFields: a field missing from this struct is a 400
	// before any of the handler runs, whatever its value.
	//
	// That is not theoretical. linstor-csi defaults LayerList to
	// [drbd, storage] in pkg/volume/parameter.go and never sends it
	// empty, so every CSI clone-from-volume was refused outright with
	// `unknown field "layer_list"` — no StorageClass could avoid it,
	// and on Cozystack the platform-wide `cloneStrategyOverride:
	// csi-clone` routes every disk clone through here.
	LayerList         []string `json:"layer_list,omitempty"`
	ResourceGroup     string   `json:"resource_group,omitempty"`
	ExternalName      string   `json:"external_name,omitempty"`
	VolumePassphrases []string `json:"volume_passphrases,omitempty"`
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

// cloneRequestIsHonourable refuses the accepted-but-unhonoured fields, and
// validates the ones the handler does act on. False means a refusal has been
// written and the caller must stop.
//
// Declaring a field so the decoder stops rejecting the body is only half the
// job. Accepting one and dropping it silently is the shape this endpoint
// already refuses for src_snap_name, and for the same reason: the caller is
// told the clone did what it asked, and it did something else.
//
//   - external_name gives the definition an identity of its own upstream.
//     Dropped, the clone comes back under a different name than requested.
//   - volume_passphrases carries the LUKS keys for the cloned volumes.
//     Dropped, the clone materialises with keys the caller does not hold.
//
// linstor-csi sends neither on this path, so refusing them costs nothing that
// works today and keeps the endpoint from lying if something starts to.
func (s *Server) cloneRequestIsHonourable(
	ctx context.Context, w http.ResponseWriter, srcName string, req *rdCloneRequest,
) bool {
	if req.ExternalName != "" {
		writeCloneRefused(w, http.StatusNotImplemented, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': external_name is not implemented",
			Cause:   "blockstor names a cloned definition by `name`; honouring external_name would change the identity the caller asked for",
			Correc:  "omit external_name, or clone under the name you want",
		})

		return false
	}

	if len(req.VolumePassphrases) > 0 {
		writeCloneRefused(w, http.StatusNotImplemented, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': volume_passphrases is not implemented",
			Cause:   "the clone would materialise with keys the caller does not hold, and report success",
			Correc:  "omit volume_passphrases; set the cluster passphrase with `linstor encryption create-passphrase` instead",
		})

		return false
	}

	// Validated the way rg-modify validates its stack, so an
	// unmaterialisable layer chain is refused here rather than persisting
	// onto the clone for a satellite to choke on.
	err := validateLayerStack(req.LayerList)
	if err != nil {
		writeCloneRefused(w, http.StatusBadRequest, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " + err.Error(),
		})

		return false
	}

	luksErr := s.refuseLUKSWithoutPassphrase(ctx, req.LayerList)
	if luksErr != nil {
		writeCloneRefused(w, http.StatusBadRequest, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " + luksErr.Error(),
		})

		return false
	}

	return true
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

	if !s.cloneRequestIsHonourable(r.Context(), w, srcName, &req) {
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
	_, err := s.materializeRestoredRD(ctx, src.Name, restoreReq, snap, true, &rdShapeOverrides{
		LayerStack:        req.LayerList,
		ResourceGroupName: req.ResourceGroup,
	})
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' failed: " + err.Error(),
		})

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

	writeJSON(w, http.StatusCreated, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + src.Name + "/clone/" + req.Name,
		SourceName: src.Name,
		CloneName:  req.Name,
		Messages: &[]apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "resource definition cloned: " + req.Name,
		}},
	})
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

	// The caller's own shape wins over the source's, on both clone paths.
	if len(req.LayerList) > 0 {
		clone.LayerStack = req.LayerList
	}

	if req.ResourceGroup != "" {
		clone.ResourceGroupName = req.ResourceGroup
	}

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

	// golinstor's ResourceDefinitionService.Clone decodes into
	// `ResourceDefinitionCloneStarted` (an object), NOT
	// `[]ApiCallRc`. Returning the bare ApiCallRc array breaks the
	// decoder with "cannot unmarshal array into Go value of type
	// client.ResourceDefinitionCloneStarted" — surfaced as a
	// CSI CreateVolume-from-source failure in csi-sanity. Emit the
	// envelope shape upstream specifies.
	writeJSON(w, http.StatusCreated, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + src.Name + "/clone/" + clone.Name,
		SourceName: src.Name,
		CloneName:  clone.Name,
		Messages: &[]apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "resource definition cloned: " + clone.Name,
		}},
	})
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
