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

	_, err = s.Store.ResourceDefinitions().Get(r.Context(), req.ToResource)
	if err != nil {
		writeStoreError(w, err)

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

	err = hydrateVolumesFromSnapshot(r.Context(), s, req.ToResource, &snap)
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

	snapName := resolveSnapshotName(r, &req)
	if snapName == "" {
		writeError(w, http.StatusBadRequest, "snapshot name required (URL path, from_snapshot, or snapshot_name)")

		return
	}

	// Cache-retry (Bug 124 class): linstor-csi's CreateVolume-from-
	// snapshot POSTs the restore right after the snapshot create;
	// absorb informer-cache lag on the source-snapshot read instead
	// of 404-ing the restore.
	snap, err := getSnapshotWithCacheRetry(r.Context(), s.Store, srcRD, snapName)
	if err != nil {
		writeStoreError(w, err)

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
	if s.restoreTargetPreexists(r.Context(), w, srcRD, snapName, req.ToResource) {
		return
	}

	newRDName, err := s.materializeRestoredRD(r.Context(), srcRD, &req, &snap, false, nil)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot restored: " + snapName + " → " + newRDName,
	}})
}

// restoreTargetPreexists makes a repeat restore idempotent. True means an
// answer has been written and the caller must stop.
//
// CSI requires CreateVolume to be idempotent: a repeat with the same name and
// the same parameters has to succeed and return the volume that already
// exists. external-provisioner has no other way to make progress after a
// partial failure, so without this the first partial failure was terminal for
// that volume name — the definition the first attempt created was still
// there, every retry hit ErrAlreadyExists, and the PVC stayed Pending until
// someone deleted the leftover by hand.
//
// The distinction that makes this safe is the restore marker. A definition
// carrying `<srcRD>:<snapName>` is the one THIS restore would have produced,
// so reporting success is accurate. Anything else under that name is a
// genuine collision and stays a refusal — the same split the clone path
// already draws.
func (s *Server) restoreTargetPreexists(ctx context.Context, w http.ResponseWriter, srcRD, snapName, toResource string) bool {
	existing, err := s.Store.ResourceDefinitions().Get(ctx, toResource)
	if err != nil {
		// NotFound, or a read blip: proceed with the create, which
		// surfaces a real store outage on its own.
		return false
	}

	if existing.Props[restoreFromSnapshotKey] == srcRD+":"+snapName {
		writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "snapshot already restored: " + snapName + " → " + toResource,
		}})

		return true
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
		Message: "resource definition '" + toResource + "' already exists and is not a restore of '" +
			snapName + "'",
		Correc: "restore under a different name, or delete the existing resource definition first",
	}})

	return true
}

// restoreFromSnapshotKey marks a definition as produced by a snapshot
// restore, encoded `<source RD>:<snapshot>`. The satellite reads it to route
// the storage provider to RestoreVolumeFromSnapshot, and the retry path above
// reads it to tell its own leftover from somebody else's definition.
const restoreFromSnapshotKey = "BlockstorRestoreFromSnapshot"

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
}

func (s *Server) materializeRestoredRD(ctx context.Context, srcRD string, req *snapshotRestoreRequest, snap *apiv1.Snapshot, eagerPlace bool, overrides *rdShapeOverrides) (string, error) {
	srcRDObj, err := s.Store.ResourceDefinitions().Get(ctx, srcRD)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced via writeStoreError
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
		Props:             maps.Clone(snap.Props),
		LayerStack:        srcRDObj.LayerStack,
	}

	// A clone may name its own group and stack; a restore inherits both.
	if overrides != nil {
		if len(overrides.LayerStack) > 0 {
			newRD.LayerStack = overrides.LayerStack
		}

		if overrides.ResourceGroupName != "" {
			newRD.ResourceGroupName = overrides.ResourceGroupName
		}
	}

	if newRD.Props == nil {
		newRD.Props = maps.Clone(srcRDObj.Props)
	}

	// Stamp the clone-source so the dispatcher's buildVolumes (called
	// at every satellite-reconcile of placed Resources) emits
	// DesiredVolume.SourceSnapshot, which routes the storage provider
	// to RestoreVolumeFromSnapshot instead of CreateVolume.
	// `<srcRD>:<snapName>` is the agreed encoding — satellite splits
	// on the colon. We persist on the RD (not per-Resource) because
	// every replica of the new RD clones from the same source.
	if newRD.Props == nil {
		newRD.Props = map[string]string{}
	}

	newRD.Props[restoreFromSnapshotKey] = srcRD + ":" + snap.Name

	err = s.Store.ResourceDefinitions().Create(ctx, &newRD)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced via writeStoreError
	}

	err = hydrateVolumesFromSnapshot(ctx, s, newRD.Name, snap)
	if err != nil {
		return "", err
	}

	// Bug 354: stamp per-node Resource CRDs so satellites have something
	// to reconcile. Pre-fix the target RD + VDs landed in the store but
	// `Store.Resources().Create()` was never called — satellites never
	// observed a Resource for the new RD, so the BlockstorRestoreFromSnapshot
	// prop marker on the RD was dead code and the restored RD stayed an
	// empty shell. Mirrors upstream CtrlSnapshotRestoreApiCallHandler.
	err = s.placeRestoredResources(ctx, srcRD, &newRD, req, snap, eagerPlace)
	if err != nil {
		return "", err
	}

	return newRD.Name, nil
}

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
func (s *Server) placeRestoredResources(ctx context.Context, srcRDName string, newRD *apiv1.ResourceDefinition, req *snapshotRestoreRequest, snap *apiv1.Snapshot, eagerPlace bool) error {
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

			return nil
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
func (s *Server) stampRestoredResourcesOnNodes(ctx context.Context, srcRDName, newRDName string, nodes []string) error {
	poolByNode, fallbackPool := storPoolsByNodeFromSourceRD(ctx, s.Store, srcRDName)

	seen := make(map[string]struct{}, len(nodes))

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

		err := s.Store.Resources().Create(ctx, &res)
		if err != nil {
			return err //nolint:wrapcheck // surfaced via writeStoreError
		}
	}

	return nil
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
func hydrateVolumesFromSnapshot(ctx context.Context, s *Server, rdName string, snap *apiv1.Snapshot) error {
	for i := range snap.VolumeDefinitions {
		svd := &snap.VolumeDefinitions[i]
		vd := apiv1.VolumeDefinition{
			VolumeNumber: svd.VolumeNumber,
			SizeKib:      svd.SizeKib,
		}

		err := s.Store.VolumeDefinitions().Create(ctx, rdName, &vd)
		if err != nil {
			return err //nolint:wrapcheck // surfaced via writeStoreError
		}
	}

	return nil
}
