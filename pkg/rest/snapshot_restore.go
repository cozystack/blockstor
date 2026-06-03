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
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
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

	snap, err := s.Store.Snapshots().Get(r.Context(), srcRD, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	_, err = s.Store.ResourceDefinitions().Get(r.Context(), req.ToResource)
	if err != nil {
		writeStoreError(w, err)

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

	snap, err := s.Store.Snapshots().Get(r.Context(), srcRD, snapName)
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

	newRDName, err := s.materializeRestoredRD(r.Context(), srcRD, &req, &snap)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot restored: " + snapName + " → " + newRDName,
	}})
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

	if len(snap.Nodes) == 0 {
		return true
	}

	snapNodes := make(map[string]struct{}, len(snap.Nodes))
	for _, n := range snap.Nodes {
		snapNodes[n] = struct{}{}
	}

	var missing []string

	seen := make(map[string]struct{}, len(nodes))

	for _, n := range nodes {
		if n == "" {
			continue
		}

		if _, dup := seen[n]; dup {
			continue
		}

		seen[n] = struct{}{}

		if _, ok := snapNodes[n]; !ok {
			missing = append(missing, n)
		}
	}

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
func (s *Server) materializeRestoredRD(ctx context.Context, srcRD string, req *snapshotRestoreRequest, snap *apiv1.Snapshot) (string, error) {
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

	newRD.Props["BlockstorRestoreFromSnapshot"] = srcRD + ":" + snap.Name

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
	err = s.placeRestoredResources(ctx, srcRD, &srcRDObj, &newRD, req)
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
//     one Resource per requested node, resolving Spec.Props["StorPoolName"]
//     from the source RD's first diskful replica.
//   - Empty node list: auto-place against the parent ResourceGroup's
//     SelectFilter.PlaceCount. Mirrors `linstor s r rst --to-resource X`
//     without `--node-name`, which upstream resolves via the placer.
//
// The Nodes / NodeNames request fields are aliased — callers may use
// either; we normalise to one canonical list before iterating.
func (s *Server) placeRestoredResources(ctx context.Context, srcRDName string, srcRD, newRD *apiv1.ResourceDefinition, req *snapshotRestoreRequest) error {
	nodes := canonicalRestoreNodeList(req)

	if len(nodes) > 0 {
		return s.stampRestoredResourcesOnNodes(ctx, srcRDName, newRD.Name, nodes)
	}

	// Empty node list → auto-place via parent RG's place_count.
	// Mirrors upstream LINSTOR's behaviour when `--node-name` is omitted:
	// the placer picks free pools matching the snapshot's provider and
	// constraint set. Bail silently when the source RD has no parent RG
	// (manually-created RDs without an RG) — the operator can still
	// drive placement explicitly via `linstor rd ap <new>`.
	if srcRD.ResourceGroupName == "" {
		return nil
	}

	rg, err := s.Store.ResourceGroups().Get(ctx, srcRD.ResourceGroupName)
	if err != nil {
		// Parent RG missing → treat the same as no-RG: defer to the
		// caller's explicit autoplace. Don't surface as an error;
		// the RD itself is already materialised correctly.
		return nil //nolint:nilerr // intentional fall-through; see comment
	}

	filter := rg.SelectFilter
	if filter.PlaceCount <= 0 {
		return nil
	}

	// Constrain to the snapshot's nodes (cross-node clone needs
	// send-recv; until that lands, the placer must stay on snapshot-
	// local nodes). Re-uses the same constraint the autoplace handler
	// applies for snapshot-restored RDs.
	constrainAutoplaceToSnapshotNodes(ctx, s.Store, newRD, &filter)

	// Pin the provider-kind filter so a ZFS_THIN snapshot can't be
	// auto-placed onto an LVM_THIN candidate pool.
	srcKind := resolveCloneSourceProviderKind(ctx, s.Store, newRD)
	if srcKind != "" {
		filter.ProviderList = []string{srcKind}
	}

	_, _, err = placer.New(s.Store).Place(ctx, newRD.Name, &filter)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	return nil
}

// stampRestoredResourcesOnNodes iterates the explicit-node list and
// stamps one Resource CRD per node, resolving the storage pool from
// the source RD's first diskful replica. Idempotent on duplicates in
// the list (one Create per unique node).
func (s *Server) stampRestoredResourcesOnNodes(ctx context.Context, srcRDName, newRDName string, nodes []string) error {
	pool := storPoolFromSourceRD(ctx, s.Store, srcRDName)

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

// storPoolFromSourceRD returns the StorPoolName of the source RD's
// first non-Diskless Resource, or "" when no diskful replica exists.
// Used to seed the restored Resources' Spec.Props["StorPoolName"]
// so satellites stage the clone on the same provider as the source.
// Best-effort: a missing pool falls through to upstream LINSTOR's
// `r c <node> <rd>` resolution path on the satellite side.
func storPoolFromSourceRD(ctx context.Context, st store.Store, srcRDName string) string {
	resList, err := st.Resources().ListByDefinition(ctx, srcRDName)
	if err != nil {
		return ""
	}

	for i := range resList {
		if slices.Contains(resList[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		if pool := resList[i].Props["StorPoolName"]; pool != "" {
			return pool
		}
	}

	return ""
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
