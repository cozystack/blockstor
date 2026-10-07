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

	"github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/clonestatus"
	"github.com/cockroachdb/errors"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"
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
	// The props-modify triple every upstream endpoint that edits
	// properties carries — override_props, delete_props,
	// delete_namespaces. Embedded from golinstor rather than respelled
	// field by field, because a respelling is how `delete_namespaces`
	// went missing in the first place: a golinstor client filling the
	// whole triple would have hit the very 400 that declaring its two
	// neighbours was meant to end. golinstor tags it omitempty and
	// linstor-csi never fills it, so no CSI clone ever carried it. The
	// python CLI's clone verb has no flag for it either —
	// it offers --external-name, --use-zfs-clone, --volume-passphrase,
	// --layer-list and --resource-group — so this is a wire-level field,
	// which is exactly why the decoder is the only thing guarding it.
	client.GenericPropsModify

	Name        string `json:"name"`
	SrcSnapName string `json:"src_snap_name,omitempty"`
	UseZfsClone bool   `json:"use_zfs_clone,omitempty"`

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

// storedDefinitionName is the name a definition already under name is stored
// with, or name when there is none. LINSTOR folds the case of a definition's
// name, and the store finds the definition under any spelling, but a
// definition's replicas are selected by the name exactly as stored: judged
// under another spelling, a finished clone or restore counted no replica,
// read as unfinished, and was re-placed onto a node the operator had emptied.
//
// A read that fails is returned, and the caller refuses: kept under the
// request's spelling, a blip that cleared by the next read let the gates
// after this one judge the definition under the wrong name.
func (s *Server) storedDefinitionName(ctx context.Context, name string) (string, error) {
	existing, err := s.Store.ResourceDefinitions().Get(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return name, nil
	}

	if err != nil {
		return name, err //nolint:wrapcheck // answered as the unreadable-target refusal
	}

	return existing.Name, nil
}

// rebindCloneTarget names the clone's target as it is stored; see
// storedDefinitionName. False means a refusal has been written.
func (s *Server) rebindCloneTarget(
	ctx context.Context, w http.ResponseWriter, srcName string, req *rdCloneRequest,
) bool {
	stored, err := s.storedDefinitionName(ctx, req.Name)
	if err != nil {
		writeCloneTargetUnreadable(w, srcName, req.Name, err)

		return false
	}

	req.Name = stored

	return true
}

// cloneRequestIsAnswerable refuses, from the body alone, a clone this endpoint
// cannot carry out. False means a refusal has been written.
func cloneRequestIsAnswerable(w http.ResponseWriter, srcName string, req *rdCloneRequest) bool {
	// Asked before the name: python-linstor drops `name` from the body when
	// it sends external_name, so a missing-name refusal would point the
	// operator at the one field they did give.
	if !cloneRequestDropsNothing(w, srcName, req) || !cloneTargetNameIsUsable(w, srcName, req) {
		return false
	}

	// Bug 239: clone-from-an-OPERATOR-NAMED-snapshot is not wired.
	// The Bug 232 decoder accepts `src_snap_name` so the CLI stops
	// crashing on the wire-shape mismatch, but silently dropping it
	// gave operators a fresh empty shell that lied about the
	// snapshot. Surface an explicit 501 so the operator sees the gap
	// (and the matching snapshot-then-restore workaround), in the
	// envelope withCloneEnvelope picks for the caller: a CloneStarted
	// object for python-linstor, an ApiCallRc array for everyone else.
	// Note the LIVE-clone path below (Bug-020) takes its own internal
	// snapshot — that is a different contract from honouring a
	// caller-chosen point-in-time.
	if req.SrcSnapName != "" {
		writeSnapshotCloneNotImplemented(w, srcName, req.Name, req.SrcSnapName)

		return false
	}

	return true
}

// cloneRequestDropsNothing refuses the accepted-but-unhonoured fields. False
// means a refusal has been written and the caller must stop.
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
func cloneRequestDropsNothing(w http.ResponseWriter, srcName string, req *rdCloneRequest) bool {
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

	// The prop edits land on the target twice: folded in at create, where the
	// marks are stamped over them, and again once the clone is done, after
	// the marks, so an edit that rewrites or removes one would undo the clone
	// under the caller: without the restore marker the satellite brings the
	// volumes up blank and the retry no longer recognises the target as this
	// clone. Refused rather than skipped, for the reason above: an edit the
	// caller asked for and did not get is the silence this endpoint refuses.
	// linstor-csi's own edit, deleting Aux/csi-provisioning-completed-by,
	// touches none of them.
	if key := store.ServerOwnedPropEdit(req.OverrideProps, req.DeleteProps, req.DeleteNamespaces); key != "" {
		writeCloneRefused(w, http.StatusBadRequest, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': the prop edits would change " +
				key + ", which blockstor sets on the clone itself",
			Cause:  key + " records what the clone is; changing it in the same request undoes the clone",
			Correc: "drop " + key + " from override_props, delete_props and delete_namespaces",
		})

		return false
	}

	return true
}

// cloneRequestShapeIsUsable validates the shape the caller picked for the
// clone: a layer stack that can be materialised at all, and a parent resource
// group that exists. The passphrase LUKS needs is checked after the replay of
// a finished clone, in clonedStackHasItsPassphrase, which writes no new LUKS
// volume and must not be refused over a passphrase that cannot be read.
func (s *Server) cloneRequestShapeIsUsable(
	ctx context.Context, w http.ResponseWriter, srcName string, req *rdCloneRequest,
) bool {
	// Validated the way rg-modify validates its stack, so an
	// unmaterialisable layer chain is refused here rather than persisting
	// onto the clone for a satellite to choke on. Stored in canonical case:
	// the readers that ask whether a definition carries DRBD compare exactly,
	// and a lowercase stack read as one without it.
	err := validateLayerStack(req.LayerList)
	if err == nil && len(req.LayerList) > 0 {
		req.LayerList, err = validate.NormalizeLayerStack(req.LayerList)
	}

	if err != nil {
		writeCloneRefused(w, http.StatusBadRequest, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " + err.Error(),
		})

		return false
	}

	return s.cloneResourceGroupExists(ctx, w, srcName, req)
}

// clonedStackHasItsPassphrase refuses a clone whose stack carries LUKS while
// the cluster passphrase is missing: the stack it names, or the source's when
// it names none. It runs after the replay of a finished clone, which writes
// no new LUKS volume, so a passphrase that went missing or could not be read
// does not fail the retry of a clone that is already done.
func (s *Server) clonedStackHasItsPassphrase(
	ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, req *rdCloneRequest,
) bool {
	stack := req.LayerList
	if len(stack) == 0 {
		stack = store.EffectiveLayerStack(src.LayerStack)
	}

	return s.clonePassphraseHolds(ctx, w, src.Name, req.Name, stack)
}

// clonePassphraseHolds refuses a clone whose stack carries LUKS while the
// cluster passphrase is missing. False means a refusal has been written.
//
// Only the missing passphrase is the caller's to fix. A Secret or the
// controller props that could not be read is a failure of this side, and a
// 400 for it reads to linstor-csi as a permanent client error on a body it
// resends unchanged.
func (s *Server) clonePassphraseHolds(
	ctx context.Context, w http.ResponseWriter, srcName, cloneName string, stack []string,
) bool {
	luksErr := s.refuseLUKSWithoutPassphrase(ctx, stack)
	if luksErr == nil {
		return true
	}

	status := http.StatusInternalServerError
	if errors.Is(luksErr, ErrLUKSRequiresPassphrase) {
		status = http.StatusBadRequest
	}

	writeCloneRefused(w, status, srcName, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone of resource definition '" + srcName + "': " + luksErr.Error(),
	})

	return false
}

// cloneResourceGroupExists applies the Bug 134 gate to the group a clone pins
// for itself. `resource_group` lands on the target on both clone paths — the
// shallow copy stamps it directly, the data-plane path hands it to
// materializeRestoredRD as a shape override — and neither went past the
// validator the RD-create path runs, so a typo produced a clone whose parent
// group does not exist. That RD lists fine and places badly: the placer's
// Controller→RG→RD prop walk drops the RG tier without a word, taking
// auto-place, auto-diskful, place_count and rebalance with it.
func (s *Server) cloneResourceGroupExists(
	ctx context.Context, w http.ResponseWriter, srcName string, req *rdCloneRequest,
) bool {
	found, err := s.lookupPinnedRG(ctx, req.ResourceGroup)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " + scrubImplDetails(err.Error()),
		})

		return false
	}

	if !found {
		writeCloneRefused(w, http.StatusNotFound, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " + unknownRGMessage(req.ResourceGroup),
			Correc:  "create the resource group first, or omit resource_group to inherit the source's",
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
	w = withCloneEnvelope(w, r)
	srcName := r.PathValue("rd")

	var req rdCloneRequest

	// Decoded without the shared answer: a malformed body, an unknown field or
	// one over the size cap reaches python-linstor's clone decode too, and it
	// reads `messages` off whatever comes back, so the array crashes it before
	// the operator sees why their body was refused.
	decodeErr := decodeJSONBody(r, &req)
	if decodeErr != nil {
		status, callRc := decodeErrorRc(decodeErr)
		writeCloneRefused(w, status, srcName, req.Name, &callRc)

		return
	}

	if !cloneRequestIsAnswerable(w, srcName, &req) {
		return
	}

	if !s.rebindCloneTarget(r.Context(), w, srcName, &req) {
		return
	}

	if !s.cloneRequestShapeIsUsable(r.Context(), w, srcName, &req) {
		return
	}

	// A replay of a finished clone is answered before the source is read at
	// all; see replayOfFinishedClone. The source may be gone, or hold no
	// volumes, by the time a finished clone is replayed, and neither says
	// anything about the clone.
	replayed, halt := s.replayOfFinishedClone(r.Context(), w, srcName, &req)
	if replayed || halt {
		return
	}

	src, err := s.Store.ResourceDefinitions().Get(r.Context(), srcName)
	if err != nil {
		writeCloneStoreError(w, srcName, req.Name, err)

		return
	}

	if !s.clonedStackHasItsPassphrase(r.Context(), w, &src, &req) {
		return
	}

	// VD-bearing sources take the snapshot-based data-plane path
	// (Bug-020); vol-less sources keep the legacy shallow-copy
	// contract Group D pins.
	srcVDs, err := s.Store.VolumeDefinitions().List(r.Context(), srcName)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: scrubImplDetails(err.Error()),
		})

		return
	}

	if len(srcVDs) > 0 {
		s.cloneWithData(w, r, &src, &req)

		return
	}

	s.cloneEmptyRDShell(w, r, &src, &req)
}

// cloneTargetNameIsUsable holds the clone door to the identifier rules every
// other door that creates a definition runs: `rd create`, `spawn` and the
// restore all validate the name they are handed, and a clone that skipped them
// could create a definition `rd create` answers 400 for, which
// tests/e2e/rd-name-validation-bulk.sh treats as a contract.
//
// The internal snapshot's name is not checked here: only the data path takes
// one, and a volume-less clone under a name `rd create` accepts has to stay
// possible. cloneSnapshotNameIsUsable checks it on that path.
func cloneTargetNameIsUsable(w http.ResponseWriter, srcName string, req *rdCloneRequest) bool {
	if req.Name == "" {
		writeCloneRefused(w, http.StatusBadRequest, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "name is required",
		})

		return false
	}

	nameErr := validateLinstorName("resource definition", req.Name)
	if nameErr != nil {
		writeCloneRefused(w, http.StatusBadRequest, srcName, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " + nameErr.Error(),
		})

		return false
	}

	return true
}

// cloneSnapshotNameIsUsable refuses a target whose internal snapshot name
// would pass the identifier ceiling. The name is derived by prefixing the
// target, so a target just inside the ceiling would take the clone through a
// snapshot create the store refuses, and it is checked before anything is
// written.
func cloneSnapshotNameIsUsable(w http.ResponseWriter, srcName, cloneName string) bool {
	nameErr := validateLinstorName("snapshot", cloneSnapshotName(cloneName))
	if nameErr == nil {
		return true
	}

	writeCloneRefused(w, http.StatusBadRequest, srcName, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone of resource definition '" + srcName + "' into '" + cloneName +
			"': the internal snapshot this clone takes would be named '" +
			cloneSnapshotName(cloneName) + "': " + nameErr.Error(),
		Correc: "pick a shorter target name",
	})

	return false
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

	if !cloneSnapshotNameIsUsable(w, src.Name, req.Name) {
		return
	}

	if !cloneLayerStackIsHonourable(w, src, req) {
		return
	}

	resume, stop := s.cloneTargetState(ctx, w, src, req)
	if stop {
		return
	}

	if !s.cloneSourceIsNotBeingDeleted(ctx, w, src, req.Name) {
		return
	}

	if resume && !s.cloneResumeKeepsItsPointInTime(ctx, w, src, req.Name) {
		return
	}

	snap, reusedSnapshot, ok := s.ensureCloneSnapshot(w, r, src, req.Name)
	if !ok {
		return
	}

	// The only question a leftover still raises: does a REUSED snapshot still
	// describe the source? It belongs to a clone about to be materialised and
	// never to one already made, which is why the finished question is asked
	// at the top rather than here.
	if reusedSnapshot && !s.cloneSnapshotIsCurrent(ctx, w, src, snap, req.Name) {
		return
	}

	s.materializeClone(ctx, w, src, req, snap, resume)
}

// cloneResumeKeepsItsPointInTime refuses to resume an unfinished clone whose
// internal snapshot is gone once the leftover holds anything restored from it.
// False means a refusal has been written.
//
// ensureCloneSnapshot takes the snapshot again when it is missing, of the
// source as it is now. A leftover that already has a volume with a replica was
// restored from the earlier point-in-time, and finishing it from the later one
// stitches two moments of the source into one clone: a source whose data moved
// on without its volume sizes changing passes every shape check and comes back
// as a clone nobody asked for. A leftover with no volume, or with no replica,
// holds no data of the earlier moment, and retaking the snapshot makes it the
// fresh clone it would have been, as long as its volumes fit the source as it
// is now (leftoverFitsARetake).
//
// The refusal names its way out, so linstor-csi's retry, which polls the
// status and POSTs again on a 404, ends at an operator rather than looping.
func (s *Server) cloneResumeKeepsItsPointInTime(
	ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, cloneName string,
) bool {
	// Every read here is the API server's: the answer refuses a clone the
	// operator then deletes by hand, and a cache that has not seen the
	// snapshot, or still lists a replica being deleted, would make that call
	// for a clone that is fine.
	err := cloneSnapshotIsPresent(ctx, s.Store, src.Name, cloneSnapshotName(cloneName))
	if err == nil {
		return true
	}

	if !errors.Is(err, store.ErrNotFound) {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "': the snapshot behind '" +
				cloneName + "' could not be read: " + scrubImplDetails(err.Error()),
			Correc: "retry the clone",
		})

		return false
	}

	vds, err := store.LiveVolumes(ctx, s.Store, cloneName)
	if err != nil {
		writeCloneStoreError(w, src.Name, cloneName, err)

		return false
	}

	count, err := countReplicas(ctx, s.Store, cloneName)
	if err != nil {
		writeCloneStoreError(w, src.Name, cloneName, err)

		return false
	}

	// Data from the earlier moment exists only where a volume has a replica
	// holding it: a volume definition alone is a size, a replica with no
	// volume holds nothing, one stamped for deletion is going with its data,
	// and a diskless one never had any.
	// Either half on its own is finished from the new snapshot like the fresh
	// clone it still is, once its volumes are known to fit that snapshot.
	if len(vds) == 0 {
		return true
	}

	if count.Holding == 0 {
		return s.leftoverFitsARetake(ctx, w, src, cloneName, vds)
	}

	writeCloneRefused(w, http.StatusConflict, src.Name, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
		Message: "clone target '" + cloneName + "' is an unfinished clone whose snapshot '" +
			cloneSnapshotName(cloneName) + "' is gone",
		Cause: "the leftover already holds data restored from that snapshot, and finishing it " +
			"from a new snapshot of the source as it is now would join two different moments " +
			"of the source into one clone",
		Correc: "delete '" + cloneName + "' and clone again",
	})

	return false
}

// cloneSnapshotIsPresent reads one snapshot from the API server, answering
// store.ErrNotFound when it is not there.
func cloneSnapshotIsPresent(ctx context.Context, st store.Store, srcName, snapName string) error {
	_, err := liveCloneSnapshot(ctx, st, srcName, snapName)

	return err
}

// cloneSnapshotIsReusable judges the internal snapshot an earlier attempt left.
// False means a refusal has been written.
func (s *Server) cloneSnapshotIsReusable(
	ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, existing *apiv1.Snapshot, cloneName string,
) bool {
	// Interrupted-clone retry: the snapshot landed on a previous
	// attempt; reuse it so the restore sees the same point-in-time.
	//
	// Whether it still describes the SOURCE is a question for the
	// caller, and only when the clone is unfinished — a finished one is
	// a copy of the point-in-time and owes the source nothing.
	//
	// What is checked here is what the create branch checks and this one
	// used to skip: a snapshot recording no nodes places no replicas, so
	// the clone would answer 201 over an empty shell, which is the Bug
	// 114 shape the create branch refuses.
	if len(existing.Nodes) == 0 {
		writeCloneRefused(w, http.StatusConflict, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' refused: the leftover snapshot '" +
				existing.Name + "' records no nodes",
			Cause: "a snapshot with no nodes places no replicas, so the clone would " +
				"report success over an empty shell",
			Correc: "delete the snapshot '" + existing.Name + "' so the clone retakes it",
		})

		return false
	}

	return s.cloneSnapshotNodesAreUsable(ctx, w, src, existing, cloneName)
}

// leftoverFitsARetake refuses, before a new snapshot is taken, a leftover
// with no replica whose volumes the source as it is now would not fit: one
// smaller than the source's volume of that number, or one the source no
// longer has. Over a smaller volume the retake snapshotted the source and then
// failed hydrating with a bare already-exists, leaving a snapshot of a
// point-in-time the leftover was never restored from; over a volume the
// source no longer has it answered complete, keeping beside the retake a
// volume nothing restored. False means a refusal has been written.
func (s *Server) leftoverFitsARetake(
	ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, cloneName string,
	vds []apiv1.VolumeDefinition,
) bool {
	srcVDs, err := store.LiveVolumes(ctx, s.Store, src.Name)
	if err != nil {
		writeCloneStoreError(w, src.Name, cloneName, err)

		return false
	}

	sizes := make(map[int32]int64, len(srcVDs))
	for i := range srcVDs {
		sizes[srcVDs[i].VolumeNumber] = srcVDs[i].SizeKib
	}

	for i := range vds {
		size, ok := sizes[vds[i].VolumeNumber]
		if ok && vds[i].SizeKib >= size {
			continue
		}

		writeCloneRefused(w, http.StatusConflict, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: "clone target '" + cloneName + "' is an unfinished clone whose snapshot '" +
				cloneSnapshotName(cloneName) + "' is gone, and its volumes do not fit the source as it is now",
			Cause: "finishing it would snapshot the source again and hydrate around a volume of " +
				"another size, or one the source no longer has",
			Correc: "delete '" + cloneName + "' and clone again",
		})

		return false
	}

	return true
}

// readCloneSnapshot reads the clone's internal snapshot past the cache, as
// cloneResumeKeepsItsPointInTime reads it: a cache that had not seen the
// snapshot's delete yet took the reuse branch that read had just ruled out,
// and stamped replicas restoring from a snapshot that no longer exists. It
// returns (snapshot, found, ok); !ok means a refusal has been written.
func readCloneSnapshot(
	ctx context.Context, w http.ResponseWriter, st store.Store, srcName, cloneName string,
) (apiv1.Snapshot, bool, bool) {
	snapName := cloneSnapshotName(cloneName)

	snap, err := liveCloneSnapshot(ctx, st, srcName, snapName)
	if errors.Is(err, store.ErrNotFound) {
		return apiv1.Snapshot{}, false, true
	}

	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "' failed: read the internal snapshot '" +
				snapName + "': " + scrubImplDetails(err.Error()),
			Correc: "retry the clone",
		})

		return apiv1.Snapshot{}, false, false
	}

	return snap, true, true
}

// liveCloneSnapshot reads the clone's internal snapshot from the API server.
func liveCloneSnapshot(ctx context.Context, st store.Store, srcName, snapName string) (apiv1.Snapshot, error) {
	snaps, err := st.Snapshots().ListByDefinitionUncached(ctx, srcName)
	if err != nil {
		return apiv1.Snapshot{}, err //nolint:wrapcheck // surfaced via writeCloneRefused with the source named
	}

	for i := range snaps {
		if strings.EqualFold(snaps[i].Name, snapName) {
			return snaps[i], nil
		}
	}

	return apiv1.Snapshot{}, errors.Wrapf(store.ErrNotFound, "snapshot %q of %q", snapName, srcName)
}

// materializeClone restores the target from the clone's snapshot, guards the
// group it was written with, applies the prop edits and answers. resume says
// whether this request is finishing a leftover, which only the wording sees:
// whether the definition is this request's to roll back is what
// materializeRestoredRD found, not what the gate expected.
func (s *Server) materializeClone(
	ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, req *rdCloneRequest,
	snap *apiv1.Snapshot, resume bool,
) {
	restoreReq := &snapshotRestoreRequest{ToResource: req.Name}

	// Clone path: eagerPlace=true. `rd clone` is a one-shot CSI
	// operation with no follow-up autoplace, so the clone replicas must
	// materialise on the snapshot-holding nodes in the source pool here
	// (same backend by construction — Bug 038).
	made, err := s.materializeRestoredRD(ctx, src.Name, restoreReq, snap, true, &rdShapeOverrides{
		LayerStack:        req.LayerList,
		ResourceGroupName: req.ResourceGroup,
		OverrideProps:     req.OverrideProps,
		DeleteProps:       req.DeleteProps,
		DeleteNamespaces:  req.DeleteNamespaces,
	})
	if err != nil {
		// The same split the restore door draws: an answer about what was
		// already there keeps its typed status (and band, where it has one;
		// see materialiseRefusalKind), and only this request's own partial
		// work goes through the rollback.
		if kind, refused := materialiseRefusalKind(err); refused {
			status, rc := storeErrorRc(err, kind)
			writeCloneRefused(w, status, src.Name, req.Name, &rc)

			return
		}

		writeCloneRefused(w, http.StatusInternalServerError, src.Name, req.Name,
			s.failedMaterialiseRefusal(ctx, "clone of resource definition '"+src.Name+"' failed: "+err.Error(),
				"clone", req.Name, made.Placed, rollbackUnlessPlaced, nil, err))

		return
	}

	// Named from here on as it is stored: a definition adopted through a
	// collided create may carry another spelling than the request's.
	req.Name = made.Name

	uncheckedRG, ok := s.cloneParentRGSurvived(ctx, w, src, req.Name, made)
	if !ok {
		return
	}

	err = s.applyClonePropEdits(ctx, req)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, req.Name, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' created, but applying " +
				"override_props/delete_props failed: " + scrubImplDetails(err.Error()),
		})

		return
	}

	writeCloneStarted(w, src.Name, req.Name, cloneDoneMessage(resume, req.Name), uncheckedRG)
}

// cloneDoneMessage names whether the clone completed a leftover, so a retry
// reads as one in the caller's own output instead of looking like a first
// run. Mirrors restoreDoneMessage, which draws the same distinction on the
// endpoint this one shares its data plane with.
func cloneDoneMessage(resumed bool, cloneName string) string {
	if resumed {
		return "resource definition clone completed on retry: " + cloneName
	}

	return "resource definition cloned: " + cloneName
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

	ctx, cancel := detachedCompensation(ctx)
	defer cancel()

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

	if !made.createdHere() {
		writeCloneRefused(w, http.StatusConflict, src.Name, cloneName,
			adoptedOverDeletedGroupRefusal("clone", cloneName, stampedRG, correcRecreateGroupThenClone))

		return nil, false
	}

	rollbackErr := s.rollBackCompensating(ctx, cloneName, made.Placed, rollbackEvenIfFinished)
	if rollbackErr != nil {
		cause, correc := rollbackFailureAdviceOverDeletedGroup(rollbackErr, cloneName, stampedRG)

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
			"clone: " + scrubImplDetails(err.Error()),
		Cause: "the clone itself succeeded; only the safety net over it could not be " +
			"inspected, so a group deleted during the clone would not have been caught",
		Correc: "confirm resource group '" + rgName + "' still exists",
		ObjRefs: map[string]string{
			objRefRscDfn: cloneName,
			objRefRscGrp: rgName,
		},
	}
}

// cloneLayerStackIsHonourable refuses a clone whose requested layer stack is
// not the source's. False means a refusal has been written and the caller must
// stop.
//
// The clone data plane restores the source's bytes onto the target and brings
// the layer stack up over them, in that order. Every layer's bring-up writes
// to the device it is given:
//
//   - LUKS: luks.Format treats a device carrying no LUKS header as one to
//     format, so adding LUKS over a plaintext source formats away the data
//     that was just restored;
//   - DRBD: create-md runs with --force (pkg/drbd/drbdadm.go) over
//     `meta-disk internal` (pkg/drbd/conffile.go), stamping metadata across
//     the tail of the same bytes. The only gate before it is HasMD, which
//     looks for DRBD metadata and never for a filesystem signature.
//
// Both directions are refused, and the rule is the whole set rather than a
// list of the layers known to write today: a clone is a copy, so a target of a
// different shape does not hold the source's data whichever layer differs, and
// a layer added to LINSTOR later inherits the refusal instead of a gap.
// Ordering and spelling are not a difference — the stack's order follows from
// the kinds in it, and LINSTOR folds name case.
//
// Only the data-bearing path calls this. A clone of a VD-less source carries
// no bytes to lose, and choosing a different stack for the shell is what
// accepting layer_list is for.
func cloneLayerStackIsHonourable(w http.ResponseWriter, src *apiv1.ResourceDefinition, req *rdCloneRequest) bool {
	if len(req.LayerList) == 0 {
		return true
	}

	// An RD stored without an explicit stack is not a definition with no
	// layers — it is one that never said, and every other reader resolves
	// that to apiv1.DefaultLayerStack (stampRDLayerDataFromStack does it on
	// the read path). Resolving it here too is what keeps the gate off the
	// CSI hot path: linstor-csi sends [DRBD, STORAGE] on every clone, and
	// against a bare source an unresolved empty stack would read as "adds
	// DRBD, STORAGE" and refuse the clone this endpoint was just unbroken
	// for.
	have := store.EffectiveLayerStack(src.LayerStack)

	added, dropped := layerSetDifference(have, req.LayerList)
	if len(added) == 0 && len(dropped) == 0 {
		return true
	}

	writeCloneRefused(w, http.StatusBadRequest, src.Name, req.Name, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone of resource definition '" + src.Name + "': layer_list " +
			describeLayerDifference(added, dropped),
		Cause: "the clone restores the source's data first and brings the layer stack up " +
			"over it, so a layer added here writes its own metadata across the bytes it " +
			"just restored, and one dropped leaves the target reading data the missing " +
			"layer wrote",
		Correc: "clone with the source's own layer stack; a definition of a different " +
			"shape has to be created and copied into",
	})

	return false
}

// layerSetDifference reports which layers the requested stack adds to the
// source's and which it drops. Case-insensitive, because LINSTOR names fold
// and the two stacks reach here from different writers.
func layerSetDifference(have, want []string) ([]string, []string) {
	var added, dropped []string

	for _, layer := range want {
		if !apiv1.LayerInStack(have, layer) {
			added = append(added, strings.ToUpper(layer))
		}
	}

	for _, layer := range have {
		if !apiv1.LayerInStack(want, layer) {
			dropped = append(dropped, strings.ToUpper(layer))
		}
	}

	return added, dropped
}

// describeLayerDifference names what the caller asked to change, so the
// refusal says which layer rather than only that the stacks differ.
func describeLayerDifference(added, dropped []string) string {
	switch {
	case len(added) > 0 && len(dropped) > 0:
		return "adds " + strings.Join(added, ", ") + " and drops " +
			strings.Join(dropped, ", ") + " relative to the source"
	case len(added) > 0:
		return "adds " + strings.Join(added, ", ") + " to the source's stack"
	default:
		return "drops " + strings.Join(dropped, ", ") + " from the source's stack"
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

// writeCloneTargetUnreadable refuses a clone whose target could not be read.
// Read as absent, the create that follows collides with what is there and is
// adopted past every judgement of a leftover (finished, torn down, parented to
// a group that is gone, taken from a later point-in-time), so a replay of a
// finished clone re-placed it; the retry this answer asks for gets the
// judgement.
func writeCloneTargetUnreadable(w http.ResponseWriter, srcName, cloneName string, err error) {
	writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone target '" + cloneName + "' could not be read: " + scrubImplDetails(err.Error()),
		Correc:  "retry the clone",
	})
}

// cloneTargetState decides what a definition already under the clone's target
// name means. It returns (resume, stop): stop when an answer has already been
// written, resume when the caller should re-run the clone over the leftover.
//
// The restore marker is what makes a leftover recognisable, and it is NOT
// evidence that the clone finished. materializeRestoredRD stamps it with the
// definition, then hydrates the volumes and places the replicas, so a failure
// in either leaves the marker sitting on an empty shell. Answering 201 on the
// marker alone — which this did — turns the retry linstor-csi issues after a
// partial failure into a silent incomplete: CSI sees the volume as ready and
// nothing ever finishes it. Re-running is safe because every clone step
// tolerates an object a previous attempt already created, so it completes what
// is missing and leaves what is there. Same split restoreTargetState draws on
// the restore path, for the same reason.
//
// A leftover mid-tear-down is refused rather than resumed: the deletion is
// reaping the very objects completing the clone would be writing.
//
// Anything else under that name is a genuine collision and stays a refusal, in
// CloneStarted shape — a bare store AlreadyExists envelope would crash
// python-linstor's clone decode.
func (s *Server) cloneTargetState(
	ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, req *rdCloneRequest,
) (bool, bool) {
	srcName, cloneName := src.Name, req.Name

	existing, err := s.Store.ResourceDefinitions().Get(ctx, cloneName)
	if errors.Is(err, store.ErrNotFound) {
		return false, false
	}

	if err != nil {
		writeCloneTargetUnreadable(w, srcName, cloneName, err)

		return false, true
	}

	if !restoreMarkerMatches(existing.Props, srcName, cloneSnapshotName(cloneName)) {
		writeCloneRefused(w, http.StatusConflict, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone target '" + cloneName + "' already exists and is not a clone of '" + srcName + "'",
			Correc:  "pick a different clone name, or delete the existing resource definition first",
		})

		return false, true
	}

	if slices.Contains(existing.Flags, rdFlagDelete) {
		writeCloneRefused(w, http.StatusConflict, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone target '" + cloneName + "' is being deleted",
			Cause: "the leftover from an earlier attempt at this clone carries the DELETE " +
				"flag; completing it would race the tear-down reaping what it writes",
			Correc: "wait for the delete to finish, then re-issue the clone",
		})

		return false, true
	}

	// The requested shape is not compared here. replayOfFinishedClone runs
	// first on the same leftover and the same fields and refuses a shape the
	// leftover was not started with, finished or not, so a second comparison
	// could only repeat its answer.
	//
	// The group and the abandoned-rollback mark are: resuming over a leftover
	// whose rollback gave up, or one parented to a group that is gone, would
	// finish exactly the definition those guards exist to stop.
	if !s.cloneLeftoverIsUsable(ctx, w, srcName, &existing) {
		return false, true
	}

	return true, false
}

// cloneShellParentRGSurvived is the vol-less half of the same guard.
//
// handleRDClone splits on the source's volume count. The branch above copies a
// bare definition, carrying the source's resource group over verbatim, and had
// no check on either side of its write — so a `rg d` landing while it runs
// leaves exactly the definition the guard exists to prevent, on the cheaper of
// the two branches.
//
// The compensation is the one both other post-write doors run, on the same
// detached context. What this branch created is a bare definition, with no
// volumes hydrated and no replicas stamped, so a single Delete would undo what
// it wrote — but the rollback is needed exactly when the caller is already
// gone, and a Delete on the request's own context fails on its first call
// then. The shared rollback earns its other steps by not assuming the
// definition is bare, which is what an auto-tiebreaker stamped underneath it
// in the meantime would make false.
func (s *Server) cloneShellParentRGSurvived(
	ctx context.Context, w http.ResponseWriter, srcName, cloneName, stampedRG string,
) (*apiv1.APICallRc, bool) {
	if stampedRG == "" {
		return nil, true
	}

	ctx, cancel := detachedCompensation(ctx)
	defer cancel()

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
	err = s.rollBackCompensating(ctx, cloneName, nil, rollbackEvenIfFinished)
	if err != nil {
		// The advice the shared rollback's other doors give, for the step
		// that failed: a snapshot on the shell makes "delete it by hand" a
		// dead end, since `rd d` refuses a definition that has snapshots.
		cause, correc := rollbackFailureAdviceOverDeletedGroup(err, cloneName, stampedRG)

		writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "': " +
				rollbackFailedMessage(cloneName, stampedRG, err),
			Cause:  cause,
			Correc: correc,
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
// So a leftover is answered for, replayed as finished or resumed, only when
// its parent group still resolves and no rollback gave up over it. An
// inconclusive read of either refuses: unlike the post-write check, which
// guards a clone this request has just made, this gate would otherwise report
// a clone nobody verified, and the CSI retry makes a refusal cheap. Whether
// the leftover is whole is assessMarkedClone's question, asked by the caller:
// a finished one is replayed, an unfinished one resumed, and one whose every
// replica is being torn down refused.
//
// The clone is named by the leftover's stored spelling, not the request's:
// LINSTOR folds name case, and the uncached read the mark gate takes is keyed
// by what was stored.
func (s *Server) cloneLeftoverIsUsable(
	ctx context.Context, w http.ResponseWriter, srcName string, existing *apiv1.ResourceDefinition,
) bool {
	status, refusal := s.cloneLeftoverRefusal(ctx, existing)
	if refusal == nil {
		return true
	}

	writeCloneRefused(w, status, srcName, existing.Name, refusal)

	return false
}

// cloneLeftoverRefusal is cloneLeftoverIsUsable's judgement without the
// answer, so the status poll asks exactly what the POST asks. Nil means the
// leftover may be answered for.
func (s *Server) cloneLeftoverRefusal(
	ctx context.Context, existing *apiv1.ResourceDefinition,
) (int, *apiv1.APICallRc) {
	stampedRG, cloneName := existing.ResourceGroupName, existing.Name

	if stampedRG == "" {
		return s.abandonedRollbackRefusal(ctx, "clone", cloneName)
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
		return http.StatusInternalServerError, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone target '" + cloneName + "' exists, but its parent resource group '" +
				stampedRG + "' could not be read: " + scrubImplDetails(err.Error()),
			Cause: "the replay only answers for a clone whose parent group resolves, and this " +
				"read failed rather than saying the group is gone",
			Correc: "retry the clone",
		}
	}

	if survived {
		return s.abandonedRollbackRefusal(ctx, "clone", cloneName)
	}

	return http.StatusConflict, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone target '" + cloneName + "' exists but is parented to resource group '" +
			stampedRG + "', which no longer exists",
		Cause: "an earlier attempt at this clone could not be rolled back after its parent " +
			"group was deleted, so the definition was left in place rather than orphaning " +
			"its replicas; answering this retry as an idempotent replay would report a " +
			"clone that is not usable",
		Correc: "delete '" + cloneName + "' by hand, re-create resource group '" + stampedRG +
			"', then clone again",
	}
}

// clearRollbackMarkCommand is the command that takes a rollback's mark off a
// definition the rollback left whole; see store.ServerOwnedPropEditByOperator.
// python-linstor has no delete-property: a set-property with no value is its
// delete, and the blockstor CLI reads it the same way.
func clearRollbackMarkCommand(rdName string) string {
	return "run `linstor resource-definition set-property " + rdName + " " + rollbackAbandonedKey +
		"` with no value"
}

// abandonedRollbackRefusal is the abandoned-rollback gate both resuming doors
// share: the clone and the restore write the mark through the same failed
// materialisation, so both have to read it back before resuming, or one door
// refuses the leftover its twin finishes. Nil means nothing to refuse.
//
// It comes last on purpose. Every earlier refusal is more precise about the
// same leftover (still being torn down, parented to a group that is gone), and
// this one only has to catch what they all let through: a leftover a cascade
// was cut short over, see rollbackAbandonedKey.
//
// The definition is read again here, from the API server. The props the gate
// started from are one cache-served read taken before the gates ahead of this
// one waited out their own cache lag, and the mark is written through the API
// server by a rollback that may have given up moments before the replay
// arrived, which is exactly when the cache has not caught up with it. A read
// that fails refuses: the replay cannot vouch for a definition it could not
// check.
func (s *Server) abandonedRollbackRefusal(
	ctx context.Context, operation, rdName string,
) (int, *apiv1.APICallRc) {
	existing, err := s.Store.ResourceDefinitions().GetUncached(ctx, rdName)
	if errors.Is(err, store.ErrNotFound) {
		// The authoritative read says nothing stands under the name: the
		// cached read that found the leftover was the stale one, and there is
		// no mark to refuse over.
		return 0, nil
	}

	if err != nil {
		return http.StatusInternalServerError, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: operation + " target '" + rdName + "' exists, but reading it back to check " +
				"for an abandoned rollback failed: " + scrubImplDetails(err.Error()),
			Cause: "an earlier attempt whose rollback gave up leaves a definition that looks " +
				"whole, and only the mark it carries tells the two apart",
			Correc: "retry the " + operation,
		}
	}

	// The DELETE flag is read here too, past the cache: the gates ahead of
	// this one read it from a cache that may not have seen a definition
	// deleted directly, and a definition being deleted is not one to answer
	// for, whatever its replicas say. It comes before the rollback mark on
	// purpose, as the cached DELETE gate in cloneTargetState does: a
	// definition a rollback has flagged for deletion is waited out, not given
	// the advice for a rollback that stopped.
	if slices.Contains(existing.Flags, rdFlagDelete) {
		return http.StatusConflict, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: operation + " target '" + rdName + "' is being deleted",
			Correc:  "wait for the delete to finish, then " + operation + " again",
		}
	}

	spelled := existing.Props[rollbackAbandonedKey]
	if spelled == "" {
		return 0, nil
	}

	// A rollback that has not reported a step is one still running, or one
	// whose process stopped mid-way. Both are refused, but the first ends on
	// its own, so the advice says so before it says to delete by hand; the
	// CLI door words it the same way (store.JudgeRestoreLeftover).
	if spelled == rollbackInProgress {
		return http.StatusConflict, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: operation + " target '" + rdName + "' is being rolled back by the attempt " +
				"that created it",
			Cause: "an earlier attempt at this " + operation + " started rolling the definition " +
				"back and has not reported how it ended: it is still running, or it stopped mid-way",
			Correc: "retry the " + operation + " once that rollback has finished, which takes under a " +
				"minute; if the mark is still there a minute after the failure, " + clearRollbackMarkCommand(rdName) +
				" to keep '" + rdName + "' as it stands, or delete it by hand. Cleared while the rollback " +
				"still runs, it stops that rollback before it deletes anything",
		}
	}

	step, known := rollbackStepByName(spelled)
	cause, correc := rollbackStepAdvice(step, known, rdName)

	return http.StatusConflict, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: operation + " target '" + rdName + "' is what an earlier attempt left when its " +
			"rollback gave up (" + spelled + ")",
		Cause: "an earlier attempt at this " + operation + " failed and could not be rolled back, " +
			"so the definition may hold less than the " + operation + " intended; " + cause,
		Correc: correc,
	}
}

// ensureCloneSnapshot takes (or reuses) the internal snapshot backing
// a data-plane clone. Returns (snap, true) when the caller may
// proceed; (nil, false) when a refusal envelope was already written.
// Guards mirror handleSnapshotCreate's: the source must have at
// least one ACTIVE diskful replica, every replica's node must be
// online, and every backing pool must be snapshot-capable (thin LVM
// / ZFS / FILE_THIN) — the clone data plane IS a snapshot restore,
// so a source that cannot be snapshotted cannot be cloned.
func (s *Server) ensureCloneSnapshot(
	w http.ResponseWriter, r *http.Request, src *apiv1.ResourceDefinition, cloneName string,
) (*apiv1.Snapshot, bool, bool) {
	ctx := r.Context()
	snapName := cloneSnapshotName(cloneName)

	existing, found, ok := readCloneSnapshot(ctx, w, s.Store, src.Name, cloneName)
	if !ok {
		return nil, false, false
	}

	if found {
		if !s.cloneSnapshotIsReusable(ctx, w, src, &existing, cloneName) {
			return nil, false, false
		}

		return &existing, true, true
	}

	snap := apiv1.Snapshot{Name: snapName, ResourceName: src.Name}

	err := s.hydrateSnapshotFromRD(ctx, &snap, src.Name)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' failed: " + scrubImplDetails(err.Error()),
		})

		return nil, false, false
	}

	if !s.cloneSnapshotPreconditionsHold(ctx, w, src, &snap, cloneName) {
		return nil, false, false
	}

	stampCloneSnapshotOwner(&snap, cloneName)

	snap.Snapshots = makeSnapshotPerNode(snapName, snap.Nodes, snap.VolumeDefinitions)

	err = s.Store.Snapshots().Create(ctx, &snap)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name +
				"' failed: internal snapshot create: " + scrubImplDetails(err.Error()),
		})

		return nil, false, false
	}

	return &snap, false, true
}

// stampCloneSnapshotOwner marks the snapshot as the clone's own, so deleting
// the clone may reap it. A name cannot carry that: an operator can call a
// snapshot `clone-<target>` and restore it under that target, and the marker a
// restore writes is the one a clone writes. The map is copied first, since the
// hydration hands over the source definition's own.
func stampCloneSnapshotOwner(snap *apiv1.Snapshot, cloneName string) {
	snap.Props = maps.Clone(snap.Props)
	if snap.Props == nil {
		snap.Props = map[string]string{}
	}

	snap.Props[store.CloneSnapshotOwnerProp] = cloneName
}

// cloneSourceIsNotBeingDeleted mirrors the snapshot-create Bug 180 gate: a
// source RD mid-tear-down would reap the internal snapshot and the clone
// marker from under the satellite's restore.
func (s *Server) cloneSourceIsNotBeingDeleted(
	ctx context.Context, w http.ResponseWriter, src *apiv1.ResourceDefinition, cloneName string,
) bool {
	if !rdHasDeleteFlag(ctx, s, src.Name) {
		return true
	}

	writeCloneRefused(w, http.StatusConflict, src.Name, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone of resource definition '" + src.Name + "' refused: the source is being deleted",
		Cause:   "the source RD carries the DELETE flag; its backing data is being torn down",
		Correc:  "clone before deleting the source, or restore from a snapshot taken earlier",
	})

	return false
}

// replayOfFinishedClone answers a retry of a clone that already completed,
// before anything on this path reads the live source. It returns
// (replayed, halt): replayed when the success has been written, halt when a
// refusal has.
//
// A replay of a clone that already completed has to survive anything that
// happened to the source since: it is a copy of a point-in-time and owes
// the source nothing. Every question asked ahead of it is a way to refuse
// one — the layer stack, the DELETE flag, and the shape comparison, whose
// "shape" is derived from the live source, so an ordinary
// `rd modify --resource-group` on the source turned every later replay
// into a 409 with a correction the caller it is aimed at cannot follow:
// linstor-csi sends the same body every time.
//
// Taking the internal snapshot is worse than a refusal. When
// `clone-<dst>` has been deleted, the create branch snapshots the CURRENT
// source before anyone asks whether the clone is done, so a pure replay
// mutates cluster state and, once the source has grown, leaves a fresh
// snapshot that diverges from the finished target — and refuses the
// replay from then on, permanently, with a correction that destroys a
// clone holding live data.
//
// The marker and the DELETE flag are read here only to decide whether this is
// even the right question. A leftover that is somebody else's definition, or
// one being torn down, is left to cloneTargetState, which owns those refusals
// and their wording.
func (s *Server) replayOfFinishedClone(
	ctx context.Context, w http.ResponseWriter, srcName string, req *rdCloneRequest,
) (bool, bool) {
	cloneName := req.Name

	existing, err := s.Store.ResourceDefinitions().Get(ctx, cloneName)
	if errors.Is(err, store.ErrNotFound) {
		return false, false
	}

	if err != nil {
		writeCloneTargetUnreadable(w, srcName, cloneName, err)

		return false, true
	}

	if !restoreMarkerMatches(existing.Props, srcName, cloneSnapshotName(cloneName)) ||
		slices.Contains(existing.Flags, rdFlagDelete) {
		return false, false
	}

	// Hoisting this check above everything that reads the live source must
	// not also hoist it above everything that reads the REQUEST. A replay
	// naming a resource group or a layer stack the finished clone does not
	// have would otherwise be told the clone completed and handed the other
	// shape — a caller who named LUKS gets plaintext. The comparison is
	// against the leftover, never the source. linstor-csi names both fields
	// on every clone, taken from the StorageClass, so its replay names the
	// shape the first request stamped on the leftover and passes.
	//
	// It runs before anything established whether the leftover is finished,
	// so its wording is the resume's: the leftover was started with a shape,
	// whether or not it got further.
	if differs := requestedShapeDiffers(&existing, req.ResourceGroup, req.LayerList); differs != "" {
		writeCloneRefused(w, http.StatusConflict, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone target '" + cloneName + "' was started with " + differs,
			Cause: "a retry keeps the definition an earlier attempt created, so the shape " +
				"this request asks for would be validated and then ignored",
			Correc: "retry with the shape the clone was started with, or delete '" +
				cloneName + "' and clone again",
		})

		return false, true
	}

	finished, halt := s.cloneLeftoverIsFinished(ctx, w, srcName, cloneName)
	if halt || !finished {
		return false, halt
	}

	if !s.cloneLeftoverIsUsable(ctx, w, srcName, &existing) {
		return false, true
	}

	if !s.answerFinishedClone(ctx, w, srcName, cloneName, existing.Name, req) {
		return false, true
	}

	return true, false
}

// answerFinishedClone answers the replay of a finished clone: it takes the
// adoption mark, applies the request's prop edits, and reports 201. False
// means a refusal has been written instead.
func (s *Server) answerFinishedClone(
	ctx context.Context, w http.ResponseWriter, srcName, cloneName, storedName string, req *rdCloneRequest,
) bool {
	if status, refusal := s.finishedLeftoverRefusal(ctx, "clone", storedName); refusal != nil {
		writeCloneRefused(w, status, srcName, cloneName, refusal)

		return false
	}

	// The prop edits still have to land. A replay carrying override_props /
	// delete_props / delete_namespaces would otherwise be answered 201 with
	// them dropped, which is the accept-and-drop this endpoint refuses
	// external_name and volume_passphrases to avoid — and it is reachable
	// without any caller changing their mind, because the first attempt can
	// fail in applyClonePropEdits AFTER the volumes are already there. The
	// edits are a patch, so re-applying what already landed is what makes the
	// replay idempotent rather than a second write.
	err := s.applyClonePropEdits(ctx, req)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "' is complete, but " +
				"applying override_props/delete_props failed: " + scrubImplDetails(err.Error()),
		})

		return false
	}

	writeCloneStarted(w, srcName, cloneName, cloneDoneMessage(true, cloneName), nil)

	return true
}

// cloneProgress is where a target carrying a clone's or a restore's marker
// stands; see store.LeftoverProgress. The replay and the status poll both ask
// it, and they have to get the same answer: a POST told 201 followed by a GET
// that is not COMPLETE for the same clone, one request apart, is a successful
// clone the driver then waits on.
type cloneProgress = store.LeftoverProgress

const (
	cloneUnfinished  = store.LeftoverUnfinished
	cloneFinished    = store.LeftoverFinished
	cloneForeign     = store.LeftoverForeign
	cloneTearingDown = store.LeftoverTearingDown
)

// assessMarkedClone classifies a target that carries this clone's marker. It
// reads and never writes, so the poll can call it as freely as the replay.
//
// Volumes alone do not decide it. materializeRestoredRD hydrates the volumes
// and THEN stamps the replicas, so a shell holding every volume and no replica
// is an ordinary intermediate state, and one whose data exists nowhere but the
// snapshot.
//
// Where the replicas are is not part of the question. A clone that has one
// replica holds its data, and which nodes carry it afterwards is placement,
// which ordinary operations change: evacuating a node moves a replica off a
// node the snapshot recorded, and scaling down removes one. Asking whether
// every snapshot node still carries a replica read both as unfinished, and the
// resume then re-stamped a replica on the node the operator had just emptied,
// restored from the point-in-time while the surviving replica had moved on
// with live writes: two replicas of one definition with different content. A
// clone left short of its placement by a first attempt that died between two
// stamps is the same state seen from outside, and topping it up is the
// placement reconciliation's job, not the clone's: linstor-csi v1.10.1 calls
// reconcileResourcePlacement right after its COMPLETE loop in
// pkg/client/linstor.go.
//
// Nothing here reads the live source. A finished clone is a copy of a
// point-in-time, so it is judged against the snapshot it was restored from
// while that exists, and against itself once it does not: an absent snapshot
// cannot unmake a clone that has its volumes and a replica, and re-taking one
// to answer the question is the write a replay must not make. "Could not read
// the snapshot" is not "the snapshot is gone", and only the second takes the
// face-value branch.
//
// The volumes and replicas are read past the cache, the status poll included,
// at two API-server reads per poll: a poll that read a stale cache could report
// COMPLETE over a replica already being torn down, and linstor-csi binds the
// volume on that answer without asking again.
func assessMarkedClone(
	ctx context.Context, st store.Store, srcName, cloneName string,
) (cloneProgress, error) {
	targetVDs, err := store.LiveVolumes(ctx, st, cloneName)
	if err != nil {
		return cloneUnfinished, errors.Wrapf(err, "list the volumes of %q", cloneName)
	}

	if len(targetVDs) == 0 {
		return assessLeftover(ctx, st, cloneName, nil, nil, true)
	}

	snap, err := st.Snapshots().Get(ctx, srcName, cloneSnapshotName(cloneName))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return cloneUnfinished, errors.Wrapf(err, "read the snapshot behind %q", cloneName)
	}

	var captured *apiv1.Snapshot
	if err == nil {
		captured = &snap
	}

	return assessLeftover(ctx, st, cloneName, targetVDs, captured, true)
}

// assessLeftover is store.AssessLeftover, the one judgement every door that
// resumes a marker-bearing leftover shares: the clone through
// assessMarkedClone, the REST restore through restoreLeftoverIsFinished, and
// the CLI restore.
func assessLeftover(
	ctx context.Context, st store.Store, targetName string,
	targetVDs []apiv1.VolumeDefinition, snap *apiv1.Snapshot, needReplica bool,
) (cloneProgress, error) {
	return store.AssessLeftover(ctx, st, targetName, targetVDs, snap, needReplica) //nolint:wrapcheck // wrapped by store with the target
}

// countReplicas is store.CountReplicas.
func countReplicas(ctx context.Context, st store.Store, rdName string) (store.ReplicaCount, error) {
	return store.CountReplicas(ctx, st, rdName) //nolint:wrapcheck // wrapped by store with the definition
}

// cloneLeftoverIsFinished answers assessMarkedClone for the replay, writing the
// refusal it implies. It returns (finished, stop).
func (s *Server) cloneLeftoverIsFinished(
	ctx context.Context, w http.ResponseWriter, srcName, cloneName string,
) (bool, bool) {
	progress, err := assessMarkedClone(ctx, s.Store, srcName, cloneName)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "' failed: " + scrubImplDetails(err.Error()),
		})

		return false, true
	}

	switch progress {
	case cloneFinished:
		return true, false
	case cloneForeign:
		writeCloneRefused(w, http.StatusConflict, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + srcName + "' refused: '" + cloneName +
				"' holds a volume this clone would not have written",
			Cause: "the definition under that name carries volumes that are not the ones " +
				"this clone restores, and hydrating skips what is already there, so the " +
				"retry would leave them and report the clone complete",
			Correc: "delete '" + cloneName + "' and clone again, or clone under a different name",
		})

		return false, true
	case cloneTearingDown:
		writeCloneRefused(w, http.StatusConflict, srcName, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone target '" + cloneName + "' exists but is still being torn down",
			Cause: "the definition under that name carries this clone's marker, and every " +
				"replica of it is already accepted for deletion; answering this retry as an " +
				"idempotent replay would bind a volume to a clone that exists on no node",
			Correc: "wait until the replicas of '" + cloneName + "' are gone, then clone again",
		})

		return false, true
	case cloneUnfinished:
	}

	return false, false
}

// cloneSnapshotIsCurrent refuses to resume a clone over a leftover snapshot
// that no longer describes the source. False means a refusal has been written.
//
// "Found" is not "still right". The first attempt takes `clone-<target>` and
// dies; the source is resized, or gains a volume; the retry hydrates the
// target from the stale snapshot and answers 201, and the clone-status poll
// then reports COMPLETE because the volume counts agree. The caller is handed
// a clone at the old shape with nothing saying so. This only became reachable
// when the retry started resuming instead of reporting the leftover done.
//
// Refusing rather than retaking is the call `blockstor rd clone` already makes
// (internal/cli/definition.go): the snapshot may be the only copy of
// something, and deleting it is the operator's decision, not this endpoint's.
func (s *Server) cloneSnapshotIsCurrent(
	ctx context.Context, w http.ResponseWriter,
	src *apiv1.ResourceDefinition, snap *apiv1.Snapshot, cloneName string,
) bool {
	current, err := store.LiveVolumes(ctx, s.Store, src.Name)
	if err != nil {
		writeCloneRefused(w, http.StatusInternalServerError, src.Name, cloneName, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "clone of resource definition '" + src.Name + "' failed: " + scrubImplDetails(err.Error()),
		})

		return false
	}

	divergence := snapshotDivergence(src.Name, snap, current)
	if divergence == "" {
		return true
	}

	// The correction has to be one that works. When the leftover already holds
	// volumes hydrated from this snapshot, deleting only the snapshot makes the
	// next attempt retake it at the new size and collide with those volumes —
	// a bare 500 with no correction at all, over a fresh snapshot of the live
	// source now claiming to be this clone's origin. Only then does the retry
	// reach the refusal that names the target. Say so on the first answer.
	correc := "delete the snapshot '" + snap.Name + "' so the clone retakes it, " +
		"or clone under a different name"

	hydrated, listErr := store.LiveVolumes(ctx, s.Store, cloneName)
	if listErr == nil && len(hydrated) > 0 {
		correc = "delete '" + cloneName + "' and the snapshot '" + snap.Name +
			"', then clone again, or clone under a different name"
	}

	writeCloneRefused(w, http.StatusConflict, src.Name, cloneName, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "clone of resource definition '" + src.Name + "' refused: " + divergence,
		Cause: "an earlier attempt at this clone left the snapshot '" + snap.Name +
			"' behind and the source has changed since; resuming from it would " +
			"materialise the clone at the old shape and report it complete",
		Correc: correc,
	})

	return false
}

// snapshotDivergence describes how a snapshot has fallen behind the volumes it
// was taken of, or "" while it still matches.
//
// Sizes are compared keyed by volume number, since neither list promises an
// order — and a resize leaves the count alone, so counting volumes answers
// only half the question.
func snapshotDivergence(rdName string, snap *apiv1.Snapshot, current []apiv1.VolumeDefinition) string {
	if len(current) != len(snap.VolumeDefinitions) {
		return snap.Name + " covers " + strconv.Itoa(len(snap.VolumeDefinitions)) +
			" volume(s) but " + rdName + " now has " + strconv.Itoa(len(current))
	}

	captured := make(map[int32]int64, len(snap.VolumeDefinitions))
	for _, vol := range snap.VolumeDefinitions {
		captured[vol.VolumeNumber] = vol.SizeKib
	}

	for i := range current {
		was, ok := captured[current[i].VolumeNumber]
		if !ok {
			return snap.Name + " does not cover volume " +
				strconv.FormatInt(int64(current[i].VolumeNumber), 10) + " of " + rdName
		}

		if was != current[i].SizeKib {
			return snap.Name + " captured volume " +
				strconv.FormatInt(int64(current[i].VolumeNumber), 10) + " at " +
				strconv.FormatInt(was, 10) + " KiB but " + rdName + " is now " +
				strconv.FormatInt(current[i].SizeKib, 10) + " KiB"
		}
	}

	return ""
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

	return s.cloneSnapshotNodesAreUsable(ctx, w, src, snap, cloneName)
}

// cloneSnapshotNodesAreUsable is the half of the preconditions that a REUSED
// snapshot has to satisfy as well as a freshly taken one.
//
// The nodes recorded on the snapshot are where the restore places the clone's
// replicas, so an offline node or a pool that cannot hold a snapshot is the
// same problem whichever attempt took it. Running these only on the branch
// that creates the snapshot gave one cluster state two answers: the first
// clone refused with 503, the retry over the leftover snapshot answered 201
// and stamped a replica on a node whose satellite cannot act on it.
func (s *Server) cloneSnapshotNodesAreUsable(
	ctx context.Context, w http.ResponseWriter,
	src *apiv1.ResourceDefinition, snap *apiv1.Snapshot, cloneName string,
) bool {
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
			Message: "clone of resource definition '" + src.Name + "' failed: " + scrubImplDetails(err.Error()),
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
	if len(req.OverrideProps) == 0 && len(req.DeleteProps) == 0 && len(req.DeleteNamespaces) == 0 {
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

			deletePropNamespaces(rd.Props, req.DeleteNamespaces)

			return nil
		})
}

// cloneEnvelope carries, for one clone request, which shape its refusals are
// written in.
//
// The two clients that clone decode a refusal in shapes that exclude each
// other. python-linstor's `resource_dfn_clone` decodes every answer, success
// and error alike, into CloneStarted, and a bare `[]ApiCallRc` crashes it with
// `AttributeError: 'list' object has no attribute 'get'` before the error line
// reaches the operator. golinstor's do() (client/client.go) decodes every
// non-2xx answer other than a 404 as `[]ApiCallRc`, so the object reached
// linstor-csi as `json: cannot unmarshal object into Go value of type
// client.ApiCallError`, and the cause and correction never reached anyone.
//
// python-linstor is the one client that needs the object, and it names itself
// on every request: `PythonLinstor/<version> (API<min>)` (linstorapi.py,
// python-linstor 1.27.1). Its refusals keep the object. Every other caller
// gets the array the rest of the API answers with: linstor-csi, which sends
// `linstor-csi/<version>`, and any other golinstor client, which may send no
// name of its own at all and decodes errors the same way. A 404 carries
// nothing to a golinstor client either way: it maps every 404 to its bare
// NotFoundError without reading the body.
//
// The trade-off is deliberate. A third-party client written against
// upstream's object shape, and naming itself something other than
// python-linstor, now gets the array. None is known; every client this API is
// built for is one of the two above, and the array is the shape the rest of
// the API answers errors with, so it is the default that fails least.
type cloneEnvelope struct {
	http.ResponseWriter

	array bool
}

// pythonLinstorUserAgentPrefix is how python-linstor names itself to the API.
const pythonLinstorUserAgentPrefix = "PythonLinstor/"

func withCloneEnvelope(w http.ResponseWriter, r *http.Request) http.ResponseWriter {
	return &cloneEnvelope{
		ResponseWriter: w,
		array:          !strings.HasPrefix(r.UserAgent(), pythonLinstorUserAgentPrefix),
	}
}

// writeCloneRefused stamps a clone refusal in the envelope the caller decodes;
// see cloneEnvelope.
func writeCloneRefused(w http.ResponseWriter, status int, srcName, cloneName string, callRc *apiv1.APICallRc) {
	if callRc.ObjRefs == nil {
		callRc.ObjRefs = map[string]string{objRefRscDfn: srcName}
	}

	if envelope, ok := w.(*cloneEnvelope); ok && envelope.array {
		writeJSON(w, status, []apiv1.APICallRc{*callRc})

		return
	}

	writeJSON(w, status, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + srcName + "/clone/" + cloneName,
		SourceName: srcName,
		CloneName:  cloneName,
		Messages:   &[]apiv1.APICallRc{*callRc},
	})
}

// writeCloneStoreError is writeStoreError in the envelope this endpoint
// answers in. A bare []ApiCallRc crashes python-linstor's clone decode, so a
// typo in the source name, or a replay of a volume-less clone, lost its message
// to an AttributeError.
func writeCloneStoreError(w http.ResponseWriter, srcName, cloneName string, err error) {
	status, callRc := storeErrorRc(err, storeKindResourceDfn)
	writeCloneRefused(w, status, srcName, cloneName, &callRc)
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
	// The same gate the data path has. A volume-less source inside its delete
	// window has nothing to lose, but the shell below is a copy of it, and a
	// definition born carrying DELETE is refused by every snapshot, restore and
	// clone that later names it, each saying to wait for a delete that never
	// comes.
	if !s.cloneSourceIsNotBeingDeleted(r.Context(), w, src, req.Name) {
		return
	}

	clone := *src
	clone.Name = req.Name
	clone.UUID = ""
	// Flags are the source's lifecycle, not part of its shape: a shell that
	// inherited them would carry a delete, or any later flag, it never had.
	clone.Flags = nil

	// The caller's own shape wins over the source's, on both clone paths.
	// Without one the shell is stamped with the source's data-plane stack, as
	// the data path stamps its target: left empty under a target group of
	// another stack, the control plane would judge it by that group while the
	// satellite brings it up as the default, once volumes are added.
	clone.LayerStack = store.EffectiveLayerStack(src.LayerStack)
	if len(req.LayerList) > 0 {
		clone.LayerStack = req.LayerList
	}

	if req.ResourceGroup != "" {
		clone.ResourceGroupName = req.ResourceGroup
	}

	if src.Props != nil || len(req.OverrideProps) > 0 {
		clone.Props = make(map[string]string, len(src.Props)+len(req.OverrideProps))
		maps.Copy(clone.Props, store.TravellingProps(src.Props))
	}

	maps.Copy(clone.Props, req.OverrideProps)

	for _, k := range req.DeleteProps {
		delete(clone.Props, k)
	}

	deletePropNamespaces(clone.Props, req.DeleteNamespaces)

	err := s.Store.ResourceDefinitions().Create(r.Context(), &clone)
	if err != nil {
		writeCloneStoreError(w, src.Name, clone.Name, err)

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

// handleRDCloneStatus answers golinstor's `CloneStatus` poll, grounded
// in actual store state (Bug 114). A target carrying this clone's marker
// is answered by answerMarkedCloneStatus; one without it by
// computeCloneStatus's volume-count comparison.
//
// Path: GET /v1/resource-definitions/{src}/clone/{target}.
// A 404 on the target signals "no finished clone under that name",
// which linstor-csi acts on by issuing the clone, rather than an
// infinite poll loop.
func (s *Server) handleRDCloneStatus(w http.ResponseWriter, r *http.Request) {
	srcName := r.PathValue("rd")
	targetName := r.PathValue("target")

	target, err := s.Store.ResourceDefinitions().Get(r.Context(), targetName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if restoreMarkerMatches(target.Props, srcName, cloneSnapshotName(targetName)) {
		s.answerMarkedCloneStatus(r.Context(), w, srcName, &target)

		return
	}

	writeJSON(w, http.StatusOK, client.ResourceDefinitionCloneStatus{
		Status: computeCloneStatus(r.Context(), s.Store, srcName, targetName),
	})
}

// answerMarkedCloneStatus answers the poll for a target that went through the
// snapshot data plane. It is judged the way the replay judges it, against its
// own point-in-time and never against the live source: comparing the live
// source's volume count told FAILED to the poll that follows a 201 replay the
// moment anyone added a volume to the source, which is legal and routine.
//
// Only a finished clone is answered with a status. Everything else is answered
// in the one shape its caller can act on. linstor-csi POSTs the clone only
// when this GET is a 404, and then polls until COMPLETE with no FAILED branch
// and no second POST (pkg/client/linstor.go, v1.10.1), so FAILED over a
// leftover the POST knows how to resume left the driver waiting on it forever,
// and so did CLONING. A 404 sends it back to the POST, which resumes an
// unfinished leftover or refuses a foreign one with a cause and a correction,
// written in the array linstor-csi decodes so they reach the PVC's events (see
// cloneEnvelope). A read failure is a 500 for the same reason:
// the driver returns it from CreateVolume and retries, where FAILED would bind
// nothing and end nothing.
//
// COMPLETE over a clone whose placement is short of the snapshot's nodes is
// safe: the driver reconciles placement right after it.
//
// A definition carrying the DELETE flag is torn down whatever its replicas say,
// the way the POST's cloneTargetState refuses it: one deleted directly rather
// than through `rd d`, which cascades the replicas first, still lists a live
// replica, and its replicas alone would read it complete.
//
// A finished one is then put through the gate the POST's replay asks before it
// answers 201 (cloneLeftoverRefusal): a parent group that is gone, a rollback
// in progress or one that gave up. The driver binds on COMPLETE without ever
// reaching the POST, so a COMPLETE over a clone the RG-deleted rollback is
// deleting binds a volume that is about to be gone. This narrows that race and
// does not close it: a poll that reads the group from a cache that has not
// seen its delete, before the rollback's mark lands, still answers COMPLETE,
// and nothing the poll does is visible to the rollback. It is the same window
// as any `rg d` racing a clone.
func (s *Server) answerMarkedCloneStatus(
	ctx context.Context, w http.ResponseWriter, srcName string, target *apiv1.ResourceDefinition,
) {
	targetName := target.Name

	progress := cloneTearingDown

	var err error
	if !slices.Contains(target.Flags, rdFlagDelete) {
		progress, err = assessMarkedClone(ctx, s.Store, srcName, targetName)
	}

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, []apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "clone status of '" + targetName + "' from '" + srcName + "' could not be read: " + scrubImplDetails(err.Error()),
		}})

		return
	}

	cause := "an earlier attempt at this clone stopped before it finished"

	switch progress {
	case cloneFinished:
		status, refusal := s.cloneLeftoverRefusal(ctx, target)
		if refusal == nil {
			writeJSON(w, http.StatusOK, client.ResourceDefinitionCloneStatus{Status: clonestatus.Complete})

			return
		}

		if status == http.StatusInternalServerError {
			writeJSON(w, status, []apiv1.APICallRc{*refusal})

			return
		}

		cause = refusal.Message
	case cloneForeign:
		cause = "the definition under that name holds a volume this clone would not have written"
	case cloneTearingDown:
		cause = "the definition under that name, or every replica of it, is being torn down"
	case cloneUnfinished:
	}

	writeJSON(w, http.StatusNotFound, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailNotFoundRscDfn,
		Message: "no finished clone '" + targetName + "' of '" + srcName + "'",
		Cause:   cause,
		Correc:  "issue the clone again: it resumes what the earlier attempt left, or says why it cannot",
	}})
}

// computeCloneStatus resolves COMPLETE vs FAILED for a clone pair whose target
// carries no marker, a clone made before the snapshot data plane existed, by
// comparing source-vs-target VolumeDefinition counts. Bug 114: an empty target
// paired with a non-empty source is structurally incomplete — golinstor's poll
// loop must see FAILED so it stops waiting on data that will never arrive.
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
	writeCloneRefused(w, http.StatusNotImplemented, srcName, cloneName, &apiv1.APICallRc{
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
