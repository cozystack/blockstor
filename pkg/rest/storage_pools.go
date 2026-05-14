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
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerStoragePools wires endpoints serving golinstor's StoragePool calls.
//
// linstor-csi calls /v1/view/storage-pools in its node-registration loop and
// /v1/nodes/{node}/storage-pools[/{pool}] for per-node operations. The POST
// path on /v1/nodes/{node}/storage-pools is hit by satellite Hello/heartbeat
// loops (and piraeus's operator) to register a pool — without it the
// satellite retry loop spins forever against a 405 (Bug 31).
func (s *Server) registerStoragePools(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/storage-pools", s.requireStore(s.handleStoragePoolsView))
	mux.HandleFunc("GET /v1/nodes/{node}/storage-pools", s.requireStore(s.handleNodeStoragePoolsList))
	mux.HandleFunc("POST /v1/nodes/{node}/storage-pools", s.requireStore(s.handleNodeStoragePoolCreate))
	mux.HandleFunc("GET /v1/nodes/{node}/storage-pools/{pool}", s.requireStore(s.handleNodeStoragePoolGet))
	mux.HandleFunc("DELETE /v1/nodes/{node}/storage-pools/{pool}", s.requireStore(s.handleNodeStoragePoolDelete))
}

// handleNodeStoragePoolDelete serves DELETE /v1/nodes/{node}/storage-pools/{pool}.
//
// Idempotent: removing a pool that's already absent folds into a 200
// success envelope. Python linstor-client's `_handle_response_error`
// path tries JSON then XML to decode error responses; a bare 405 from
// Go's default mux (the previous shape with no handler registered)
// trips its `xml.etree` parser and crashes the CLI. Returning a
// proper ApiCallRc envelope on every code path keeps both
// golinstor and the Python CLI happy.
//
// Semantics: this is REGISTRY-ONLY. The store-level Delete deregisters
// the StoragePool CRD from blockstor's pool map (and the satellite
// finalizer in pkg/satellite/controllers/storagepool.go releases the
// in-memory provider). The underlying disk-level pool (LVM VG, ZFS
// zpool, file directory) stays intact — on-disk teardown is an
// out-of-band operator concern. blockstor refuses to `vgremove` /
// `zpool destroy` to avoid surprising data loss.
//
// Per-key (node, pool) resolution goes through the store's
// Spec-based resolver (Bug 55), so operator-managed CRDs whose
// metadata.name doesn't follow blockstor's canonical "<node>.<pool>"
// shape (e.g. piraeus's `zfs-thin-w3` created via `kubectl apply -f`)
// are still deletable through this endpoint. Without the resolver,
// `linstor sp d` would optimistically report "already absent" while
// the CRD lived on and List() kept showing the pool.
//
// Scenario 6.W06: refuses with 409 + FAIL_IN_USE if any Resource
// replica on `(node, pool)` still references the pool (via a Volume
// whose `StoragePool` matches). The operator must drop the
// referencing replicas first — blockstor never cascades a pool-delete
// into replica-deletes, matching upstream LINSTOR's
// `CtrlStorPoolApiCallHandler` refusal pattern. The check runs BEFORE
// the store-level Delete so a refused call leaves the pool's CRD in
// place.
func (s *Server) handleNodeStoragePoolDelete(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	// Refuse if any Resource replica on this node still references
	// the pool. We scan the full Resource list rather than per-RD
	// because the store has no (pool → resources) reverse index;
	// the cost is bounded by the cluster-wide replica count, which
	// is well below the typical Resource-list path latency budget.
	refs, err := s.referencingResources(r.Context(), node, pool)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if len(refs) > 0 {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailInUse,
			Message: "The specified storage pool '" + pool +
				"' on node '" + node + "' can not be deleted as " +
				"volumes / snapshot-volumes are still using it.",
			Details: "Volumes that are still using the storage pool: " +
				strings.Join(refs, ", "),
			Correc: "Delete the listed volumes first.",
			ObjRefs: map[string]string{
				objRefNode:     node,
				objRefStorPool: pool,
			},
		}})

		return
	}

	err = s.Store.StoragePools().Delete(r.Context(), node, pool)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	// Promote the "already absent" path to the warn band (Bug 66
	// alignment): the original Bug 52 fix used maskInfo for both
	// outcomes, hiding the no-op replay from audit-log greppers.
	// All other delete handlers fold NotFound into the warn band
	// (warnRGNotFound, warnVDNotFound, warnVGNotFound, …); keeping
	// this one in step makes `grep WARN` reliably surface every
	// no-op replay across the entire delete surface.
	if err != nil {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnStoragePoolNotFound,
			Message: "storage pool already absent: " + pool + " on " + node,
		}})

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "storage pool deleted: " + pool + " on " + node,
	}})
}

// referencingResources returns the `<rd>/<vol_nr>` keys of every
// Volume on `node` whose `StoragePool` matches `pool`. Used by the
// storage-pool delete refusal path (scenario 6.W06).
//
// Resources whose satellite hasn't reported Volumes yet (no
// per-replica observation) intentionally do NOT count as "using" the
// pool — without a populated Volumes slice we can't prove the
// replica's storage came from this specific pool, and refusing on a
// not-yet-observed replica would block legitimate pool removal on a
// freshly-restored cluster. Matches upstream LINSTOR, which iterates
// `VlmProviderObject`s on the pool — a replica that has not yet been
// materialized has no provider objects bound to the pool either.
func (s *Server) referencingResources(ctx context.Context, node, pool string) ([]string, error) {
	all, err := s.Store.Resources().List(ctx)
	if err != nil {
		return nil, err
	}

	var refs []string

	for i := range all {
		if all[i].NodeName != node {
			continue
		}

		for _, v := range all[i].Volumes {
			if v.StoragePool == pool {
				refs = append(refs,
					all[i].Name+"/"+strconv.FormatInt(int64(v.VolumeNumber), 10))

				break
			}
		}
	}

	return refs, nil
}

func (s *Server) handleStoragePoolsView(w http.ResponseWriter, r *http.Request) {
	// Optional filters golinstor sends: ?nodes=a,b&storage_pools=p1,p2.
	// Java LINSTOR honours both as case-insensitive set-membership;
	// we match that so /v1/view/storage-pools?nodes=X — the call
	// linstor-csi makes on every NodeRegister — does not return the
	// whole cluster's pools when only one node is asked about.
	out, err := s.listFilteredStoragePools(r.Context(),
		multiValueQuery(r, "nodes"),
		multiValueQuery(r, "storage_pools"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, out)
}

// handleNodeStoragePoolsList serves GET /v1/nodes/{node}/storage-pools.
//
// Implementation note: we deliberately go through the same List()+filter
// pipeline that /v1/view/storage-pools uses (rather than the store's
// ListByNode shortcut) for two reasons:
//
//  1. The k8s backend's ListByNode relies on a label selector that is only
//     populated when the CRD was created through our Create() path. Pools
//     that land in the cluster via operator `kubectl apply -f` or a
//     migration won't carry the label and would silently disappear from
//     the per-node listing — but they show up correctly in the view.
//     linstor-csi's autoplace probes /v1/nodes/{node}/storage-pools per
//     node, so an empty per-node response means "no candidate nodes",
//     leading to ResourceExhausted and stuck-Pending PVCs even though
//     the pools are visible in the aggregate view.
//  2. Java LINSTOR matches node names case-insensitively in both paths.
//     Routing the per-node handler through the same matchAnyFold filter
//     keeps the two endpoints in lockstep — a parity invariant the
//     storage_pools_test.go MatchesViewFiltering test pins.
func (s *Server) handleNodeStoragePoolsList(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	out, err := s.listFilteredStoragePools(r.Context(), []string{node}, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, out)
}

// listFilteredStoragePools returns the wire-shape pool slice after applying
// the case-insensitive node / pool filters Java LINSTOR uses. Empty filters
// mean "no constraint on this dimension". The slice is always non-nil so
// JSON encoding yields [] instead of null — linstor-csi rejects null bodies.
func (s *Server) listFilteredStoragePools(ctx context.Context, nodeFilter, poolFilter []string) ([]apiv1.StoragePool, error) {
	pools, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list storage pools")
	}

	out := make([]apiv1.StoragePool, 0, len(pools))

	for i := range pools {
		if !matchAnyFold(nodeFilter, pools[i].NodeName) {
			continue
		}

		if !matchAnyFold(poolFilter, pools[i].StoragePoolName) {
			continue
		}

		out = append(out, pools[i])
	}

	return out, nil
}

func (s *Server) handleNodeStoragePoolGet(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	sp, err := s.Store.StoragePools().Get(r.Context(), node, pool)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, sp)
}

// isKnownStoragePoolKind mirrors the set of apiv1.StoragePoolKind*
// constants we support on the satellite side, minus the two upstream
// kinds (OPENFLEX_TARGET, REMOTE_SPDK) we deliberately stub as
// out-of-scope. Validating here gives the satellite an immediate 400
// instead of letting a garbage CRD land in the store and surface as
// a downstream NPE.
func isKnownStoragePoolKind(kind string) bool {
	switch kind {
	case apiv1.StoragePoolKindLVM,
		apiv1.StoragePoolKindLVMThin,
		apiv1.StoragePoolKindZFS,
		apiv1.StoragePoolKindZFSThin,
		apiv1.StoragePoolKindFile,
		apiv1.StoragePoolKindFileThin,
		apiv1.StoragePoolKindDiskless:
		return true
	default:
		return false
	}
}

// handleNodeStoragePoolCreate serves POST /v1/nodes/{node}/storage-pools.
//
// Body shape mirrors upstream's `JsonGenTypes.StoragePool` (the same struct
// GET emits). The path's {node} wins over any node_name in the body so we
// can't end up creating a pool on a different node than the URL claims —
// piraeus assumes that invariant when it iterates over per-node SPs in
// its reconcile loop.
//
// Idempotency: re-POSTing the same (node, pool) is a no-op success rather
// than 409 Conflict. Piraeus's satellite Hello/heartbeat reposts the same
// pool on every registration tick; treating it as upsert keeps the retry
// loop quiet and matches the operator's expectation that "re-announce"
// is safe. Upstream LINSTOR returns an error on duplicate-create, but
// piraeus retries through that error indefinitely, so the practical
// outcome is the same — we just skip the retry storm.
func (s *Server) handleNodeStoragePoolCreate(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	body, ok := decodeStoragePoolCreate(w, r, node)
	if !ok {
		return
	}

	// Bug 63: linstor-client's `sp c <node> <pool> --pool-name <name>`
	// emits the provider-kind-agnostic `StorDriver/StorPoolName` key
	// rather than the kind-specific `StorDriver/LvmVg` / `ZPool` /
	// `ZPoolThin` / `FileDir` keys the satellite's NewProviderFromKind
	// reads. Without normalization the CRD lands in the store but
	// the satellite refuses to register the provider on its next
	// ApplyStoragePools tick — the pool shows up in `sp l` yet every
	// volume placement against it fails with `requires "StorDriver/..."
	// in props`. Expand the alias here so CLI-created pools are
	// usable end-to-end; the original key is retained because some
	// upstream clients read it back from `sp l` output.
	expandStorPoolNameAlias(&body)

	// Reject pools on ghost nodes with a clean 404. Without this the
	// store-level create would succeed (the StoragePoolStore key is
	// (node, pool) and doesn't FK to NodeStore) and the satellite
	// would learn about a pool on a node the controller doesn't
	// know — a permanent orphan from the controller's POV.
	_, nodeErr := s.Store.Nodes().Get(r.Context(), node)
	if nodeErr != nil {
		if errors.Is(nodeErr, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "node not found: "+node)

			return
		}

		writeError(w, http.StatusInternalServerError, nodeErr.Error())

		return
	}

	createErr := s.Store.StoragePools().Create(r.Context(), &body)
	if createErr == nil {
		writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "storage pool created: " + body.StoragePoolName + " on " + node,
		}})

		return
	}

	if errors.Is(createErr, store.ErrAlreadyExists) {
		s.upsertStoragePool(w, r, &body, node)

		return
	}

	writeStoreError(w, createErr)
}

// decodeStoragePoolCreate parses the POST body, normalises NodeName
// from the URL path, and validates the mandatory fields. Writes the
// 400 response itself and returns ok=false if anything is off.
func decodeStoragePoolCreate(w http.ResponseWriter, r *http.Request, node string) (apiv1.StoragePool, bool) {
	var body apiv1.StoragePool

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return apiv1.StoragePool{}, false
	}

	body.NodeName = node

	if body.StoragePoolName == "" {
		writeError(w, http.StatusBadRequest, "storage_pool_name is required")

		return apiv1.StoragePool{}, false
	}

	// Enforce the cluster-wide naming convention up front: the CRD
	// metadata.name will be `<pool>.<node>`, so a pool name carrying a
	// '.' would silently shift the boundary and either collide with
	// another (pool, node) pair or stage a CRD the CEL rule on the
	// type would later reject with a 422. Catch it here with a
	// friendly 400 so callers don't have to parse the k8s Invalid
	// envelope to figure out what went wrong.
	if strings.Contains(body.StoragePoolName, ".") {
		writeError(w, http.StatusBadRequest,
			"storage_pool_name must not contain '.': metadata.name must equal <pool>.<node>")

		return apiv1.StoragePool{}, false
	}

	if body.ProviderKind == "" {
		writeError(w, http.StatusBadRequest, "provider_kind is required")

		return apiv1.StoragePool{}, false
	}

	// Scenario 6.W02: the python-linstor-client emits provider names
	// in the lowercase compressed form (`lvmthin`, `zfsthin`, `filethin`)
	// — exactly what `sp create lvmthin <node> <pool> <vg>/<thinpool>`
	// puts on the wire. The StoragePool CRD enum and the satellite's
	// NewProviderFromKind switch only accept the upstream-canonical
	// uppercase tokens, so we normalise here in one shared place.
	// Mirrors the same normalisation `physical-storage create-device-pool`
	// already applies (Bug 73 parity); both endpoints must stay in
	// lockstep so a CLI-typed name lands canonical regardless of which
	// path created it. The legacy `lvm-thin` hyphenated form is NOT
	// accepted — see TestSPCreateLVMThinHyphenRejected for the rationale.
	normalized, ok := normalizeProviderKind(body.ProviderKind)
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown provider_kind: "+body.ProviderKind)

		return apiv1.StoragePool{}, false
	}

	body.ProviderKind = normalized

	if !isKnownStoragePoolKind(body.ProviderKind) {
		writeError(w, http.StatusBadRequest, "unknown provider_kind: "+body.ProviderKind)

		return apiv1.StoragePool{}, false
	}

	return body, true
}

// upsertStoragePool merges the POST body's Spec fields onto the
// existing row when a pool with the same (node, pool) already exists.
// Capacity / status fields stay where SetCapacity put them. Returns
// 201 + envelope on success.
func (s *Server) upsertStoragePool(w http.ResponseWriter, r *http.Request, body *apiv1.StoragePool, node string) {
	existing, getErr := s.Store.StoragePools().Get(r.Context(), node, body.StoragePoolName)
	if getErr != nil {
		writeStoreError(w, getErr)

		return
	}

	existing.ProviderKind = body.ProviderKind
	if body.Props != nil {
		existing.Props = body.Props
	}

	if body.FreeSpaceMgrName != "" {
		existing.FreeSpaceMgrName = body.FreeSpaceMgrName
	}

	if body.SharedSpaceID != "" {
		existing.SharedSpaceID = body.SharedSpaceID
	}

	existing.ExternalLocking = body.ExternalLocking

	updateErr := s.Store.StoragePools().Update(r.Context(), &existing)
	if updateErr != nil {
		writeStoreError(w, updateErr)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "storage pool already present, updated in place: " + body.StoragePoolName + " on " + node,
	}})
}

// Upstream-LINSTOR property keys used to configure each provider
// kind. Duplicated from pkg/satellite/factory.go (the canonical
// declarations) to avoid an import cycle — the satellite package
// already depends on pkg/rest types indirectly through pkg/store.
// If the satellite list changes, this list must follow.
const (
	propStorPoolName = "StorDriver/StorPoolName"
	propLvmVG        = "StorDriver/LvmVg"
	propThinPool     = "StorDriver/ThinPool"
	propZPool        = "StorDriver/ZPool"
	propZPoolThin    = "StorDriver/ZPoolThin"
	propFileDir      = "StorDriver/FileDir"
)

// expandStorPoolNameAlias normalises a pool's Spec.Props for Bug 63.
//
// Python linstor-client's `sp c <node> <pool> --pool-name <vg-or-zpool>`
// writes the provider-kind-agnostic `StorDriver/StorPoolName` prop and
// does NOT set the kind-specific key the satellite's NewProviderFromKind
// reads (`StorDriver/LvmVg`, `ZPool`, `ZPoolThin`, `FileDir`, plus
// `ThinPool` for LVM_THIN). The result was a pool that registered in
// the CRD store but failed every provider lookup on the satellite,
// with a misleading `requires "StorDriver/<key>" in props` error.
//
// Behaviour:
//
//   - If the kind-specific key for the body's ProviderKind is already
//     set, the explicit value wins and the alias is ignored (matches
//     upstream LINSTOR's "explicit > implicit" precedence).
//   - If only StorPoolName is set, copy its value into the canonical
//     kind-specific key(s). For LVM_THIN the alias is parsed as
//     `<vg>/<thin>` (the format `linstor sp c lvmthin ...` emits) and
//     each half lands in `LvmVg` / `ThinPool` respectively. A bare
//     value with no slash for LVM_THIN copies into `LvmVg` only and
//     leaves `ThinPool` empty — the satellite will surface a clear
//     "requires ThinPool" error rather than silently using the wrong
//     volume.
//   - The original `StorDriver/StorPoolName` is retained because
//     `linstor sp l` echoes it back and some operator tooling
//     compares against it.
//   - DISKLESS / unknown kinds are no-ops — DISKLESS has no underlying
//     storage and unknown kinds already 400 in decodeStoragePoolCreate.
func expandStorPoolNameAlias(body *apiv1.StoragePool) {
	if body.Props == nil {
		return
	}

	alias := body.Props[propStorPoolName]
	if alias == "" {
		return
	}

	switch body.ProviderKind {
	case apiv1.StoragePoolKindLVM:
		setPropIfEmpty(body.Props, propLvmVG, alias)
	case apiv1.StoragePoolKindLVMThin:
		expandLVMThinAlias(body.Props, alias)
	case apiv1.StoragePoolKindZFS:
		setPropIfEmpty(body.Props, propZPool, alias)
	case apiv1.StoragePoolKindZFSThin:
		setPropIfEmpty(body.Props, propZPoolThin, alias)
	case apiv1.StoragePoolKindFile, apiv1.StoragePoolKindFileThin:
		setPropIfEmpty(body.Props, propFileDir, alias)
	}
}

// setPropIfEmpty writes value at key only when key is currently empty.
// Mirrors the "explicit > implicit" precedence: a pre-set kind-specific
// key beats any alias we'd derive from StorPoolName.
func setPropIfEmpty(props map[string]string, key, value string) {
	if props[key] == "" {
		props[key] = value
	}
}

// expandLVMThinAlias splits the linstor-client LVM_THIN alias into its
// (VG, ThinPool) halves and writes whichever halves are still empty.
//
// linstor-client emits `<vg>/<thin>` for LVM_THIN aliases. Split on
// the first '/' so VG names containing additional slashes (unusual
// but legal in some lvm.conf setups) round-trip into LvmVg unmodified;
// the thin pool half is whatever remains. A bare alias with no slash
// lands in LvmVg only — ThinPool stays empty and the satellite will
// surface a clear "requires ThinPool" error on registration.
func expandLVMThinAlias(props map[string]string, alias string) {
	vg, thin, hasSlash := strings.Cut(alias, "/")
	if hasSlash {
		setPropIfEmpty(props, propLvmVG, vg)
		setPropIfEmpty(props, propThinPool, thin)

		return
	}

	setPropIfEmpty(props, propLvmVG, alias)
}
