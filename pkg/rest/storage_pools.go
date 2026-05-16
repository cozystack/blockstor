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
	mux.HandleFunc("PUT /v1/nodes/{node}/storage-pools/{pool}", s.requireStore(s.handleNodeStoragePoolModify))
	mux.HandleFunc("DELETE /v1/nodes/{node}/storage-pools/{pool}", s.requireStore(s.handleNodeStoragePoolDelete))
}

// handleNodeStoragePoolModify serves PUT /v1/nodes/{node}/storage-pools/{pool}.
//
// Wire shape mirrors upstream's `StoragePoolDefinitionModify` —
// `{override_props, delete_props, delete_namespaces}` — the same
// GenericPropsModify envelope golinstor's StoragePoolService.Modify
// and python-linstor-client's `linstor sp set-property` send. Without
// this route the CLI gets a bare 405 from Go's default mux, which
// trips the python parser's xml.etree fallback (Bug 85).
//
// Semantics: load the existing StoragePool, merge override_props on
// top, drop delete_props / delete_namespaces, persist via the store.
// Behaviour mirrors handleNodeUpdate's GenericPropsModify path so a
// CLI typist gets the same wire-shape contract across n/r/rg/rd/sp
// modify endpoints.
func (s *Server) handleNodeStoragePoolModify(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	// Bug 163: decode into the full StoragePoolModify shape (which
	// embeds GenericPropsModify and adds the read-side keys the GET
	// response emits — free_space_mgr_name, state, uuid, etc.) so
	// operators can `curl GET | jq | curl PUT` without tripping
	// Bug 161's DisallowUnknownFields.
	var body apiv1.StoragePoolModify

	if !decodeJSON(w, r, &body) {
		return
	}

	patch := body.GenericPropsModify

	existing, err := s.Store.StoragePools().Get(r.Context(), node, pool)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if existing.Props == nil && (len(patch.OverrideProps) > 0 ||
		len(patch.DeleteProps) > 0 || len(patch.DeleteNamespace) > 0) {
		existing.Props = map[string]string{}
	}

	maps.Copy(existing.Props, patch.OverrideProps)

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}

	// DeleteNamespace: drop every key under the given namespace prefix.
	// Mirrors upstream LINSTOR's `delete_namespaces` behaviour — the
	// namespace separator is '/' and the prefix is matched literally
	// (no glob). An entry "Aux" drops every "Aux/..." key.
	for _, ns := range patch.DeleteNamespace {
		prefix := ns + "/"

		for k := range existing.Props {
			if strings.HasPrefix(k, prefix) {
				delete(existing.Props, k)
			}
		}
	}

	err = s.Store.StoragePools().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "storage pool modified: " + pool + " on " + node,
	}})
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
	force := isForce(r)

	(&deleteWithRollback[apiv1.StoragePool]{
		refuseIfReferenced: func() bool {
			// Bug 152 escape hatch (mirrors Bug 92 node delete,
			// Bug 111 single-node evacuate, W13 VD shrink):
			// `?force=true` skips the still-referenced refusal
			// so an operator can reclaim a pool on a dead node
			// whose Resource CRDs are already tombstoned. The
			// referencing replicas are left as-is for out-of-
			// band cleanup. Without this knob the only escape
			// path is dropping every replica by hand first,
			// which races the satellite reconciler.
			if force {
				return false
			}

			return s.refuseSPDeleteIfReferenced(w, r, node, pool)
		},
		capture: func() (apiv1.StoragePool, bool) {
			return s.captureStoragePool(r.Context(), node, pool)
		},
		remove: func() error {
			return s.Store.StoragePools().Delete(r.Context(), node, pool)
		},
		rolledBackIfRaced: func(captured apiv1.StoragePool, capturedOK bool) bool {
			// `?force=true` callers opt past this check too.
			if force || !capturedOK {
				return false
			}

			return s.rollbackSPDeleteIfRaced(w, r, node, pool, &captured)
		},
		writeWarn: func() {
			// Promote the "already absent" path to the warn band
			// (Bug 66 alignment): the original Bug 52 fix used
			// maskInfo for both outcomes, hiding the no-op
			// replay from audit-log greppers. All other delete
			// handlers fold NotFound into the warn band
			// (warnRGNotFound, warnVDNotFound, warnVGNotFound,
			// warnNodeNotFound) — keeping this one in step makes
			// `grep WARN` reliably surface every no-op replay
			// across the entire delete surface.
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnStoragePoolNotFound,
				Message: "storage pool already absent: " + pool + " on " + node,
			}})
		},
		writeSuccess: func() {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: maskInfo,
				Message: "storage pool deleted: " + pool + " on " + node,
			}})
		},
	}).run(w)
}

// refuseSPDeleteIfReferenced runs the pre-Delete Bug 152 refusal
// walk. Returns true when the HTTP error has already been
// written (the caller must stop processing) and false when the
// delete may proceed.
func (s *Server) refuseSPDeleteIfReferenced(w http.ResponseWriter, r *http.Request, node, pool string) bool {
	// Refuse if any Resource replica on this node still references
	// the pool. We scan the full Resource list rather than per-RD
	// because the store has no (pool → resources) reverse index;
	// the cost is bounded by the cluster-wide replica count, which
	// is well below the typical Resource-list path latency budget.
	refs, err := s.referencingResources(r.Context(), node, pool)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if len(refs) == 0 {
		return false
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: "The specified storage pool '" + pool +
			"' on node '" + node + "' can not be deleted as " +
			"volumes / snapshot-volumes are still using it.",
		Details: "Volumes that are still using the storage pool: " +
			strings.Join(refs, ", "),
		Correc: "Delete the listed volumes first, or pass " +
			"`?force=true` to bypass the refusal and accept " +
			"the orphan replicas.",
		ObjRefs: map[string]string{
			objRefNode:     node,
			objRefStorPool: pool,
		},
	}})

	return true
}

// captureStoragePool grabs a snapshot of the (node, pool) SP CRD
// so the Bug 145 post-delete re-scan has something to restore
// when a racing `r c -s <pool>` slipped past the pre-walk. The
// second return is false when the pool no longer exists at
// capture time (a benign idempotent-delete replay) — the
// rollback path is skipped in that case.
func (s *Server) captureStoragePool(ctx context.Context, node, pool string) (apiv1.StoragePool, bool) {
	sp, err := s.Store.StoragePools().Get(ctx, node, pool)
	if err != nil {
		return apiv1.StoragePool{}, false
	}

	return sp, true
}

// rollbackSPDeleteIfRaced runs the Bug 145 post-Delete re-scan.
// If a Resource reference appeared between the pre-walk and the
// Delete, restore the captured SP and write the 409 envelope the
// pre-walk would have written. Returns true when the rollback
// fired (HTTP error already written, caller must stop) and false
// when the delete is safe to commit.
func (s *Server) rollbackSPDeleteIfRaced(w http.ResponseWriter, r *http.Request, node, pool string, captured *apiv1.StoragePool) bool {
	refs, err := s.referencingResources(r.Context(), node, pool)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if len(refs) == 0 {
		return false
	}

	// Bug 178: a Create error here used to be silently swallowed,
	// so the cluster ended up with the SP deleted, the racing
	// replica's `Props["StorPoolName"]` pointing at a dropped row,
	// and the operator handed a 409 "still in use" envelope that
	// referenced a pool which no longer exists. Surface a 500
	// envelope that names the rollback failure so the operator
	// knows the deleted primary may need manual restoration.
	createErr := s.Store.StoragePools().Create(r.Context(), captured)
	if createErr != nil {
		writeRollbackRestoreFailure(r.Context(), w, createErr,
			objRefStorPool, node+"/"+pool, "linstor sp l")

		return true
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: "The specified storage pool '" + pool +
			"' on node '" + node + "' can not be deleted as " +
			"volumes / snapshot-volumes are still using it.",
		Details: "Volumes that are still using the storage pool: " +
			strings.Join(refs, ", "),
		Correc: "Delete the listed volumes first, or pass " +
			"`?force=true` to bypass the refusal and accept " +
			"the orphan replicas.",
		ObjRefs: map[string]string{
			objRefNode:     node,
			objRefStorPool: pool,
		},
	}})

	return true
}

// referencingResources returns the `<rd>/<vol_nr>` keys of every
// Volume on `node` whose `StoragePool` matches `pool`. Used by the
// storage-pool delete refusal path (scenario 6.W06).
//
// Resources whose satellite hasn't reported Volumes yet (no
// per-replica observation) but already carry a pinned
// `Props["StorPoolName"]` reference (Bug 145: a `r c -s <pool>`
// that just persisted in the same window as the racing `sp d`)
// MUST count as "using" the pool too — without this carve-out
// the TOCTOU race window between `r c` and `sp d` would leak
// orphan Resource CRDs through the post-delete re-scan. The
// `<rd>/spec` key shape disambiguates these "spec-only" refs
// from the satellite-observed `<rd>/<vol_nr>` refs.
//
// Resources whose satellite hasn't reported Volumes AND have no
// pinned `Props["StorPoolName"]` still do NOT count — without
// either signal we can't prove the replica is bound to this
// specific pool, and refusing on a not-yet-observed replica
// would block legitimate pool removal on a freshly-restored
// cluster. Matches upstream LINSTOR, which iterates
// `VlmProviderObject`s on the pool — a replica that has not yet
// been materialized has no provider objects bound to the pool
// either.
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

		matched := false

		for _, v := range all[i].Volumes {
			if v.StoragePool == pool {
				refs = append(refs,
					all[i].Name+"/"+strconv.FormatInt(int64(v.VolumeNumber), 10))

				matched = true

				break
			}
		}

		// Bug 145: pick up specs whose Volumes haven't been
		// observed yet — a freshly-created `r c -s <pool>`
		// carries the pool name in Props["StorPoolName"] before
		// the satellite reports any Volumes. Without this the
		// post-delete re-scan would never see the racing
		// resource and the SP delete would proceed even though
		// the spec-only Resource is about to dangle.
		if !matched && all[i].Props["StorPoolName"] == pool {
			refs = append(refs, all[i].Name+"/spec")
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

	// Bug 184: scrub sensitive keys from every pool's Props bag
	// before emit. Mirrors Bug 115's RD-side redaction — `linstor sp
	// lp` and `linstor sp l` both surface controller-side props
	// (StorDriver/EncryptPassphrase + Aux/* secrets) that must not
	// cross the read boundary.
	redactStoragePoolsInPlace(out)

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

	// Bug 184: same redaction as the cluster-wide view path. The
	// per-node list is the surface linstor-csi hits on every
	// NodeRegister, so a leak here surfaces on every node bootstrap.
	redactStoragePoolsInPlace(out)

	writeJSON(w, http.StatusOK, out)
}

// redactStoragePoolsInPlace walks every StoragePool's Props map and
// scrubs deny-listed keys. Centralised so the per-node + cluster-
// wide + per-pool GET paths share the same wire-edge invariant.
// Idempotent: a second pass is a no-op.
func redactStoragePoolsInPlace(pools []apiv1.StoragePool) {
	for i := range pools {
		redactSensitiveProps(pools[i].Props)
	}
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

	// Bug 184: redact sensitive Props at the REST boundary. The
	// Get() returns a value copy so the mutation is local to this
	// response — the store cache stays un-redacted.
	redactSensitiveProps(sp.Props)

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
	nodeObj, nodeErr := s.Store.Nodes().Get(r.Context(), node)
	if nodeErr != nil {
		if errors.Is(nodeErr, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "node not found: "+node)

			return
		}

		writeError(w, http.StatusInternalServerError, nodeErr.Error())

		return
	}

	// Bug 135: refuse pools whose backing VG / zpool isn't in the
	// satellite's stamped discovery set. Mirrors the Bug 89 shape
	// (busy-device refusal): the source of truth is the satellite,
	// which advertises its enumerated VGs/zpools as `Aux/DiscoveredVGs`
	// / `Aux/DiscoveredZPools` on the Node CRD. Pre-flight here
	// keeps a garbage CRD out of the store; without it the create
	// 201s, the pool surfaces State=Ok in `sp l`, and the satellite
	// fails silently on the first volume placement (the v3 report's
	// exact symptom). Permissive when the satellite hasn't published
	// a discovery list yet — mid-bootstrap or older-build satellites
	// MUST NOT deadlock fresh clusters.
	if refusalMsg, refused := refuseUnknownBackingStorage(&nodeObj, &body); refused {
		writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailInvldStorPoolName,
			Message: refusalMsg,
			Correc: "Apply the backing VG / zpool on the node first " +
				"(e.g. `linstor physical-storage create-device-pool`), " +
				"or check `kubectl get node.blockstor " + node +
				" -o jsonpath='{.spec.props}'` for the discovery list.",
			ObjRefs: map[string]string{
				objRefNode:     node,
				objRefStorPool: body.StoragePoolName,
			},
		}})

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

	if !decodeJSON(w, r, &body) {
		return apiv1.StoragePool{}, false
	}

	body.NodeName = node

	// Bug 97: REST-boundary identifier validation runs before any
	// store call so a whitespace-only / RFC-1123-illegal pool name
	// fails fast with a LINSTOR envelope (rather than leaking the
	// k8s "metadata.name is invalid: <hex>-" error after Name()
	// mangling). See pkg/rest/input_validation.go.
	poolNameErr := validateLinstorName("storage pool", body.StoragePoolName)
	if poolNameErr != nil {
		writeError(w, http.StatusBadRequest, poolNameErr.Error())

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

// Bug 135 — discovery-list pre-flight for storage-pool create.
//
// The satellite's discovery loop enumerates the host's VGs (via
// `vgs --noheadings -o vg_name`) and zpools (via `zpool list -H -o
// name`) on every tick and stamps them onto the Node CRD's Spec.Props
// under the LINSTOR `Aux/` namespace. Two keys:
//
//   - `Aux/DiscoveredVGs="vg1,vg2"`    — populated on every satellite
//     tick where `vgs` exits zero, even if the list is empty (then
//     the key holds "" — which still counts as "advertised").
//   - `Aux/DiscoveredZPools="zp1,zp2"` — same shape for `zpool list`.
//
// The two keys are distinct: a host without ZFS leaves
// `Aux/DiscoveredZPools` unset (vs. set-and-empty). The validator
// treats unset as "satellite hasn't advertised, fall through
// permissive"; an explicitly empty value counts as "advertised,
// nothing matches" → refusal.
//
// FILE / FILE_THIN / DISKLESS providers have no VG/zpool to validate
// against. They short-circuit out of the function before the lookup.
const (
	nodePropDiscoveredVGs    = "Aux/DiscoveredVGs"
	nodePropDiscoveredZPools = "Aux/DiscoveredZPools"
)

// refuseUnknownBackingStorage runs the Bug 135 pre-flight check on a
// pool create. Returns `(msg, true)` to refuse the create, or
// `("", false)` to let it proceed.
//
// Branching follows the apiv1 provider-kind constants:
//
//   - LVM      → check Props["StorDriver/LvmVg"]    against discovered VGs.
//   - LVM_THIN → same; ThinPool check is delegated to the satellite's
//     NewProviderFromKind on registration (the validator only
//     pins existence of the VG, not the thin LV inside it).
//   - ZFS      → check Props["StorDriver/ZPool"]     against discovered zpools.
//   - ZFS_THIN → check Props["StorDriver/ZPoolThin"] against discovered zpools.
//   - FILE / FILE_THIN / DISKLESS → no validation.
//
// The validator stays permissive when the satellite hasn't published
// the corresponding `Aux/Discovered*` key (mid-bootstrap, older
// satellite build). Without this carve-out a freshly-bootstrapped
// cluster would deadlock waiting for the first discovery tick.
// Mirrors the Bug 89 nil-Free fall-through.
func refuseUnknownBackingStorage(nodeObj *apiv1.Node, body *apiv1.StoragePool) (string, bool) {
	switch body.ProviderKind {
	case apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindLVMThin:
		return checkAdvertised(nodeObj, nodePropDiscoveredVGs,
			body.Props[propLvmVG], "VG", body.ProviderKind)
	case apiv1.StoragePoolKindZFS:
		return checkAdvertised(nodeObj, nodePropDiscoveredZPools,
			body.Props[propZPool], "ZPool", body.ProviderKind)
	case apiv1.StoragePoolKindZFSThin:
		return checkAdvertised(nodeObj, nodePropDiscoveredZPools,
			body.Props[propZPoolThin], "ZPool", body.ProviderKind)
	}

	// FILE / FILE_THIN / DISKLESS: no backing VG/zpool to validate.
	return "", false
}

// checkAdvertised looks up the comma-separated discovery list on the
// node's Props bag and reports whether `want` is in the set. Returns
// `("", false)` (no refusal) when:
//
//   - The discovery key is absent entirely (satellite hasn't stamped
//     it yet — bootstrap path, see refuseUnknownBackingStorage doc).
//   - `want` is present in the advertised set.
//
// Returns `(refusalMsg, true)` when:
//
//   - The kind-specific key is empty (e.g. Bug 63's `expandLVMThinAlias`
//     decomposed `/no/such/path` into VG="" + ThinPool="no/such/path"
//     because the alias starts with `/`) AND the satellite has spoken.
//     A garbage `--pool-name` value still has to be refused even when
//     the alias expander produced a syntactically empty VG slot — the
//     v3 report's `linstor sp c lvmthin <node> <pool> /total/garbage/path`
//     case lands here.
//   - The kind-specific key is non-empty but isn't in the advertised
//     set (the headline Bug 135 case).
func checkAdvertised(nodeObj *apiv1.Node, propKey, want, label, kind string) (string, bool) {
	raw, advertised := nodeObj.Props[propKey]
	if !advertised {
		// Satellite hasn't published a discovery list yet (mid-bootstrap
		// or older build). Permissive fall-through, matches the Bug 89
		// nil-Free fall-through.
		return "", false
	}

	if want == "" {
		// Discovery list is present but the caller didn't supply a
		// backing-storage identifier — e.g. Bug 63's alias decomposed
		// `/garbage` into VG="" because the path starts with `/`, or
		// the operator hand-rolled a body with no StorDriver/* key.
		// Refuse rather than fall through to the satellite — without
		// this carve-out the v3 report's garbage-path case still slips
		// past the pre-flight.
		return "missing " + label + " in props (provider kind " + kind +
			") — satellite advertised " + label + "s: " + strconv.Quote(raw), true
	}

	for candidate := range strings.SplitSeq(raw, ",") {
		if strings.TrimSpace(candidate) == want {
			return "", false
		}
	}

	return "no " + label + " named " + strconv.Quote(want) +
		" on node " + strconv.Quote(nodeObj.Name) +
		" — satellite advertised " + label + "s: " + strconv.Quote(raw) +
		" (provider kind " + kind + ")", true
}
