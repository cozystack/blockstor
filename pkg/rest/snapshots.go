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
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerSnapshots wires snapshot endpoints. Three different paths land
// here: per-RD CRUD, the cross-RD aggregate (/v1/view/snapshots), and the
// multi-snapshot atomic action upstream uses for snapshot-of-many.
func (s *Server) registerSnapshots(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/snapshots", s.requireStore(s.handleSnapshotsView))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/snapshots",
		s.requireStore(s.handleSnapshotList))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshots",
		s.requireStore(s.handleSnapshotCreate))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/snapshots/{snap}",
		s.requireStore(s.handleSnapshotGet))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/snapshots/{snap}",
		s.requireStore(s.handleSnapshotDelete))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshots/{snap}/rollback",
		s.requireStore(s.handleSnapshotRollback))
	// Bug 98: python-linstor 1.27.1 (and upstream LINSTOR's Java
	// server) POST `linstor snapshot rollback` at this canonical
	// shape — without it the CLI crashes on the bare 404 page.
	// Wire it to the same handler so both shapes return identical
	// envelopes; the legacy `/snapshots/{snap}/rollback` stays for
	// existing internal callers.
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshot-rollback/{snap}",
		s.requireStore(s.handleSnapshotRollback))
}

// handleSnapshotRollback answers the upstream `linstor snapshot rollback`
// endpoint. blockstor does NOT expose `zfs rollback`: it destroys every
// snapshot newer than the rollback target, which is a hard data-loss
// footgun we refuse to make reachable over REST. The operator-facing 501
// message points at `snapshot-restore-resource` — the safe,
// non-destructive path that materialises the snapshot into a new
// resource-definition via `zfs clone` (pkg/storage/zfs/zfs.go:257).
//
// The route exists (rather than 404'ing) so the upstream CLI surfaces a
// structured ApiCallRc error the operator can act on, instead of the
// `linstor: unable to parse server response` 404 path that confuses
// people into thinking the controller crashed.
//
// Validation cascade (matches upstream LINSTOR's "typos > preconditions
// > strategy" ordering, scenario 8.W04 / wave1 4.13 / Bug 21):
//
//  1. Unknown (rd, snap) → 404 from the existence probe so the operator
//     learns about the typo first.
//  2. Any replica `InUse` (Primary, consumer attached) → 409. `zfs
//     rollback` or `lvconvert --merge` underneath a live consumer's
//     filesystem cache silently corrupts data — refuse rather than risk
//     it. Tri-state semantic on `*InUse`: only an explicit `true`
//     refuses; `nil` (un-observed) and `false` fall through, matching
//     handleNodeEvacuate / validateMigrateSrc.
//  3. Everything else → 501 + actionable text pointing at
//     snapshot-restore-resource.
func (s *Server) handleSnapshotRollback(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	snapName := r.PathValue("snap")

	// Probe (rd, snap) first so typos/unknown inputs surface as 404
	// rather than getting swallowed by the blanket 501. Mirrors
	// upstream LINSTOR which validates the snapshot reference before
	// kicking off the rollback strategy.
	_, err := s.Store.Snapshots().Get(r.Context(), rd, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Scenario 8.W04: refuse if ANY replica is InUse. The check
	// scopes to resources of the target RD only — sibling RDs'
	// Primary state is irrelevant. The backend rollback operation
	// (`zfs rollback` / `lvconvert --merge`) is per-replica, so any
	// one offending node taints the cluster-wide rollback.
	resList, err := s.Store.Resources().ListByDefinition(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	inUseNodes := make([]string, 0, len(resList))

	for i := range resList {
		// Tri-state: nil ("un-observed") and *false ("Secondary")
		// are NOT refusals. Only an explicit *true ("Primary,
		// consumer attached") triggers the 409. A precautionary
		// refusal on un-observed state would lock out every
		// rollback on a fresh-spawned RD before the satellite has
		// reported its first state heartbeat.
		if resList[i].State.InUse != nil && *resList[i].State.InUse {
			inUseNodes = append(inUseNodes, resList[i].NodeName)
		}
	}

	if len(inUseNodes) > 0 {
		slices.Sort(inUseNodes)
		writeError(w, http.StatusConflict,
			"snapshot rollback refused: resource '"+rd+
				"' is InUse on node(s) "+strings.Join(inUseNodes, ", ")+
				"; demote/unmount the consumer(s) before retrying "+
				"(rollback would corrupt a live filesystem)")

		return
	}

	writeError(w, http.StatusNotImplemented,
		"snapshot rollback not implemented; use POST "+
			"/v1/resource-definitions/"+rd+"/snapshot-restore-resource "+
			"to materialise this snapshot into a new resource-definition "+
			"(safe, non-destructive). Direct zfs/lvm rollback would destroy "+
			"intervening snapshots and is deliberately not exposed.")
}

func (s *Server) handleSnapshotsView(w http.ResponseWriter, r *http.Request) {
	snaps, err := s.Store.Snapshots().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Optional filters golinstor sends: ?resources=rd1,rd2 &
	// snapshots=name1,name2 — case-insensitive set membership against
	// Java LINSTOR's behaviour. Without filtering linstor-csi's "do
	// any snapshots exist for this volume?" poll has to scan the
	// whole cluster every cycle.
	rdFilter := multiValueQuery(r, "resources")
	nameFilter := multiValueQuery(r, "snapshots")

	filtered := make([]apiv1.Snapshot, 0, len(snaps))

	for i := range snaps {
		if !matchAnyFold(rdFilter, snaps[i].ResourceName) {
			continue
		}

		if !matchAnyFold(nameFilter, snaps[i].Name) {
			continue
		}

		filtered = append(filtered, snaps[i])
	}

	// Derive the State-column flag for the python CLI's `s l` table.
	// Runs after filter (don't bother computing for snapshots the
	// caller filtered out) but before paginate so all callers — full
	// page or windowed — see the same Flags slice.
	err = s.stampSnapshotSuccessful(r.Context(), filtered)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Pagination: golinstor's ListOpts.{Offset,Limit} surface as
	// `?offset=N&limit=M`. linstor-csi forwards csi-sanity's
	// max_entries + starting_token into these, and CSI's
	// ListSnapshots "next token" path expects to see the next
	// batch on subsequent calls. Sort to make pagination
	// deterministic across calls — without a stable order,
	// offset slicing into a map-backed list returns inconsistent
	// pages.
	slices.SortFunc(filtered, func(a, b apiv1.Snapshot) int {
		if a.ResourceName != b.ResourceName {
			return strings.Compare(a.ResourceName, b.ResourceName)
		}

		return strings.Compare(a.Name, b.Name)
	})

	writeJSON(w, http.StatusOK, paginateSnapshots(r, filtered))
}

func (s *Server) handleSnapshotList(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	// Verify the parent RD exists so missing RD is 404, not [].
	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	snaps, err := s.Store.Snapshots().ListByDefinition(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Defensive: serialise an empty array as `[]`, never `null`.
	// linstor-csi's ListSnapshots decoder treats a `null` body as
	// "malformed response" and surfaces it as Internal; csi-sanity's
	// "empty snapshot list" assertion expects `[]`. The k8s + in-memory
	// stores both `make()` their result slices, but a partial mock or
	// future store impl that returns a nil slice on the no-rows path
	// would silently regress this envelope — pinning it at the handler
	// edge keeps the invariant local to where it's wire-visible.
	if snaps == nil {
		snaps = []apiv1.Snapshot{}
	}

	err = s.stampSnapshotSuccessful(r.Context(), snaps)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, snaps)
}

func (s *Server) handleSnapshotGet(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	snapName := r.PathValue("snap")

	snap, err := s.Store.Snapshots().Get(r.Context(), rd, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Single-snapshot view: reuse the slice helper so the SUCCESSFUL
	// derivation logic lives in one place. Cheap (one Resources()
	// list call per snapshot — the same call the multi-snapshot path
	// makes per RD anyway).
	single := []apiv1.Snapshot{snap}

	err = s.stampSnapshotSuccessful(r.Context(), single)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, single[0])
}

// stampSnapshotSuccessful derives the python-CLI `State` column from
// the per-node materialisation. For each snapshot:
//
//  1. If `Flags` already carries `FAILED_DEPLOYMENT` / `FAILED_DISCONNECT`
//     / `FAILED` (the satellite-stamped terminal-error marker) — leave
//     the entry alone; those flags outrank SUCCESSFUL in the CLI.
//  2. Else, look up the parent RD's diskful peer set (Resources whose
//     Flags slice contains neither DISKLESS nor TIE_BREAKER — the
//     TIE_BREAKER witness holds no data so it MUST be excluded from
//     the success denominator, otherwise auto-placed 2-diskful + 1-TB
//     topologies hang in Incomplete forever).
//  3. If every diskful peer has a `Snapshots[]` entry with non-zero
//     CreateTimestamp (the satellite-reported "I took the snapshot"
//     signal), stamp `SUCCESSFUL` on the wire `Flags` so `linstor s l`
//     renders the State column as `Successful`.
//
// Failure to enumerate Resources soft-fails to "no stamp": better to
// leave the row at `Incomplete` than 5xx the whole list call.
func (s *Server) stampSnapshotSuccessful(ctx context.Context, snaps []apiv1.Snapshot) error {
	// Cache the per-RD diskful node set so a List of N snapshots
	// against the same RD only walks Resources once.
	diskfulByRD := map[string]map[string]struct{}{}

	for i := range snaps {
		snap := &snaps[i]

		if slices.ContainsFunc(snap.Flags, isTerminalSnapshotFlag) {
			continue
		}

		diskful, ok := diskfulByRD[snap.ResourceName]
		if !ok {
			built, err := s.diskfulPeerSet(ctx, snap.ResourceName)
			if err != nil {
				return err
			}

			diskful = built
			diskfulByRD[snap.ResourceName] = diskful
		}

		if isSnapshotSuccessful(snap, diskful) {
			snap.Flags = append(snap.Flags, apiv1.SnapshotFlagSuccessful)
		}
	}

	return nil
}

// legacySnapshotFlagFailed is the satellite snapshot reconciler's
// terminal-error stamp (F18 cli-parity wiring) kept for backwards-compat
// with existing CRDs that carry the token in Status.Flags. Mirrored
// from the SnapshotStatusFlagFailed constant on the CRD package;
// duplicated here so the REST package doesn't need a v1alpha1 import
// just to read one literal.
const legacySnapshotFlagFailed = "FAILED"

// isTerminalSnapshotFlag reports whether a flag value already pins the
// snapshot to a non-Incomplete State that outranks `SUCCESSFUL`.
// Mirrors the python CLI's elif-chain order in snapshot_cmds.show.
func isTerminalSnapshotFlag(flag string) bool {
	return flag == apiv1.SnapshotFlagFailedDeployment ||
		flag == apiv1.SnapshotFlagFailedDisconnect ||
		flag == apiv1.SnapshotFlagSuccessful ||
		flag == legacySnapshotFlagFailed
}

// isSnapshotSuccessful is true iff every diskful peer of the parent RD
// has a per-node entry with non-zero CreateTimestamp. An empty
// diskful peer set returns false — a 0-replica RD with a snapshot row
// is a degenerate state the operator should investigate, not silently
// auto-mark as success.
func isSnapshotSuccessful(snap *apiv1.Snapshot, diskful map[string]struct{}) bool {
	if len(diskful) == 0 {
		return false
	}

	reported := map[string]struct{}{}

	for j := range snap.Snapshots {
		entry := &snap.Snapshots[j]
		if entry.CreateTimestamp == 0 {
			continue
		}

		reported[entry.NodeName] = struct{}{}
	}

	for node := range diskful {
		if _, ok := reported[node]; !ok {
			return false
		}
	}

	return true
}

// diskfulPeerSet enumerates the Resources of an RD and returns the
// set of node names whose Flags slice contains neither DISKLESS nor
// TIE_BREAKER. NotFound on the RD soft-fails to an empty set —
// matches handleSnapshotList's "treat orphan snapshot rows as
// renderable" stance.
func (s *Server) diskfulPeerSet(ctx context.Context, rdName string) (map[string]struct{}, error) {
	resList, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return map[string]struct{}{}, nil
		}

		return nil, err //nolint:wrapcheck // surfaced via writeError
	}

	out := make(map[string]struct{}, len(resList))

	for i := range resList {
		flags := resList[i].Flags
		if slices.Contains(flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		if slices.Contains(flags, apiv1.ResourceFlagTieBreaker) {
			continue
		}

		out[resList[i].NodeName] = struct{}{}
	}

	return out, nil
}

func (s *Server) handleSnapshotCreate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	// Reject whitespace-only RD names before the JSON decode: csi-sanity's
	// "CreateSnapshot should fail when the source volume is not specified"
	// path forwards an empty source-volume-id into linstor-csi which
	// concatenates it into the path. Without an explicit trim, a `%20`
	// or pure-empty {rd} segment slugs into a real-looking row that no
	// subsequent reconcile can address (the satellite scans by RD name
	// and never matches a blank one). Distinct message from the snap
	// validation below so the CSI driver's error surface tells the
	// operator which field was wrong.
	if strings.TrimSpace(rd) == "" {
		writeError(w, http.StatusBadRequest, "resource definition name is required")

		return
	}

	var snap apiv1.Snapshot

	err := json.NewDecoder(r.Body).Decode(&snap)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// TrimSpace guards the "silent slug-of-empty" bug class csi-sanity
	// surfaces with "CreateSnapshot should fail when the name field is
	// missing"/"empty". A bare `""` already 400'd here, but a
	// whitespace-only `"   "` previously slipped through and persisted
	// an unaddressable snapshot row (zfs barfs on the snap name later;
	// linstor-csi sees a "created" response and never retries).
	//
	// Bug 97: tighten further to the full RFC-1123 subdomain rule so
	// `linstor s c <rd> "Foo Bar"` is refused with a LINSTOR envelope
	// before pkg/store/k8s.Name() slugifies the input.
	snapNameErr := validateLinstorName("snapshot", strings.TrimSpace(snap.Name))
	if snapNameErr != nil {
		writeError(w, http.StatusBadRequest, snapNameErr.Error())

		return
	}

	snap.ResourceName = rd

	err = s.hydrateSnapshotFromRD(r.Context(), &snap, rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Materialise the per-node `Snapshots[]` array so reads see one
	// SnapshotNode per diskful peer. linstor-csi's CreateSnapshot
	// flow lists snapshots immediately after create and treats an
	// empty `snapshots[]` array as "the satellite never took it" —
	// surfaced as "failed to create snapshot: missing snapshots".
	// blockstor's actual snapshot is taken by the satellite during
	// reconcile, but the REST shim's view of "where it landed"
	// derives deterministically from Spec.Nodes. F20: each per-node
	// entry also carries the `snapshot_volumes[]` slot array so the
	// CLI's per-node volume table renders the volume_number column.
	snap.Snapshots = makeSnapshotPerNode(snap.Name, snap.Nodes, snap.VolumeDefinitions)

	// Idempotent create: a CSI driver retries CreateSnapshot for the
	// same (rd, snap_name) until success, so a re-request must
	// return 200 + ApiCallRc rather than 409. Mirrors upstream
	// LINSTOR's behaviour for snapshot name collisions on the same
	// RD. Different-source name collision is detected at the
	// linstor-csi layer (it maps CSI snapshot ids to LINSTOR
	// (rd, snap_name) tuples).
	existing, getErr := s.Store.Snapshots().Get(r.Context(), rd, snap.Name)
	if getErr == nil {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "snapshot already exists: " + existing.Name,
		}})

		return
	}

	err = s.Store.Snapshots().Create(r.Context(), &snap)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot created: " + snap.Name,
	}})
}

// paginateSnapshots applies golinstor's ListOpts.{Offset,Limit}
// query params to the filtered slice. CSI's ListSnapshots
// max_entries + starting_token forward into these. A zero/missing
// `limit` returns everything; a negative `offset` clamps to zero.
// Out-of-range offset returns an empty slice (NOT 416) — matches
// upstream LINSTOR which silently empties the list when paginated
// past the end.
//
// The returned slice is always non-nil so `json.Marshal` yields `[]`
// rather than `null`. linstor-csi's CSI ListSnapshots loop forwards
// `max_entries + starting_token` into `?limit + offset`; when the
// caller paginates past the last item, csi-sanity expects an empty
// JSON array body, not a null. A null body decodes to a nil slice in
// the csi-sanity client and the assertion path treats that as a
// malformed envelope.
//
// Exact-fit pagination: when `limit == len(in)-offset` (i.e. the page
// boundary lines up with the end of the data), the slice for the
// current page is returned at full length. The CSI client then
// issues the follow-up call with `offset += limit`, which lands in
// the `offset >= len(in)` branch above and returns the empty array
// that signals "no more pages". This two-call dance is the only
// way a flat-array REST surface can communicate end-of-data without
// inventing a next_token envelope; csi-sanity tolerates the extra
// round-trip.
func paginateSnapshots(r *http.Request, in []apiv1.Snapshot) []apiv1.Snapshot {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	if offset >= len(in) {
		return []apiv1.Snapshot{}
	}

	out := in[offset:]

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	// Belt + braces: `in[offset:]` on a non-nil slice is always
	// non-nil, but a future caller passing a nil `in` (e.g. a stub
	// store implementation that elides the make()) would slip through
	// the offset guard for offset==0 and return nil. Reify so the
	// JSON envelope is `[]`, not `null`.
	if out == nil {
		return []apiv1.Snapshot{}
	}

	return out
}

// makeSnapshotPerNode builds the `Snapshots[]` per-node materialisation
// from the slice of node names a Snapshot targets. Used at create time
// so subsequent GETs surface one SnapshotNode entry per diskful peer —
// linstor-csi's "did the satellite actually take it?" probe. F20:
// each per-node entry carries one `SnapshotVolume` per VolumeDefinition
// slot so the upstream `linstor s l` CLI renders the per-node
// volume_number column without an empty list.
func makeSnapshotPerNode(name string, nodes []string, vds []apiv1.SnapshotVolumeDef) []apiv1.SnapshotPerNode {
	out := make([]apiv1.SnapshotPerNode, 0, len(nodes))

	vols := make([]apiv1.SnapshotVolume, 0, len(vds))
	for i := range vds {
		vols = append(vols, apiv1.SnapshotVolume{VolumeNumber: vds[i].VolumeNumber})
	}

	for _, node := range nodes {
		entry := apiv1.SnapshotPerNode{
			SnapshotName: name,
			NodeName:     node,
		}

		if len(vols) > 0 {
			// Defensive copy — per-node entries must not share the
			// same backing array (a future per-node `State` mutation
			// would race across SnapshotPerNode siblings otherwise).
			entry.SnapshotVolumes = append([]apiv1.SnapshotVolume(nil), vols...)
		}

		out = append(out, entry)
	}

	return out
}

// hydrateSnapshotFromRD fills in the per-snapshot fields the
// snapshot-restore-resource handler + the autoplace constraint need
// downstream. Derivations:
//
//   - VolumeDefinitions: copied from the source RD when absent; without
//     these a restore-target RD comes up with zero volumes. F20:
//     each entry also carries the parent VD's `Props` so the
//     snapshot DTO surfaces `volume_definition_props` to upstream
//     tooling (`linstor backup`, schedule reconciler).
//   - Props: inherited from the source RD when absent.
//   - Nodes: upstream-LINSTOR semantic — empty means "every diskful
//     replica". The satellite reconciler gates per-snapshot work on
//     slices.Contains(snap.Spec.Nodes, self), so an empty list would
//     silently produce a zero-replica snapshot.
//   - F20 DTO fields: `SnapshotDefinitionProps` (== Snapshot's own
//     props bag) and `ResourceDefinitionProps` (a snapshot-time copy
//     of the parent RD's props) — both are surfaced via the wire
//     DTO so CLI consumers don't need a second round-trip.
func (s *Server) hydrateSnapshotFromRD(ctx context.Context, snap *apiv1.Snapshot, rd string) error {
	srcRD, err := s.Store.ResourceDefinitions().Get(ctx, rd)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	vds, err := s.Store.VolumeDefinitions().List(ctx, rd)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	vdPropsByNumber := make(map[int32]map[string]string, len(vds))
	for _, vd := range vds {
		if len(vd.Props) > 0 {
			vdPropsByNumber[vd.VolumeNumber] = vd.Props
		}
	}

	if len(snap.VolumeDefinitions) == 0 {
		snap.VolumeDefinitions = make([]apiv1.SnapshotVolumeDef, 0, len(vds))
		for _, vd := range vds {
			snap.VolumeDefinitions = append(snap.VolumeDefinitions, apiv1.SnapshotVolumeDef{
				VolumeNumber:          vd.VolumeNumber,
				SizeKib:               vd.SizeKib,
				VolumeDefinitionProps: vdPropsByNumber[vd.VolumeNumber],
			})
		}
	} else {
		// Caller-supplied VDs: still backfill the parent-RD per-VD
		// props (F20) when the slot doesn't carry its own —
		// the inherited block is what `linstor backup` reads.
		for i := range snap.VolumeDefinitions {
			if snap.VolumeDefinitions[i].VolumeDefinitionProps == nil {
				snap.VolumeDefinitions[i].VolumeDefinitionProps = vdPropsByNumber[snap.VolumeDefinitions[i].VolumeNumber]
			}
		}
	}

	if snap.Props == nil {
		snap.Props = srcRD.Props
	}

	if len(snap.Nodes) == 0 {
		snap.Nodes, err = listDiskfulNodes(ctx, s, rd)
		if err != nil {
			return err
		}
	}

	// F20 wire-shape fields. SnapshotDefinitionProps mirrors the
	// snapshot's own props bag (upstream surfaces both — the
	// SnapshotDefinition is the cluster-scope object, the Snapshot
	// is the per-node materialisation, and props live on the
	// definition). ResourceDefinitionProps is a snapshot-time copy
	// of the parent RD's props — a later RD-prop mutation does NOT
	// retroactively change this field.
	if snap.SnapshotDefinitionProps == nil {
		snap.SnapshotDefinitionProps = snap.Props
	}

	if snap.ResourceDefinitionProps == nil && len(srcRD.Props) > 0 {
		snap.ResourceDefinitionProps = srcRD.Props
	}

	return nil
}

// listDiskfulNodes returns the node names that host a diskful
// (non-DISKLESS) replica of rd. Used to default snap.Nodes when the
// caller didn't pin a per-node list — matches upstream's
// "snapshot all diskful replicas" semantic.
func listDiskfulNodes(ctx context.Context, s *Server, rd string) ([]string, error) {
	resList, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return nil, err //nolint:wrapcheck // surfaced via writeStoreError
	}

	out := make([]string, 0, len(resList))

	for i := range resList {
		if slices.Contains(resList[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		out = append(out, resList[i].NodeName)
	}

	return out, nil
}

// handleSnapshotDelete answers `DELETE /v1/resource-definitions/{rd}/snapshots/{snap}`
// with an idempotent 200 + ApiCallRc envelope. CSI spec §DeleteSnapshot
// mandates idempotence: the driver retries until it sees success, so
// a 404 on either an unknown RD or an unknown snapshot breaks the
// second-delete-after-success retry path that csi-sanity's "should
// succeed when an invalid snapshot id is used" check exercises.
//
// Both "unknown RD" and "unknown snapshot on known RD" fold to a 200
// + WARN-mask envelope. The mask flip from maskInfo to warnSnapshotNotFound
// is the cli-parity-audit #33 fix: upstream LINSTOR returns a
// `WARNING: Snapshot definition <snap> of resource <rd> not found.`
// entry on the same input (RC mask `0x4000_0000`), not a SUCCESS line.
// Tools that classify ret_code by mask (the contract-normaliser at
// tests/contract/normalize.go, python-linstor's print loop) were
// putting our no-op replay into the <info> bucket instead of <warn>.
// CSI doesn't read the mask so it still got its idempotent success;
// operators tailing the API log now see the same "no-op" annotation
// upstream emits.
func (s *Server) handleSnapshotDelete(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	snapName := r.PathValue("snap")

	err := s.Store.Snapshots().Delete(r.Context(), rd, snapName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnSnapshotNotFound,
				Message: "snapshot already absent: " + snapName,
				ObjRefs: map[string]string{
					objRefRscDfn:      rd,
					objRefSnapshotDfn: snapName,
				},
			}})

			return
		}

		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot deleted: " + snapName,
	}})
}
