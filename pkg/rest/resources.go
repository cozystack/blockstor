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
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerResources wires the /v1/view/resources aggregate. linstor-csi
// relies on this in its volume reconciliation loop.
func (s *Server) registerResources(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/resources", s.requireStore(s.handleResourcesView))
}

func (s *Server) handleResourcesView(w http.ResponseWriter, r *http.Request) {
	resList, err := s.Store.Resources().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Optional filters. Two wire dialects in the wild:
	//   - golinstor (csi side): comma-joined `?nodes=a,b`
	//   - python-linstor CLI:   repeat-key `?nodes=a&nodes=b` (via
	//     urlencode(doseq=True))
	// `multiValueQuery` accepts both, so `linstor r l -r foo -n bar`
	// and linstor-csi's existing requests land in the same filter.
	// Java LINSTOR honours both as case-insensitive set-membership; we
	// match that so linstor-csi's "is this resource on this node?"
	// poll returns a non-empty list when the answer is yes.
	nodeFilter := multiValueQuery(r, "nodes")
	rdFilter := multiValueQuery(r, "resources")
	faultyOnly := boolQuery(r, "faulty")

	// Per-RD UpToDate tally — drives the `?faulty=true` filter and
	// the recovery-copilot's "broken first" ranking. Recovery
	// playbooks key on "zero healthy copies" because that's the
	// failure mode that can't self-heal: a single UpToDate replica
	// is enough for DRBD to seed from, so RDs with 0 UpToDate need
	// operator attention first.
	rdStats := aggregateRDStats(resList)

	out := make([]apiv1.ResourceWithVolumes, 0, len(resList))

	vdSizes := vdSizeIndex(r.Context(), s, resList)

	for i := range resList {
		if !matchAnyFold(nodeFilter, resList[i].NodeName) {
			continue
		}

		if !matchAnyFold(rdFilter, resList[i].Name) {
			continue
		}

		if faultyOnly && !rdStats[resList[i].Name].faulty {
			continue
		}

		rwv, buildErr := s.buildResourceView(r.Context(), &resList[i], vdSizes[resList[i].Name])
		if buildErr != nil {
			writeError(w, http.StatusInternalServerError, buildErr.Error())

			return
		}

		out = append(out, rwv)
	}

	// Stable order. Default keys on Name+NodeName so CSI ListVolumes
	// pagination (offset+limit forwarded from max_entries +
	// starting_token) is deterministic across calls. When
	// `?faulty=true` is set, the recovery-copilot wants the
	// worst-off RDs first, so prepend an UpToDate-count primary key:
	// RDs with 0 UpToDate copies come before RDs with 1+.
	slices.SortFunc(out, func(left, right apiv1.ResourceWithVolumes) int {
		if faultyOnly {
			if c := compareResourceFaulty(rdStats, &left, &right); c != 0 {
				return c
			}
		}

		if left.Name != right.Name {
			return strings.Compare(left.Name, right.Name)
		}

		return strings.Compare(left.NodeName, right.NodeName)
	})

	writeJSON(w, http.StatusOK, paginateResources(r, out))
}

// buildResourceView annotates one replica with sync progress and
// resolves its effective-prop bag. Extracted from
// handleResourcesView to keep that handler under the funlen budget;
// the parent fetch (Controller / RG / RD) is soft-fail so a partial
// hierarchy returns a usable response.
//
// Scenario 6.W13: a LUKS-stack replica also gets `state.suspended`
// stamped here — true while the controller is locked (master
// passphrase not yet entered in this process), false once unlocked.
// stampSuspendedOnLUKS centralises the layer-walk + flag-read.
//
// Bug 95: the resolved parent RD's LayerStack is re-stamped onto the
// replica's LayerObject + per-volume LayerDataList here. The k8s
// store's CRD projection defaults to `[DRBD,STORAGE]` because the
// Resource CRD does not carry the parent's LayerStack — without the
// re-stamp, an RD created with `--layer-list DRBD,LUKS,STORAGE`
// silently emits the default chain, so operators see plaintext while
// believing they have encryption.
func (s *Server) buildResourceView(ctx context.Context, rsc *apiv1.Resource, vdSizes map[int32]int64) (apiv1.ResourceWithVolumes, error) {
	annotated := annotateSyncProgress(rsc.Volumes, vdSizes)

	// Bug 137: diskless / TIE_BREAKER replicas never get a
	// per-volume Status row written by the satellite (no local
	// backing device, no usage to report). The CRD-to-wire
	// projection therefore returns Volumes=nil, the JSON encoder
	// drops the `volumes` key under `omitempty`, and python-linstor
	// crashes on `rsc._rest_data['volumes'][0]` with AttributeError
	// because the key is `None`. Synthesise one placeholder per
	// parent-RD VolumeDefinition so the wire shape mirrors a
	// diskful replica — same volume_number, same layer chain — but
	// with no satellite-observed allocation. Non-diskless replicas
	// with an empty Volumes slice still get a `[]` (the absent-key
	// regression strikes them too: e.g. a freshly-created diskful
	// replica before the satellite has reported usage).
	annotated = ensureVolumesForView(annotated, rsc, vdSizes)

	eff, rd, err := effectivePropsAndRDForResource(ctx, s.Client, s.Store, rsc)
	if err != nil {
		return apiv1.ResourceWithVolumes{}, err
	}

	// Resource.Volumes is sourced from CRD Status by
	// crdToWireResource; ResourceWithVolumes is kept as a
	// distinct wrapper for backwards-compat with anything
	// still consuming the embedded shape — its Volumes field
	// shadows Resource.Volumes via field promotion ordering,
	// so the JSON output remains a single `volumes` key.
	rwv := apiv1.ResourceWithVolumes{
		Resource: *rsc,
		Volumes:  annotated,
	}
	rwv.Volumes = annotated

	// Re-stamp the layer surfaces from the RD's LayerStack so a
	// `--layer-list DRBD,LUKS,STORAGE` RD doesn't get silently
	// projected as `[DRBD,STORAGE]` by the store-level default
	// (Bug 95). nil/empty stack falls through to the existing
	// projection so back-compat with the legacy default-stack
	// path is preserved.
	if rd != nil && len(rd.LayerStack) > 0 {
		apiv1.ApplyLayerStack(&rwv.Resource, rd.LayerStack)
		apiv1.ApplyLayerStackToVolumes(rwv.Volumes, rd.LayerStack)
	}

	if len(eff) > 0 {
		// Bug 115: scrub deny-listed sensitive keys before exposing
		// the inheritance-merged view on `/v1/view/resources`. Without
		// this, an operator with read-only LINSTOR access could grep
		// `DrbdOptions/EncryptPassphrase` out of the per-replica
		// effective_props bag.
		redactSensitiveEffectiveProps(eff)
		rwv.EffectiveProps = eff
	}

	// Bug 115 sibling: the bare per-resource Props map (rsc.Props)
	// can also carry an Aux/secret* override; redact in place so the
	// wire view stays clean even on RDs that locally stamped a
	// sensitive key.
	redactSensitiveProps(rwv.Props)

	s.stampSuspendedOnLUKS(&rwv)

	return rwv, nil
}

// disklessStorPoolName is the upstream LINSTOR sentinel for the
// per-volume `storage_pool_name` field on diskless replicas. It is
// NOT the wire-canonical `apiv1.StoragePoolKindDiskless` ("DISKLESS")
// provider-kind string — that one names a *kind* of pool. The Python
// CLI's `n describe` literal-matches the volume's `storage_pool_name`
// against `node_cmds.DISKLESS_STORAGE_POOL = "DfltDisklessStorPool"`
// to route a diskless volume under a synthetic "diskless resource"
// subtree. Any other spelling (including "DISKLESS") causes the
// CLI's `node_map[node].find_child(storage_pool_name)` to return
// None on a node that has no such pool, and the next `add_child`
// crashes the CLI with AttributeError.
const disklessStorPoolName = "DfltDisklessStorPool"

// ensureVolumesForView returns a non-nil Volume slice for the wire
// view. Bug 137 follow-up: a nil slice OR a zero-length non-nil slice
// both serialise as a missing JSON key when the field carries
// `omitempty` (the initial fix kept the tag and only solved half the
// case). The wire shape contract is now: `volumes` key always
// present.
//
// We solve the empty-slice case in two tiers:
//
//  1. Diskless / TIE_BREAKER replica with no observed Volumes:
//     synthesise one placeholder per parent-RD VolumeDefinition so
//     the wire entry has the same volume_number cardinality as a
//     diskful replica. `storage_pool_name` is stamped with the
//     upstream-LINSTOR sentinel `DfltDisklessStorPool` (see
//     disklessStorPoolName) so `linstor n describe` recognises the
//     row as a diskless witness instead of crashing on
//     `find_child(...)` returning None.
//  2. Any other replica that happens to arrive with an empty
//     Volumes slice (e.g. fresh diskful replica before the
//     satellite has written Status.Volumes): emit `[]` rather than
//     omitting the key. Same crash class, just less common.
//
// Diskful replicas with already-populated Volumes are passed
// through verbatim — the placeholder path must not shadow real
// satellite-observed allocation / disk_state.
func ensureVolumesForView(observed []apiv1.Volume, rsc *apiv1.Resource, vdSizes map[int32]int64) []apiv1.Volume {
	if len(observed) > 0 {
		return observed
	}

	if !isDisklessReplica(rsc) {
		// Empty slice instead of nil so the JSON encoder emits
		// `[]` not the absent key. Bug 137 wire contract.
		return []apiv1.Volume{}
	}

	// Stable iteration over the VD index: map keys are
	// non-deterministic and the wire order has to be stable across
	// calls so paginated CLI consumers don't see volume_number
	// jitter between requests.
	volNums := make([]int32, 0, len(vdSizes))
	for vn := range vdSizes {
		volNums = append(volNums, vn)
	}

	slices.Sort(volNums)

	out := make([]apiv1.Volume, 0, len(volNums))

	for _, vn := range volNums {
		out = append(out, apiv1.Volume{
			VolumeNumber: vn,
			StoragePool:  disklessStorPoolName,
			// AllocatedKib defaults to 0 (the Bug 112 contract:
			// always emit the key as an int >= 0). Diskless has
			// no real allocation, so 0 is the truthful value.
		})
	}

	return out
}

// isDisklessReplica reports whether the replica carries a DISKLESS
// or TIE_BREAKER flag — i.e. has no local backing storage and
// therefore no satellite-observed Volumes rows. Both flags imply
// "no local disk", so both trigger the placeholder-synthesis path.
func isDisklessReplica(rsc *apiv1.Resource) bool {
	for _, f := range rsc.Flags {
		if f == apiv1.ResourceFlagDiskless || f == apiv1.ResourceFlagTieBreaker {
			return true
		}
	}

	return false
}

// stampSuspendedOnLUKS sets `state.suspended` on a resource that
// carries a LUKS layer in its LayerObject chain. true while the
// controller process is locked (no enter-passphrase yet), false
// once unlocked. Non-LUKS resources are left with state.suspended
// nil so the field is omitted on the wire — see ResourceState.Suspended
// for the tri-state contract. Scenario 6.W13.
func (s *Server) stampSuspendedOnLUKS(rwv *apiv1.ResourceWithVolumes) {
	if !hasLUKSLayer(rwv.LayerObject) {
		return
	}

	suspended := !s.passphraseUnlocked.Load()
	rwv.State.Suspended = &suspended
}

// hasLUKSLayer walks the ResourceLayer tree and returns true iff
// any node in the chain advertises `LayerKindLUKS`. The chain is
// shallow in practice (DRBD → LUKS → STORAGE), so a recursive walk
// is cheap and matches the upstream Python CLI's
// `_walk(layer_data, type==LUKS)` predicate used to pick the State
// column suffix.
func hasLUKSLayer(layer *apiv1.ResourceLayer) bool {
	if layer == nil {
		return false
	}

	if layer.Type == apiv1.LayerKindLUKS {
		return true
	}

	for i := range layer.Children {
		if hasLUKSLayer(&layer.Children[i]) {
			return true
		}
	}

	return false
}

// rdFaultyStats summarises the per-RD aggregate state used by the
// `?faulty=true` filter + sort: how many replicas of this RD report
// the UpToDate disk_state, and whether at least one replica looks
// broken (i.e. needs operator attention).
type rdFaultyStats struct {
	upToDate int
	faulty   bool
}

// diskStateUpToDate is the canonical DRBD-9 "this replica is fully
// caught up" disk_state. Hoisted to a constant because both the
// faulty-flag computation and the UpToDate-tally hot path key off
// it — without one source of truth, a typo would silently desync
// the two halves of `?faulty=true`.
const diskStateUpToDate = "UpToDate"

// aggregateRDStats walks the flat per-replica resource list and
// folds each entry into its parent-RD bucket. The Python CLI's
// `--faulty` semantics work at RD granularity — "is this resource
// healthy?" is answered by looking at every replica's disk_state,
// not just one — so the REST surface mirrors that: a single
// non-UpToDate replica taints the whole RD as faulty in this view.
func aggregateRDStats(resList []apiv1.Resource) map[string]rdFaultyStats {
	out := map[string]rdFaultyStats{}

	for i := range resList {
		stats := out[resList[i].Name]

		for j := range resList[i].Volumes {
			disk := resList[i].Volumes[j].State.DiskState
			if disk == "" {
				continue
			}

			if isUpToDateDiskState(disk) {
				stats.upToDate++
			}
		}

		if isResourceFaulty(&resList[i]) {
			stats.faulty = true
		}

		out[resList[i].Name] = stats
	}

	return out
}

// isResourceFaulty reports whether THIS single replica looks broken
// (any non-empty volume disk_state that isn't UpToDate). The
// RD-level aggregation in aggregateRDStats folds these per-replica
// verdicts into a single per-RD faulty flag — one bad replica is
// enough to mark the whole RD as needing operator attention.
func isResourceFaulty(r *apiv1.Resource) bool {
	for i := range r.Volumes {
		disk := r.Volumes[i].State.DiskState
		if disk == "" {
			continue
		}

		if !isUpToDateDiskState(disk) {
			return true
		}
	}

	return false
}

// isUpToDateDiskState matches "UpToDate" plus the sync-progress-
// annotated variant "UpToDate(NN%)" emitted by annotateSyncProgress.
// We never want a fully-synced replica that happens to carry a
// progress suffix to count as faulty.
func isUpToDateDiskState(disk string) bool {
	return disk == diskStateUpToDate || strings.HasPrefix(disk, diskStateUpToDate+"(")
}

// compareResourceFaulty is the recovery-copilot's "rank by
// faultyness" primary sort key. RDs with zero UpToDate copies are
// the ones that can't self-heal (DRBD needs one good replica to
// seed from), so they go first; RDs with 1+ UpToDate copies follow.
// Returns 0 when both belong to RDs with identical UpToDate counts —
// the caller falls back to the deterministic Name+NodeName tiebreak.
func compareResourceFaulty(stats map[string]rdFaultyStats, a, b *apiv1.ResourceWithVolumes) int {
	au := stats[a.Name].upToDate
	bu := stats[b.Name].upToDate

	if au != bu {
		return au - bu
	}

	return 0
}

// boolQuery parses a `?key=true|1|yes|on` query param. Mirrors
// strconv.ParseBool's accepted forms plus the python-linstor CLI's
// `yes` / `on` shorthands, since both wire dialects are observed.
// Empty / unparseable / explicit-false returns false.
func boolQuery(r *http.Request, key string) bool {
	v := strings.TrimSpace(r.URL.Query().Get(key))
	if v == "" {
		return false
	}

	switch strings.ToLower(v) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	}

	return false
}

// paginateResources applies golinstor's ListOpts.{Offset,Limit}
// query params. Mirrors paginateSnapshots — same semantics, same
// "silent empty past the end" behaviour.
func paginateResources(r *http.Request, in []apiv1.ResourceWithVolumes) []apiv1.ResourceWithVolumes {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	if offset >= len(in) {
		return []apiv1.ResourceWithVolumes{}
	}

	out := in[offset:]

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	// Belt + braces: keep the wire envelope as `[]` not `null` even if
	// a future caller hands in a nil slice (matches paginateSnapshots).
	if out == nil {
		return []apiv1.ResourceWithVolumes{}
	}

	return out
}

// vdSizeIndex builds a {rdName → {volumeNumber → sizeKib}} lookup so
// annotateSyncProgress can compute a sync % without per-volume RD
// fetches. One ListAll over the store (the RD set is small relative
// to Resource count) is cheaper than N round-trips.
func vdSizeIndex(ctx context.Context, s *Server, resList []apiv1.Resource) map[string]map[int32]int64 {
	out := map[string]map[int32]int64{}

	seen := map[string]struct{}{}

	for i := range resList {
		seen[resList[i].Name] = struct{}{}
	}

	for rd := range seen {
		vds, err := s.Store.VolumeDefinitions().List(ctx, rd)
		if err != nil {
			continue
		}

		idx := map[int32]int64{}
		for j := range vds {
			idx[vds[j].VolumeNumber] = vds[j].SizeKib
		}

		out[rd] = idx
	}

	return out
}

// annotateSyncProgress decorates each Volume.State.DiskState with a
// "(N%)" suffix when OutOfSyncKib > 0 and the VD size is known.
// Matches the CDI/upstream-LINSTOR rendering style — `linstor r list`
// users see e.g. `Inconsistent(45%)` instead of a stale `Inconsistent`
// label that gives no progress feedback. UpToDate replicas are left
// alone since the suffix would just be `(100%)`.
func annotateSyncProgress(volumes []apiv1.Volume, sizes map[int32]int64) []apiv1.Volume {
	if len(volumes) == 0 {
		return volumes
	}

	out := make([]apiv1.Volume, len(volumes))
	copy(out, volumes)

	for i := range out {
		size := sizes[out[i].VolumeNumber]
		if size <= 0 || out[i].State.OutOfSyncKib <= 0 || out[i].State.DiskState == "" {
			continue
		}

		percent := max(0, 100-(out[i].State.OutOfSyncKib*100)/size)

		out[i].State.DiskState = fmt.Sprintf("%s(%d%%)", out[i].State.DiskState, percent)
	}

	return out
}

// multiValueQuery returns the union of all values for a query
// parameter, supporting both wire dialects:
//
//   - `?key=a,b,c`        (comma-joined — golinstor)
//   - `?key=a&key=b&key=c` (repeat-key — python-linstor urlencode(doseq=True))
//
// Empty result = no filter on this key.
func multiValueQuery(r *http.Request, key string) []string {
	values := r.URL.Query()[key]
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))

	for _, v := range values {
		out = append(out, splitCSV(v)...)
	}

	return out
}

// splitCSV parses the comma-separated query value, trimming whitespace
// and dropping empty segments. Empty input means no filter.
func splitCSV(value string) []string {
	if value == "" {
		return nil
	}

	var out []string

	for s := range strings.SplitSeq(value, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}

	return out
}

// matchAnyFold reports whether candidate matches any of needles
// case-insensitively. Empty needles means "no filter — accept".
func matchAnyFold(needles []string, candidate string) bool {
	if len(needles) == 0 {
		return true
	}

	for _, n := range needles {
		if strings.EqualFold(n, candidate) {
			return true
		}
	}

	return false
}
