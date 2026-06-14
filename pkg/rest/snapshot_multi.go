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
	"crypto/rand"
	"encoding/hex"
	"math"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerSnapshotMulti wires `POST /v1/actions/snapshot/multi`. The
// linstor CLI's `snapshot create-multiple` and a few operator
// bookkeeping flows (consistency groups, scheduled-snapshot jobs)
// fan out one snapshot per (rd, snap, nodes) tuple here. Best-
// effort: per-entry outcomes land in the ApiCallRc envelope so
// partial successes are visible — matches upstream LINSTOR's
// behaviour (the controller cannot two-phase-commit across the
// store + satellite reconciler chain either).
func (s *Server) registerSnapshotMulti(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/actions/snapshot/multi",
		s.requireStore(s.handleSnapshotCreateMulti))
}

// multiSnapshotCreateBody is the wire shape upstream LINSTOR's
// `linstor snapshot create-multiple` uses for the batch endpoint.
// Each entry is the same per-RD POST shape — fanned out one
// Snapshot at a time.
type multiSnapshotCreateBody struct {
	Snapshots []multiSnapshotCreateEntry `json:"snapshots"`
}

// multiSnapshotCreateEntry is one per-RD slot in the multi-create
// request. Mirrors apiv1.Snapshot's JSON keys so callers can build a
// single envelope without learning two wire shapes.
type multiSnapshotCreateEntry struct {
	ResourceName string                    `json:"resource_name"`
	Name         string                    `json:"name"`
	Nodes        []string                  `json:"nodes,omitempty"`
	Props        map[string]string         `json:"props,omitempty"`
	Flags        []string                  `json:"flags,omitempty"`
	VolumeDefs   []apiv1.SnapshotVolumeDef `json:"volume_definitions,omitempty"`
}

// handleSnapshotCreateMulti POSTs one snapshot per entry. The wire
// path matches upstream LINSTOR's `/v1/actions/snapshot/multi`
// action shape. Per-entry errors land in the ApiCallRc envelope
// rather than aborting the batch.
//
// b353: every entry in the batch is stamped with the same
// crypto/rand-generated GroupID + SuspendIO=true so the
// controller-side SnapshotReconciler gates phase advancement on
// the UNION of every sibling's targeted nodes — a single
// suspend-io broadcast spans the whole multi-RD batch, giving
// cross-RD point-in-time consistency (DB + WAL on separate RDs
// snapshot at the same instant rather than at independently
// drifting per-RD suspend windows).
func (s *Server) handleSnapshotCreateMulti(w http.ResponseWriter, r *http.Request) {
	var body multiSnapshotCreateBody

	if !decodeJSON(w, r, &body) {
		return
	}

	if len(body.Snapshots) == 0 {
		writeError(w, http.StatusBadRequest, "snapshots list is required and must be non-empty")

		return
	}

	groupID, err := newSnapshotGroupID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "generate snapshot group id: "+err.Error())

		return
	}

	results := make([]apiv1.APICallRc, 0, len(body.Snapshots))

	// groupSize is the denominator the controller-side
	// SnapshotReconciler uses to decide the group is fully ASSEMBLED
	// before opening the suspend-io barrier. The siblings are created
	// sequentially below (one Store.Create per entry, with hydration +
	// an offline pre-check between each), so the last sibling's CRD can
	// land seconds after the first; stamping the expected count lets
	// the controller hold every sibling at Phase 0 until all are
	// present and then flip SuspendIO on the whole group in one pass —
	// bounding the suspend-entry slip under the consistency budget
	// (Bug 046 / Bug-353 atomicity fix).
	//
	// Clamp the count to int32 defensively (gosec G115). A real group
	// has a handful of RDs; the clamp only matters for a pathological
	// request and a saturated denominator still gates correctly (the
	// controller never sees more siblings than were created).
	groupSize := clampInt32(len(body.Snapshots))

	for i := range body.Snapshots {
		results = append(results, s.createOneFromMulti(r.Context(), &body.Snapshots[i], groupID, groupSize))
	}

	writeJSON(w, http.StatusCreated, results)
}

// clampInt32 narrows a non-negative count to int32, saturating at
// math.MaxInt32 rather than wrapping (gosec G115). The multi-snapshot
// batch size is always a small positive number in practice; the clamp
// only guards the pathological case and never produces a negative
// denominator the controller's group-assembled gate could misread.
func clampInt32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}

	if n < 0 {
		return 0
	}

	return int32(n)
}

// newSnapshotGroupID returns a 128-bit hex group identifier
// produced from crypto/rand. The value is opaque to the
// controller (it only does string equality), so any
// collision-resistant random encoding works — hex keeps the label
// value DNS-1123-friendly (kubernetes label values reject `+/=`
// that base64 would emit).
func newSnapshotGroupID() (string, error) {
	var buf [16]byte

	_, err := rand.Read(buf[:])
	if err != nil {
		return "", errors.Wrap(err, "read crypto/rand")
	}

	return hex.EncodeToString(buf[:]), nil
}

// createOneFromMulti turns one multi-entry into the existing
// per-snapshot create pipeline and packages the result as an
// ApiCallRc. Validation failures + store errors all land in the
// returned envelope rather than 4xx the whole batch.
//
// `groupID` is the crypto/rand-generated transactional-batch
// handle stamped onto every entry's apiv1.Snapshot.GroupID. The
// store-side wireToCRDSnapshot mirrors it onto the CRD's
// Spec.GroupID + a metadata.label so the controller-side
// SnapshotReconciler can list same-Group siblings via a label
// selector. b353.
func (s *Server) createOneFromMulti(
	ctx context.Context, entry *multiSnapshotCreateEntry, groupID string, groupSize int32,
) apiv1.APICallRc {
	if entry.ResourceName == "" || entry.Name == "" {
		return apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "snapshot create-multiple entry needs resource_name + name",
		}
	}

	snap := apiv1.Snapshot{
		Name:              entry.Name,
		ResourceName:      entry.ResourceName,
		Nodes:             entry.Nodes,
		Props:             entry.Props,
		Flags:             entry.Flags,
		VolumeDefinitions: entry.VolumeDefs,
		GroupID:           groupID,
		GroupSize:         groupSize,
	}

	err := s.hydrateSnapshotFromRD(ctx, &snap, entry.ResourceName)
	if err != nil {
		return multiSnapshotEntryErr(entry, err)
	}

	// Fail-fast offline pre-check, same contract as the per-RD create
	// path: refuse before stamping SuspendIO so a snapshot that can
	// never complete doesn't freeze the reachable replicas. In a
	// multi-RD batch the per-entry refusal lands in the ApiCallRc
	// envelope (best-effort batch semantics) — the controller's
	// transactional GroupID gate then resumes any sibling that did
	// get suspended once this entry never acks, but refusing here
	// avoids the freeze entirely for the offline-target entry.
	if offline := s.offlineTargetNodes(ctx, snap.Nodes); len(offline) > 0 {
		return apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: entry.ResourceName + "/" + entry.Name +
				": targeted node(s) " + strings.Join(offline, ", ") +
				" are offline; retry once they reconnect",
		}
	}

	err = s.Store.Snapshots().Create(ctx, &snap)
	if err != nil {
		return multiSnapshotEntryErr(entry, err)
	}

	return apiv1.APICallRc{
		RetCode: maskInfo,
		Message: "snapshot created: " + entry.ResourceName + "/" + entry.Name,
	}
}

// multiSnapshotEntryErr packages a per-entry failure into an ApiCallRc
// envelope, routing the underlying error string through
// `scrubImplDetails` so backend identifiers (etcd / apimachinery /
// k8s.io / `*.blockstor.cozystack.io`) never reach the wire. Bug 199 wrapped
// `writeError` at the envelope-emission seam, but `createOneFromMulti`
// returns its envelopes to `handleSnapshotCreateMulti` which calls
// `writeJSON` directly — that path bypasses the writeError-level
// scrub. Bug 200 plugs the multi-create batch path by centralising
// the inline `APICallRc{Message: ...err.Error()}` construction here:
// every multi-create failure goes through this single seam, every
// future addition to `createOneFromMulti` reuses it for free.
//
// The "rd/snap: " per-entry prefix is operator context and is NOT
// scrubbed — only the underlying err string is rewritten, so an
// already-scrubbed literal ("snapshot already exists") passes
// through byte-for-byte.
func multiSnapshotEntryErr(entry *multiSnapshotCreateEntry, err error) apiv1.APICallRc {
	return apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: entry.ResourceName + "/" + entry.Name + ": " + scrubImplDetails(err.Error()),
	}
}
