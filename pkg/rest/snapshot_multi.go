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
	"net/http"

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
func (s *Server) handleSnapshotCreateMulti(w http.ResponseWriter, r *http.Request) {
	var body multiSnapshotCreateBody

	if !decodeJSON(w, r, &body) {
		return
	}

	if len(body.Snapshots) == 0 {
		writeError(w, http.StatusBadRequest, "snapshots list is required and must be non-empty")

		return
	}

	results := make([]apiv1.APICallRc, 0, len(body.Snapshots))

	for i := range body.Snapshots {
		results = append(results, s.createOneFromMulti(r.Context(), &body.Snapshots[i]))
	}

	writeJSON(w, http.StatusCreated, results)
}

// createOneFromMulti turns one multi-entry into the existing
// per-snapshot create pipeline and packages the result as an
// ApiCallRc. Validation failures + store errors all land in the
// returned envelope rather than 4xx the whole batch.
func (s *Server) createOneFromMulti(ctx context.Context, entry *multiSnapshotCreateEntry) apiv1.APICallRc {
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
	}

	err := s.hydrateSnapshotFromRD(ctx, &snap, entry.ResourceName)
	if err != nil {
		return multiSnapshotEntryErr(entry, err)
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
// k8s.io / `*.blockstor.io`) never reach the wire. Bug 199 wrapped
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
