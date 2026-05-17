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

package v1

// AutoPlaceRequest is the body upstream LINSTOR (and golinstor) expect on
// `POST /v1/resource-definitions/{rd}/autoplace`. Mirrors golinstor exactly.
//
// Bug 237: `copy_all_snaps` / `snap_names` are 1.27-era fields python-
// linstor's `_require_version()` opened up after Bug 222's wire-version
// bump. Accepted-and-no-op for now; the snapshot-fanout / restore data-
// plane orchestration lands separately. Without these fields the
// DisallowUnknownFields decoder in `decodeAutoplaceBody` 400's and the
// CLI's snapshot-restore-then-autoplace path crashes before any
// placement runs.
//
// TODO(bug-237-followup): wire `copy_all_snaps` / `snap_names` into the
// snapshot-restore + autoplace orchestration when the satellite-side
// snapshot fan-out lands.
type AutoPlaceRequest struct {
	DisklessOnRemaining bool             `json:"diskless_on_remaining,omitempty"`
	SelectFilter        AutoSelectFilter `json:"select_filter,omitzero"`
	LayerList           []string         `json:"layer_list,omitempty"`
	CopyAllSnaps        bool             `json:"copy_all_snaps,omitempty"`
	SnapNames           []string         `json:"snap_names,omitempty"`
}

// ResourceCreate is the body upstream LINSTOR uses on
// `POST /v1/resource-definitions/{rd}/resources`. The Resources field is a
// list because callers can place a resource on multiple nodes in one call.
//
// Bug 237: `drbd_tcp_ports` / `drbd_tcp_port_count` / `copy_all_snaps` /
// `snap_names` are 1.27-era python-linstor fields gated open by Bug 222's
// wire-version bump. Accepted-and-no-op so the DisallowUnknownFields
// decoder in `decodeResourceCreateBody` stops 400'ing. Real wire-through:
//
//   - DrbdTCPPorts / DrbdTCPPortCount: a future placer hook will reserve
//     these (vs. allocating fresh) so the operator can pin specific DRBD
//     TCP ports per replica. Today the existing per-RD port allocator
//     owns this end-to-end, so accepting the hint is a no-op.
//   - CopyAllSnaps / SnapNames: same snapshot-fanout follow-up as
//     AutoPlaceRequest above.
//
// TODO(bug-237-followup): wire DrbdTCPPorts / DrbdTCPPortCount through
// the placer's port-reservation path, and CopyAllSnaps / SnapNames into
// the snapshot-fanout orchestration.
type ResourceCreate struct {
	Resource         Resource `json:"resource,omitzero"`
	LayerList        []string `json:"layer_list,omitempty"`
	DrbdNodeID       *int32   `json:"drbd_node_id,omitempty"`
	NetInterface     string   `json:"net_interface,omitempty"`
	DrbdTCPPorts     []int32  `json:"drbd_tcp_ports,omitempty"`
	DrbdTCPPortCount *int32   `json:"drbd_tcp_port_count,omitempty"`
	CopyAllSnaps     bool     `json:"copy_all_snaps,omitempty"`
	SnapNames        []string `json:"snap_names,omitempty"`
}

// ResourceMakeAvailable is the body upstream LINSTOR uses on
// `POST /v1/resource-definitions/{rd}/resources/{node}/make-available`.
// linstor-csi's `Attach` (ControllerPublishVolume) posts
// `{diskful:false}` here to either promote an existing
// TIE_BREAKER/DISKLESS witness on the target node, or create a fresh
// DISKLESS replica. Diskful=true forces a regular diskful replica
// even when a diskless one would otherwise satisfy the request.
//
// Bug 237: `drbd_tcp_ports` / `copy_all_snaps` / `snap_names` mirror the
// ResourceCreate additions — same `_require_version` gate, same accept-
// and-no-op disposition. Without them the make-available decoder 400's
// and linstor-csi's Attach can never publish the volume.
type ResourceMakeAvailable struct {
	LayerList    []string `json:"layer_list,omitempty"`
	Diskful      bool     `json:"diskful,omitempty"`
	DrbdTCPPorts []int32  `json:"drbd_tcp_ports,omitempty"`
	CopyAllSnaps bool     `json:"copy_all_snaps,omitempty"`
	SnapNames    []string `json:"snap_names,omitempty"`
}
