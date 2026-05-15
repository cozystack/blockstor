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

// Snapshot mirrors the upstream `Snapshot` shape. linstor-csi calls
// /v1/view/snapshots in its CSI ListSnapshots loop.
//
// Phase 10.4: `Annotations` carries the K8s-native
// metadata.annotations of the underlying CRD. Used by the REST
// KV-store rewrite to surface `blockstor.io/csi-shipping-data`
// (linstor-csi-snapshot-shipper per-snapshot bookkeeping)
// without going through the deprecated KVEntry CRD. Not part of
// upstream LINSTOR's wire shape — golinstor ignores unknown JSON
// fields, so the new key flows through transparently.
//
// F20 (CLI-parity): the per-snapshot view also carries the
// `snapshot_definition_props` and `resource_definition_props` maps
// upstream surfaces — `linstor backup` and the schedule tooling
// read inherited RD props through the snapshot DTO rather than
// re-fetching the parent RD. ResourceDefinitionProps is a
// *snapshot-time* copy of the parent RD's props, so a later RD
// prop mutation does not retroactively change what `linstor s l`
// reports for already-taken snapshots.
type Snapshot struct {
	Name                    string              `json:"name"`
	ResourceName            string              `json:"resource_name"`
	Nodes                   []string            `json:"nodes,omitempty"`
	Props                   map[string]string   `json:"props,omitempty"`
	Annotations             map[string]string   `json:"annotations,omitempty"`
	Flags                   []string            `json:"flags,omitempty"`
	VolumeDefinitions       []SnapshotVolumeDef `json:"volume_definitions,omitempty"`
	Snapshots               []SnapshotPerNode   `json:"snapshots,omitempty"`
	SnapshotDefinitionProps map[string]string   `json:"snapshot_definition_props,omitempty"`
	ResourceDefinitionProps map[string]string   `json:"resource_definition_props,omitempty"`
	UUID                    string              `json:"uuid,omitempty"`
}

// SnapshotVolumeDef is one volume slot inside a Snapshot. F20:
// `VolumeDefinitionProps` is the snapshot-time copy of the parent
// RD's per-volume props — exposed via the snapshot DTO so the
// CLI doesn't need a second round-trip to the RD endpoint.
type SnapshotVolumeDef struct {
	VolumeNumber          int32             `json:"volume_number"`
	SizeKib               int64             `json:"size_kib"`
	VolumeDefinitionProps map[string]string `json:"volume_definition_props,omitempty"`
}

// SnapshotPerNode is the per-node materialisation of a Snapshot.
// F20: `Flags` carries the upstream LINSTOR per-node snapshot
// flags (FAILED_DEPLOYMENT, FAILED_DISCONNECT, ...); blockstor
// does not yet derive these from satellite state, so the field is
// a passthrough today (callers may set, GET surfaces). `SnapshotVolumes`
// carries one entry per volume the snapshot captured on this
// node — `linstor backup` and the snapshot-shipping tooling
// inspect the per-volume `state` to decide which volume to ship.
type SnapshotPerNode struct {
	SnapshotName    string           `json:"snapshot_name"`
	NodeName        string           `json:"node_name"`
	CreateTimestamp int64            `json:"create_timestamp,omitempty"`
	Flags           []string         `json:"flags,omitempty"`
	SnapshotVolumes []SnapshotVolume `json:"snapshot_volumes,omitempty"`
	UUID            string           `json:"uuid,omitempty"`
}

// SnapshotVolume is one per-node, per-volume slot inside a
// SnapshotPerNode entry. Mirrors upstream's `SnapshotVolumeNode`
// (vlm_nr + state); `linstor backup` reads `state` to surface the
// satellite-reported snapshot status. blockstor leaves `State`
// blank today — the CRD does not yet track per-volume per-node
// snapshot state — but the slot is still emitted so the CLI
// table renders the volume_number column.
type SnapshotVolume struct {
	VolumeNumber int32  `json:"vlm_nr"`
	State        string `json:"state,omitempty"`
}

// Snapshot-definition flag constants on the wire. Match the upstream
// LINSTOR `FLAG_*` literals python-linstor-client reads in
// snapshot_cmds.show — the State column resolves to:
//
//	FAILED_DEPLOYMENT  -> "Failed"
//	FAILED_DISCONNECT  -> "Satellite disconnected"
//	SUCCESSFUL         -> "Successful"
//	(anything else)    -> "Incomplete"
//
// blockstor used to ship the `FAILED` shorthand (terminal-error stamp
// from the satellite snapshot reconciler); we keep that one and ALSO
// stamp `SUCCESSFUL` on the wire view once every diskful peer reports
// Ready. TIE_BREAKER + DISKLESS replicas hold no data and never take
// the snapshot, so they MUST be excluded from the "every replica is
// ready" denominator — otherwise the State column hangs in Incomplete
// forever on auto-placed 2-diskful + 1-tiebreaker topologies.
const (
	SnapshotFlagFailedDeployment = "FAILED_DEPLOYMENT"
	SnapshotFlagFailedDisconnect = "FAILED_DISCONNECT"
	SnapshotFlagSuccessful       = "SUCCESSFUL"
)
