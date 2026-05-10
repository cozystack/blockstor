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
type Snapshot struct {
	Name              string              `json:"name"`
	ResourceName      string              `json:"resource_name"`
	Nodes             []string            `json:"nodes,omitempty"`
	Props             map[string]string   `json:"props,omitempty"`
	Annotations       map[string]string   `json:"annotations,omitempty"`
	Flags             []string            `json:"flags,omitempty"`
	VolumeDefinitions []SnapshotVolumeDef `json:"volume_definitions,omitempty"`
	Snapshots         []SnapshotPerNode   `json:"snapshots,omitempty"`
	UUID              string              `json:"uuid,omitempty"`
}

// SnapshotVolumeDef is one volume slot inside a Snapshot.
type SnapshotVolumeDef struct {
	VolumeNumber int32 `json:"volume_number"`
	SizeKib      int64 `json:"size_kib"`
}

// SnapshotPerNode is the per-node materialisation of a Snapshot.
type SnapshotPerNode struct {
	SnapshotName    string `json:"snapshot_name"`
	NodeName        string `json:"node_name"`
	CreateTimestamp int64  `json:"create_timestamp,omitempty"`
	UUID            string `json:"uuid,omitempty"`
}
