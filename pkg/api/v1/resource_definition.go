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

// ResourceDefinition mirrors the upstream `ResourceDefinition` shape. Each
// PVC ends up as one ResourceDefinition with one or more VolumeDefinitions.
//
// Phase 10.4: `Annotations` carries the K8s-native metadata.annotations
// of the underlying CRD. Used by the REST KV-store rewrite to surface
// `blockstor.io/csi-volume-data` (linstor-csi per-PVC JSON metadata)
// without going through the deprecated KVEntry CRD. Not part of
// upstream LINSTOR's wire shape — golinstor ignores unknown JSON
// fields, so the new key flows through transparently.
type ResourceDefinition struct {
	Name              string            `json:"name"`
	ExternalName      string            `json:"external_name,omitempty"`
	ResourceGroupName string            `json:"resource_group_name,omitempty"`
	Props             map[string]string `json:"props,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	Flags             []string          `json:"flags,omitempty"`
	LayerData         []ResourceLayer   `json:"layer_data,omitempty"`
	// LayerStack is the layer composition (e.g. ["DRBD","STORAGE"]).
	// Wire field name matches upstream LINSTOR (`resource_definition.layer_stack`).
	LayerStack []string `json:"layer_stack,omitempty"`
	UUID       string   `json:"uuid,omitempty"`
	// VolumeDefinitions is the inline VD list emitted when the caller
	// asks for `GET /v1/resource-definitions?with_volume_definitions=true`.
	// Upstream LINSTOR returns RD + its VDs in a single round-trip on
	// this query; python-linstor-client's `vd l` reads from here
	// (linstorapi.py::resource_dfn_list). Omitted by default so the
	// plain `rd l` view stays compact.
	VolumeDefinitions []VolumeDefinition `json:"volume_definitions,omitempty"`

	// EffectiveProps is the merged Controller→RG→RD view of the
	// property bag for this RD. Populated on GET and List read paths;
	// ignored on writes. Mirrors `Resource.EffectiveProps` (the
	// `/v1/view/resources` shape) so callers that already understand
	// scope tags can read inherited values with the origin recorded.
	//
	// Inherited-prop visibility (Bug-105 follow-up): `linstor rd
	// list-properties` (`rd lp`) reads the bare `props` map for its
	// table, so the read-side handlers also merge inherited Controller
	// / RG keys into `Props` (without overwriting locally-set RD keys).
	// Operators that `c sp <key> <value>` then `rd lp <rd>` now see
	// the inherited entry rather than thinking the controller-scope
	// prop never propagated.
	EffectiveProps EffectiveProperties `json:"effective_props,omitempty"`
}

// Layer kind constants — the strings LINSTOR uses on the wire.
const (
	LayerKindDRBD    = "DRBD"
	LayerKindLUKS    = "LUKS"
	LayerKindStorage = "STORAGE"
)

// DefaultLayerStack returns the layer stack used when the RD spec
// (and its parent RG) leave it empty. Matches upstream LINSTOR's
// default — full DRBD-over-STORAGE replication.
func DefaultLayerStack() []string {
	return []string{LayerKindDRBD, LayerKindStorage}
}

// ResourceLayer is the per-layer descriptor on a ResourceDefinition. We
// store the discriminator and an opaque payload — golinstor's structs vary
// per layer (DRBD, LUKS, …) and we don't yet need to interpret them.
//
// Children + NameSuffix mirror upstream's `ResourceLayerData`. The
// Python CLI's `rsc.layer_data.layer_stack` walks Children recursively;
// without these, `linstor r list` fails with AttributeError on
// `layer_data is None` because the JSON key is omitted entirely.
//
// Drbd is populated only on DRBD-layer entries. The Python CLI reads
// `layer_data.drbd_resource.connections` to color disconnected peers
// red and to decide `--faulty` inclusion — empty `drbd` produces an
// "Ok" Conns column regardless of actual peer state.
type ResourceLayer struct {
	Type       string                `json:"type"`
	NameSuffix string                `json:"rsc_name_suffix,omitempty"`
	Children   []ResourceLayer       `json:"children,omitempty"`
	Drbd       *DrbdResourceLayer    `json:"drbd,omitempty"`
	Storage    *StorageResourceLayer `json:"storage,omitempty"`
	Data       map[string]any        `json:"data,omitempty"`
}

// StorageResourceLayer carries STORAGE-layer-specific runtime state
// surfaced to the REST CLI. Subset of upstream LINSTOR's
// `StorageRscData` — we only fill in the fields the CLI's
// `linstor r list` Layers column and `--faulty` filter actually read.
//
// ProviderKind discriminates between a real backing (LVM / LVM_THIN /
// ZFS / ZFS_THIN / FILE / …) and a DISKLESS witness — the CLI's
// rsc_state derivation never special-cases the STORAGE layer for
// DISKLESS, so the layer must still be present in the children chain;
// otherwise `Layers` shows `DRBD` alone instead of upstream's
// `DRBD,STORAGE`. F19.
//
// StorageVolumes is empty on DISKLESS replicas (no backing device);
// diskful replicas carry one entry per volume with `volume_number` +
// `device_path` (and any size hints the satellite has reported).
type StorageResourceLayer struct {
	ProviderKind   string               `json:"provider_kind,omitempty"`
	StorageVolumes []StorageVolumeLayer `json:"storage_volumes,omitempty"`
}

// StorageVolumeLayer is one entry of StorageResourceLayer.StorageVolumes.
// Wire shape matches upstream LINSTOR's `StorageVolume` — the Python
// CLI's `linstor v l` reads `device_path` here as a fallback when the
// per-Volume DRBD layer omits it.
//
// Bug 112: `allocated_size_kib` MUST always be emitted as an int.
// The Python CLI walks `layer_data_list[i].data.storage_volumes[j].
// allocated_size_kib` in addition to the top-level `volumes[i].
// allocated_size_kib`; a Go-zero collapsing to absent under
// `omitempty` produced the same `None` crash in `n describe` as the
// top-level path. Wire contract: present as int, default 0.
type StorageVolumeLayer struct {
	VolumeNumber     int32  `json:"volume_number"`
	DevicePath       string `json:"device_path,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	DiskState        string `json:"disk_state,omitempty"`
}

// DrbdResourceLayer carries DRBD-layer-specific runtime state surfaced
// to the REST CLI. Subset of upstream LINSTOR's `DrbdRscData` — we
// only fill in the fields the CLI / linstor-csi actually read.
type DrbdResourceLayer struct {
	// TCPPorts is the list of DRBD listen ports for this replica;
	// upstream surfaces this as a list because multi-volume / proxy
	// setups can advertise more than one. We typically have exactly
	// one entry (the per-replica DRBDPort).
	TCPPorts []int32 `json:"tcp_ports,omitempty"`

	// Connections maps peer node name → per-peer connection state.
	// Empty map = no peers (single-replica setup) — distinct from
	// missing field which the Python CLI interprets as "no DRBD
	// layer present".
	Connections map[string]DrbdConnection `json:"connections,omitempty"`

	// DrbdVolumes is the per-volume disk-state surface the Python
	// CLI reads for the `linstor r l` State column — without this
	// field populated the CLI falls back to a literal "Created"
	// regardless of the volumes[i].state.disk_state we'd already
	// computed. Mirrors upstream LINSTOR's `DrbdRscData.drbdVlmList`.
	DrbdVolumes []DrbdVolume `json:"drbd_volumes,omitempty"`
}

// DrbdVolume is one volume's DRBD-layer-specific state. The Python
// CLI reads `disk_state` here for its State column, falling back to
// "Created" when the field is missing.
type DrbdVolume struct {
	VolumeNumber int32  `json:"volume_number"`
	DiskState    string `json:"disk_state,omitempty"`
	DevicePath   string `json:"device_path,omitempty"`
}

// DrbdConnection is one peer's connection state as reported by
// `drbdsetup events2` on the local replica. Wire shape matches
// upstream's `DrbdConnection` exactly so the Python CLI's
// `conn.connected` / `conn.message` properties parse without
// translation.
type DrbdConnection struct {
	Connected bool   `json:"connected"`
	Message   string `json:"message,omitempty"`

	// ReplicationState is the DRBD-9 replication state for this
	// peer as reported by `drbdsetup events2 --statistics`
	// peer-device frames: Established / SyncSource / SyncTarget /
	// PausedSync* / VerifyS / VerifyT / Ahead / Behind / Off /
	// WFBitMap* / WFSyncUUID / StartingSyncS / StartingSyncT.
	// The Python CLI's `linstor v list` Repl column reads this to
	// summarise per-volume replication progress.
	ReplicationState string `json:"replication_state,omitempty"`
}

// VolumeDefinition is one volume slot inside a ResourceDefinition.
type VolumeDefinition struct {
	VolumeNumber int32             `json:"volume_number"`
	SizeKib      int64             `json:"size_kib"`
	Props        map[string]string `json:"props,omitempty"`
	Flags        []string          `json:"flags,omitempty"`
	UUID         string            `json:"uuid,omitempty"`
}

// VolumeDefinitionCreate is the upstream POST envelope for
// /v1/resource-definitions/{rd}/volume-definitions. Mirrors golinstor.
type VolumeDefinitionCreate struct {
	VolumeDefinition VolumeDefinition `json:"volume_definition"`
	DrbdMinorNumber  int32            `json:"drbd_minor_number,omitempty"`
}

// ResourceDefinitionCreate is the body upstream LINSTOR (and golinstor)
// expect on `POST /v1/resource-definitions`. It wraps the RD plus optional
// per-create fields like the DRBD secret.
type ResourceDefinitionCreate struct {
	ResourceDefinition ResourceDefinition `json:"resource_definition"`
	DrbdSecret         string             `json:"drbd_secret,omitempty"`
	ExternalName       string             `json:"external_name,omitempty"`

	// LayerList is the top-level layer composition the upstream
	// golinstor SDK and several non-python clients populate on RD-
	// create (the field surfaces at the same level as
	// `resource_definition`, NOT inside it — see Bug 116). Accepted
	// as a peer of `resource_definition.layer_stack` and
	// `resource_definition.layer_data`; the wire handler merges all
	// three views before running validation so no shape silently
	// bypasses the LUKS-prereq gate.
	LayerList []string `json:"layer_list,omitempty"`

	// OverrideProps mirrors GenericPropsModify — golinstor stuffs RG
	// spawn defaults into this when creating an RD via spawn, so the
	// REST handler must accept it even if we don't yet diff the values.
	OverrideProps   map[string]string `json:"override_props,omitempty"`
	DeleteProps     []string          `json:"delete_props,omitempty"`
	DeleteNamespace []string          `json:"delete_namespaces,omitempty"`
}
