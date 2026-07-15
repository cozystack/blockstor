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

// Package linstormigrate converts a LINSTOR k8s-backend database dump
// (the `*.internal.linstor.linbit.com` CRDs LINSTOR uses as its SQL
// tables when running with the k8s connector) into blockstor
// `blockstor.cozystack.io/v1alpha1` CRD manifests.
//
// The input is the directory produced by:
//
//	kubectl get crds | grep -o ".*.internal.linstor.linbit.com" | \
//	  xargs -I{} sh -c "kubectl get {} -ojson > {}.json"
//
// Each file is a v1.List whose items carry one SQL row in `spec` —
// the schema mirrors LINSTOR's relational tables (RESOURCE_DEFINITIONS,
// VOLUME_DEFINITIONS, LAYER_DRBD_RESOURCES, PROPS_CONTAINERS, ...).
package linstormigrate

// NodeRow is one row of the NODES table.
type NodeRow struct {
	NodeName    string `json:"node_name"`     // uppercase key
	NodeDspName string `json:"node_dsp_name"` // display case
	NodeFlags   int64  `json:"node_flags"`
	NodeType    int32  `json:"node_type"`
	UUID        string `json:"uuid"`
}

// NodeNetInterfaceRow is one row of the NODE_NET_INTERFACES table.
type NodeNetInterfaceRow struct {
	NodeName         string `json:"node_name"`
	NodeNetName      string `json:"node_net_name"`
	NodeNetDspName   string `json:"node_net_dsp_name"`
	InetAddress      string `json:"inet_address"`
	StltConnPort     *int32 `json:"stlt_conn_port,omitempty"`
	StltConnEncrType string `json:"stlt_conn_encr_type,omitempty"`
	UUID             string `json:"uuid"`
}

// NodeStorPoolRow is one row of the NODE_STOR_POOL table.
type NodeStorPoolRow struct {
	NodeName            string `json:"node_name"`
	PoolName            string `json:"pool_name"`
	DriverName          string `json:"driver_name"`
	FreeSpaceMgrName    string `json:"free_space_mgr_name,omitempty"`
	FreeSpaceMgrDspName string `json:"free_space_mgr_dsp_name,omitempty"`
	ExternalLocking     bool   `json:"external_locking,omitempty"`
	UUID                string `json:"uuid"`
}

// StorPoolDefinitionRow is one row of the STOR_POOL_DEFINITIONS table.
type StorPoolDefinitionRow struct {
	PoolName    string `json:"pool_name"`
	PoolDspName string `json:"pool_dsp_name"`
	UUID        string `json:"uuid"`
}

// ResourceGroupRow is one row of the RESOURCE_GROUPS table. The list
// fields are JSON-encoded arrays stored as strings (`"[\"data\"]"`).
type ResourceGroupRow struct {
	ResourceGroupName    string `json:"resource_group_name"`
	ResourceGroupDspName string `json:"resource_group_dsp_name"`
	Description          string `json:"description,omitempty"`
	LayerStack           string `json:"layer_stack,omitempty"`
	ReplicaCount         int32  `json:"replica_count,omitempty"`
	NodeNameList         string `json:"node_name_list,omitempty"`
	PoolName             string `json:"pool_name,omitempty"`
	PoolNameDiskless     string `json:"pool_name_diskless,omitempty"`
	DoNotPlaceWithRsc    string `json:"do_not_place_with_rsc_list,omitempty"`
	ReplicasOnSame       string `json:"replicas_on_same,omitempty"`
	ReplicasOnDifferent  string `json:"replicas_on_different,omitempty"`
	AllowedProviderList  string `json:"allowed_provider_list,omitempty"`
	UUID                 string `json:"uuid"`
}

// VolumeGroupRow is one row of the VOLUME_GROUPS table.
type VolumeGroupRow struct {
	ResourceGroupName string `json:"resource_group_name"`
	VlmNr             int32  `json:"vlm_nr"`
	Flags             int64  `json:"flags"`
	UUID              string `json:"uuid"`
}

// ResourceDefinitionRow is one row of the RESOURCE_DEFINITIONS table.
// Rows with SnapshotName != "" are snapshot definitions, not RDs.
type ResourceDefinitionRow struct {
	ResourceName      string `json:"resource_name"`
	ResourceDspName   string `json:"resource_dsp_name"`
	ResourceGroupName string `json:"resource_group_name,omitempty"`
	SnapshotName      string `json:"snapshot_name,omitempty"`
	SnapshotDspName   string `json:"snapshot_dsp_name,omitempty"`
	ResourceFlags     int64  `json:"resource_flags"`
	LayerStack        string `json:"layer_stack,omitempty"`
	ParentUUID        string `json:"parent_uuid,omitempty"`
	UUID              string `json:"uuid"`
}

// VolumeDefinitionRow is one row of the VOLUME_DEFINITIONS table.
// Rows with SnapshotName != "" belong to a snapshot definition.
type VolumeDefinitionRow struct {
	ResourceName string `json:"resource_name"`
	SnapshotName string `json:"snapshot_name,omitempty"`
	VlmNr        int32  `json:"vlm_nr"`
	VlmSize      int64  `json:"vlm_size"` // KiB
	VlmFlags     int64  `json:"vlm_flags"`
	UUID         string `json:"uuid"`
}

// ResourceRow is one row of the RESOURCES table (one replica placement).
// Rows with SnapshotName != "" record a snapshot's presence on a node.
type ResourceRow struct {
	NodeName        string `json:"node_name"`
	ResourceName    string `json:"resource_name"`
	SnapshotName    string `json:"snapshot_name,omitempty"`
	ResourceFlags   int64  `json:"resource_flags"`
	CreateTimestamp int64  `json:"create_timestamp,omitempty"` // ms epoch
	UUID            string `json:"uuid"`
}

// VolumeRow is one row of the VOLUMES table.
type VolumeRow struct {
	NodeName     string `json:"node_name"`
	ResourceName string `json:"resource_name"`
	SnapshotName string `json:"snapshot_name,omitempty"`
	VlmNr        int32  `json:"vlm_nr"`
	VlmFlags     int64  `json:"vlm_flags"`
	UUID         string `json:"uuid"`
}

// PropsContainerRow is one row of the PROPS_CONTAINERS table — a single
// (instance, key) = value property. Instance paths look like
// `/RSC_DFNS/<RSC>`, `/RSCS/<NODE>/<RSC>`, `/VLM_DFNS/<RSC>/<VLM>`,
// `/SNAPS/<NODE>/<RSC>/<SNAP>`, `/NODES/<NODE>`, `/RSC_GRPS/<RG>`,
// `/STOR_POOLS/<NODE>/<POOL>`? (observed shapes vary), `/CTRL`.
type PropsContainerRow struct {
	PropsInstance string `json:"props_instance"`
	PropKey       string `json:"prop_key"`
	PropValue     string `json:"prop_value"`
}

// LayerResourceIDRow is one row of LAYER_RESOURCE_IDS — the join table
// that assigns every (node, resource[, snapshot]) layer instance an
// integer id the per-layer tables reference. ParentID links a child
// layer (STORAGE) to its parent (DRBD/LUKS) in the same stack.
type LayerResourceIDRow struct {
	LayerResourceID        int32  `json:"layer_resource_id"`
	LayerResourceKind      string `json:"layer_resource_kind"` // DRBD | STORAGE | LUKS | ...
	LayerResourceParentID  *int32 `json:"layer_resource_parent_id,omitempty"`
	LayerResourceSuffix    string `json:"layer_resource_suffix,omitempty"`
	LayerResourceSuspended bool   `json:"layer_resource_suspended,omitempty"`
	NodeName               string `json:"node_name"`
	ResourceName           string `json:"resource_name"`
	SnapshotName           string `json:"snapshot_name,omitempty"`
}

// LayerDrbdResourceDefinitionRow is one row of
// LAYER_DRBD_RESOURCE_DEFINITIONS — RD-scoped DRBD config.
type LayerDrbdResourceDefinitionRow struct {
	ResourceName       string `json:"resource_name"`
	ResourceNameSuffix string `json:"resource_name_suffix,omitempty"`
	SnapshotName       string `json:"snapshot_name,omitempty"`
	PeerSlots          int32  `json:"peer_slots"`
	AlStripes          int64  `json:"al_stripes"`
	AlStripeSize       int64  `json:"al_stripe_size"`
	TransportType      string `json:"transport_type,omitempty"`
	Secret             string `json:"secret,omitempty"`
	TCPPort            *int32 `json:"tcp_port,omitempty"`
}

// LayerDrbdResourceRow is one row of LAYER_DRBD_RESOURCES — per-replica
// DRBD identity (node_id).
type LayerDrbdResourceRow struct {
	LayerResourceID int32 `json:"layer_resource_id"`
	NodeID          int32 `json:"node_id"`
	PeerSlots       int32 `json:"peer_slots"`
	AlStripes       int64 `json:"al_stripes"`
	AlStripeSize    int64 `json:"al_stripe_size"`
	Flags           int64 `json:"flags"`
}

// LayerDrbdVolumeDefinitionRow is one row of
// LAYER_DRBD_VOLUME_DEFINITIONS — the per-volume DRBD minor.
type LayerDrbdVolumeDefinitionRow struct {
	ResourceName       string `json:"resource_name"`
	ResourceNameSuffix string `json:"resource_name_suffix,omitempty"`
	SnapshotName       string `json:"snapshot_name,omitempty"`
	VlmNr              int32  `json:"vlm_nr"`
	VlmMinorNr         *int32 `json:"vlm_minor_nr,omitempty"`
}

// LayerDrbdVolumeRow is one row of LAYER_DRBD_VOLUMES.
type LayerDrbdVolumeRow struct {
	LayerResourceID int32 `json:"layer_resource_id"`
	VlmNr           int32 `json:"vlm_nr"`
}

// LayerStorageVolumeRow is one row of LAYER_STORAGE_VOLUMES — which
// storage pool backs each volume of each layer instance.
type LayerStorageVolumeRow struct {
	LayerResourceID int32  `json:"layer_resource_id"`
	VlmNr           int32  `json:"vlm_nr"`
	NodeName        string `json:"node_name"`
	ProviderKind    string `json:"provider_kind"`
	StorPoolName    string `json:"stor_pool_name"`
}

// LayerLuksVolumeRow is one row of LAYER_LUKS_VOLUMES — the per-volume
// LUKS passphrase encrypted with the LINSTOR master key.
type LayerLuksVolumeRow struct {
	LayerResourceID   int32  `json:"layer_resource_id"`
	VlmNr             int32  `json:"vlm_nr"`
	EncryptedPassword string `json:"encrypted_password"`
}

// Dump is the loaded LINSTOR database.
type Dump struct {
	Nodes                        []NodeRow
	NodeNetInterfaces            []NodeNetInterfaceRow
	NodeStorPools                []NodeStorPoolRow
	StorPoolDefinitions          []StorPoolDefinitionRow
	ResourceGroups               []ResourceGroupRow
	VolumeGroups                 []VolumeGroupRow
	ResourceDefinitions          []ResourceDefinitionRow
	VolumeDefinitions            []VolumeDefinitionRow
	Resources                    []ResourceRow
	Volumes                      []VolumeRow
	PropsContainers              []PropsContainerRow
	LayerResourceIDs             []LayerResourceIDRow
	LayerDrbdResourceDefinitions []LayerDrbdResourceDefinitionRow
	LayerDrbdResources           []LayerDrbdResourceRow
	LayerDrbdVolumeDefinitions   []LayerDrbdVolumeDefinitionRow
	LayerDrbdVolumes             []LayerDrbdVolumeRow
	LayerStorageVolumes          []LayerStorageVolumeRow
	LayerLuksVolumes             []LayerLuksVolumeRow
}
