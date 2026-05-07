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

// StoragePool mirrors the upstream `StoragePool` shape.
// Field tags match upstream JSON exactly so golinstor unmarshals it.
type StoragePool struct {
	StoragePoolName  string            `json:"storage_pool_name"`
	NodeName         string            `json:"node_name"`
	ProviderKind     string            `json:"provider_kind"`
	Props            map[string]string `json:"props,omitempty"`
	StaticTraits     map[string]string `json:"static_traits,omitempty"`
	FreeCapacity     int64             `json:"free_capacity,omitempty"`
	TotalCapacity    int64             `json:"total_capacity,omitempty"`
	FreeSpaceMgrName string            `json:"free_space_mgr_name,omitempty"`
	Reports          []APICallRc       `json:"reports,omitempty"`
	SupportsSnapshot bool              `json:"supports_snapshots,omitempty"`
	ExternalLocking  bool              `json:"external_locking,omitempty"`
	UUID             string            `json:"uuid,omitempty"`
}

// APICallRc is the upstream `ApiCallRc` envelope. We define a minimal subset
// here so types compile. Phase 2 will populate it from `golinstor` apiconsts.
type APICallRc struct {
	RetCode  uint64            `json:"ret_code"`
	Message  string            `json:"message,omitempty"`
	Cause    string            `json:"cause,omitempty"`
	Details  string            `json:"details,omitempty"`
	Correc   string            `json:"correction,omitempty"`
	ObjRefs  map[string]string `json:"obj_refs,omitempty"`
	ErrorRep string            `json:"error_report_ids,omitempty"`
}

// Storage provider kind constants — the canonical strings LINSTOR uses.
const (
	StoragePoolKindLVM        = "LVM"
	StoragePoolKindLVMThin    = "LVM_THIN"
	StoragePoolKindZFS        = "ZFS"
	StoragePoolKindZFSThin    = "ZFS_THIN"
	StoragePoolKindFile       = "FILE"
	StoragePoolKindFileThin   = "FILE_THIN"
	StoragePoolKindDiskless   = "DISKLESS"
	StoragePoolKindOpenflex   = "OPENFLEX_TARGET" // explicitly out-of-scope (501)
	StoragePoolKindRemoteSPDK = "REMOTE_SPDK"     // explicitly out-of-scope (501)
)
