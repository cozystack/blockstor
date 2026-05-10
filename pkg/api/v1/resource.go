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

// Resource is a single replica of a ResourceDefinition placed on a node.
// linstor-csi reads this heavily via /v1/view/resources during volume
// reconciliation.
type Resource struct {
	Name      string            `json:"name"`
	NodeName  string            `json:"node_name"`
	Props     map[string]string `json:"props,omitempty"`
	Flags     []string          `json:"flags,omitempty"`
	State     ResourceState     `json:"state,omitzero"`
	UUID      string            `json:"uuid,omitempty"`
	LayerData []ResourceLayer   `json:"layer_object,omitempty"`
}

// ResourceState is the runtime state surface of a Resource.
type ResourceState struct {
	InUse bool `json:"in_use,omitempty"`
}

// ResourceWithVolumes is the shape `/v1/view/resources` returns — Resource
// plus the per-volume runtime details. linstor-csi expects this exact key
// (`volumes`) on the same level as the embedded Resource fields.
type ResourceWithVolumes struct {
	Resource

	Volumes []Volume `json:"volumes,omitempty"`
}

// Volume is a single volume of a placed resource (replica) on a node.
type Volume struct {
	VolumeNumber int32             `json:"volume_number"`
	StoragePool  string            `json:"storage_pool_name,omitempty"`
	DevicePath   string            `json:"device_path,omitempty"`
	AllocatedKib int64             `json:"allocated_size_kib,omitempty"`
	UsableKib    int64             `json:"usable_size_kib,omitempty"`
	Props        map[string]string `json:"props,omitempty"`
	Flags        []string          `json:"flags,omitempty"`
	UUID         string            `json:"uuid,omitempty"`
	State        VolumeState       `json:"state,omitzero"`
}

// VolumeState is the runtime state surface of a Volume.
type VolumeState struct {
	DiskState string `json:"disk_state,omitempty"`

	// CurrentGi is the DRBD-9 generation identifier reported by
	// `drbdsetup events2 --full` for this replica's local volume.
	// The controller reads it when adding a new replica to skip the
	// full initial-sync (Phase 8.1).
	CurrentGi string `json:"current_gi,omitempty"`
}

// VolumeObservation carries per-volume observed state propagated from
// the satellite's events2 observer into the store. Used by SetState
// to update per-volume Status fields without touching Spec.
type VolumeObservation struct {
	VolumeNumber int32
	State        VolumeState
}
