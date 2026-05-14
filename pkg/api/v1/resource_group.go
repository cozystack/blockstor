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

// AnnotationRGRebalancePending is stamped onto the parent ResourceGroup
// CRD when a REST `linstor rg modify` raises PlaceCount or alters a
// placement-affecting SelectFilter field. The value is the RFC3339
// timestamp of the trigger — operators can grep for stale entries
// (annotation not stripped within a reconcile window indicates the
// reconciler is wedged). RGRebalanceReconciler observes this key,
// re-runs the additive placer on every child RD, then strips it.
//
// Bug 60 (cli-parity-audit row #41): upstream LINSTOR's
// `CtrlRscGrpApiCallHandler.modify` triggers RescheduleAutoPlace
// synchronously from the REST call. Phase 11.x split moved
// reconcilers into a separate process, so the REST handler can't
// walk RDs inline; the annotation is the cross-process trigger.
const AnnotationRGRebalancePending = "blockstor.io/rebalance-pending"

// ResourceGroup mirrors `ResourceGroup` from upstream LINSTOR. A resource
// group is a template — linstor-csi creates one per Kubernetes StorageClass
// and `Spawn`s individual ResourceDefinitions from it.
//
// `Annotations` carries the K8s-native metadata.annotations of the
// underlying RG CRD. Mirrors the same field on ResourceDefinition —
// REST handlers stamp orchestration markers (e.g. the Bug 60
// `blockstor.io/rebalance-pending` flag) here; controller-side
// reconcilers observe the annotation to schedule deferred work.
// Not part of upstream LINSTOR's wire shape; golinstor ignores
// unknown JSON fields so the new key passes through transparently.
type ResourceGroup struct {
	Name         string            `json:"name"`
	Description  string            `json:"description,omitempty"`
	Props        map[string]string `json:"props,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	SelectFilter AutoSelectFilter  `json:"select_filter,omitzero"`
	VolumeGroups []VolumeGroup     `json:"volume_groups,omitempty"`
	UUID         string            `json:"uuid,omitempty"`
	PeerSlots    int32             `json:"peer_slots,omitempty"`

	// OverrideProps / DeleteProps / DeleteNamespace mirror
	// GenericPropsModify. golinstor sends them on RG create/modify
	// through a shared body type, so DisallowUnknownFields decoders
	// must accept them even though we treat the call idempotently.
	OverrideProps   map[string]string `json:"override_props,omitempty"`
	DeleteProps     []string          `json:"delete_props,omitempty"`
	DeleteNamespace []string          `json:"delete_namespaces,omitempty"`
}

// VolumeGroup mirrors upstream `VolumeGroup`. It is a per-volume template
// inside a ResourceGroup.
type VolumeGroup struct {
	VolumeNumber int32             `json:"volume_number"`
	Props        map[string]string `json:"props,omitempty"`
	Flags        []string          `json:"flags,omitempty"`
	UUID         string            `json:"uuid,omitempty"`
}

// AutoSelectFilter is the placement constraint set used by Autoplacer.
//
// PlaceCount and AdditionalPlaceCount use the LaxInt32 type because
// upstream LINSTOR's CLI (linstor-client) marshals integer flags as
// quoted strings (e.g. `"place_count":"2"`) — the wire shape differs
// from the OpenAPI int32 spec. LaxInt32 accepts both forms so neither
// the CLI nor a strictly-typed client breaks.
type AutoSelectFilter struct {
	PlaceCount              LaxInt32         `json:"place_count,omitempty"`
	AdditionalPlaceCount    LaxInt32         `json:"additional_place_count,omitempty"`
	NodeNameList            []string         `json:"node_name_list,omitempty"`
	StoragePool             string           `json:"storage_pool,omitempty"`
	StoragePoolList         []string         `json:"storage_pool_list,omitempty"`
	StoragePoolDisklessList []string         `json:"storage_pool_diskless_list,omitempty"`
	NotPlaceWithRsc         []string         `json:"not_place_with_rsc,omitempty"`
	NotPlaceWithRscRegex    string           `json:"not_place_with_rsc_regex,omitempty"`
	ReplicasOnSame          []string         `json:"replicas_on_same,omitempty"`
	ReplicasOnDifferent     []string         `json:"replicas_on_different,omitempty"`
	XReplicasOnDifferentMap map[string]int32 `json:"x_replicas_on_different_map,omitempty"`
	LayerStack              []string         `json:"layer_stack,omitempty"`
	ProviderList            []string         `json:"provider_list,omitempty"`
	DisklessOnRemaining     bool             `json:"diskless_on_remaining,omitempty"`
	OverrideVlmID           string           `json:"override_vlm_id,omitempty"`
}

// ResourceGroupSpawn is the payload for POST /resource-groups/{rg}/spawn —
// the call linstor-csi uses to actually create a Resource from a group.
// Upstream LINSTOR reuses GenericPropsModify here too, so we accept the
// override_props / delete_props fields even though we don't consume them.
type ResourceGroupSpawn struct {
	ResourceDefinitionName     string            `json:"resource_definition_name,omitempty"`
	ResourceDefinitionExternal string            `json:"resource_definition_external_name,omitempty"`
	VolumeSizes                []int64           `json:"volume_sizes,omitempty"`
	PartialFlag                bool              `json:"partial,omitempty"`
	DefinitionsOnly            bool              `json:"definitions_only,omitempty"`
	SelectFilter               AutoSelectFilter  `json:"select_filter,omitzero"`
	OverrideProps              map[string]string `json:"override_props,omitempty"`
	DeleteProps                []string          `json:"delete_props,omitempty"`
	DeleteNamespace            []string          `json:"delete_namespaces,omitempty"`
}
