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
type ResourceDefinition struct {
	Name              string            `json:"name"`
	ExternalName      string            `json:"external_name,omitempty"`
	ResourceGroupName string            `json:"resource_group_name,omitempty"`
	Props             map[string]string `json:"props,omitempty"`
	Flags             []string          `json:"flags,omitempty"`
	LayerData         []ResourceLayer   `json:"layer_data,omitempty"`
	// LayerStack is the layer composition (e.g. ["DRBD","STORAGE"]).
	// Wire field name matches upstream LINSTOR (`resource_definition.layer_stack`).
	LayerStack []string `json:"layer_stack,omitempty"`
	UUID       string   `json:"uuid,omitempty"`
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
type ResourceLayer struct {
	Type string         `json:"type"`
	Data map[string]any `json:"data,omitempty"`
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

	// OverrideProps mirrors GenericPropsModify — golinstor stuffs RG
	// spawn defaults into this when creating an RD via spawn, so the
	// REST handler must accept it even if we don't yet diff the values.
	OverrideProps   map[string]string `json:"override_props,omitempty"`
	DeleteProps     []string          `json:"delete_props,omitempty"`
	DeleteNamespace []string          `json:"delete_namespaces,omitempty"`
}
