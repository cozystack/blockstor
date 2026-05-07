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
type AutoPlaceRequest struct {
	DisklessOnRemaining bool             `json:"diskless_on_remaining,omitempty"`
	SelectFilter        AutoSelectFilter `json:"select_filter,omitzero"`
	LayerList           []string         `json:"layer_list,omitempty"`
}

// ResourceCreate is the body upstream LINSTOR uses on
// `POST /v1/resource-definitions/{rd}/resources`. The Resources field is a
// list because callers can place a resource on multiple nodes in one call.
type ResourceCreate struct {
	Resource     Resource `json:"resource,omitzero"`
	LayerList    []string `json:"layer_list,omitempty"`
	DrbdNodeID   *int32   `json:"drbd_node_id,omitempty"`
	NetInterface string   `json:"net_interface,omitempty"`
}
