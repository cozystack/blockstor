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

// Node mirrors `Node` from the upstream LINSTOR OpenAPI spec. We list only
// the fields that current consumers (linstor-csi, piraeus-operator) read or
// write; richer fields (KeyVault, etc.) get added as we wire them up.
//
// Field order and JSON tags MUST match upstream so golinstor unmarshals
// cleanly. The wire shape is golinstor.Node.
type Node struct {
	Name             string            `json:"name"`
	Type             string            `json:"type"`
	Flags            []string          `json:"flags,omitempty"`
	Props            map[string]string `json:"props,omitempty"`
	NetInterfaces    []NetInterface    `json:"net_interfaces,omitempty"`
	ConnectionStatus string            `json:"connection_status,omitempty"`
}

// NetInterface mirrors `NetInterface` from upstream.
type NetInterface struct {
	Name                    string `json:"name"`
	Address                 string `json:"address"`
	SatellitePort           int    `json:"satellite_port,omitempty"`
	SatelliteEncryptionType string `json:"satellite_encryption_type,omitempty"`
	IsActive                bool   `json:"is_active,omitempty"`
}

// Node Type constants — these are the strings LINSTOR uses on the wire.
const (
	NodeTypeController          = "CONTROLLER"
	NodeTypeSatellite           = "SATELLITE"
	NodeTypeCombined            = "COMBINED"
	NodeTypeAuxiliary           = "AUXILIARY"
	NodeTypeRemoteSpdk          = "REMOTE_SPDK"
	NodeTypeOpenflexTarget      = "OPENFLEX_TARGET"
	NodeTypeEbsTarget           = "EBS_TARGET"
	NodeTypeEbsInitiator        = "EBS_INIT"
	NodeTypeStandalone          = "STANDALONE"
	NodeTypeOnline              = "ONLINE"
	NodeTypeOffline             = "OFFLINE"
	NodeTypeUnknown             = "UNKNOWN"
	NodeTypeConnecting          = "CONNECTING"
	NodeTypeAuthenticationError = "AUTHENTICATION_ERROR"
)

// Node flag constants — values that appear in `Node.Flags`. EVICTED is
// the soft "drain me" hint; LOST is set after eviction confirms the
// node is gone for good.
const (
	NodeFlagEvicted = "EVICTED"
	NodeFlagLost    = "LOST"
)

// Resource flag constants — values that appear in `Resource.Flags`.
// DISKLESS marks a connection-mesh-only replica that doesn't allocate
// storage; INACTIVE means `drbdadm down` (operator deactivation);
// TIE_BREAKER tags a controller-created witness so the cleanup path
// can distinguish it from operator-placed disklesses.
const (
	ResourceFlagDiskless   = "DISKLESS"
	ResourceFlagInactive   = "INACTIVE"
	ResourceFlagTieBreaker = "TIE_BREAKER"
)
