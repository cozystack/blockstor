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

import "github.com/google/uuid"

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

// NodeModify is the upstream payload for `linstor n set-property` /
// `linstor n modify`. golinstor's NodeService.Modify sends this
// envelope; treating it like a full Node body wipes net_interfaces
// + type on every prop-only mutation.
type NodeModify struct {
	GenericPropsModify

	NodeType string `json:"node_type,omitempty"`
}

// NetInterface mirrors `NetInterface` from upstream. UUID is synthesized
// on read by the store backends (k8s and inmemory) — we don't persist it
// on the CRD because the wire identity is `(node, ifname)` and a stored
// UUID would just drift if the CRD got rewritten by an older controller.
// See `SyntheticNetInterfaceUUID` for the derivation rule.
type NetInterface struct {
	UUID                    string `json:"uuid,omitempty"`
	Name                    string `json:"name"`
	Address                 string `json:"address"`
	SatellitePort           int    `json:"satellite_port,omitempty"`
	SatelliteEncryptionType string `json:"satellite_encryption_type,omitempty"`
	IsActive                bool   `json:"is_active,omitempty"`
}

// Upstream LINSTOR defaults for NetInterface fields. The Java
// controller emits these when no explicit satellite_port /
// satellite_encryption_type was set at node create time, and the
// Python CLI renders the Addresses column from them. Blockstor
// retired the gRPC wire in Phase 10.6 (satellite ↔ controller now
// flows through the apiserver), so 3366 is descriptive metadata
// rather than a routable port — but the parity audit requires the
// fields populated so `linstor n l` doesn't render a blank Addresses
// column.
const (
	DefaultSatellitePort           = 3366
	DefaultSatelliteEncryptionType = "PLAIN"
)

// DefaultNetInterfaceFields fills upstream-default port + encryption
// type on every interface that has an Address but missing port/type,
// marks the first interface IsActive, and synthesizes a stable UUID
// per (node, ifname). Both store backends call this on read so the
// REST surface emits a consistent wire shape regardless of which
// backend persists the Node.
func DefaultNetInterfaceFields(nodeName string, ifaces []NetInterface) []NetInterface {
	for i := range ifaces {
		if ifaces[i].Address == "" {
			continue
		}

		if ifaces[i].SatellitePort == 0 {
			ifaces[i].SatellitePort = DefaultSatellitePort
		}

		if ifaces[i].SatelliteEncryptionType == "" {
			ifaces[i].SatelliteEncryptionType = DefaultSatelliteEncryptionType
		}

		ifaces[i].IsActive = i == 0

		if ifaces[i].UUID == "" {
			ifaces[i].UUID = SyntheticNetInterfaceUUID(nodeName, ifaces[i].Name)
		}
	}

	return ifaces
}

// SyntheticNetInterfaceUUID derives a stable v5 UUID per (node, ifname).
// Upstream LINSTOR persists a UUID on every NetInterface DTO; some
// tooling diffs interface state across reconciles by that UUID. We
// don't persist on the CRD — same input always produces the same
// output, so a synthesized value is just as stable for diffing.
func SyntheticNetInterfaceUUID(nodeName, ifaceName string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("blockstor:netif:"+nodeName+":"+ifaceName)).String()
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
