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

import (
	"maps"
	"strings"

	"github.com/google/uuid"
)

// Node mirrors `Node` from the upstream LINSTOR OpenAPI spec. We list only
// the fields that current consumers (linstor-csi, piraeus-operator) read or
// write; richer fields (KeyVault, etc.) get added as we wire them up.
//
// Field order and JSON tags MUST match upstream so golinstor unmarshals
// cleanly. The wire shape is golinstor.Node.
type Node struct {
	Name                 string              `json:"name"`
	Type                 string              `json:"type"`
	UUID                 string              `json:"uuid,omitempty"`
	Flags                []string            `json:"flags,omitempty"`
	Props                map[string]string   `json:"props,omitempty"`
	NetInterfaces        []NetInterface      `json:"net_interfaces,omitempty"`
	ConnectionStatus     string              `json:"connection_status,omitempty"`
	ResourceLayers       []string            `json:"resource_layers,omitempty"`
	StorageProviders     []string            `json:"storage_providers,omitempty"`
	UnsupportedLayers    map[string][]string `json:"unsupported_layers,omitempty"`
	UnsupportedProviders map[string][]string `json:"unsupported_providers,omitempty"`
}

// NodeModify is the upstream payload for `linstor n set-property` /
// `linstor n modify`. golinstor's NodeService.Modify sends this
// envelope; treating it like a full Node body wipes net_interfaces
// + type on every prop-only mutation.
type NodeModify struct {
	GenericPropsModify

	NodeType string `json:"node_type,omitempty"`

	// Bug 161 (DisallowUnknownFields): legacy callers PUT the full
	// `Node` read-side shape verbatim — accept those keys so the gate
	// doesn't reject them. The path's `{node}` segment is the
	// authoritative target; Type drives the merge via NodeType above,
	// the rest are informational. Same shape as the v0 spec field-set.
	Name                 string              `json:"name,omitempty"`
	Type                 string              `json:"type,omitempty"`
	UUID                 string              `json:"uuid,omitempty"`
	Flags                []string            `json:"flags,omitempty"`
	NetInterfaces        []NetInterface      `json:"net_interfaces,omitempty"`
	ConnectionStatus     string              `json:"connection_status,omitempty"`
	ResourceLayers       []string            `json:"resource_layers,omitempty"`
	StorageProviders     []string            `json:"storage_providers,omitempty"`
	UnsupportedLayers    map[string][]string `json:"unsupported_layers,omitempty"`
	UnsupportedProviders map[string][]string `json:"unsupported_providers,omitempty"`
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

// nodeNamespaceUUID is the v5 namespace used to derive stable per-Node
// UUIDs from `Node.Name`. Generated once via uuid.NewSHA1 on the
// literal string "blockstor.cozystack.io/node"; pinned as a constant
// so the UUID a given node name produces is stable across processes
// and store backends. Operators script `linstor n l` output against
// this — the value MUST NOT change.
//
//nolint:gochecknoglobals // immutable namespace UUID — `const` can't hold uuid.UUID
var nodeNamespaceUUID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("blockstor.cozystack.io/node"))

// StableNodeUUID derives a deterministic UUID v5 from `name`. Same
// name → same UUID on every controller restart, every store backend,
// every replica of the apiserver. Operators expect `linstor n l`
// UUIDs to be stable (some piraeus-operator tooling caches by UUID).
func StableNodeUUID(name string) string {
	return uuid.NewSHA1(nodeNamespaceUUID, []byte(name)).String()
}

// SupportedResourceLayers lists the LINSTOR layer types the blockstor
// satellite implements. Surfaced in `Node.resource_layers` so the
// `linstor` CLI / `linstor advise` and the autoplacer's layer-stack
// validation see the same capability set upstream does. Order matches
// upstream's `Node.supportedLayers` output: top-of-stack first.
//
//nolint:gochecknoglobals // immutable capability table — `const` can't hold a slice
var SupportedResourceLayers = []string{"DRBD", "STORAGE", "LUKS"}

// SupportedStorageProviders lists the provider kinds blockstor's
// `pkg/satellite/factory.go::NewProviderFromKind` actually
// instantiates. Surfaced in `Node.storage_providers` so the placer
// can advertise diskless support and `linstor sp c` validation has
// the same kind-list upstream does.
//
//nolint:gochecknoglobals // immutable capability table — `const` can't hold a slice
var SupportedStorageProviders = []string{
	"LVM",
	"LVM_THIN",
	"ZFS",
	"ZFS_THIN",
	"FILE",
	"FILE_THIN",
	"DISKLESS",
}

// unsupportedLayerReason and unsupportedProviderReason document why a
// given layer/provider is absent from blockstor. Upstream LINSTOR
// uses these maps to surface "missing module" / "vendor not built"
// reasons in `linstor n l --pastable`; here the reasons are
// architectural ("scoped out of blockstor") rather than runtime.
const (
	unsupportedLayerReason    = "not implemented in blockstor satellite"
	unsupportedProviderReason = "scoped out of blockstor (upstream-only provider)"
)

// NodeInfo is the compact per-node capability table returned by
// `GET /v1/nodes/{node}/info`. It is the wire shape behind the
// `linstor node info <node>` CLI diagnostic — the operator's fastest
// answer to "why didn't autoplace pick this node?". Scenario 4.W08.
//
// Field order: `Name` first so JSON-streamed output reads top-down,
// then the supported sets, then the unsupported sets. The supported
// sets MUST mirror what `pkg/satellite/factory.go::NewProviderFromKind`
// (for providers) and the satellite's layer dispatcher (for layers)
// actually instantiate, so `linstor advise` only proposes reachable
// configurations.
type NodeInfo struct {
	Name                 string              `json:"name"`
	SupportedProviders   []string            `json:"supported_providers"`
	SupportedLayers      []string            `json:"supported_layers"`
	UnsupportedProviders map[string][]string `json:"unsupported_providers,omitempty"`
	UnsupportedLayers    map[string][]string `json:"unsupported_layers,omitempty"`
}

// SynthesizeNodeCapabilities populates the upstream-LINSTOR
// capability fields (UUID, resource_layers, storage_providers,
// unsupported_layers, unsupported_providers, props.NodeUname,
// props.CurStltConnName) on `n` in place. Mutates the receiver — the
// caller is responsible for handing in a defensive copy if the
// pre-synthesis shape matters. Both the K8s and in-memory stores call
// this on every read so the REST surface emits a consistent wire
// shape regardless of which backend persists the Node.
//
// Synthesis sources:
//   - UUID:            SHA1(NodeNamespace, n.Name) — stable, deterministic.
//   - props.NodeUname: existing props["NodeUname"] if set, else n.Name
//     (matches `uname -n` on most piraeus-operator
//     deployments where the LINSTOR node name == hostname).
//   - props.CurStltConnName: "default" — blockstor has one connection
//     per node (no multi-tenant net-iface routing).
//   - resource_layers / storage_providers: SupportedResourceLayers /
//     SupportedStorageProviders constants above.
//   - unsupported_layers / unsupported_providers: pinned reason
//     strings (architectural exclusions, not runtime failures).
func SynthesizeNodeCapabilities(n *Node) {
	if n == nil || n.Name == "" {
		return
	}

	if n.UUID == "" {
		n.UUID = StableNodeUUID(n.Name)
	}

	if n.Props == nil {
		n.Props = map[string]string{}
	} else {
		// Don't mutate a Props map the caller may still own — clone
		// on first write so this helper is safe to call on store
		// values that are returned by reference.
		n.Props = maps.Clone(n.Props)
	}

	if _, exists := n.Props["NodeUname"]; !exists {
		// Strip the K8s-suffix hash that the store may have applied
		// to the CRD name (Name() truncates+suffixes if too long).
		// For the wire we always want the operator-visible name.
		n.Props["NodeUname"] = strings.TrimSpace(n.Name)
	}

	if _, exists := n.Props["CurStltConnName"]; !exists {
		n.Props["CurStltConnName"] = "default"
	}

	if len(n.ResourceLayers) == 0 {
		n.ResourceLayers = append([]string(nil), SupportedResourceLayers...)
	}

	if len(n.StorageProviders) == 0 {
		n.StorageProviders = append([]string(nil), SupportedStorageProviders...)
	}

	if n.UnsupportedLayers == nil {
		n.UnsupportedLayers = map[string][]string{
			"CACHE":      {unsupportedLayerReason},
			"WRITECACHE": {unsupportedLayerReason},
			"NVME":       {unsupportedLayerReason},
		}
	}

	if n.UnsupportedProviders == nil {
		n.UnsupportedProviders = map[string][]string{
			"OPENFLEX_TARGET":     {unsupportedProviderReason},
			"REMOTE_SPDK":         {unsupportedProviderReason},
			"SPDK":                {unsupportedProviderReason},
			"STORAGE_SPACES":      {unsupportedProviderReason},
			"STORAGE_SPACES_THIN": {unsupportedProviderReason},
			"EBS_TARGET":          {unsupportedProviderReason},
			"EBS_INIT":            {unsupportedProviderReason},
		}
	}
}
