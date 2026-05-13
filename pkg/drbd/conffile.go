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

// Package drbd builds drbd-9 `.res` files and (later) wraps drbdadm /
// drbdsetup. The Builder produces deterministic output — same input →
// byte-identical file — so a reconciler can `cmp -s` against the on-disk
// version and skip a noisy `drbdadm adjust` when nothing changed.
package drbd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// Resource is the top-level drbd-9 resource definition. It maps 1:1 to
// the contents of `/etc/drbd.d/<name>.res`.
type Resource struct {
	// Name is the resource name (typically the LINSTOR resource
	// definition name, e.g. "pvc-1"). Required.
	Name string

	// Net configures the `net { ... }` section.
	Net Net

	// Hosts lists every peer participating in this resource. The full
	// set is emitted as `on <node> { ... }` blocks plus a complete
	// connection mesh.
	Hosts []Host

	// Volumes are the data volumes attached to this resource. A
	// resource may have N volumes (multi-volume support); each gets
	// its own `volume <n> { ... }` block under every `on` block.
	Volumes []Volume

	// Options is a passthrough for top-level drbd `options { ... }`
	// keys — e.g. `on-no-quorum`, `quorum`, `auto-promote`. Sorted
	// before emission for stable output.
	Options map[string]string
}

// Net mirrors the drbd-9 `net { ... }` section.
type Net struct {
	// ProtocolC selects synchronous replication. drbd-9 supports A/B/C
	// but in practice we always run C; a bool keeps the API tight.
	ProtocolC bool

	// SharedSecret is the cluster-internal authentication secret. When
	// non-empty it is emitted as `shared-secret "<value>";`.
	SharedSecret string

	// Options are arbitrary `net` knobs (after-sb-0pri, max-buffers,
	// rcvbuf-size, …). Keys are sorted before emission.
	Options map[string]string
}

// Host is one node participating in the resource.
type Host struct {
	// NodeName matches the LINSTOR Node name and the `on <name>`
	// header drbd uses for routing.
	NodeName string

	// Address is the IP address drbd binds to. We don't currently
	// support IPv6 with a different syntax — that lands when needed.
	Address string

	// Port is the TCP port for replication; identical across all
	// peers of a single resource.
	Port int

	// NodeID is the drbd-9 node identifier (0..31). Must be unique
	// within the resource's host list.
	NodeID int

	// IsLocal marks this host as the satellite rendering the .res
	// file (only one host per resource has this true). Peer hosts
	// get a placeholder `disk` value upstream LINSTOR uses
	// (`/dev/drbd/this/is/not/used`) — drbd never reads the peer
	// host's `disk` field, but the parser refuses an empty one and
	// treats `none` as DISKLESS, so a constant placeholder is what
	// upstream emits.
	IsLocal bool

	// Diskless marks the host as a DRBD diskless replica — `disk`
	// must be rendered as the literal `none`. Same value on local
	// vs. peer hosts: a peer that's diskless still gets `none`,
	// not the placeholder.
	Diskless bool
}

// Volume is one data volume on the resource.
type Volume struct {
	// Number is the drbd volume index (0-based). Stable across peers.
	Number int

	// Device is the consumer-side device node path (typically
	// `/dev/drbdNNNN`).
	Device string

	// Disk is the backing block device on this peer (the LV / zvol /
	// loop file produced by the storage provider).
	Disk string

	// Minor is the kernel minor number that backs Device. drbd
	// assigns these globally on the node, so it is the controller's
	// job to allocate one and pin it here.
	Minor int

	// MetaDisk, when non-empty, routes DRBD activity-log + bitmap +
	// generation-id state to a SEPARATE backing block device — the
	// upstream LINSTOR `StorPoolNameDrbdMeta` feature (UG9
	// §"Using external DRBD metadata"). The renderer emits
	// `meta-disk <MetaDisk>;` for the local diskful host instead of
	// the default `meta-disk internal;` line. Empty value → internal
	// metadata (the default, where metadata lives at the tail of the
	// data device).
	//
	// Peer hosts still get `meta-disk internal;` rendered — drbd
	// never reads the peer-side meta-disk path, and emitting a real
	// path here would tie every peer's .res to this satellite's
	// local meta-pool layout, breaking the deterministic render
	// across peers.
	MetaDisk string
}

// Build renders r into a drbd-9 `.res` file body. The output is
// deterministic: map keys are sorted, host order follows Hosts as
// passed, and the connection mesh emits pairs in lexicographic (i, j)
// order with i < j.
//
//nolint:gocritic // value receiver matches the upstream LINSTOR builder API and ergonomic for one-shot callers.
func Build(r Resource) (string, error) {
	if r.Name == "" {
		return "", errors.New("drbd: resource name is required")
	}

	var b strings.Builder

	fmt.Fprintf(&b, "resource %s {\n", r.Name)

	if r.Net.ProtocolC {
		b.WriteString("  protocol C;\n")
	}

	writeNet(&b, r.Net)
	writeOptions(&b, r.Options)

	for i := range r.Hosts {
		writeOnBlock(&b, &r.Hosts[i], r.Volumes)
	}

	writeConnectionMesh(&b, r.Hosts)

	b.WriteString("}\n")

	return b.String(), nil
}

// writeNet emits the `net { … }` block when there is anything to emit
// (a shared secret or any free-form option). drbd treats an empty `net
// {}` as legal but noisy, so we skip it entirely when unused.
func writeNet(b *strings.Builder, n Net) {
	if n.SharedSecret == "" && len(n.Options) == 0 {
		return
	}

	b.WriteString("  net {\n")

	if n.SharedSecret != "" {
		fmt.Fprintf(b, "    shared-secret %q;\n", n.SharedSecret)
	}

	for _, k := range sortedKeys(n.Options) {
		fmt.Fprintf(b, "    %s %s;\n", k, n.Options[k])
	}

	b.WriteString("  }\n")
}

// writeOptions emits the top-level `options { … }` block. Empty map →
// no block; matches drbd's "absent means default" semantics.
func writeOptions(b *strings.Builder, opts map[string]string) {
	if len(opts) == 0 {
		return
	}

	b.WriteString("  options {\n")

	for _, k := range sortedKeys(opts) {
		fmt.Fprintf(b, "    %s %s;\n", k, opts[k])
	}

	b.WriteString("  }\n")
}

// writeOnBlock emits one `on <node> { … }` block including every volume
// definition for this peer. The `disk` value follows upstream LINSTOR's
// ConfFileBuilder:
//   - DISKLESS host (local or peer) → `none`
//   - local diskful host → the real backing path (Volume.Disk)
//   - peer diskful host → the literal placeholder `/dev/drbd/this/is/not/used`
//
// The peer placeholder exists because drbd never reads the peer's
// `disk` field but the parser rejects empty / requires a stable
// non-`none` token; using each peer's actual backing path would
// also clash when a peer is mid-conversion from diskless and its
// path is reported as `none`.
const peerDiskPlaceholder = "/dev/drbd/this/is/not/used"

func writeOnBlock(b *strings.Builder, host *Host, vols []Volume) {
	fmt.Fprintf(b, "  on %s {\n", host.NodeName)
	fmt.Fprintf(b, "    address %s:%d;\n", host.Address, host.Port)
	fmt.Fprintf(b, "    node-id %d;\n", host.NodeID)

	for _, vol := range vols {
		fmt.Fprintf(b, "    volume %d {\n", vol.Number)
		fmt.Fprintf(b, "      device %s minor %d;\n", vol.Device, vol.Minor)
		fmt.Fprintf(b, "      disk %s;\n", diskField(host, vol))
		fmt.Fprintf(b, "      meta-disk %s;\n", metaField(host, vol))
		b.WriteString("    }\n")
	}

	b.WriteString("  }\n")
}

// diskField picks the `disk` value to render for one (host, volume)
// pair. See writeOnBlock for the precedence.
func diskField(host *Host, vol Volume) string {
	switch {
	case host.Diskless:
		return "none"
	case host.IsLocal:
		return vol.Disk
	default:
		return peerDiskPlaceholder
	}
}

// metaField picks the `meta-disk` value to render. For the local
// diskful host with a non-empty Volume.MetaDisk we emit the external
// path (scenario 6.18 — `StorPoolNameDrbdMeta`); for everyone else
// (diskless, peers, local with empty MetaDisk) we emit `internal`.
//
// Note: drbd's `meta-disk` accepts `internal`, `<device>`, or
// `<device> [<index>]`. We render only the `internal` and bare-
// device forms — indexed external metadata isn't on the surface yet
// and upstream LINSTOR's StorPoolNameDrbdMeta path always carves a
// per-volume LV/zvol rather than packing multiple replicas into one
// metadata device.
func metaField(host *Host, vol Volume) string {
	if host.IsLocal && !host.Diskless && vol.MetaDisk != "" {
		return vol.MetaDisk
	}

	return "internal"
}

// writeConnectionMesh emits one `connection { … }` block per (i, j)
// host pair with i < j. drbd-9 requires the mesh to be explicit; with
// N>2 peers an implicit "everyone talks to everyone" doesn't exist.
func writeConnectionMesh(b *strings.Builder, hosts []Host) {
	if len(hosts) < minMeshPeers {
		return
	}

	for i := range hosts {
		for j := i + 1; j < len(hosts); j++ {
			b.WriteString("  connection {\n")
			fmt.Fprintf(b, "    host %s address %s:%d;\n", hosts[i].NodeName, hosts[i].Address, hosts[i].Port)
			fmt.Fprintf(b, "    host %s address %s:%d;\n", hosts[j].NodeName, hosts[j].Address, hosts[j].Port)
			b.WriteString("  }\n")
		}
	}
}

// sortedKeys returns the keys of m in deterministic order. We don't
// bother with a heap or anything fancy — option maps are tiny (a
// dozen keys at most).
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

const minMeshPeers = 2
