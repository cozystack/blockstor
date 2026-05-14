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

	// Connections carries explicit per-peer-pair overrides used by
	// the multi-path-DRBD feature (scenario 3.7, UG9 §"Creating
	// multiple DRBD paths with LINSTOR"). For any (NodeA, NodeB)
	// pair present here with a non-empty Paths slice, the rendered
	// `connection { … }` block emits one `path { host A address …;
	// host B address …; }` sub-block per Path INSTEAD of the
	// default `host A address …; host B address …;` lines derived
	// from each Host's Address. drbd-9 drops the implicit default
	// path as soon as any explicit path is present, so the renderer
	// mirrors that.
	//
	// Pairs missing from Connections (or present with no Paths)
	// keep the default render: a single connection block with the
	// two implicit host-address lines. Pair ordering inside a
	// ResourceConnection is logical, not lexicographic — the
	// renderer always emits the entry whose NodeA matches the
	// lexicographically-smaller host as `host A` of each path.
	Connections []ResourceConnection
}

// ResourceConnection is one peer-pair override used to render
// multi-path DRBD connections. See Resource.Connections.
type ResourceConnection struct {
	// NodeA, NodeB identify the two endpoints — must match the
	// NodeName of two entries in Resource.Hosts.
	NodeA string
	NodeB string
	// Paths is the explicit list of replication paths. Empty slice
	// means "fall back to the default render" (renderer treats it
	// the same as the connection not being present).
	Paths []ResourcePath
}

// ResourcePath is one `path { … }` sub-block inside a multi-path
// connection.
type ResourcePath struct {
	// Name is the operator-facing identifier (path1, path2, …).
	// drbd-9 does not write it to the .res file — DRBD identifies
	// paths positionally — but we keep it on the Go struct so the
	// REST layer can do idempotent UPSERT and per-path DELETE.
	Name string
	// AddressA, AddressB are the IPv4/IPv6 host addresses for the
	// `host <NodeA> address …;` / `host <NodeB> address …;` lines
	// inside the path block. Port is reused from each Host's Port
	// — drbd uses one port per connection, not per path.
	AddressA string
	AddressB string
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

	writeConnectionMesh(&b, r.Hosts, r.Connections)

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
//
// When `conns` carries an entry whose (NodeA, NodeB) matches the
// pair (in either order) AND has at least one Path, the connection
// block emits one `path { host A address …; host B address …; }`
// sub-block per Path instead of the default inline
// `host A address …; host B address …;` pair. drbd-9 ignores the
// implicit default path as soon as any explicit path appears, so we
// drop the default lines from the connection block in that case —
// otherwise drbdadm-adjust would silently rewrite our .res and
// trigger phantom reloads on every reconcile.
func writeConnectionMesh(b *strings.Builder, hosts []Host, conns []ResourceConnection) {
	if len(hosts) < minMeshPeers {
		return
	}

	for i := range hosts {
		for k := i + 1; k < len(hosts); k++ {
			writeOneConnection(b, &hosts[i], &hosts[k], conns)
		}
	}
}

// writeOneConnection emits a single `connection { … }` block for the
// (hostA, hostB) pair. Either a multi-path block (when conns has an
// entry with at least one Path) or the default two-line inline pair.
func writeOneConnection(b *strings.Builder, hostA, hostB *Host, conns []ResourceConnection) {
	b.WriteString("  connection {\n")

	if paths := lookupPaths(conns, hostA.NodeName, hostB.NodeName); len(paths) > 0 {
		writePathBlocks(b, hostA, hostB, paths)
	} else {
		fmt.Fprintf(b, "    host %s address %s:%d;\n", hostA.NodeName, hostA.Address, hostA.Port)
		fmt.Fprintf(b, "    host %s address %s:%d;\n", hostB.NodeName, hostB.Address, hostB.Port)
	}

	b.WriteString("  }\n")
}

// lookupPaths returns the explicit Paths for the (a, b) host pair,
// matching either (NodeA=a, NodeB=b) or the swapped form. Returns
// nil when no entry matches or the entry carries no paths. The
// returned slice is oriented so each Path's AddressA belongs to
// `a` (the lexicographically-first host of the rendered mesh pair),
// regardless of which order the operator stored on the
// ResourceConnection — keeps the rendered `host a address …;`
// line consistent with the on-block address we'd otherwise emit.
func lookupPaths(conns []ResourceConnection, a, b string) []ResourcePath {
	for i := range conns {
		switch {
		case conns[i].NodeA == a && conns[i].NodeB == b:
			return conns[i].Paths
		case conns[i].NodeA == b && conns[i].NodeB == a:
			return swapPathSides(conns[i].Paths)
		}
	}

	return nil
}

// swapPathSides returns a copy of paths with AddressA/AddressB
// flipped on every entry. Used when the operator stored a
// ResourceConnection as (b, a) but the mesh walks (a, b).
func swapPathSides(paths []ResourcePath) []ResourcePath {
	if len(paths) == 0 {
		return nil
	}

	out := make([]ResourcePath, len(paths))
	for i, p := range paths {
		out[i] = ResourcePath{Name: p.Name, AddressA: p.AddressB, AddressB: p.AddressA}
	}

	return out
}

// writePathBlocks emits one `path { host hostA … host hostB … }`
// sub-block per Path. Port is taken from each Host (drbd uses one
// port per connection, not per path).
func writePathBlocks(b *strings.Builder, hostA, hostB *Host, paths []ResourcePath) {
	for _, p := range paths {
		b.WriteString("    path {\n")
		fmt.Fprintf(b, "      host %s address %s:%d;\n", hostA.NodeName, p.AddressA, hostA.Port)
		fmt.Fprintf(b, "      host %s address %s:%d;\n", hostB.NodeName, p.AddressB, hostB.Port)
		b.WriteString("    }\n")
	}
}

// lookupConnection returns the NetOptions for the (a, b) pair from
// conns, matching unordered (HostA / HostB may be in either slot).
// Nil result when no matching entry exists.
func lookupConnection(conns []Connection, a, b string) map[string]string {
	for i := range conns {
		if (conns[i].HostA == a && conns[i].HostB == b) ||
			(conns[i].HostA == b && conns[i].HostB == a) {
			return conns[i].NetOptions
		}
	}

	return nil
}

// writeConnectionNet emits the nested `net { … }` block of a
// connection. Empty map → no block at all (drbd accepts but logs an
// empty net block; we keep the rendered .res tight).
func writeConnectionNet(b *strings.Builder, opts map[string]string) {
	if len(opts) == 0 {
		return
	}

	b.WriteString("    net {\n")

	for _, k := range sortedKeys(opts) {
		fmt.Fprintf(b, "      %s %s;\n", k, opts[k])
	}

	b.WriteString("    }\n")
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
