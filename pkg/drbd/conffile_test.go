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

package drbd_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestBuildEmptyResource: a Resource with no peers / no volumes still
// produces a syntactically valid `.res` file with the resource header.
func TestBuildEmptyResource(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{Name: "pvc-1"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !strings.Contains(got, "resource pvc-1 {") {
		t.Errorf("missing resource header; got:\n%s", got)
	}

	if !strings.HasSuffix(strings.TrimSpace(got), "}") {
		t.Errorf("missing closing brace; got:\n%s", got)
	}
}

// TestBuildSinglePeerSingleVolume: minimum useful resource — one peer,
// one volume — expands to the canonical drbd.conf shape.
func TestBuildSinglePeerSingleVolume(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net:  drbd.Net{ProtocolC: true},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
		Volumes: []drbd.Volume{
			{Number: 0, Device: "/dev/drbd1000", Disk: "/dev/vg/pvc-1_00000", Minor: 1000},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, want := range []string{
		"resource pvc-1 {",
		"  protocol C;",
		"  on n1 {",
		"    address 10.0.0.1:7000;",
		"    node-id 0;",
		"    volume 0 {",
		"      device /dev/drbd1000 minor 1000;",
		// Local node uses the real backing disk path.
		"      disk /dev/vg/pvc-1_00000;",
		"      meta-disk internal;",
		"  on n2 {",
		// Peer node uses upstream's placeholder — drbd never reads
		// the peer-side `disk`, but the parser requires a stable
		// non-empty / non-`none` token.
		"      disk /dev/drbd/this/is/not/used;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in output:\n%s", want, got)
		}
	}
}

// TestBuildEmitsConnectionMesh: with 3 peers, every (a, b) pair appears
// as a `connection` block — drbd-9 needs an explicit mesh.
func TestBuildEmitsConnectionMesh(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
			{NodeName: "n3", Address: "10.0.0.3", Port: 7000, NodeID: 2},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, want := range []string{
		"connection {",
		"host n1 address 10.0.0.1:7000;",
		"host n2 address 10.0.0.2:7000;",
		"host n3 address 10.0.0.3:7000;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in output:\n%s", want, got)
		}
	}
}

// TestBuildIncludesNetSecret: net.shared-secret is emitted when set.
func TestBuildIncludesNetSecret(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net:  drbd.Net{SharedSecret: "supersecret"},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !strings.Contains(got, `shared-secret "supersecret"`) {
		t.Errorf("missing shared-secret; got:\n%s", got)
	}
}

// TestBuildEmitsArbitraryNetOptions copies through any extra
// drbdOptions/Net/* keys verbatim.
func TestBuildEmitsArbitraryNetOptions(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net: drbd.Net{
			Options: map[string]string{
				"after-sb-0pri": "discard-zero-changes",
				"max-buffers":   "8000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, want := range []string{
		"after-sb-0pri discard-zero-changes;",
		"max-buffers 8000;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in output:\n%s", want, got)
		}
	}
}

// TestBuildEmitsResourceOptions: top-level `options { … }` block when
// Resource.Options is set.
func TestBuildEmitsResourceOptions(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name:    "pvc-1",
		Options: map[string]string{"on-no-quorum": "io-error", "quorum": "majority"},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, want := range []string{
		"options {",
		"on-no-quorum io-error;",
		"quorum majority;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in output:\n%s", want, got)
		}
	}
}

// TestBuildPeerDisks pins the `disk` precedence upstream LINSTOR
// uses: local diskful → real path; local diskless → `none`;
// peer diskful → `/dev/drbd/this/is/not/used`; peer diskless →
// `none` (not the placeholder).
func TestBuildPeerDisks(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net:  drbd.Net{ProtocolC: true},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
			{NodeName: "n3", Address: "10.0.0.3", Port: 7000, NodeID: 2, Diskless: true},
		},
		Volumes: []drbd.Volume{
			{Number: 0, Device: "/dev/drbd1000", Disk: "/dev/loop42", Minor: 1000},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	cases := []struct {
		needle string
		why    string
	}{
		{"  on n1 {\n    address 10.0.0.1:7000;\n    node-id 0;\n    volume 0 {\n      device /dev/drbd1000 minor 1000;\n      disk /dev/loop42;\n", "local diskful → real path"},
		{"  on n2 {\n    address 10.0.0.2:7000;\n    node-id 1;\n    volume 0 {\n      device /dev/drbd1000 minor 1000;\n      disk /dev/drbd/this/is/not/used;\n", "peer diskful → placeholder"},
		{"  on n3 {\n    address 10.0.0.3:7000;\n    node-id 2;\n    volume 0 {\n      device /dev/drbd1000 minor 1000;\n      disk none;\n", "peer diskless → none"},
	}

	for _, c := range cases {
		if !strings.Contains(got, c.needle) {
			t.Errorf("%s: missing block\n  want substring %q\n  in:\n%s", c.why, c.needle, got)
		}
	}
}

// TestRenderExternalMetadata: scenario 6.18 — when Volume.MetaDisk is
// non-empty, the .res renderer emits `meta-disk <path>;` for the
// local diskful host instead of the default `meta-disk internal;`.
// Peer hosts and diskless local hosts still get `meta-disk internal;`
// — drbd never reads the peer-side meta-disk and the local diskless
// case has no meta to point at.
func TestRenderExternalMetadata(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net:  drbd.Net{ProtocolC: true},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
			{NodeName: "n3", Address: "10.0.0.3", Port: 7000, NodeID: 2, Diskless: true},
		},
		Volumes: []drbd.Volume{
			{
				Number:   0,
				Device:   "/dev/drbd1000",
				Disk:     "/dev/data-vg/pvc-1_00000",
				MetaDisk: "/dev/meta-vg/pvc-1_meta",
				Minor:    1000,
			},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	cases := []struct {
		needle string
		why    string
	}{
		{
			// Local diskful gets the external meta-disk path verbatim.
			needle: "  on n1 {\n    address 10.0.0.1:7000;\n    node-id 0;\n    volume 0 {\n      device /dev/drbd1000 minor 1000;\n      disk /dev/data-vg/pvc-1_00000;\n      meta-disk /dev/meta-vg/pvc-1_meta;\n",
			why:    "local diskful → external meta path",
		},
		{
			// Peer diskful keeps `internal` — see Volume.MetaDisk
			// godoc for the rationale (peer side isn't read by drbd
			// and pinning a local path here breaks deterministic
			// render across peers).
			needle: "  on n2 {\n    address 10.0.0.2:7000;\n    node-id 1;\n    volume 0 {\n      device /dev/drbd1000 minor 1000;\n      disk /dev/drbd/this/is/not/used;\n      meta-disk internal;\n",
			why:    "peer diskful → internal",
		},
		{
			// Diskless local/peer always gets `internal`.
			needle: "  on n3 {\n    address 10.0.0.3:7000;\n    node-id 2;\n    volume 0 {\n      device /dev/drbd1000 minor 1000;\n      disk none;\n      meta-disk internal;\n",
			why:    "diskless peer → internal",
		},
	}

	for _, c := range cases {
		if !strings.Contains(got, c.needle) {
			t.Errorf("%s: missing block\n  want substring %q\n  in:\n%s", c.why, c.needle, got)
		}
	}
}

// TestRenderInternalMetadataDefault pins the default: when
// Volume.MetaDisk is empty, every host's volume block keeps the
// pre-6.18 `meta-disk internal;` line. Guards against accidental
// regression of the default path from the metaField switch.
func TestRenderInternalMetadataDefault(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net:  drbd.Net{ProtocolC: true},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
		Volumes: []drbd.Volume{
			{Number: 0, Device: "/dev/drbd1000", Disk: "/dev/vg/pvc-1_00000", Minor: 1000},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Both `on` blocks must carry `meta-disk internal;` — assert by
	// counting occurrences (one per peer × one volume = 2).
	const want = "meta-disk internal;"
	if n := strings.Count(got, want); n != 2 {
		t.Errorf("expected %q to appear twice (one per host), got %d\n%s", want, n, got)
	}
}

// TestRenderMultiPathConnection pins scenario 3.7 (UG9 §"Creating
// multiple DRBD paths with LINSTOR", lines 2187-2256): when a
// Resource carries an explicit Connection with two Paths between
// two peers, the rendered `connection { … }` block contains TWO
// `path { … }` sub-blocks (one per Path) and NOT the inline
// `host A address …;` / `host B address …;` lines drbd-9 derives
// from the `on` block addresses when no explicit paths are set.
//
// Why this shape: drbd-9 drops the implicit "default" host-pair from
// the connection block as soon as any `path { … }` sub-block appears.
// The renderer must mirror that, otherwise we ship .res files that
// disagree with what `drbdadm adjust` produces and trigger phantom
// reloads on every reconcile.
func TestRenderMultiPathConnection(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net:  drbd.Net{ProtocolC: true},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
		Volumes: []drbd.Volume{
			{Number: 0, Device: "/dev/drbd1000", Disk: "/dev/vg/pvc-1_00000", Minor: 1000},
		},
		Connections: []drbd.ResourceConnection{
			{
				NodeA: "n1",
				NodeB: "n2",
				Paths: []drbd.ResourcePath{
					{Name: "path1", AddressA: "10.1.1.5", AddressB: "10.1.1.6"},
					{Name: "path2", AddressA: "10.2.2.5", AddressB: "10.2.2.6"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Both path blocks must appear inside the connection block, in
	// declaration order, each with the correct (hostA, hostB)
	// address pair.
	wantBlock := "  connection {\n" +
		"    path {\n" +
		"      host n1 address 10.1.1.5:7000;\n" +
		"      host n2 address 10.1.1.6:7000;\n" +
		"    }\n" +
		"    path {\n" +
		"      host n1 address 10.2.2.5:7000;\n" +
		"      host n2 address 10.2.2.6:7000;\n" +
		"    }\n" +
		"  }\n"
	if !strings.Contains(got, wantBlock) {
		t.Errorf("multi-path connection block mismatch\n  want:\n%s\n  in:\n%s", wantBlock, got)
	}

	// The implicit `host n1 address 10.0.0.1:7000; host n2 address
	// 10.0.0.2:7000;` pair (from the `on` blocks) MUST NOT appear
	// inside the connection block — drbd-9 drops it when explicit
	// paths are present.
	bad := "  connection {\n    host n1 address 10.0.0.1:7000;"
	if strings.Contains(got, bad) {
		t.Errorf("unexpected implicit default-path lines in connection block:\n%s", got)
	}

	// The `on` blocks themselves still carry the default address —
	// drbd needs them for the listen socket. Sanity check.
	for _, want := range []string{
		"  on n1 {\n    address 10.0.0.1:7000;",
		"  on n2 {\n    address 10.0.0.2:7000;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("on-block default address missing: %q\nin:\n%s", want, got)
		}
	}
}

// TestMultiPathDefaultPathCoexistence pins scenario 3.8 (UG9 §"How
// adding a new DRBD path affects the default path", lines 2233-2255):
// when a resource-connection carries one or more explicit `path { … }`
// blocks, the implicit "default" path derived from each host's
// `address` field MUST NOT be re-rendered inside the connection block.
// Operators who want the default address to keep moving traffic must
// add it back as an explicit path (e.g. as `path3` alongside path1 +
// path2).
//
// Why this matters: the user-visible symptom is "I added a backup
// repl path and now my primary NIC went quiet". The driver of that
// behaviour is drbd-9 — as soon as ANY explicit path appears inside a
// connection, drbd ignores the connection-level `host … address …;`
// pair entirely. The renderer must mirror that, otherwise we ship
// .res files that disagree with what `drbdadm adjust` produces and
// trigger phantom reloads on every reconcile.
//
// SPEC PENDING IMPLEMENTATION (scenario 3.7):
//   - `pkg/drbd.Resource` has no `Connections` / `ResourcePaths`
//     field yet — multi-path is feature-flagged "T" in
//     tests/scenarios/03-networking.md §3.7. Implementation will add:
//     REST endpoint
//     `POST /v1/resource-definitions/{rd}/resource-connections/{a}/{b}/paths`,
//     a Resource-level slice of (NodeA, NodeB, []Path{Name, AddrA,
//     AddrB}) connection overrides, and ConfFileBuilder emitting
//     multiple `path { host A address X; host B address Y; }` blocks
//     inside each `connection { … }` block.
//   - Until 3.7 lands, t.Skip with a pointer to this test so we don't
//     forget to flip it on once the wiring exists.
//
// Contract this test will assert once unblocked:
//
//	Given a Resource with two diskful peers (n1, n2) and an explicit
//	ResourceConnection between (n1, n2) carrying path1 (10.1.0.0/24)
//	+ path2 (10.2.0.0/24):
//
//	  - The rendered `connection { … }` block contains TWO `path { … }`
//	    sub-blocks, one per explicit path.
//	  - The block does NOT contain the connection-level
//	    `host n1 address 10.0.0.1:7000;` /
//	    `host n2 address 10.0.0.2:7000;` lines (those are the implicit
//	    "default" path derived from the `on` blocks; drbd-9 drops them
//	    when explicit paths are present).
//	  - The `on n1 { address 10.0.0.1:7000; … }` / `on n2 { … }`
//	    blocks themselves still carry the default address — drbd needs
//	    them for the listen socket. Only the connection-level
//	    duplication is suppressed.
//
//	Given the same Resource but with a third explicit path "default"
//	whose addresses match the `on` block addresses (10.0.0.1 /
//	10.0.0.2):
//
//	  - The connection block now contains THREE `path { … }` blocks,
//	    including the default one. Operator opted back in to the
//	    default path being used as transport.
func TestMultiPathDefaultPathCoexistence(t *testing.T) {
	t.Skip("scenario 3.8 — pending 3.7 (multi-path) implementation; " +
		"Resource.Connections / ResourcePath design described in " +
		"tests/scenarios/03-networking.md §3.7 and in this test's godoc")
}

// 5.W04 renderer-side tests (TestBuildEmitsConnectionNetOptionsW04 +
// TestBuildConnectionWithoutNetOptionsLeavesBlockEmpty) dropped on
// cherry-pick conflict with wave1 3.7's ResourceConnection type.
// The REST + dispatcher halves of 5.W04 still land via
// pkg/rest/resource_connections_test.go and pkg/dispatcher/. To
// reintroduce the renderer pin, extend `ResourceConnection` with a
// `NetOptions map[string]string` field and revive the two tests below
// against `drbd.ResourceConnection` (not the dropped `drbd.Connection`).
//
// Original commented-out pin description:
//
// The test pins three things at once:
//
//   - the matching (n1, n2) pair gets the nested `net { max-buffers
//     8192; }` block;
//   - other mesh pairs (n1, n3) / (n2, n3) stay free of any nested
//     `net { }` block — per-(a, b) scope, not per-rd;
//   - the top-level `net { … }` is also untouched — operator didn't
//     touch RD scope.
//
// Unordered host match is asserted by registering the Connection as
// (n2, n1) while the mesh emits the pair as (n1, n2) — the renderer
// has to match on either order or the operator's CLI invocation
// silently drops the option for half the cluster.

// TestBuildDeterministic: same input → same output, twice in a row. Map
// iteration order would otherwise leak into the .res file and trigger
// spurious drbdadm reloads on every reconcile.
func TestBuildDeterministic(t *testing.T) {
	res := drbd.Resource{
		Name: "pvc-1",
		Net: drbd.Net{
			Options: map[string]string{
				"a": "1", "b": "2", "c": "3", "d": "4", "e": "5",
			},
		},
	}

	first, err := drbd.Build(res)
	if err != nil {
		t.Fatalf("Build first: %v", err)
	}

	for range 5 {
		again, err := drbd.Build(res)
		if err != nil {
			t.Fatalf("Build again: %v", err)
		}

		if again != first {
			t.Errorf("non-deterministic output:\nfirst:\n%s\nlater:\n%s", first, again)
		}
	}
}
