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
