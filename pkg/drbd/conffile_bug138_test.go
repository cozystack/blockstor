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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// Regression coverage for the conflicting-protocol render path: a
// `.res` file emitted with `protocol C` at the resource top level
// AND inside `net { protocol A; }` after a user override via
// `linstor rd m <rd> DrbdOptions/Net/protocol A`. drbd parses the
// outer clause first, so the user-set protocol is silently ignored.
//
// These tests pin the post-fix shape: every rendered `.res` carries
// exactly one `protocol …;` clause, and any user override under
// `DrbdOptions/Net/protocol` wins.

// TestBug138NetProtocolFromUserOverridesDefault — user-set
// `DrbdOptions/Net/protocol=A` must produce ONE `protocol A;` clause
// and NO conflicting top-level `protocol C;`.
func TestBug138NetProtocolFromUserOverridesDefault(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net: drbd.Net{
			ProtocolC: true, // legacy default the renderer would otherwise stamp
			Options:   map[string]string{"protocol": "A"},
		},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Exactly one `protocol …;` clause anywhere in the .res file.
	if n := countProtocolClauses(got); n != 1 {
		t.Errorf("want exactly 1 `protocol …;` clause, got %d\n%s", n, got)
	}

	// The single clause must be `protocol A;`, NOT `protocol C;`.
	if !strings.Contains(got, "protocol A;") {
		t.Errorf("missing user-set `protocol A;` clause\n%s", got)
	}

	if strings.Contains(got, "protocol C;") {
		t.Errorf("conflicting `protocol C;` clause leaked through user override\n%s", got)
	}

	// The clause must live inside `net { … }`, not at the resource
	// top level — drbd reads net{} for the network protocol.
	netStart := strings.Index(got, "net {")
	if netStart < 0 {
		t.Fatalf("net{} block missing\n%s", got)
	}

	netEnd := netStart + strings.Index(got[netStart:], "}")
	if !strings.Contains(got[netStart:netEnd], "protocol A;") {
		t.Errorf("`protocol A;` not inside net{} block\n%s", got)
	}
}

// TestBug138NetProtocolDefaultUsedWhenUnset — no operator override.
// The renderer must still emit exactly one `protocol C;` clause; the
// pre-bug location (resource top level) is preserved when no override
// is in play so unrelated assertions across the codebase don't shift.
func TestBug138NetProtocolDefaultUsedWhenUnset(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net:  drbd.Net{ProtocolC: true},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if n := countProtocolClauses(got); n != 1 {
		t.Errorf("want exactly 1 `protocol …;` clause, got %d\n%s", n, got)
	}

	if !strings.Contains(got, "protocol C;") {
		t.Errorf("missing default `protocol C;`\n%s", got)
	}
}

// TestBug138MultipleNetOptionsApplied — `protocol=A`, `ko-count=7`,
// `ping-timeout=100` all surface under `net { … }`. Pinning the
// systemic fix: every DrbdOptions/Net/<key> value the user sets must
// reach the rendered .res verbatim, with no second copy stamped
// elsewhere by the renderer's defaults.
func TestBug138MultipleNetOptionsApplied(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net: drbd.Net{
			ProtocolC: true,
			Options: map[string]string{
				"protocol":     "A",
				"ko-count":     "7",
				"ping-timeout": "100",
			},
		},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	netStart := strings.Index(got, "net {")
	if netStart < 0 {
		t.Fatalf("net{} block missing\n%s", got)
	}

	netEnd := netStart + strings.Index(got[netStart:], "}")
	netBlock := got[netStart:netEnd]

	for _, want := range []string{
		"protocol A;",
		"ko-count 7;",
		"ping-timeout 100;",
	} {
		if !strings.Contains(netBlock, want) {
			t.Errorf("net{} block missing %q\nnet-block=%q\nfull=%s", want, netBlock, got)
		}
	}

	// No leftover `protocol C;` from the default path.
	if strings.Contains(got, "protocol C;") {
		t.Errorf("default `protocol C;` not suppressed by user override\n%s", got)
	}

	if n := countProtocolClauses(got); n != 1 {
		t.Errorf("want exactly 1 `protocol …;` clause, got %d\n%s", n, got)
	}
}

// TestBug138RenderedConfIsDrbdadmParseable — best-effort syntactic
// validation via `drbdadm -c <tmp.res> dump <name>`. Skipped when
// drbdadm isn't available in the test environment (which is the
// common case for laptop / CI runs without DRBD kmod / userland).
func TestBug138RenderedConfIsDrbdadmParseable(t *testing.T) {
	const resName = "pvc-1"

	body, err := drbd.Build(drbd.Resource{
		Name: resName,
		Net: drbd.Net{
			ProtocolC: true,
			Options:   map[string]string{"protocol": "A"},
		},
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

	bin, err := exec.LookPath("drbdadm")
	if err != nil {
		t.Skipf("drbdadm not available (%v); skipping syntactic validation", err)
	}

	dir := t.TempDir()
	resPath := filepath.Join(dir, resName+".res")

	if err := os.WriteFile(resPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write .res: %v", err)
	}

	out, err := exec.CommandContext(t.Context(), bin, "-c", resPath, "dump", resName).CombinedOutput()
	if err != nil {
		t.Fatalf("drbdadm dump failed: %v\noutput=%s\nres=\n%s", err, out, body)
	}
}

// countProtocolClauses returns the number of `protocol <X>;` clauses
// in the rendered .res — counts standalone occurrences (resource-top
// + net-block) but not occurrences inside the substring `protocol_*`
// of other keys (drbd has no such key today, but defensive).
func countProtocolClauses(s string) int {
	n := 0
	rest := s

	for {
		i := strings.Index(rest, "protocol ")
		if i < 0 {
			return n
		}

		// Reject false positives where `protocol ` is a tail of
		// another identifier (e.g. `xprotocol `). The only legal
		// preceding characters are line-starts and whitespace.
		if i > 0 {
			prev := rest[i-1]
			if prev != ' ' && prev != '\t' && prev != '\n' {
				rest = rest[i+len("protocol "):]

				continue
			}
		}

		// Must end with `;` on the same line.
		semi := strings.IndexByte(rest[i:], ';')
		nl := strings.IndexByte(rest[i:], '\n')

		if semi > 0 && (nl < 0 || semi < nl) {
			n++
		}

		rest = rest[i+len("protocol "):]
	}
}
