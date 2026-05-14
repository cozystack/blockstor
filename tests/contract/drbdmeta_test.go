//go:build contract

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

// Tier 3 drbd-utils contract tests. See docs/test-strategy.md "Tier 3"
// for the why. Every test exec-s a real drbdmeta / drbdadm binary
// inside an Alpine container against a 64 MiB loopback file, then
// asserts byte-shape (exit code, stderr substring, parser output) of
// the result. The class of regression these guard is "wrapper still
// compiles + unit-test FakeExec capture matches, but the real CLI
// rejected our flag combo" — exactly the bug Phase 2 Bug 81 surfaced.

package contract_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/tests/contract"
)

// TestDrbdmetaCreateMDSucceeds pins the production CreateMD call
// shape (pkg/drbd/drbdadm.go:CreateMD) against real drbdmeta. The
// production code shells out via `drbdadm create-md --force
// --max-peers=N <res>`; drbdadm parses the .res file and translates
// that to `drbdmeta --force <minor> v09 <device> internal create-md
// <max_peers>` (the kebab-case --max-peers=N is a drbdadm-only
// alias; drbdmeta itself takes max_peers as a trailing positional).
//
// We exec drbdmeta directly here because drbdadm needs a resource
// block from a .res file plus a usable backing disk — neither of
// which docker-without-privileged can provide. The translated
// drbdmeta arg shape is the wire-contract every CreateMD invocation
// ultimately hits, so a regression in either form fails this test.
//
// Asserts: exit 0 + stdout/stderr contain a marker that confirms
// drbdmeta wrote new metadata (the "New drbd meta data block
// successfully created." line is stable across drbd-utils 9.x).
func TestDrbdmetaCreateMDSucceeds(t *testing.T) {
	res := contract.RunDrbdmeta(t,
		"--force",
		"0", "v09", contract.LoopDevicePath, "internal",
		"create-md",
		// MaxPeers-1 mirrors pkg/drbd/drbdadm.go:CreateMD exactly —
		// drbdadm translates --max-peers=N to this trailing positional.
		itoa(drbd.MaxPeers-1),
	)

	if res.ExitCode != 0 {
		t.Fatalf("drbdmeta create-md: exit=%d\nstdout: %s\nstderr: %s",
			res.ExitCode, res.Stdout, res.Stderr)
	}

	// drbdmeta prints "Writing meta data..." + "New drbd meta data
	// block successfully created." on success. Either line is stable
	// enough across drbd-utils releases that a substring match is a
	// fair regression pin.
	combined := res.Stdout + res.Stderr
	if !strings.Contains(combined, "successfully created") &&
		!strings.Contains(combined, "Writing meta data") {
		t.Errorf("expected create-md success marker in output\nstdout: %s\nstderr: %s",
			res.Stdout, res.Stderr)
	}
}

// TestDrbdmetaSetGiRequiresNodeId is the **Bug 81** contract pin.
//
// On DRBD 9.2+, drbdmeta's `set-gi` subcommand refuses to run without
// `--node-id <N>`: it returns non-zero + prints "The set-gi command
// requires the --node-id option" to stderr. Older drbd-utils (≤9.1)
// accepted the no-node-id form, which is the call shape our
// pkg/drbd/drbdadm.go:SetGi still emits — so on a modern stand
// satellite-side first-activation GI seeding silently no-ops (the
// reconciler intentionally downgrades the error to a log to avoid
// crashing first-Apply, see pkg/satellite/reconciler.go:1180).
//
// This test pins the failure shape so a future drbd-utils upgrade
// that drops the message text (or changes the exit code) is caught
// immediately. When the bug is properly fixed (per-peer iteration
// with --node-id; tracked as Bug 81 follow-up), this test stays
// green and the sibling TestDrbdmetaSetGiPerPeer becomes the
// happy-path companion.
func TestDrbdmetaSetGiRequiresNodeId(t *testing.T) {
	// The error path drbdmeta exercises (CLI flag parser) fires
	// BEFORE the metadata read, so a fresh-zeroed loopback is fine
	// — drbdmeta refuses the call long before it touches bytes.
	// Skipping pre-create-md keeps this test single-container.
	res := contract.RunDrbdmeta(t,
		"--force",
		"0", "v09", contract.LoopDevicePath, "internal",
		"set-gi",
		// Legacy single-arg GI tuple — the form pkg/drbd/drbdadm.go:SetGi
		// emits. DRBD 9.2+ rejects this without --node-id.
		"0000000000000001:0000000000000001:0:0",
	)

	// Modern drbd-utils returns exit 10 for missing-required-flag;
	// older releases returned 20. Accept either non-zero to keep the
	// pin distro-portable, but assert the message text — that's the
	// stable contract.
	if res.ExitCode == 0 {
		t.Fatalf("set-gi without --node-id unexpectedly succeeded\nstdout: %s\nstderr: %s",
			res.Stdout, res.Stderr)
	}

	combined := res.Stdout + res.Stderr
	if !strings.Contains(combined, "requires the --node-id option") {
		t.Errorf("expected 'requires the --node-id option' in output, got\nstdout: %s\nstderr: %s",
			res.Stdout, res.Stderr)
	}
}

// TestDrbdmetaSetGiPerPeer pins the FIXED call shape — `set-gi
// --node-id <id> <gi-tuple>` — exits 0 on a fresh metadata block.
// This is the shape the Bug 81 follow-up will switch pkg/drbd to.
// Pinning it here lets that fix land without re-running e2e.
func TestDrbdmetaSetGiPerPeer(t *testing.T) {
	// Single RunDrbdmeta invocation per test gives us one loopback
	// file with one container; create-md + set-gi must run in the
	// same container against the same file, which neither helper
	// exposes — so we use the createThenSetGi composite helper in
	// the local package to keep the harness API minimal.
	res := createThenSetGi(t,
		"--node-id", "1",
		"0000000000000001:0000000000000001:0:0",
	)

	if res.ExitCode != 0 {
		t.Fatalf("set-gi --node-id 1 ... : exit=%d\nstdout: %s\nstderr: %s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
}

// TestDrbdmetaDumpMdParses runs create-md + dump-md and asserts the
// dump output contains the canonical v09 metadata fields. We don't
// require a full structured parser in production code — HasMD just
// checks exit 0 (pkg/drbd/drbdadm.go:HasMD) — so this test pins the
// presence of the keys downstream tooling depends on:
//
//   - `version "v09";`  — without this, HasMD's success means
//     "drbdmeta parsed _something_" which could be a regression
//     to v08 layout
//   - `max-peers 15;`   — proves the create-md max_peers positional
//     actually stuck (Bug-81-adjacent — wrong arg shape would
//     silently default to 7)
//   - `current-uuid`    — the GI tuple set-gi mutates; existence
//     here proves the metadata block is usable for set-gi
//   - `peer[`           — per-peer slot present, matching the
//     N=max_peers expansion (DRBD 9.2+ layout)
func TestDrbdmetaDumpMdParses(t *testing.T) {
	res := createThenDumpMd(t)

	if res.ExitCode != 0 {
		t.Fatalf("dump-md: exit=%d\nstdout: %s\nstderr: %s",
			res.ExitCode, res.Stdout, res.Stderr)
	}

	mustContain(t, res.Stdout, `version "v09"`, "version line")
	mustContain(t, res.Stdout, "max-peers "+itoa(drbd.MaxPeers-1), "max-peers value")
	mustContain(t, res.Stdout, "current-uuid", "current-uuid line")
	mustContain(t, res.Stdout, "peer[", "per-peer slot")
}

// TestDrbdadmConfigValidate renders a 2-replica .res via
// pkg/drbd/conffile.go:Build with a representative Resource (matches
// the shape the satellite emits for a 2-replica DRBD RD with one
// volume), writes it to a temp file, mounts it into the container,
// and runs `drbdadm dump <rsc>` — drbdadm parses, validates, and
// re-emits the resource. Exit 0 proves the .res is syntactically
// valid drbd-9 from a real drbd-utils parser's perspective, which is
// stronger than the existing pkg/drbd/conffile_test.go assertions
// (those only check our renderer's own output against a golden
// string).
func TestDrbdadmConfigValidate(t *testing.T) {
	contract.SkipIfNotLinux(t)
	contract.EnsureImage(t)

	r := drbd.Resource{
		Name: "pvc-contract",
		Net: drbd.Net{
			ProtocolC:    true,
			SharedSecret: "0123456789abcdef",
		},
		Hosts: []drbd.Host{
			{
				NodeName: "worker-a",
				Address:  "10.0.0.1",
				Port:     7000,
				NodeID:   0,
				IsLocal:  true,
			},
			{
				NodeName: "worker-b",
				Address:  "10.0.0.2",
				Port:     7000,
				NodeID:   1,
			},
		},
		Volumes: []drbd.Volume{
			{
				Number: 0,
				Device: "/dev/drbd1000",
				Disk:   "/dev/vg0/pvc-contract_00000",
				Minor:  1000,
			},
		},
		Options: map[string]string{
			"quorum":       "majority",
			"on-no-quorum": "io-error",
			"auto-promote": "yes",
		},
	}

	body, err := drbd.Build(r)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	resPath := filepath.Join(dir, r.Name+".res")
	if err := os.WriteFile(resPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write res: %v", err)
	}

	// `drbdadm -c /etc/drbd.d/<name>.res dump <name>` validates the
	// referenced file. Without -c drbdadm reads /etc/drbd.conf which
	// our minimal image doesn't ship; pointing it directly at the
	// resource file makes the test self-contained. --hostname pinned
	// to the first host's NodeName so drbdadm's local-host lookup
	// resolves cleanly.
	res := contract.RunDrbdadm(t, resPath, r.Hosts[0].NodeName,
		"-c", "/etc/drbd.d/"+filepath.Base(resPath),
		"dump", r.Name,
	)

	if res.ExitCode != 0 {
		t.Fatalf("drbdadm dump %s: exit=%d\nstdout: %s\nstderr: %s\n--- res file ---\n%s",
			r.Name, res.ExitCode, res.Stdout, res.Stderr, body)
	}

	// Bonus: drbdadm dump re-emits the parsed resource. Confirm the
	// resource name round-tripped — guards against drbdadm silently
	// accepting an empty file.
	if !strings.Contains(res.Stdout, "resource "+r.Name) {
		t.Errorf("drbdadm dump did not re-emit resource %s\nstdout: %s",
			r.Name, res.Stdout)
	}
}

// ----- helpers -----

// createThenSetGi runs create-md + set-gi against the same loopback
// file inside a single container, so the metadata written by
// create-md is the same metadata set-gi mutates.
func createThenSetGi(t *testing.T, setGiArgs ...string) contract.RunResult {
	t.Helper()

	return contract.RunDrbdmetaChain(t,
		[][]string{
			{"--force", "0", "v09", contract.LoopDevicePath, "internal", "create-md", itoa(drbd.MaxPeers - 1)},
			append([]string{"--force", "0", "v09", contract.LoopDevicePath, "internal", "set-gi"}, setGiArgs...),
		})
}

// createThenDumpMd runs create-md + dump-md against the same loopback.
// dump-md needs `--force` here because the loopback target is a
// regular file rather than a real block device; drbdmeta's
// `is_block_device` check otherwise rejects it with exit 20.
func createThenDumpMd(t *testing.T) contract.RunResult {
	t.Helper()

	return contract.RunDrbdmetaChain(t,
		[][]string{
			{"--force", "0", "v09", contract.LoopDevicePath, "internal", "create-md", itoa(drbd.MaxPeers - 1)},
			{"--force", "0", "v09", contract.LoopDevicePath, "internal", "dump-md"},
		})
}

// itoa is a one-line strconv.Itoa replacement that keeps the test
// file's import list short.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	if neg {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}

// mustContain asserts substring presence with a labelled error.
func mustContain(t *testing.T, hay, needle, label string) {
	t.Helper()

	if !strings.Contains(hay, needle) {
		t.Errorf("%s: missing %q in:\n%s", label, needle, hay)
	}
}
