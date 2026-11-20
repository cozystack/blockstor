// SPDX-License-Identifier: Apache-2.0

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

package manifests

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// repoRoot240 returns the directory containing the project's go.mod, by
// walking up from the test file's location. Mirrors the lookup
// probes_test.go uses but is kept local for hermetic test execution.
func repoRoot240(t *testing.T) string {
	t.Helper()

	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	dir := filepath.Dir(here)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from test file")
		}

		dir = parent
	}
}

// TestSatelliteImageShipsMkfs pins Bug 240 — the satellite `Apply`
// path shells out to `mkfs.<fsType>` when a Resource's mkfs prop is
// set, but the satellite's Debian-slim image historically didn't
// install `e2fsprogs` (ext4) or `xfsprogs` (xfs). The auto-format
// silently failed at runtime and operators saw "Resource never
// ready" with no obvious cause.
func TestSatelliteImageShipsMkfs(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile(filepath.Join(repoRoot240(t), "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}

	src := string(body)

	for _, pkg := range []string{"e2fsprogs", "xfsprogs"} {
		if !strings.Contains(src, pkg) {
			t.Errorf("Bug 240: Dockerfile must install %q so the satellite "+
				"auto-mkfs path can format ext4 / xfs volumes without silently "+
				"failing at runtime; pkg not found in Dockerfile", pkg)
		}
	}
}

// TestSatellitePreStopHookHasNoUnshippedInterpreter pins Bug 241 —
// the daemonset's preStop hook previously piped `drbdsetup status
// --json` through `python3`, but python3 is not in the satellite
// image. The 2>/dev/null suppression turned the missing-interpreter
// crash into a silent no-op, so the for-loop iterated zero times
// and `drbdadm down` never ran on graceful pod termination,
// silently undoing Bug 82's per-resource down-on-shutdown contract.
//
// Parses the YAML to extract the actual preStop exec command (vs.
// raw source text) so YAML comments describing the historical fix
// don't trip the guard. The allow-list is intentionally tiny
// (POSIX shell + tools the satellite image already ships).
func TestSatellitePreStopHookHasNoUnshippedInterpreter(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile(filepath.Join(repoRoot240(t),
		"stand/blockstor-satellite-daemonset.yaml"))
	if err != nil {
		t.Fatalf("read daemonset: %v", err)
	}

	// Decode every YAML document, find the DaemonSet, walk to the
	// satellite container's lifecycle.preStop.exec.command. Treat
	// the joined command argv as the hook body to scan.
	for doc := range strings.SplitSeq(string(body), "\n---\n") {
		if !strings.Contains(doc, "kind: DaemonSet") {
			continue
		}

		var obj struct {
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name      string `json:"name"`
							Lifecycle struct {
								PreStop struct {
									Exec struct {
										Command []string `json:"command"`
									} `json:"exec"`
								} `json:"preStop"`
							} `json:"lifecycle"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}

		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("unmarshal DaemonSet: %v", err)
		}

		for _, c := range obj.Spec.Template.Spec.Containers {
			if c.Name != "satellite" {
				continue
			}

			hook := strings.Join(c.Lifecycle.PreStop.Exec.Command, " ")

			// Forbidden tokens that would re-open Bug 241. Trailing
			// space avoids matching prose like "pythonic".
			forbidden := []string{"python3", "python ", "perl ", "ruby ", "jq "}

			for _, token := range forbidden {
				if strings.Contains(hook, token) {
					t.Errorf("Bug 241: preStop hook contains %q which is not in "+
						"the satellite image; the hook will silently no-op via "+
						"2>/dev/null and drbdadm down won't run on pod "+
						"termination. Use pure POSIX shell (awk on `drbdsetup "+
						"status` without --json works).", token)
				}
			}

			return
		}
	}

	t.Fatal("satellite container with preStop hook not found in daemonset")
}

// Every file the image build downloads is checked against a pinned
// sha256 before use. The LINBIT keyring decides which packages apt
// trusts, so fetching it on TLS alone would let a changed file at that
// URL install a key nobody reviewed, and the build could not be
// reproduced.
func TestDockerfileDownloadsArePinnedByDigest(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile(filepath.Join(repoRoot240(t), "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}

	src := string(body)
	digest := regexp.MustCompile(`(?m)^ARG (\w+_SHA256)=[0-9a-f]{64}$`)

	for line := range strings.SplitSeq(src, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "ADD" || !strings.Contains(fields[1], "://") {
			continue
		}

		if strings.HasPrefix(fields[1], "--checksum=sha256:") {
			continue
		}

		dest := fields[len(fields)-1]
		verified := false

		for _, m := range digest.FindAllStringSubmatch(src, -1) {
			if strings.Contains(src, `echo "${`+m[1]+`}  `+dest+`" | sha256sum -c -`) {
				verified = true
			}
		}

		if !verified {
			t.Errorf("Dockerfile downloads %s into %s without checking it against a pinned sha256", fields[1], dest)
		}
	}
}

// drbdmeta before 9.36.0 clears DRBD_MDFF_BITMAP_AUTHORITATIVE whenever
// it writes the superblock. apply-al does that after every crash, and
// DRBD 9.3.4 then treats every out-of-sync bit of the crashed node as
// one its peers lack: at equal current UUIDs the node that was down
// becomes the resync source of an UpToDate peer, or a Primary peer
// refuses the handshake as a SyncTarget.
func TestSatelliteDrbdUtilsKeepsBitmapProvenance(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile(filepath.Join(repoRoot240(t), "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}

	src := string(body)

	pin := regexp.MustCompile(`(?m)^ARG DRBD_UTILS_VERSION=(\d+)\.(\d+)\.(\d+)-\d+$`).FindStringSubmatch(src)
	if pin == nil {
		t.Fatal("Dockerfile must pin drbd-utils with ARG DRBD_UTILS_VERSION=<major>.<minor>.<patch>-<rev>")
	}

	if !strings.Contains(src, `drbd-utils="${DRBD_UTILS_VERSION}"`) {
		t.Error("the satellite stage must install drbd-utils at DRBD_UTILS_VERSION")
	}

	got := [3]int{}
	for i := range got {
		got[i], err = strconv.Atoi(pin[i+1])
		if err != nil {
			t.Fatalf("parse DRBD_UTILS_VERSION: %v", err)
		}
	}

	if want := [3]int{9, 36, 0}; slices.Compare(got[:], want[:]) < 0 {
		t.Errorf("drbd-utils %d.%d.%d clears the bitmap provenance feature on apply-al; need at least 9.36.0",
			got[0], got[1], got[2])
	}
}
