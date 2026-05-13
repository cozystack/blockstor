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

// Package cheatsheet holds static guards for the operator
// cheat-sheet's Level-3 satellite-container utilities (scenario 1.25,
// tests/scenarios/01-api-contract.md). The cheat-sheet expects
// drbdadm / drbdsetup / lsblk / lvs / vgs / pvs / zfs / zpool /
// cryptsetup to be available inside the blockstor-satellite image.
// These tests pin that promise at build time so a Dockerfile drift
// (e.g. dropping zfsutils-linux from the apt install line) breaks
// `go test ./...` rather than only the live e2e smoke.
package cheatsheet

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// repoRoot finds the repository root by walking up from this test
// file until we find go.mod. Avoids hard-coding a path that breaks
// when this test moves.
func repoRoot(t *testing.T) string {
	t.Helper()

	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller(0) returned !ok")
	}

	d := filepath.Dir(here)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}
		d = filepath.Dir(d)
	}

	t.Fatalf("go.mod not found walking up from %s", here)

	return ""
}

// TestSatelliteImageShipsCheatSheetUtilities asserts the satellite
// Dockerfile installs each apt package the cheat-sheet's Level-3
// commands depend on. This is a static guard — the live runtime check
// is tests/e2e/satellite-utils-smoke.sh, which also verifies the
// binaries are on PATH and answer a read-only invocation under the
// pod's capability set.
func TestSatelliteImageShipsCheatSheetUtilities(t *testing.T) {
	root := repoRoot(t)

	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}

	// Locate the satellite stage so we don't accidentally pass on
	// packages the controller image happens to list.
	src := string(dockerfile)

	satIdx := strings.Index(src, "AS satellite")
	if satIdx < 0 {
		t.Fatalf("Dockerfile missing `AS satellite` stage")
	}
	stage := src[satIdx:]

	// Each package the cheat-sheet's Level-3 row depends on.
	// |   drbdadm / drbdsetup          → drbd-utils
	// |   lvs / vgs / pvs              → lvm2
	// |   zfs / zpool                  → zfsutils-linux
	// |   cryptsetup                   → cryptsetup-bin
	// |   lsblk + sh + (dmesg via util-linux) → ships with debian-slim base
	//
	// We don't pin lsblk/dmesg here — they come from the debian-slim
	// base layer's util-linux package and aren't an explicit `apt
	// install` line. The e2e smoke catches a regression where the
	// base image swaps to something without them.
	wantPkgs := []string{
		"drbd-utils",
		"lvm2",
		"cryptsetup-bin",
		"zfsutils-linux",
	}

	for _, p := range wantPkgs {
		if !strings.Contains(stage, p) {
			t.Errorf("satellite stage missing apt package %q (cheat-sheet Level-3 dependency)", p)
		}
	}
}

// TestSatelliteImageBaseHasShell asserts the satellite base image is
// not distroless — the cheat-sheet's drbdadm + .res workflow relies
// on an interactive shell for `kubectl exec` debugging. A regression
// to gcr.io/distroless/* would silently break the entire Level-3
// flow (no /bin/sh → no `kubectl exec -ti ... -- bash`).
func TestSatelliteImageBaseHasShell(t *testing.T) {
	root := repoRoot(t)

	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}

	src := string(dockerfile)

	satIdx := strings.Index(src, "AS satellite")
	if satIdx < 0 {
		t.Fatalf("Dockerfile missing `AS satellite` stage")
	}

	// Walk backward to the FROM line of the satellite stage.
	preamble := src[:satIdx]

	lastFrom := strings.LastIndex(preamble, "FROM ")
	if lastFrom < 0 {
		t.Fatalf("Dockerfile satellite stage has no `FROM` line")
	}

	fromLine := preamble[lastFrom:]
	if nl := strings.Index(fromLine, "\n"); nl > 0 {
		fromLine = fromLine[:nl]
	}

	// distroless-static / distroless-base have no /bin/sh; reject.
	if strings.Contains(fromLine, "distroless") {
		t.Errorf("satellite stage uses distroless (%q) — breaks `kubectl exec -ti ... -- bash` cheat-sheet flow", fromLine)
	}
}
