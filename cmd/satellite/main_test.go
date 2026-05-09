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

package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// TestPortFromListen pins the parser the satellite uses to derive its
// advertised port from the --listen flag. The advertised endpoint
// computation depends on this — wrong port means controller can't dial
// back in.
func TestPortFromListen(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{":7000", "7000"},
		{"0.0.0.0:7000", "7000"},
		{"[::]:7000", "7000"},
		{"localhost:7001", "7001"},
		{"no-port", ""},
		{"", ""},
		{":", ""},
	}

	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := portFromListen(c.in)
			if got != c.want {
				t.Errorf("portFromListen(%q): got %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestCleanStateDirWipesResFiles: the satellite wipes stale .res files
// from prior incarnations on startup so drbdadm doesn't trip on a
// half-rendered file from the previous run. Non-.res files
// (global_common.conf, operator overrides) must be left alone.
func TestCleanStateDirWipesResFiles(t *testing.T) {
	dir := t.TempDir()

	files := map[string]bool{
		"pvc-1.res":          true,  // expect deleted
		"pvc-2.res":          true,  // expect deleted
		"global_common.conf": false, // must survive
		"operator-overlay":   false, // must survive (no .res suffix)
	}

	for name := range files {
		err := os.WriteFile(filepath.Join(dir, name), []byte("# test"), 0o600)
		if err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	logger := slog.New(slog.DiscardHandler)
	cleanStateDir(dir, logger)

	for name, shouldBeGone := range files {
		_, statErr := os.Stat(filepath.Join(dir, name))
		exists := statErr == nil

		switch {
		case shouldBeGone && exists:
			t.Errorf("%s should have been wiped", name)
		case !shouldBeGone && !exists:
			t.Errorf("%s should have survived: %v", name, statErr)
		}
	}
}

// TestCleanStateDirMissingDirIsFine: the satellite's first Apply
// creates the dir on demand. cleanStateDir on a missing path must
// silently no-op rather than fail startup.
func TestCleanStateDirMissingDirIsFine(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	// Should not panic, log noisily, or leave any side-effect.
	cleanStateDir(filepath.Join(t.TempDir(), "does-not-exist"), logger)
}

// TestCleanStateDirSkipsSubdirectories: subdirectories under the
// state-dir (none today, but defensive) must not be wiped — we only
// scrub regular .res files.
func TestCleanStateDirSkipsSubdirectories(t *testing.T) {
	dir := t.TempDir()

	subDir := filepath.Join(dir, "sub.res")
	if err := os.Mkdir(subDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	logger := slog.New(slog.DiscardHandler)
	cleanStateDir(dir, logger)

	if _, statErr := os.Stat(subDir); statErr != nil {
		t.Errorf("subdirectory ending in .res should survive cleanStateDir; got %v", statErr)
	}
}
