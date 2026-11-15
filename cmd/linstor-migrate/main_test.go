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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A port file with both line forms reaches the converter options.
func TestBuildOptionsLoadsBothPortForms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drbd-ports.txt")
	if err := os.WriteFile(path, []byte("PVC-A node-1 7000\npvc-a node-2 7001\n# comment\npvc-b 7010\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	opts, err := buildOptions(path)
	if err != nil {
		t.Fatalf("buildOptions: %v", err)
	}

	if got := opts.ReplicaDRBDPorts["pvc-a"]; got["node-1"] != 7000 || got["node-2"] != 7001 {
		t.Errorf("ReplicaDRBDPorts[pvc-a] = %v, want node-1:7000 node-2:7001", got)
	}

	if got := opts.DRBDPorts["pvc-b"]; got != 7010 {
		t.Errorf("DRBDPorts[pvc-b] = %d, want 7010", got)
	}
}
