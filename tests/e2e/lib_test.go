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

package e2e_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeKubectl answers the node and satellite queries the e2e lib makes at
// source time: no node without the control-plane label, three satellites in
// blockstor-system, a Succeeded one there on an earlier-sorted node, and a
// matching pod on an earlier-sorted node in another namespace.
const fakeKubectl = `#!/usr/bin/env bash
args="$*"
case "$args" in
  *"get nodes"*) ;;
  *"-n blockstor-system get pods"*"status.phase=Running"*) echo "node-b node-a node-c" ;;
  *"-n blockstor-system get pods"*) echo "completed-0 node-b node-a node-c" ;;
  *"get pods -A"*) echo "foreign-0 node-b node-a node-c" ;;
  *) echo "unexpected kubectl $args" >&2; exit 1 ;;
esac
`

// The satellite fallback picks the workers from the Running satellites of the
// namespace the run works in: on_node looks for a Running pod there, so a node
// with a satellite only elsewhere, or only a completed one, would be a worker
// no cell can reach.
func TestTheSatelliteFallbackStaysInTheRunsNamespace(t *testing.T) {
	t.Parallel()

	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(fakeKubectl), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cmd := exec.CommandContext(t.Context(), "bash", "-c", `source ./lib.sh && echo "$WORKER_1 $WORKER_2 $WORKER_3"`)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "NS=blockstor-system", "BS_WORKERS=")

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("source lib.sh: %v\n%s", err, out)
	}

	if got := strings.TrimSpace(string(out)); got != "node-a node-b node-c" {
		t.Errorf("workers %q, want the Running satellites of blockstor-system only", got)
	}
}
