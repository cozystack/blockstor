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

package output_test

import (
	"bytes"
	"encoding/json"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/output"
)

// `--machine-readable` list output is DOUBLE-nested: `[[obj, ...]]`.
// Every jq expression in tests/e2e/cli-matrix and the operator harness
// is written against that shape (`.[][]?`, `.[0][]?`), and the
// integration harness unwraps it explicitly. A single-nested array
// would make all of them silently match nothing.
func TestMachineReadableListIsDoubleNested(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := output.MachineList(&buf, []apiv1.Resource{
		{Name: "pvc-a", NodeName: "node-1"},
		{Name: "pvc-b", NodeName: "node-2"},
	})
	if err != nil {
		t.Fatalf("MachineList: %v", err)
	}

	var outer []json.RawMessage

	err = json.Unmarshal(buf.Bytes(), &outer)
	if err != nil {
		t.Fatalf("top level is not an array: %v\n%s", err, buf.String())
	}

	if len(outer) != 1 {
		t.Fatalf("top-level array has %d entries, want exactly 1 (the envelope)", len(outer))
	}

	var inner []apiv1.Resource

	err = json.Unmarshal(outer[0], &inner)
	if err != nil {
		t.Fatalf("second level is not an array of objects: %v", err)
	}

	if len(inner) != 2 || inner[0].Name != "pvc-a" || inner[1].NodeName != "node-2" {
		t.Errorf("inner array = %+v, want the two resources in order", inner)
	}
}

// An empty result still emits the envelope, so `jq '.[][]?'` yields
// nothing instead of erroring on a null.
func TestMachineReadableEmptyListKeepsEnvelope(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := output.MachineList(&buf, []apiv1.Resource{})
	if err != nil {
		t.Fatalf("MachineList: %v", err)
	}

	var outer [][]apiv1.Resource

	err = json.Unmarshal(buf.Bytes(), &outer)
	if err != nil {
		t.Fatalf("empty list is not a nested array: %v\n%s", err, buf.String())
	}

	if len(outer) != 1 || len(outer[0]) != 0 {
		t.Errorf("empty list = %v, want [[]]", outer)
	}
}

// Singleton payloads (an API response envelope, a property bag) are
// FLAT — `[obj]`, not `[[obj]]`. The harness helper that unwraps
// repeatedly copes with either, but the CLI must emit what the

// Machine output must be newline-terminated and free of ANSI, whatever
// the colour settings are: it is consumed by jq, never by a human.
func TestMachineReadableIsPlainAndNewlineTerminated(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := output.MachineList(&buf, []apiv1.Resource{{Name: "pvc-a"}})
	if err != nil {
		t.Fatalf("MachineList: %v", err)
	}

	out := buf.String()
	if len(out) == 0 || out[len(out)-1] != '\n' {
		t.Errorf("machine output is not newline-terminated: %q", out)
	}

	if bytes.Contains(buf.Bytes(), []byte{0x1b}) {
		t.Errorf("machine output contains an escape sequence: %q", out)
	}
}

// The JSON keys are the wire DTO's own tags — the jq paths in this
// repo read snake_case fields like `node_name` and `create_timestamp`,
// so the encoder must not re-case them.
func TestMachineReadableUsesWireFieldNames(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := output.MachineList(&buf, []apiv1.Resource{{
		Name:            "pvc-a",
		NodeName:        "node-1",
		CreateTimestamp: 1_784_000_000_000,
	}})
	if err != nil {
		t.Fatalf("MachineList: %v", err)
	}

	for _, key := range []string{`"name"`, `"node_name"`, `"create_timestamp"`} {
		if !bytes.Contains(buf.Bytes(), []byte(key)) {
			t.Errorf("machine output is missing the wire key %s:\n%s", key, buf.String())
		}
	}
}
