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

package cli_test

import (
	"strings"
	"testing"
)

// `rd drbd-options --<knob> <value>` stores the value under the
// property key for the knob's DRBD section. The section is what
// decides which `.res` block the option is rendered into: an
// uncatalogued net knob like verify-alg must reach net{}, not the
// resource options{} block where drbdadm would reject the file.
func TestResourceDefinitionDrbdOptions(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"rd", "drbd-options", "--verify-alg", "crc32c", "pvc-x"}); got != 0 {
		t.Fatalf("set verify-alg exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"rd", "drbd-options", "--max-buffers", "36864", "pvc-x"}); got != 0 {
		t.Fatalf("set max-buffers exit = %d (stderr: %s)", got, errBuf.String())
	}

	def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	want := map[string]string{
		"DrbdOptions/Net/verify-alg":  "crc32c",
		"DrbdOptions/Net/max-buffers": "36864",
	}

	for key, value := range want {
		if def.Props[key] != value {
			t.Errorf("props[%q] = %q, want %q (all: %v)", key, def.Props[key], value, def.Props)
		}
	}

	// `--unset-<knob>` removes it again.
	if got := app.Run(t.Context(), []string{"rd", "drbd-options", "--unset-max-buffers", "pvc-x"}); got != 0 {
		t.Fatalf("unset exit = %d (stderr: %s)", got, errBuf.String())
	}

	out.Reset()

	if got := app.Run(t.Context(), []string{"rd", "lp", "pvc-x"}); got != 0 {
		t.Fatalf("list-properties exit = %d", got)
	}

	if strings.Contains(out.String(), "max-buffers") {
		t.Errorf("--unset-max-buffers did not remove the key:\n%s", out.String())
	}

	if !strings.Contains(out.String(), "verify-alg") {
		t.Errorf("--unset-max-buffers removed an unrelated key:\n%s", out.String())
	}
}

// The controller's cluster-wide bag takes the same verb with no
// positional.
func TestControllerDrbdOptions(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"controller", "drbd-options", "--max-buffers", "36864"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"c", "lp"}); got != 0 {
		t.Fatalf("list-properties exit = %d", got)
	}

	if !strings.Contains(out.String(), "DrbdOptions/Net/max-buffers") {
		t.Errorf("controller drbd-options did not store the key:\n%s", out.String())
	}
}

// A volume definition takes the same verb with its two identifying
// positionals.
func TestVolumeDefinitionDrbdOptions(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedVolumeDefinition)

	argv := []string{"vd", "drbd-options", "--rs-discard-granularity", "65536", "pvc-x", "0"}
	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	vds, err := appStore(t, app).VolumeDefinitions().List(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if vds[0].Props["DrbdOptions/Disk/rs-discard-granularity"] != "65536" {
		t.Errorf("props = %v, want the disk-namespaced key", vds[0].Props)
	}
}

// A knob blockstor has no section for is a client-side rejection, not
// a property written under a guessed namespace.
func TestDrbdOptionsUnknownKnobRejected(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"rd", "drbd-options", "--not-a-knob", "1", "pvc-x"}); got != 2 {
		t.Errorf("unknown knob exit = %d, want 2 (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"rd", "drbd-options", "--unset-not-a-knob", "pvc-x"}); got != 2 {
		t.Errorf("unknown unset exit = %d, want 2", got)
	}
}

// drbd-options with no option at all is a usage error rather than a
// silent no-op write.
func TestDrbdOptionsNeedsAnOption(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"rd", "drbd-options", "pvc-x"}); got != 2 {
		t.Errorf("bare drbd-options exit = %d, want 2", got)
	}
}
