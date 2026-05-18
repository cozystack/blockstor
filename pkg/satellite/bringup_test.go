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

package satellite

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errDrbdadmUpFail is the sentinel canned-failure for the
// `drbdadm up <name>` shell-out in TestBringUpResourcePropagates
// Error. Package-level static error keeps err113 happy (no
// dynamic errors.New at the test call site).
var errDrbdadmUpFail = errors.New("drbdadm: exit status 1")

// TestBringUpResourceCallsDrbdadmUp pins the Phase 11.2.c Stage 3c
// invariant on the extracted helper directly: bringUpResource MUST
// shell out to `drbdadm up <name>` and return nil on success. The
// helper is the first-load path after createMetadata — distinct
// from runAdjust's Bug-287 `(158) Unknown resource` fallback to
// `drbdadm up` which is the recovery verb in the half-torn
// kernel-slot window and lives at its own call site.
//
// Targets the helper directly (rather than going through applyDRBD)
// so a regression in the helper's shell-out surfaces here rather
// than only via the end-to-end first-activation tests.
func TestBringUpResourceCallsDrbdadmUp(t *testing.T) {
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-bringup-ok",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	if err := rec.bringUpResource(context.Background(), dr); err != nil {
		t.Fatalf("bringUpResource: %v", err)
	}

	want := "drbdadm up pvc-bringup-ok"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestBringUpResourcePropagatesError pins the helper's error
// contract: when `drbdadm up <name>` fails (FakeExec returns a
// canned error to mimic exit 1), bringUpResource MUST return a
// wrapped error that preserves the resource name in the message so
// the reconciler retry loop and operator logs have actionable
// context. A regression that swallowed the error would silently
// strand the resource down across reconciles.
func TestBringUpResourcePropagatesError(t *testing.T) {
	fx := storage.NewFakeExec()

	// Canned failure for the up shell-out: FakeResponse.Err drives
	// the wrapper into the error arm. Mirrors `exit 1` on the real
	// drbdadm CLI.
	fx.Expect("drbdadm up pvc-bringup-fail", storage.FakeResponse{
		Err: errDrbdadmUpFail,
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-bringup-fail",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	err := rec.bringUpResource(context.Background(), dr)
	if err == nil {
		t.Fatalf("bringUpResource: expected wrapped error, got nil")
	}

	// Wrap MUST preserve the resource name + the verb so operator
	// logs surface "which resource failed which verb". The exact
	// format mirrors the inline call site this helper replaced.
	if !strings.Contains(err.Error(), "drbdadm up pvc-bringup-fail") {
		t.Errorf("error wrap missing %q context: %v", "drbdadm up pvc-bringup-fail", err)
	}
}
