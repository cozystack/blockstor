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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestCreateMetadataIdempotentOnPreExistingMd pins the Phase 11.2.c
// Stage 3a invariant: when `drbdadm dump-md <res>` already reports
// parseable metadata on the lower disk (operator-stamped, satellite
// restart between create-md and marker write, etc.), the helper
// MUST NOT re-run `drbdadm create-md` — `create-md --force` would
// wipe the operator-stamped GI + bitmap state. It MUST still fire
// the per-peer drbdmeta set-gi seeds (Bug 319 invariant: stamp the
// fresh-replica day0 GI tuple on every peer node-id slot AND the
// local slot, even when metadata adoption skipped create-md), MUST
// write the `.md-created` marker so subsequent reconciles take the
// firstActivation=false branch, and MUST stamp the
// `MetadataCreated=True` Status Condition via the injected
// stamper.
//
// This test targets the extracted helper directly (rather than
// going through applyDRBD) so a regression in the helper's
// internal flow surfaces at this layer rather than only via the
// end-to-end applyDRBD tests. The set of post-conditions mirrors
// the W09 disk-replace recovery path that the e2e
// `disk-replace-internal-metadata.sh` exercises end-to-end.
func TestCreateMetadataIdempotentOnPreExistingMd(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// dump-md returns a parseable metadata block — the satellite's
	// HasMD only needs `err == nil && len(out) > 0`, so a minimal
	// canned response suffices to drive the adopt-existing branch.
	fx.Expect("drbdadm dump-md pvc-md-adopt",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})

	// Thin LVM provider so resolveSeedGi synthesises a deterministic
	// day0 GI (IsThinOrZFS path) and the per-peer set-gi loop fires.
	// Without a registered provider the seed resolution returns
	// ok=false and the set-gi loop is a no-op — the test would
	// silently mis-assert the seed-still-fires invariant.
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

	stamper := &fakeMetadataStamperInternal{}
	rec := NewReconciler(ReconcilerConfig{
		Providers:              map[string]storage.Provider{"thin1": thin},
		Adm:                    drbd.NewAdm(fx),
		StateDir:               dir,
		NodeName:               "n1",
		LocalAddress:           "10.0.0.1",
		MetadataCreatedStamper: stamper,
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-md-adopt",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []string{"n2"},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "1",
			"peer.n2.port":    "7000",
		},
	}
	devices := map[int32]string{0: "/dev/vg/pvc-md-adopt_00000"}

	err := rec.createMetadata(context.Background(), dr, devices)
	if err != nil {
		t.Fatalf("createMetadata: %v", err)
	}

	calls := fx.CommandLines()

	// Adopt-existing safety: create-md MUST NOT fire when dump-md
	// reports a healthy metadata block. Re-running create-md would
	// wipe the operator-stamped state and orphan local data.
	for _, line := range calls {
		if strings.HasPrefix(line, "drbdadm create-md") {
			t.Errorf("createMetadata re-ran create-md despite HasMD=true (would wipe metadata): %s", line)
		}
	}

	// HasMD probe MUST have fired — without it the create-md guard
	// is structurally bypassed.
	var sawDumpMd bool
	for _, line := range calls {
		if line == "drbdadm dump-md pvc-md-adopt" {
			sawDumpMd = true
			break
		}
	}
	if !sawDumpMd {
		t.Errorf("HasMD probe (drbdadm dump-md) missing: %v", calls)
	}

	// Per-peer set-gi MUST still fire — the helper's contract is
	// "stamp the fresh-replica GI tuple regardless of metadata
	// adoption" (Bug 319 invariant). Expect at least one set-gi
	// command for the local node-id slot.
	var sawSetGi bool
	for _, line := range calls {
		if strings.HasPrefix(line, "drbdmeta") && strings.Contains(line, "set-gi") {
			sawSetGi = true
			break
		}
	}
	if !sawSetGi {
		t.Errorf("expected per-peer drbdmeta set-gi to fire even on adopted metadata; calls=%v", calls)
	}

	// .md-created marker MUST be written — it gates firstActivation
	// across satellite restarts.
	if _, statErr := os.Stat(filepath.Join(dir, "pvc-md-adopt.md-created")); statErr != nil {
		t.Errorf(".md-created marker not written: %v", statErr)
	}

	// MetadataCreated Status Condition MUST be stamped exactly once
	// per createMetadata invocation. Bug 344: stamper receives the
	// per-node Resource CRD name (`<rd>.<node>`), not the RD-only
	// name — the SSA patch targets `Resource` objects which are
	// sharded per node.
	if got := stamper.calls; len(got) != 1 || got[0] != "pvc-md-adopt.n1" {
		t.Errorf("MetadataCreated stamper calls = %v, want [pvc-md-adopt.n1]", got)
	}
}

// fakeMetadataStamperInternal is the in-package analogue of
// reconciler_metadata_created_test.go's fakeMetadataStamper. We
// can't reuse the test_test type because it lives in
// `package satellite_test` — this helper test runs inside
// `package satellite` so it can call the unexported
// createMetadata method directly.
type fakeMetadataStamperInternal struct {
	calls []string
}

func (f *fakeMetadataStamperInternal) StampMetadataCreated(_ context.Context, resourceName string) error {
	f.calls = append(f.calls, resourceName)
	return nil
}
