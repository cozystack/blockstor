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
	"io"
	"os"
	"path/filepath"
	"strconv"
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
		Peers: []intent.DesiredPeer{{Name: "n2"}},
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

// sequencedFakeExec wraps storage.FakeExec to deliver a sequence of
// canned responses for the same command line on successive calls.
// FakeExec keys responses by cmdline — fine for one-shot stubs but
// not for the create-md collision-recovery test which needs call #1
// to fail and call #2 to succeed against the SAME command line.
type sequencedFakeExec struct {
	fx       *storage.FakeExec
	seqKey   string
	seqResps []storage.FakeResponse
	seqHits  int
}

func (s *sequencedFakeExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	key := name
	if len(args) > 0 {
		key += " " + strings.Join(args, " ")
	}

	if key == s.seqKey && s.seqHits < len(s.seqResps) {
		resp := s.seqResps[s.seqHits]
		s.seqHits++

		// Forward the call into the underlying FakeExec so
		// CommandLines() still records it for assertion.
		_, _ = s.fx.Run(ctx, name, args...)

		return resp.Stdout, resp.Err
	}

	return s.fx.Run(ctx, name, args...) //nolint:wrapcheck // test helper, errors are canned
}

func (s *sequencedFakeExec) RunWithStdin(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	return s.Run(ctx, name, args...)
}

// TestCreateMDWithCollisionRecoveryFreesForeignZombie pins the
// recovery path for the multi-volume-RD collision documented in
// `blockstor_drbd_stuck_state`: a zombie kernel slot from a
// torn-down test (or a `drbdsetup down` blocked by a D-state lvs
// holding the device open inside __drbd_make_request) still owns
// the minor the allocator just handed to a new RD's vol-1. Without
// recovery, `drbdadm create-md` fails with `Device '<minor>' is
// configured!` and the new RD wedges on Inconsistent forever.
//
// Recovery sequence under test:
//  1. CreateMD fails with the configured-device error.
//  2. Wrapper parses the minor, looks up the owner via
//     drbdsetup status --verbose.
//  3. Confirms owner != self (safety guard against tearing down
//     our own freshly-loaded slot in a HasMD/CreateMD race).
//  4. drbdsetup down the zombie.
//  5. Retries CreateMD; second call succeeds.
//
// The fake exec replays the production error verbatim — a
// regression that changes the error parser or the status output
// shape immediately fails this test instead of silently reverting
// to the manual-reboot recovery this fix replaces.
func TestCreateMDWithCollisionRecoveryFreesForeignZombie(t *testing.T) {
	fx := storage.NewFakeExec()

	// drbdsetup status --verbose tells the recovery code that
	// minor 20001 is owned by a foreign resource. Use a name
	// distinct from selfRD so the safety guard greenlights the
	// teardown.
	fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(
		`cli-matrix-multi-life-b node-id:0 role:Secondary suspended:user
  volume:0 minor:20001 disk:UpToDate backing_dev:/dev/loop6 quorum:yes
`)})

	// drbdsetup down on the zombie succeeds. Empty stdout + nil
	// err is the canned default from NewFakeExec; no Expect needed.

	// First create-md call: fails with the configured-device error
	// carrying minor 20001. Second call: succeeds. sequencedFakeExec
	// delivers them in order.
	seq := &sequencedFakeExec{
		fx:     fx,
		seqKey: "drbdadm create-md --force --max-peers=15 e2e-twovol",
		seqResps: []storage.FakeResponse{
			{Err: drbdFakeConfiguredDeviceErr(20001)},
			{},
		},
	}

	rec := NewReconciler(ReconcilerConfig{
		Adm: drbd.NewAdm(seq),
	})

	err := rec.createMDWithCollisionRecovery(context.Background(), "e2e-twovol", "e2e-twovol")
	if err != nil {
		t.Fatalf("createMDWithCollisionRecovery: %v", err)
	}

	calls := fx.CommandLines()

	// Recovery must (a) detect collision, (b) drbdsetup down the
	// zombie, (c) retry create-md.
	var sawDown, sawRetry bool

	createMDCount := 0
	for _, line := range calls {
		if strings.HasPrefix(line, "drbdadm create-md") {
			createMDCount++
			if createMDCount == 2 {
				sawRetry = true
			}
		}

		if line == "drbdsetup down cli-matrix-multi-life-b" {
			sawDown = true
		}
	}

	if !sawDown {
		t.Errorf("expected drbdsetup down on zombie cli-matrix-multi-life-b; calls=%v", calls)
	}

	if !sawRetry {
		t.Errorf("expected second drbdadm create-md after recovery; calls=%v", calls)
	}
}

// TestCreateMDWithCollisionRecoveryRefusesSelfTearDown pins the
// safety guard: when the kernel slot owning the colliding minor is
// OUR OWN resource (a HasMD/CreateMD race where the legacy path
// already loaded the slot between probe and shell-out), the
// recovery code MUST NOT issue `drbdsetup down` against it — that
// would destroy our own freshly-loaded metadata. Surface the
// original create-md error instead so the higher idempotent path
// re-runs HasMD and short-circuits.
func TestCreateMDWithCollisionRecoveryRefusesSelfTearDown(t *testing.T) {
	fx := storage.NewFakeExec()

	configuredErr := drbdFakeConfiguredDeviceErr(20000)
	fx.Expect("drbdadm create-md --force --max-peers=15 e2e-twovol",
		storage.FakeResponse{Err: configuredErr})

	// drbdsetup status reports the offending minor is held by
	// `e2e-twovol` itself — the in-flight resource we are
	// initialising.
	fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(
		`e2e-twovol node-id:0 role:Secondary
  volume:0 minor:20000 disk:UpToDate backing_dev:/dev/loop5
`)})

	rec := NewReconciler(ReconcilerConfig{
		Adm: drbd.NewAdm(fx),
	})

	err := rec.createMDWithCollisionRecovery(context.Background(), "e2e-twovol", "e2e-twovol")
	if err == nil {
		t.Fatalf("createMDWithCollisionRecovery: expected error on self-owned collision, got nil")
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdsetup down ") {
			t.Errorf("recovery tore down our own kernel slot: %s", line)
		}
	}
}

// drbdFakeConfiguredDeviceErr returns an error whose message
// matches the real drbdadm create-md collision shape (`Device
// '<minor>' is configured!`). Used by the recovery-path unit tests
// to drive the failure branch of CreateMD without depending on a
// real drbdmeta binary.
func drbdFakeConfiguredDeviceErr(minor int32) error {
	return fakeConfiguredDeviceErr(minor)
}

// fakeConfiguredDeviceErr is the unexported leaf so the test
// keeps a single source of truth for the wire-shape literal.
type fakeConfiguredDeviceError struct{ minor int32 }

func (e fakeConfiguredDeviceError) Error() string {
	return "drbdadm create-md: Device '" +
		strconvI32(e.minor) +
		"' is configured!\nCommand 'drbdmeta ... create-md ... --force' terminated with exit code 20: exit status 20"
}

func fakeConfiguredDeviceErr(minor int32) error { return fakeConfiguredDeviceError{minor: minor} }

func strconvI32(v int32) string { return strconv.FormatInt(int64(v), 10) }
