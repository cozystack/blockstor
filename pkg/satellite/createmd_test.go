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

package satellite

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
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
	// Bug B.4: probe is per-volume (`<rd>/<volNumber>`), not per-RD,
	// so a multi-volume RD with vol-0 already attached doesn't EBUSY
	// against the per-RD `drbdmeta create-md` walk.
	fx.Expect("drbdadm dump-md pvc-md-adopt/0",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})

	// Thin LVM provider so resolveSeedGI synthesises a deterministic
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

	// Fresh-deployment skip case: the controller stamped
	// SkipInitialSync=true (RD not yet initialized), which is what
	// authorises the day0 set-gi seed under the skip-init-sync
	// hardening. Without it resolveVolumeSeed conservatively refuses the
	// skip and no set-gi fires.
	skipTrue := true

	dr := &intent.DesiredResource{
		Name:            "pvc-md-adopt",
		NodeName:        "n1",
		SkipInitialSync: &skipTrue,
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
	// is structurally bypassed. Per-volume target (Bug B.4).
	var sawDumpMd bool
	for _, line := range calls {
		if line == "drbdadm dump-md pvc-md-adopt/0" {
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
	var sawSetGI bool
	for _, line := range calls {
		if strings.HasPrefix(line, "drbdmeta") && strings.Contains(line, "set-gi") {
			sawSetGI = true
			break
		}
	}
	if !sawSetGI {
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

// TestCreateMdScopedToMissingVolumeOnly pins the Bug B.4 (P0)
// invariant: when `ensureMetadata` runs against a multi-volume RD
// where vol-0 already carries DRBD-9 metadata and vol-1 does not
// (the operator-observed `linstor vd c <rd> 100M` after vol-0
// reached UpToDate), the satellite MUST:
//
//   - probe HasMD per-volume (`drbdadm dump-md <rd>/0`, `<rd>/1`),
//   - call create-md ONLY against `<rd>/1` — the volume that lacks
//     metadata — and
//   - NOT call the RD-scoped `drbdadm create-md <rd>` form, which
//     `drbdmeta` walks across every volume in .res and bails EBUSY
//     against vol-0's already-attached minor (the hot-loop surface
//     of Bug B.4).
//
// The RD-scoped form is also UNSAFE in the general multi-volume
// case: `create-md --force <rd>` rewrites the AL + bitmap + GI
// state on every per-volume sub-block, so vol-0's existing
// metadata would be silently wiped — orphaning the local replica
// from the cluster.
func TestCreateMdScopedToMissingVolumeOnly(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// vol-0 already carries metadata (the pre-Bug B.4 happy path
	// for a single-volume RD), vol-1 does not (the late-added
	// volume from `vd c <rd> N`).
	fx.Expect("drbdadm dump-md pvc-b4-multi/0",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})
	fx.Expect("drbdadm dump-md pvc-b4-multi/1",
		storage.FakeResponse{Err: errDumpMdNoMeta})
	// Per-volume create-md for vol-1 only — the missing-MD volume.
	fx.Expect("drbdadm create-md --force --max-peers=15 pvc-b4-multi/1",
		storage.FakeResponse{})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers:    map[string]storage.Provider{"thin1": thin},
		Adm:          drbd.NewAdm(fx),
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-b4-multi",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 1, SizeKib: 100 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"peer.n2.address": "10.0.0.2", "peer.n2.node-id": "1", "peer.n2.port": "7000",
		},
	}
	devices := map[int32]string{
		0: "/dev/vg/pvc-b4-multi_00000",
		1: "/dev/vg/pvc-b4-multi_00001",
	}

	// Diskless→diskful flip path with firstActivation=false — the
	// exact arm Bug B.4 hot-loops on when vd c adds a new volume
	// while vol-0 is attached.
	mdMarkerPath := filepath.Join(dir, "pvc-b4-multi.md-created")
	if err := os.WriteFile(mdMarkerPath, nil, 0o600); err != nil {
		t.Fatalf("seed .md-created marker: %v", err)
	}

	err := rec.ensureMetadata(context.Background(), dr, devices, mdMarkerPath, false)
	if err != nil {
		t.Fatalf("ensureMetadata: %v", err)
	}

	calls := fx.CommandLines()

	// Per-volume create-md for vol-1 MUST fire.
	wantPerVol := "drbdadm create-md --force --max-peers=15 pvc-b4-multi/1"
	if !slices.Contains(calls, wantPerVol) {
		t.Errorf("expected %q in %v", wantPerVol, calls)
	}

	// Per-volume create-md for vol-0 MUST NOT fire — vol-0 already
	// has metadata; re-creating would wipe its GI + bitmap.
	forbidPerVol0 := "drbdadm create-md --force --max-peers=15 pvc-b4-multi/0"
	if slices.Contains(calls, forbidPerVol0) {
		t.Errorf("per-volume create-md MUST NOT re-create vol-0 metadata; cmds: %v", calls)
	}

	// RD-scoped create-md MUST NOT fire — that is the exact verb
	// that hot-loops EBUSY against vol-0's attached minor.
	forbidRDScoped := "drbdadm create-md --force --max-peers=15 pvc-b4-multi"
	if slices.Contains(calls, forbidRDScoped) {
		t.Errorf("RD-scoped create-md MUST NOT run (Bug B.4 hot-loop surface); cmds: %v", calls)
	}
}

// TestCreateMdSkipsAttachedVolume pins the EBUSY-avoidance
// invariant of Bug B.4: the per-volume `HasMD` probe MUST short-
// circuit `create-md` for any volume whose lower disk already
// carries DRBD-9 metadata. The previous per-RD code path issued
// `drbdadm create-md <rd>` which walked into vol-0's attached
// minor unconditionally; the per-volume scope means
// `drbdadm dump-md <rd>/0` (which succeeds when vol-0 is attached
// — drbdmeta reads via /proc on attached minors) returns
// `HasMD=true` and skips the create-md call entirely.
//
// The fake's `dump-md <rd>/0` return is the same shape a real
// attached vol-0 produces (`# Output might be stale, since minor
// 20000 is attached` header + the canonical v09 dump). The unit
// test only needs `err == nil && len(out) > 0` to drive HasMD=true,
// so the canned response is the minimal viable shape.
func TestCreateMdSkipsAttachedVolume(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Both volumes already carry metadata — the steady-state shape
	// after the late-add reconcile lands. Subsequent reconcile
	// passes (driven by Status updates / events2) must be no-ops
	// for create-md.
	fx.Expect("drbdadm dump-md pvc-b4-attached/0",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})
	fx.Expect("drbdadm dump-md pvc-b4-attached/1",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers:    map[string]storage.Provider{"thin1": thin},
		Adm:          drbd.NewAdm(fx),
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-b4-attached",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 1, SizeKib: 100 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"peer.n2.address": "10.0.0.2", "peer.n2.node-id": "1", "peer.n2.port": "7000",
		},
	}
	devices := map[int32]string{
		0: "/dev/vg/pvc-b4-attached_00000",
		1: "/dev/vg/pvc-b4-attached_00001",
	}

	mdMarkerPath := filepath.Join(dir, "pvc-b4-attached.md-created")
	if err := os.WriteFile(mdMarkerPath, nil, 0o600); err != nil {
		t.Fatalf("seed .md-created marker: %v", err)
	}

	err := rec.ensureMetadata(context.Background(), dr, devices, mdMarkerPath, false)
	if err != nil {
		t.Fatalf("ensureMetadata: %v", err)
	}

	// Zero `drbdadm create-md` calls — every volume already has
	// metadata. The historical RD-scoped form would have called
	// `drbdadm create-md pvc-b4-attached` which EBUSY-loops; the
	// per-volume form must short-circuit before that point.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm create-md") {
			t.Errorf("create-md fired on a fully-stamped multi-volume RD (EBUSY-loop surface): %s", line)
		}
	}
}

// errDumpMdNoMeta mirrors the verbatim stderr drbdadm dump-md
// returns when no metadata block is present on the lower disk
// ("drbdadm: No valid meta data found"). Defined here as a
// package-level static error so the Bug B.4 unit tests can drive
// the missing-MD branch without dynamic-error lint noise.
var errDumpMdNoMeta = errors.New("drbdadm: No valid meta data found")

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

// errDumpMdUnclean is the verbatim exit drbdadm returns for a volume
// whose activity log was never closed — the shape a machine leaves
// behind when it goes down while Primary and writing, and the same
// shape a ZFS snapshot of a live volume carries into a clone.
var errDumpMdUnclean = errors.New(
	`drbdadm dump-md pvc-unclean/0: Found meta data is "unclean", ` +
		`please apply-al first: exit status 1`)

// uncleanFixture wires a single-volume reconciler whose dump-md reports
// an unapplied activity log.
func uncleanFixture(
	t *testing.T,
) (*Reconciler, *intent.DesiredResource, map[int32]string, string, *storage.FakeExec) {
	t.Helper()

	dir := t.TempDir()

	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md pvc-unclean/0", storage.FakeResponse{Err: errDumpMdUnclean})
	fx.Expect("drbdadm create-md --force --max-peers=15 pvc-unclean/0", storage.FakeResponse{})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers:    map[string]storage.Provider{"thin1": thin},
		Adm:          drbd.NewAdm(fx),
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-unclean",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"peer.n2.address": "10.0.0.2", "peer.n2.node-id": "1", "peer.n2.port": "7000",
		},
	}

	devices := map[int32]string{0: "/dev/vg/pvc-unclean_00000"}

	return rec, dr, devices, filepath.Join(dir, "pvc-unclean.md-created"), fx
}

// A lower disk carrying an unclean superblock is initialised, on both
// activation arms.
//
// It is worth being explicit about what these two pin, because the
// obvious reading of the second one is wrong. An unapplied activity log
// does NOT mean "this volume is established": a volume added to a live
// resource arrives with firstActivation=false on a lower disk that is
// brand new, and one carved from a recycled pool extent carries
// whatever superblock the previous tenant left. Gating re-initialisation
// on firstActivation stalled exactly that shape — the e2e suite caught
// vol-1 never leaving Diskless while the kernel had it UpToDate.
//
// The cost is that a replica which crashed while Primary and writing
// looks the same to drbdmeta and gets re-initialised too. Separating
// them needs a per-volume record of "we have initialised this before",
// which does not exist yet; these tests pin the behaviour as it stands
// rather than the behaviour we want.
func TestEnsureMetadataInitialisesAnUncleanLowerDisk(t *testing.T) {
	for _, firstActivation := range []bool{true, false} {
		rec, dr, devices, mdMarkerPath, fx := uncleanFixture(t)

		if !firstActivation {
			if err := os.WriteFile(mdMarkerPath, nil, 0o600); err != nil {
				t.Fatalf("seed .md-created marker: %v", err)
			}
		}

		err := rec.ensureMetadata(context.Background(), dr, devices, mdMarkerPath, firstActivation)
		if err != nil {
			t.Fatalf("firstActivation=%v: ensureMetadata: %v", firstActivation, err)
		}

		created := false

		for _, line := range fx.CommandLines() {
			if strings.Contains(line, "create-md") {
				created = true
			}
		}

		if !created {
			t.Errorf("firstActivation=%v: the volume was left without metadata", firstActivation)
		}
	}
}
