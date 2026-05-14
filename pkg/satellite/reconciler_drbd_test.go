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

package satellite_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

var (
	errNotALUKSDevice      = errors.New("not a luks device")
	errLUKSOpenAlready     = errors.New("device pvc-luks-only-0-luks already exists")
	errDrbdadmAdjustFail   = errors.New("drbdadm: simulated mid-Apply abort")
	errDrbdadmResizeFail   = errors.New("drbdadm: resize failed (peer disconnected)")
	errDrbdsetupNoResource = errors.New("drbdsetup: exit status 10")
)

// TestApplyWritesResFile: Apply leaves a /etc/drbd.d/<name>.res file
// (here under StateDir) reflecting the DesiredResource. The reconciler
// owns this file — controller never touches it directly.
func TestApplyWritesResFile(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-1",
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
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-1.res")

	body, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	got := string(body)

	for _, want := range []string{
		"resource pvc-1 {",
		"on n1 {",
		"address 10.0.0.1:7000;",
		"on n2 {",
		"address 10.0.0.2:7000;",
		"connection {",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

// TestApplyMultiVolumeRDRendersOneResourceWithVolumes pins scenario
// 4.W25 (wave2-04-lifecycle.md): an RD that carries multiple
// VolumeDefinitions is ONE DRBD resource = one consistency group.
// The reconciler must render a SINGLE `resource <name> { ... }`
// block whose `on <node> { ... }` body has a `volume <N>` sub-block
// per VolumeNumber, with kernel minors offset from the resource's
// base minor (vol 0 → minor, vol 1 → minor+1, …).
//
// This is the .res-renderer contract that lets DRBD treat the
// volumes as a write-order-preserving consistency group: snapshots
// against the resource capture every volume atomically and primary
// state is shared. The test guards against a regression where the
// dispatcher / reconciler accidentally fans one RD out into
// separate single-volume resources, breaking that guarantee.
//
// Scenario 4.W26 (per-VD storage pool) is exercised alongside —
// vol 0 lands on `fast` (NVMe-ish pool) and vol 1 on `slow` (HDD
// pool) — so the .res shows the per-volume backing-disk routing
// the dispatcher's per-VD `StorPoolName` override produces.
func TestApplyMultiVolumeRDRendersOneResourceWithVolumes(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// Two distinct pools, two distinct LV names — vol 0 on `fast`,
	// vol 1 on `slow`. lvs returns empty (fresh create) for both.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name fast/pvc-multi_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name slow/pvc-multi_00001",
		storage.FakeResponse{Stdout: []byte("")})

	fast := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "fast", ThinPool: "tp"}, fx)
	slow := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "slow", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"fast": fast, "slow": slow},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-multi",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				// 4.W26: per-VD pool routing — vol 0 fast, vol 1 slow.
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "fast"},
				{VolumeNumber: 1, SizeKib: 2 * 1024 * 1024, StoragePool: "slow"},
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
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "pvc-multi.res"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	got := string(body)

	// One DRBD resource block — NOT two. If the reconciler fanned
	// the RD out into per-volume resources, we'd see no
	// `resource pvc-multi {` (and instead `pvc-multi_0`/`pvc-multi_1`).
	if c := strings.Count(got, "resource pvc-multi {"); c != 1 {
		t.Fatalf("want exactly 1 `resource pvc-multi {`, got %d in:\n%s", c, got)
	}

	// Two `volume <N> {` sub-blocks per `on <node> {}` block, one
	// each for local and peer = 4 total volume blocks across the
	// whole resource. This is the consistency-group shape.
	for _, want := range []string{
		"volume 0 {",
		"volume 1 {",
		// Per-VD pool routing — vol 0 on `fast`, vol 1 on `slow`
		// (scenario 4.W26). The local diskful host renders the
		// real backing path; peer renders the placeholder, which
		// stays the same across pools.
		"disk /dev/fast/pvc-multi_00000;",
		"disk /dev/slow/pvc-multi_00001;",
		// Kernel minors offset from base — vol 0 → minor 1000,
		// vol 1 → minor 1001. The renderer wires this in lockstep
		// with /dev/drbd<N> device-node names.
		"device /dev/drbd1000 minor 1000;",
		"device /dev/drbd1001 minor 1001;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}

	// Per `on {}` block: two volume sub-blocks each, one for
	// local (`on n1`) and one for peer (`on n2`) = 4 occurrences
	// of `volume ` in the rendered file.
	if c := strings.Count(got, "    volume "); c != 4 {
		t.Errorf("want 4 `volume` sub-blocks (2 vols x 2 hosts), got %d in:\n%s", c, got)
	}

	// `drbdadm create-md <rd>` initialises metadata for ALL volumes
	// in the resource (DRBD walks the rendered .res and creates AL +
	// bitmap + GI state per `volume {}` sub-block). The reconciler
	// therefore issues one create-md against the resource name, not
	// one per volume number — matches upstream LINSTOR's DrbdAdm.
	// We just guard that it ran at all; per-volume metadata is
	// covered by the on-disk `volume {}` sub-blocks asserted above
	// (DRBD wouldn't initialise vol 1 if its block were missing).
	var sawCreateMD bool

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm") && strings.Contains(line, "create-md") &&
			strings.HasSuffix(line, "pvc-multi") {
			sawCreateMD = true
			break
		}
	}

	if !sawCreateMD {
		t.Errorf("want one `drbdadm create-md ... pvc-multi`; cmds:\n%v", fx.CommandLines())
	}

	// adjust runs against the resource name too — DRBD applies the
	// new .res to every volume in the consistency group atomically.
	if !slices.Contains(fx.CommandLines(), "drbdadm adjust pvc-multi") {
		t.Errorf("want `drbdadm adjust pvc-multi`; cmds:\n%v", fx.CommandLines())
	}
}

// TestApplyInvokesDrbdadmAdjust: writing the .res isn't enough — DRBD
// needs `adjust` to pick up changes. Apply must call it.
func TestApplyInvokesDrbdadmAdjust(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-1",
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
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !slices.Contains(fx.CommandLines(), "drbdadm adjust pvc-1") {
		t.Errorf("expected drbdadm adjust; got %v", fx.CommandLines())
	}
}

// TestApplyDisklessNoCreateMD: DISKLESS replicas have no metadata to
// initialise. Even though they get a .res file (DRBD still needs to
// know how to reach them), create-md must not run.
func TestApplyDisklessNoCreateMD(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Flags:    []string{"DISKLESS"},
			DrbdOptions: map[string]string{
				"port":    "7000",
				"node-id": "0",
				"address": "10.0.0.1",
				"minor":   "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm create-md") {
			t.Errorf("DISKLESS issued create-md: %s", line)
		}
	}
}

// TestApplyTriggersResizeOnGrow simulates the satellite picking up a
// VolumeDefinition update that grew the volume: lvs reports the LV
// already exists at 1 GiB, the desired size is 2 GiB → reconciler
// must call lvextend then drbdadm resize. Pins the upstream-style
// growth-path semantics CSI ControllerExpandVolume relies on.
func TestApplyTriggersResizeOnGrow(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// Volume already exists.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-grow_00000",
		storage.FakeResponse{Stdout: []byte("pvc-grow_00000\n")})
	// VolumeStatus: 1 GiB on disk (1024*1024 KiB).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-grow_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-grow_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Desired: 2 GiB.
	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-grow",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 2 * 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	want := []string{
		"lvextend --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --size 2048MiB vg/pvc-grow_00000",
		"drbdadm resize --assume-clean pvc-grow",
	}

	for _, w := range want {
		if !slices.Contains(fx.CommandLines(), w) {
			t.Errorf("expected %q in calls; got %v", w, fx.CommandLines())
		}
	}
}

// TestApplyNoResizeOnFreshCreate: when the volume doesn't exist yet
// CreateVolume runs but ResizeVolume must NOT — there's nothing to
// grow. drbdadm resize is also skipped.
func TestApplyNoResizeOnFreshCreate(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-new_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-new",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 2 * 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvextend --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } ") {
			t.Errorf("fresh create issued lvextend: %s", line)
		}

		if strings.HasPrefix(line, "drbdadm resize") {
			t.Errorf("fresh create issued drbdadm resize: %s", line)
		}
	}
}

// TestApplyRendersAllowTwoPrimaries verifies the option-hierarchy
// pipeline lands `allow-two-primaries yes;` in the generated .res
// file. Required for Ganesha-RWX (NFS export flips Primary on
// failover) and KubeVirt live-migration (both nodes Primary briefly).
func TestApplyRendersAllowTwoPrimaries(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-rwx_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-rwx",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",

				// The controller-side resolver folds this in from
				// `linstor c sp DrbdOptions/Net/allow-two-primaries yes`.
				"DrbdOptions/Net/allow-two-primaries": "yes",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "pvc-rwx.res"))
	if err != nil {
		t.Fatalf("read .res: %v", err)
	}

	if !strings.Contains(string(body), "allow-two-primaries yes;") {
		t.Errorf(".res missing allow-two-primaries; body=%s", body)
	}
}

// TestApplyDropsLinstorOnlyOptions: section-less DrbdOptions/* keys
// (e.g. DrbdOptions/AutoEvictAllowEviction set by piraeus-operator
// via /v1/controller/properties) must NOT land in the rendered .res
// — they're LINSTOR-controller-only knobs and drbdadm rejects the
// whole file with "Parse error: ... but got 'AutoEvictAllowEviction'"
// on the next `drbdadm primary`. Regression for stand-side smoke
// failure observed 2026-05-09.
func TestApplyDropsLinstorOnlyOptions(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-noeviction_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-noeviction",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",

				// Section-less: must be dropped
				"DrbdOptions/AutoEvictAllowEviction": "false",
				// Section-less: must be dropped
				"DrbdOptions/AutoplaceTarget": "3",
				// Real DRBD option: must land in net{} block
				"DrbdOptions/Net/rr-conflict": "retry-connect",
				// Real DRBD option: must land in options{} block
				"DrbdOptions/Resource/on-no-quorum": "suspend-io",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "pvc-noeviction.res"))
	if err != nil {
		t.Fatalf("read .res: %v", err)
	}

	if strings.Contains(string(body), "AutoEvictAllowEviction") {
		t.Errorf("LINSTOR-only key leaked into .res; body=%s", body)
	}

	if strings.Contains(string(body), "AutoplaceTarget") {
		t.Errorf("LINSTOR-only key (AutoplaceTarget) leaked into .res; body=%s", body)
	}

	if !strings.Contains(string(body), "rr-conflict retry-connect;") {
		t.Errorf("real DRBD net option missing; body=%s", body)
	}

	if !strings.Contains(string(body), "on-no-quorum suspend-io;") {
		t.Errorf("real DRBD resource option missing; body=%s", body)
	}
}

// TestApplyRendersRDProtocolIntoNetBlock: scenario 5.W01 — an RD-scope
// `DrbdOptions/Net/protocol=C` set by `linstor rd drbd-options
// --protocol C <rd>` reaches the satellite as a flat entry in
// DesiredResource.DrbdOptions and must land verbatim as
// `protocol C;` inside the rendered `net { }` block. Pinning the
// `net {` framing (not just the bare `protocol C;` substring) keeps
// the assertion honest — the renderer also stamps `protocol C;` at
// the resource-top level via the legacy Net.ProtocolC default, so a
// regression that drops the prop from splitDRBDOptions would still
// leave the substring present at the wrong scope.
func TestApplyRendersRDProtocolIntoNetBlock(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/backups_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "backups",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",

				// RD-scope prop the controller stamped onto the
				// effective DRBD options bag after `linstor rd
				// drbd-options --protocol C backups`.
				"DrbdOptions/Net/protocol": "C",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "backups.res"))
	if err != nil {
		t.Fatalf("read .res: %v", err)
	}

	got := string(body)

	// Must contain a `net { … protocol C; … }` block — the
	// scenario's load-bearing assertion against `grep protocol
	// /var/lib/linstor.d/backups.res`.
	if !strings.Contains(got, "net {") {
		t.Errorf(".res missing net{} block; body=%s", got)
	}

	netStart := strings.Index(got, "net {")
	netEnd := strings.Index(got[netStart:], "}")

	if netStart < 0 || netEnd < 0 {
		t.Fatalf("net{} block not delimited; body=%s", got)
	}

	netBlock := got[netStart : netStart+netEnd]
	if !strings.Contains(netBlock, "protocol C;") {
		t.Errorf("net{} block missing `protocol C;`; net-block=%q\nfull=%s", netBlock, got)
	}
}

// TestApplySkipsDRBDWhenLayerStackOmits: a Resource with explicit
// LayerStack=["STORAGE"] must NOT render a .res file or invoke
// drbdadm. Storage provider still runs (volume is created); the
// DRBD half is skipped wholesale.
//
// This is the foundation of Phase 9 single-replica local-storage
// mode: a PVC that doesn't need DRBD (e.g. ephemeral cache, single
// replica scratch space) provisions just an LV and the consumer Pod
// mounts it directly without a DRBD layer.
func TestApplySkipsDRBDWhenLayerStackOmits(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-no-drbd_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-no-drbd",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack:  []string{"STORAGE"},
			DrbdOptions: map[string]string{},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-no-drbd.res")
	if _, statErr := os.Stat(resPath); statErr == nil {
		t.Errorf(".res file rendered despite LayerStack=[STORAGE]: %s", resPath)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm ") || strings.HasPrefix(line, "drbdsetup ") {
			t.Errorf("DRBD command issued despite LayerStack=[STORAGE]: %s", line)
		}
	}
}

// TestApplyLayersLUKS: a Resource with LayerStack=["LUKS","STORAGE"]
// must run cryptsetup luksFormat (first activation) + luksOpen, then
// hand the /dev/mapper/<rd>-<vol>-luks path to the DRBD layer (when
// DRBD is also in the stack — here we omit it to isolate the LUKS
// path). Pins the Phase 9 LUKS plumbing.
func TestApplyLayersLUKS(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-luks_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// VolumeStatus query → reports the LV at a known path so the LUKS
	// layer has a non-empty device to format/open.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-luks_00000|1048576\n")})
	// cryptsetup isLuks fails on a fresh device → format runs.
	fx.Expect("cryptsetup isLuks /dev/vg/pvc-luks_00000",
		storage.FakeResponse{Err: errNotALUKSDevice})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-luks",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"LUKS", "STORAGE"},
			Props:      map[string]string{"LuksPassphrase": "topsecret"},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	saw := func(needle string) bool {
		for _, line := range fx.CommandLines() {
			if strings.Contains(line, needle) {
				return true
			}
		}

		return false
	}

	if !saw("luksFormat") {
		t.Errorf("expected cryptsetup luksFormat call; got %v", fx.CommandLines())
	}

	if !saw("luksOpen") {
		t.Errorf("expected cryptsetup luksOpen call; got %v", fx.CommandLines())
	}
}

// TestApplyLUKSFailsWithoutPassphrase: explicit LUKS in stack but no
// passphrase prop → apply fails fast rather than silently producing
// an unencrypted volume.
func TestApplyLUKSFailsWithoutPassphrase(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-luks-empty_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-luks-empty",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"LUKS", "STORAGE"},
		},
	})
	if err != nil {
		t.Fatalf("Apply outer error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results: got %d, want 1", len(results))
	}

	if results[0].GetOk() {
		t.Errorf("expected LUKS-without-passphrase to fail")
	}

	if !strings.Contains(strings.ToLower(results[0].GetMessage()), "passphrase") {
		t.Errorf("error message should mention passphrase; got %q", results[0].GetMessage())
	}
}

// TestApplyLUKSStorageNeverDRBD pins the satellite contract for
// `[LUKS,STORAGE]`: cryptsetup luksFormat on first activation,
// cryptsetup luksOpen on every reconcile, and *never* drbdadm /
// drbdsetup. Pairs with TestApplySkipsDRBDWhenLayerStackOmits — both
// are exit criteria for Phase 9.
func TestApplyLUKSStorageNeverDRBD(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-luks-only_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-only_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-luks-only_00000|1048576\n")})
	// First reconcile: not yet a LUKS device → luksFormat will run.
	fx.Expect("cryptsetup isLuks /dev/vg/pvc-luks-only_00000",
		storage.FakeResponse{Err: errNotALUKSDevice})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-luks-only",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"LUKS", "STORAGE"},
			Props:      map[string]string{"LuksPassphrase": "topsecret"},
		},
	}

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (1st): %v", err)
	}

	// Second reconcile: device is now LUKS-formatted (probe succeeds);
	// luksOpen returns "already exists" because the mapper is still
	// open from the previous reconcile. Format must NOT run again.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-luks-only_00000",
		storage.FakeResponse{Stdout: []byte("pvc-luks-only_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-only_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-luks-only_00000|1048576\n")})
	fx.Expect("cryptsetup isLuks /dev/vg/pvc-luks-only_00000",
		storage.FakeResponse{}) // success — already a LUKS device
	fx.Expect("cryptsetup luksOpen /dev/vg/pvc-luks-only_00000 pvc-luks-only-0-luks --key-file -",
		storage.FakeResponse{Err: errLUKSOpenAlready})

	_, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (2nd): %v", err)
	}

	saw := func(needle string) bool {
		for _, line := range fx.CommandLines() {
			if strings.Contains(line, needle) {
				return true
			}
		}

		return false
	}

	if saw("luksFormat") {
		t.Errorf("idempotent reconcile re-ran luksFormat: %v", fx.CommandLines())
	}

	if !saw("luksOpen") {
		t.Errorf("idempotent reconcile must still call luksOpen (re-attach mapper after restart): %v",
			fx.CommandLines())
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm ") || strings.HasPrefix(line, "drbdsetup ") {
			t.Errorf("DRBD command issued despite LayerStack=[LUKS,STORAGE]: %s", line)
		}
	}

	if _, statErr := os.Stat(filepath.Join(dir, "pvc-luks-only.res")); statErr == nil {
		t.Errorf(".res file rendered despite LayerStack=[LUKS,STORAGE]")
	}
}

// TestApplyDRBDLUKSStorageStack pins `[DRBD,LUKS,STORAGE]`: the .res
// file's `disk` line must point at /dev/mapper/<rd>-<vol>-luks (the
// LUKS mapper), NOT the raw LV path. That's what makes DRBD replicate
// ciphertext between peers — each peer encrypts independently, but the
// data DRBD ships over the wire is post-LUKS.
func TestApplyDRBDLUKSStorageStack(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-stack_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-stack_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-stack_00000|1048576\n")})
	fx.Expect("cryptsetup isLuks /dev/vg/pvc-stack_00000",
		storage.FakeResponse{Err: errNotALUKSDevice})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-stack",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			Props:      map[string]string{"LuksPassphrase": "topsecret"},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "pvc-stack.res"))
	if err != nil {
		t.Fatalf("read .res: %v", err)
	}

	if !strings.Contains(string(body), "/dev/mapper/pvc-stack-0-luks") {
		t.Errorf(".res must point disk at the LUKS mapper, not the raw LV; body=%s", body)
	}

	if strings.Contains(string(body), "/dev/vg/pvc-stack_00000") {
		t.Errorf(".res must NOT point disk at the raw LV when LUKS is in the stack; body=%s", body)
	}

	saw := func(needle string) bool {
		for _, line := range fx.CommandLines() {
			if strings.Contains(line, needle) {
				return true
			}
		}

		return false
	}

	if !saw("luksFormat") {
		t.Errorf("expected cryptsetup luksFormat in [DRBD,LUKS,STORAGE] apply; got %v", fx.CommandLines())
	}

	if !saw("luksOpen") {
		t.Errorf("expected cryptsetup luksOpen in [DRBD,LUKS,STORAGE] apply; got %v", fx.CommandLines())
	}
}

// TestApplyDRBDResizeErrorSurfaces: when storage grew and `drbdadm
// resize` then fails, the per-resource result must reflect Ok=false
// with the resize error in the message. The .res file has already
// been written by this point — the next reconcile picks up where
// this one left off (same firstActivation=false / no double-create-md
// invariant the abort-mid-Apply test pins).
func TestApplyDRBDResizeErrorSurfaces(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// LV already exists — triggers the resize path in applyStorage.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-resize-fail_00000",
		storage.FakeResponse{Stdout: []byte("pvc-resize-fail_00000\n")})
	// VolumeStatus reports current size 1 GiB; desired is 2 GiB.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-resize-fail_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-resize-fail_00000|1048576\n")})
	// drbdadm resize fails (e.g. peer not connected).
	fx.Expect("drbdadm resize --assume-clean pvc-resize-fail",
		storage.FakeResponse{Err: errDrbdadmResizeFail})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-resize-fail",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 2 * 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply outer error: %v", err)
	}

	if results[0].GetOk() {
		t.Errorf("Ok=true on drbdadm resize failure; want false")
	}

	if !strings.Contains(strings.ToLower(results[0].GetMessage()), "resize") {
		t.Errorf("error message must mention resize; got %q", results[0].GetMessage())
	}

	// .res file must already be on disk so the next reconcile sees
	// firstActivation=false. Pins the same convergence guarantee the
	// abort-mid-Apply test relies on.
	if _, statErr := os.Stat(filepath.Join(dir, "pvc-resize-fail.res")); statErr != nil {
		t.Errorf(".res file should be written before drbdadm resize; got %v", statErr)
	}
}

// TestApplyUnknownStoragePool: a DesiredVolume pointing at a pool
// not registered with the satellite must surface as a per-resource
// Ok=false with a clear "unknown storage pool" message — never as a
// gRPC error. The controller distinguishes pool-misconfiguration
// (operator visible, retryable) from transport faults; conflating
// the two would break the ApplyResources batch contract.
func TestApplyUnknownStoragePool(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		// Only "thin1" is registered; the test asks for "ghost-pool".
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-bad-pool",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "ghost-pool"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply outer error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results: got %d, want 1", len(results))
	}

	if results[0].GetOk() {
		t.Errorf("Ok=true on unknown pool; want false")
	}

	if !strings.Contains(strings.ToLower(results[0].GetMessage()), "ghost-pool") {
		t.Errorf("error message must mention the missing pool name; got %q",
			results[0].GetMessage())
	}

	// No DRBD commands should fire — we bail before applyDRBD.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm ") {
			t.Errorf("drbdadm should not run when storage step fails: %s", line)
		}
	}
}

// TestApplyInactiveOnlyDownsDRBD: a Resource with the INACTIVE flag
// (the operator called `linstor r deactivate`) must NOT touch storage
// or render the .res file — just `drbdadm down` to remove the kernel
// resource. Pins the node-maintenance path piraeus-operator uses:
// later activate restores without losing port/node-id allocations or
// having to re-sync.
func TestApplyInactiveOnlyDownsDRBD(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-inactive",
			NodeName: "n1",
			Flags:    []string{"INACTIVE"},
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !results[0].GetOk() {
		t.Errorf("Ok=false on INACTIVE: %s", results[0].GetMessage())
	}

	saw := func(needle string) bool {
		for _, line := range fx.CommandLines() {
			if strings.Contains(line, needle) {
				return true
			}
		}

		return false
	}

	if !saw("drbdadm down pvc-inactive") {
		t.Errorf("INACTIVE must run drbdadm down; got %v", fx.CommandLines())
	}

	if saw("lvcreate") {
		t.Errorf("INACTIVE must not touch storage; got %v", fx.CommandLines())
	}

	if saw("drbdadm adjust") || saw("drbdadm create-md") {
		t.Errorf("INACTIVE must skip the rest of the DRBD cycle; got %v",
			fx.CommandLines())
	}

	// .res file must NOT have been touched (deactivate preserves
	// the on-disk artefact for later activation).
	if _, statErr := os.Stat(filepath.Join(dir, "pvc-inactive.res")); statErr == nil {
		t.Errorf("INACTIVE must not write a .res file")
	}
}

// TestApplyLUKSWithoutCryptsetupWrapper: LayerStack contains LUKS
// but the satellite was configured without a Cryptsetup wrapper
// (e.g. cryptsetup binary missing). applyLUKS must fail loudly with
// a clear message rather than silently produce an unencrypted
// volume — pinning the second of two "fail loud, never silent" gates
// (the first is the empty-passphrase gate).
func TestApplyLUKSWithoutCryptsetupWrapper(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-no-cs_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
		// Cryptsetup intentionally nil.
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-no-cs",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"LUKS", "STORAGE"},
			Props:      map[string]string{"LuksPassphrase": "topsecret"},
		},
	})
	if err != nil {
		t.Fatalf("Apply outer error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results: got %d, want 1", len(results))
	}

	if results[0].GetOk() {
		t.Errorf("expected LUKS-without-cryptsetup-wrapper to fail; got Ok=true")
	}

	if !strings.Contains(strings.ToLower(results[0].GetMessage()), "cryptsetup") {
		t.Errorf("error message should mention cryptsetup; got %q", results[0].GetMessage())
	}
}

// TestApplyLUKSResizeChainsThroughMapper: when the storage layer just
// grew (existing LV resized to a larger SizeKib), applyLUKS must run
// `cryptsetup resize` on the mapper so DRBD's subsequent resize sees
// the full grown device. Without this step the consumer's view stays
// at the original LUKS-mapped portion.
//
// Pins the chain: storage grow → cryptsetup resize → drbdadm resize.
// Critical for ControllerExpandVolume on encrypted PVCs.
func TestApplyLUKSResizeChainsThroughMapper(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// LV already exists (resize path, not first create).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-luks-grow_00000",
		storage.FakeResponse{Stdout: []byte("pvc-luks-grow_00000\n")})
	// VolumeStatus reports current size (1 GiB) — desired is 2 GiB.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-grow_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-luks-grow_00000|1048576\n")})
	// isLuks succeeds → already a LUKS device, format skipped.
	fx.Expect("cryptsetup isLuks /dev/vg/pvc-luks-grow_00000",
		storage.FakeResponse{})
	// luksOpen returns "already exists" — mapper carried over from
	// previous reconcile.
	fx.Expect("cryptsetup luksOpen /dev/vg/pvc-luks-grow_00000 pvc-luks-grow-0-luks --key-file -",
		storage.FakeResponse{Err: errLUKSOpenAlready})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-luks-grow",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 2 * 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"LUKS", "STORAGE"},
			Props:      map[string]string{"LuksPassphrase": "topsecret"},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	saw := func(needle string) bool {
		for _, line := range fx.CommandLines() {
			if strings.Contains(line, needle) {
				return true
			}
		}

		return false
	}

	if !saw("lvextend") {
		t.Errorf("storage layer must run lvextend on grow; got %v", fx.CommandLines())
	}

	// The cryptsetup resize is the chain link between storage grow
	// and DRBD resize — without it the consumer's view stays at the
	// original LUKS-mapped portion. runWithKey single-quotes args so
	// the dm-name appears as 'pvc-luks-grow-0-luks' in the recorded
	// pipeline.
	if !saw("'resize' 'pvc-luks-grow-0-luks'") {
		t.Errorf("expected cryptsetup resize on the LUKS mapper; got %v",
			fx.CommandLines())
	}
}

// TestApplyAutoPrimarySeedFiresOnceOnFirstActivation: with the
// `auto-primary=true` DRBD option set on first Apply, the satellite
// must run `drbdadm primary --force` followed by `drbdadm secondary`
// to seed the resource out of `Inconsistent`. On subsequent reconciles
// (firstActivation=false because the .res file persists) the seed
// must NOT fire — running it twice would needlessly bump the bitmap
// and trigger a network re-sync between peers.
func TestApplyAutoPrimarySeedFiresOnceOnFirstActivation(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-seed_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-seed",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
				"auto-primary": "true",
			},
		},
	}

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (1st): %v", err)
	}

	first := fx.CommandLines()

	saw := func(lines []string, needle string) int {
		n := 0
		for _, line := range lines {
			if strings.Contains(line, needle) {
				n++
			}
		}

		return n
	}

	if saw(first, "drbdadm primary --force pvc-seed") != 1 {
		t.Errorf("first Apply must run primary --force exactly once; got %v", first)
	}

	if saw(first, "drbdadm secondary pvc-seed") != 1 {
		t.Errorf("first Apply must run drbdadm secondary exactly once; got %v", first)
	}

	// Second Apply: .res persists → firstActivation=false → seed
	// must NOT fire again.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-seed_00000",
		storage.FakeResponse{Stdout: []byte("pvc-seed_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-seed_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-seed_00000|1048576\n")})

	_, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (2nd): %v", err)
	}

	second := fx.CommandLines()
	if saw(second, "primary --force") != 0 {
		t.Errorf("idempotent reconcile must NOT re-seed; got %v", second)
	}
}

// TestDeleteResourceClosesLUKSMapper: when the satellite tears down a
// LUKS-encrypted resource, it must `cryptsetup luksClose` the mapper
// BEFORE DeleteVolume removes the underlying LV — otherwise the
// dangling /dev/mapper/<rd>-<vol>-luks node prevents a clean
// re-create with the same name on the next provision cycle. Pins the
// last open Phase 9 LUKS gap (luks.Close on teardown).
func TestDeleteResourceClosesLUKSMapper(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// DeleteVolume's idempotency probe sees the LV exists.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-luks-del_00000",
		storage.FakeResponse{Stdout: []byte("pvc-luks-del_00000\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	resp, err := rec.DeleteResource(t.Context(), &intent.DeleteResourceRequest{
		Name:          "pvc-luks-del",
		StoragePool:   "thin1",
		VolumeNumbers: []int32{0},
	})
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	if !resp.GetOk() {
		t.Fatalf("DeleteResource Ok=false: %s", resp.GetMessage())
	}

	calls := fx.CommandLines()

	closeIdx := -1
	removeIdx := -1
	for i, line := range calls {
		if strings.Contains(line, "luksClose pvc-luks-del-0-luks") {
			closeIdx = i
		}
		if strings.HasPrefix(line, "lvremove") {
			removeIdx = i
		}
	}

	if closeIdx < 0 {
		t.Errorf("expected cryptsetup luksClose pvc-luks-del-0-luks; got %v", calls)
	}

	if removeIdx < 0 {
		t.Errorf("expected lvremove; got %v", calls)
	}

	if closeIdx >= 0 && removeIdx >= 0 && closeIdx > removeIdx {
		t.Errorf("luksClose must run BEFORE lvremove (mapper would dangle on a missing LV); got close@%d remove@%d in %v",
			closeIdx, removeIdx, calls)
	}
}

// TestApplyConvergesAfterMidApplyAbort: simulates a hard satellite kill
// (SIGKILL of the daemonset pod) between applyStorage and applyDRBD.
// On the first Apply, drbdadm adjust fails — equivalent to "got SIGKILL
// before the drbdadm child finished" — and the result reports Ok=false.
// The next Apply must converge: storage was already provisioned (LV
// idempotency keeps it intact), the .res file from the failed first
// pass is still on disk so firstActivation flips to false, and the
// re-run drbdadm adjust now succeeds. No double-create, no double-md,
// just the same result the controller would see if Apply had completed
// the first time.
//
// Pins the Phase 8 PLAN.md item "Hard satellite kill mid-Apply —
// reconcile must be idempotent". This is the unit-level proof; the
// stand-side scenario is the same retry path under SIGKILL pressure.
func TestApplyConvergesAfterMidApplyAbort(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// applyStorage path (lvs idempotency probe + lvcreate).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-abort_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// VolumeStatus query for the .res builder.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-abort_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-abort_00000|1048576\n")})
	// First Apply: drbdadm adjust fails — the simulated mid-Apply abort.
	fx.Expect("drbdadm adjust pvc-abort", storage.FakeResponse{Err: errDrbdadmAdjustFail})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-abort",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	}

	results, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (1st) outer error: %v", err)
	}

	if len(results) != 1 || results[0].GetOk() {
		t.Fatalf("expected Ok=false on first Apply (drbdadm aborted); got results=%+v", results)
	}

	saw := func(lines []string, needle string) int {
		n := 0
		for _, line := range lines {
			if strings.Contains(line, needle) {
				n++
			}
		}

		return n
	}

	first := fx.CommandLines()
	if saw(first, "lvcreate") < 1 {
		t.Errorf("first Apply must run lvcreate; got %v", first)
	}

	if _, statErr := os.Stat(filepath.Join(dir, "pvc-abort.res")); os.IsNotExist(statErr) {
		t.Errorf(".res file must persist across an aborted Apply; the next reconcile relies on it to skip create-md")
	}

	// Second Apply: clear the drbdadm error so the same desired state
	// converges. lvs probe reports the LV exists this time → no
	// second lvcreate. The reconciler must see firstActivation=false
	// (the .res file lingers from the aborted first pass) and skip
	// create-md.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-abort_00000",
		storage.FakeResponse{Stdout: []byte("pvc-abort_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-abort_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-abort_00000|1048576\n")})
	// Steady-state kernel probe (Bug 47 / scenario 5.32): the first
	// pass succeeded in loading the kernel slot via the original
	// `drbdadm adjust`; the second pass uses `drbdsetup status` to
	// detect it's already loaded and emits a fresh `adjust`. Stage
	// a non-empty status output so the probe reads as "loaded".
	fx.Expect("drbdsetup status pvc-abort", storage.FakeResponse{
		Stdout: []byte("pvc-abort role:Secondary\n  volume:0 disk:UpToDate\n"),
	})
	// Overwrite the previously-failing drbdadm response with a clean
	// success — the simulated SIGKILL window has passed.
	fx.Expect("drbdadm adjust pvc-abort", storage.FakeResponse{})

	results, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (2nd) outer error: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("expected Ok=true on retry; got results=%+v", results)
	}

	second := fx.CommandLines()
	if saw(second, "lvcreate") != 0 {
		t.Errorf("retry must NOT re-run lvcreate (idempotency); got %v", second)
	}

	if saw(second, "create-md") != 0 {
		t.Errorf("retry must NOT re-run create-md (.res persists across abort, firstActivation=false); got %v", second)
	}

	if saw(second, "drbdadm adjust pvc-abort") != 1 {
		t.Errorf("retry must re-run drbdadm adjust to pick up where the abort left off; got %v", second)
	}
}

// TestApplyLUKSFormatErrorWraps pins the Format error-wrap path of
// applyLUKS (was 81.8%). When `cryptsetup luksFormat` fails (disk
// busy, hardware lock, etc.), applyOne must surface the error
// tagged with the "luks format" wrap keyword in the per-resource
// ApplyResources reply, NOT bubble it as a transport-level gRPC
// error.
//
// The dispatcher distinguishes "satellite said no" (Ok=false body-
// level) from "transport failed". Without the wrap keyword, an
// operator can't grep the satellite log to identify a stuck format
// vs. e.g. a stuck open.
func TestApplyLUKSFormatErrorWraps(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-luks-format_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-format_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-luks-format_00000|1048576\n")})
	fx.Expect("cryptsetup isLuks /dev/vg/pvc-luks-format_00000",
		storage.FakeResponse{Err: errNotALUKSDevice})
	// luksFormat fails: simulate the device being busy.
	fx.Expect(`sh -c printf %s "topsecret" | cryptsetup 'luksFormat' '--batch-mode' '/dev/vg/pvc-luks-format_00000' '--key-file' '-'`,
		storage.FakeResponse{Err: errLUKSFormatBusy})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-luks-format",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"LUKS", "STORAGE"},
			Props:      map[string]string{"LuksPassphrase": "topsecret"},
		},
	})
	if err != nil {
		t.Fatalf("Apply: got transport error %v, want per-resource Ok=false", err)
	}

	if len(results) != 1 {
		t.Fatalf("results: got %d, want 1", len(results))
	}

	if results[0].GetOk() {
		t.Errorf("Ok: got true, want false on luksFormat failure")
	}

	if !strings.Contains(results[0].GetMessage(), "luks format") {
		t.Errorf("message: got %q, want substring \"luks format\"", results[0].GetMessage())
	}
}

var errLUKSFormatBusy = errors.New("cryptsetup: device busy")

// TestApplyDRBDCreateMDErrorWraps pins the create-md error-wrap
// branch of applyDRBD (was 82.1%): on first activation of a
// diskful replica, when `drbdadm create-md` fails (metadata area
// unwritable, kernel module missing), applyOne must surface
// the error tagged with "create-md" in the per-resource
// ApplyResources reply. The dispatcher needs to see this as
// Ok=false body-level so it doesn't tear down the entire batch.
func TestApplyDRBDCreateMDErrorWraps(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-md-fail_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// drbdadm create-md fails.
	fx.Expect(fmt.Sprintf("drbdadm create-md --force --max-peers=%d pvc-md-fail", drbd.MaxPeers-1),
		storage.FakeResponse{Err: errCreateMDFailed})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-md-fail",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: got transport error %v, want Ok=false body-level", err)
	}

	if results[0].GetOk() {
		t.Errorf("Ok: got true, want false on create-md failure")
	}

	if !strings.Contains(results[0].GetMessage(), "create-md") {
		t.Errorf("message: got %q, want substring \"create-md\"", results[0].GetMessage())
	}
}

var errCreateMDFailed = errors.New("drbdadm: create-md kernel module missing")

// TestApplyDRBDLateVDDoesNotPinDiskless pins Bug 79: an operator
// who creates an RD and Resources before adding any VolumeDefinition
// must not get stuck in "Unintentional Diskless" once the VD lands.
//
// The empty-volume first pass MUST NOT write the .md-created marker
// (or run create-md, since there is nothing to create metadata on).
// Otherwise the second pass — after the VD is added — sees the
// marker, treats it as a non-first activation, skips create-md, and
// `drbdadm adjust` finds no on-disk metadata for the newly-present
// volume. The kernel then reports disk:Diskless even though
// Spec.Flags lacks DISKLESS — exactly the "Unintentional Diskless"
// runtime drift that surprised the operator on the production
// cluster.
//
// The contract pinned here: Apply with Volumes=nil is a no-op
// reconcile; a follow-up Apply with one volume triggers a full
// firstActivation (create-md + marker write + adjust).
func TestApplyDRBDLateVDDoesNotPinDiskless(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Second pass (with a volume) expectations: lvs probe + create-md
	// + drbdadm adjust. No drbdadm calls at all on the first pass.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-late-vd_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect(fmt.Sprintf("drbdadm create-md --force --max-peers=%d pvc-late-vd", drbd.MaxPeers-1),
		storage.FakeResponse{})
	fx.Expect("drbdadm adjust pvc-late-vd", storage.FakeResponse{})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// First pass: RD + Resource exist but no VolumeDefinition yet.
	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-late-vd",
			NodeName: "n1",
			Volumes:  nil, // ← Bug 79 repro: empty volumes on first apply
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply (empty-volume pass): transport error %v", err)
	}

	if !results[0].GetOk() {
		t.Fatalf("Apply (empty-volume pass): Ok=false, message=%q", results[0].GetMessage())
	}

	// The marker MUST NOT exist after the empty-volume pass. If it
	// does, the second pass will skip create-md and the kernel
	// reports the new volume as Diskless.
	markerPath := dir + "/pvc-late-vd.md-created"
	if _, err := os.Stat(markerPath); err == nil {
		t.Fatalf("md-created marker written on empty-volume pass — would pin late VD to Diskless on next reconcile")
	}

	// .res should also not be present yet — we have nothing to
	// render. Even if a partial render slipped through, the kernel
	// has no use for a 0-volume .res file.
	if _, err := os.Stat(dir + "/pvc-late-vd.res"); err == nil {
		t.Fatalf("written .res on empty-volume pass; expected satellite to leave the resource un-rendered until a VD arrives")
	}

	// Second pass: VD has been added, the volume now appears in the
	// desired state. firstActivation must fire (create-md + marker).
	results, err = rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-late-vd",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply (VD-added pass): transport error %v", err)
	}

	if !results[0].GetOk() {
		t.Fatalf("Apply (VD-added pass): Ok=false, message=%q", results[0].GetMessage())
	}

	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("md-created marker missing after VD-added pass: %v — late VD should run firstActivation", err)
	}
}

// TestApplyAutoPrimaryForceErrorWraps pins the auto-primary force
// error-wrap branch of applyDRBD: when the seed step
// `drbdadm primary --force` fails on first activation, applyOne
// must surface the error tagged with "auto-primary" in the
// per-resource reply.
//
// Without the wrap keyword, an operator can't distinguish a stuck
// seed (kernel module missing, /dev/drbdN already busy) from the
// downstream `drbdadm secondary` failure mode that follows the
// same metadata-shape concerns.
func TestApplyAutoPrimaryForceErrorWraps(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-seed-fail_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("drbdadm primary --force pvc-seed-fail",
		storage.FakeResponse{Err: errPrimaryForceFailed})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-seed-fail",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
				"auto-primary": "true",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: got transport error %v, want Ok=false body-level", err)
	}

	if results[0].GetOk() {
		t.Errorf("Ok: got true, want false on auto-primary failure")
	}

	if !strings.Contains(results[0].GetMessage(), "auto-primary") {
		t.Errorf("message: got %q, want substring \"auto-primary\"", results[0].GetMessage())
	}
}

var errPrimaryForceFailed = errors.New("drbdadm: device busy")

// TestApplyFirstActivationSeedsGiBeforeAdjust pins the Phase 8.1
// initial-sync skip pipeline: when the controller has filled in
// SeedFromGi on a freshly-created replica, the satellite must
// (a) run create-md to lay down a fresh metadata block, then
// (b) run drbdmeta set-gi to stamp it with the peer's GI, then
// (c) run drbdadm adjust to bring the resource up — all in that
// order. A regression that swaps (b)/(c) or skips (b) would let
// DRBD bring the resource up with zero GI, mismatch the peer's
// current_uuid on first connect, and trigger the full initial
// sync we built this whole pipeline to skip.
func TestApplyFirstActivationSeedsGiBeforeAdjust(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-seed_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// VolumeStatus reports the LV's path after CreateVolume so the
	// reconciler picks up the device for drbdmeta seeding.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-seed_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-seed_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-seed",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{
					VolumeNumber: 0,
					SizeKib:      1024 * 1024,
					StoragePool:  "thin1",
					SeedFromGi:   "78A0DDDABCDEF000",
				},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
				// Bug 81: set-gi is per-peer in DRBD 9.2+, so the seed
				// loop only fires when a peer is wired into DrbdOptions.
				// Single-replica RDs don't need a seed at all (no peer
				// to handshake with), but every multi-replica RD has at
				// least one `peer.<name>.node-id` entry — mirror that
				// shape here so the test exercises the per-peer path.
				"peer.n2.node-id": "1", "peer.n2.address": "10.0.0.2",
			},
			Peers: []string{"n2"},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	calls := fx.CommandLines()

	createMD := indexOfPrefix(calls, fmt.Sprintf("drbdadm create-md --force --max-peers=%d pvc-seed", drbd.MaxPeers-1))
	setGi := indexOfPrefix(calls, "drbdmeta --force pvc-seed/0 v09 ")
	adjust := indexOfPrefix(calls, "drbdadm adjust pvc-seed")

	if createMD < 0 {
		t.Fatalf("missing drbdadm create-md in calls: %v", calls)
	}

	if setGi < 0 {
		t.Fatalf("missing drbdmeta set-gi in calls: %v", calls)
	}

	if adjust < 0 {
		t.Fatalf("missing drbdadm adjust in calls: %v", calls)
	}

	if createMD >= setGi || setGi >= adjust {
		t.Errorf("ordering: create-md@%d → set-gi@%d → adjust@%d (want strictly ascending); calls=%v",
			createMD, setGi, adjust, calls)
	}

	// Pin the exact GI tuple shape so the seed gets the peer's
	// current_uuid in BOTH current_uuid and bitmap_uuid slots.
	// Bug 81: drbdmeta in DRBD 9.2+ requires --node-id, identifying
	// which peer-bitmap slot this GI applies to. We stamp peer n2
	// (node-id=1).
	wantSetGi := "drbdmeta --force pvc-seed/0 v09 /dev/vg/pvc-seed_00000 internal set-gi --node-id 1 78A0DDDABCDEF000:78A0DDDABCDEF000:0:0"
	if !slices.Contains(calls, wantSetGi) {
		t.Errorf("missing exact set-gi command %q in calls: %v", wantSetGi, calls)
	}
}

// TestApplyFirstActivationDiskReplaceInternalMetadata (scenario 5.W09)
// pins the satellite-side ordering for the upstream drbd-troubleshooting
// "Replacing a failed disk when using internal metadata" recipe.
//
// The recipe, applied at the operator-shell level on a satellite, is:
//
//	drbdadm detach --force <rd>         # local disk drops to Diskless
//	# swap underlying LV/zvol/file out of band
//	drbdmeta --force <rd>/<vol> v09 <dev> internal create-md <peers>
//	drbdadm attach <rd>                  # kernel re-reads fresh metadata
//
// (Cross-listed with wave1 6.18 + 6.19; the e2e walks the operator-shell
// path end-to-end in tests/e2e/disk-replace-internal-metadata.sh.)
//
// This test pins the *satellite-managed equivalent*: when the controller
// re-creates the Resource CRD (e.g. via `linstor r d <node> <rd>` + `rd
// ap <rd>`, the LINSTOR-managed shape of the same recipe), the
// satellite's first activation on the new replica MUST issue create-md
// BEFORE adjust — the v09 metadata format and the internal-metadata
// shape both come from the satellite's own `drbdadm create-md
// --force --max-peers=<N>` call, not from peer or controller state.
//
// Pin shape:
//   - on the fresh replica (no .res, no .md-created marker on disk yet)
//   - apply runs and emits exec calls in this order:
//     1. provider create / probe (LV / zvol / file)
//     2. `drbdadm create-md --force --max-peers=31 <rd>`
//     (stamps a fresh DRBD-9 v09 metadata block; the `v09` format
//     is implicit in `drbdadm create-md` for any DRBD-9 build)
//     3. `drbdadm adjust <rd>`
//     (the satellite's analog of `drbdadm attach` — adjust
//     attaches the lower disk + connects peers in one go for
//     first activation)
//
// What this catches: a regression that flips the order (e.g. running
// adjust before create-md, which would fail with "No valid meta data
// found"), drops create-md (e.g. mis-treating a re-created Resource
// as firstActivation=false), or stamps metadata via a different verb
// (e.g. `drbdmeta create-md` directly — which works at the operator
// shell but is NOT the satellite's contract; the satellite always
// goes through `drbdadm create-md` so the wrapper handles --max-peers
// + activity-log sizing consistently).
//
// Cross-listed pin: scenario 5.W09 also asserts a *separate* property
// — that when the OPERATOR runs `drbdmeta create-md + drbdadm attach`
// outside LINSTOR, the reconciler must NOT overwrite `.res` mid-recipe.
// That second property is covered by
// `TestApplyAdoptsExistingMetadataAfterDiskReplace` below, which pins
// the `HasMD` adopt-on-existing branch the recipe relies on.
func TestApplyFirstActivationDiskReplaceInternalMetadata(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// LV absent on first activation — the post-disk-replace shape from
	// the LINSTOR-managed recovery: the controller just dropped the
	// stale Resource CRD and re-created it, so the provider does a
	// fresh lvcreate (LVM-thin) before the DRBD bring-up.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-w09-replace_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-w09-replace",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2"},
			DrbdOptions: map[string]string{
				"port":            "7900",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1900",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7900",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	calls := fx.CommandLines()

	// The exact create-md invocation the satellite issues — pins the
	// v09 metadata format (implicit in `drbdadm create-md`) and the
	// internal-metadata shape (no `meta-disk <ext-dev>` in .res, which
	// means `drbdadm create-md` lays down internal metadata in the
	// trailing bytes of the lower disk). --max-peers is the
	// 5.W09-critical knob: a fresh replica MUST be sized for the
	// cluster's eventual peer count, not drbd-utils' default of 7,
	// or future replicas would fail to attach with
	// "peer-id out of range".
	wantCreateMD := fmt.Sprintf("drbdadm create-md --force --max-peers=%d pvc-w09-replace", drbd.MaxPeers-1)
	wantAdjust := "drbdadm adjust pvc-w09-replace"

	createMD := indexOfPrefix(calls, wantCreateMD)
	adjust := indexOfPrefix(calls, wantAdjust)
	lvcreate := indexOfPrefix(calls, "lvcreate")

	if lvcreate < 0 {
		t.Fatalf("missing lvcreate (LV must be carved before DRBD bring-up): %v", calls)
	}

	if createMD < 0 {
		t.Fatalf("missing %q in calls: %v", wantCreateMD, calls)
	}

	if adjust < 0 {
		t.Fatalf("missing %q in calls: %v", wantAdjust, calls)
	}

	// Hard ordering: lvcreate (lower-disk carve) → create-md (stamp
	// metadata) → adjust (kernel attach + peer connect). Any other
	// order is a regression of the upstream recipe — and adjust before
	// create-md would fail in production with "No valid meta data
	// found" on the freshly-allocated lower disk.
	if !(lvcreate < createMD && createMD < adjust) {
		t.Errorf("ordering: lvcreate@%d → create-md@%d → adjust@%d (want strictly ascending); calls=%v",
			lvcreate, createMD, adjust, calls)
	}

	// .md-created marker must exist on disk after the first activation
	// — without it, a satellite restart would re-run create-md on the
	// next reconcile and wipe the freshly-stamped metadata block.
	if _, statErr := os.Stat(filepath.Join(dir, "pvc-w09-replace.md-created")); statErr != nil {
		t.Errorf(".md-created marker missing after first activation: %v", statErr)
	}
}

// TestApplyAdoptsExistingMetadataAfterDiskReplace (scenario 5.W09,
// "raw `drbdmeta create-md + attach` outside LINSTOR" assertion)
// pins the safety guard the reconciler uses when an operator has
// already laid down a fresh DRBD-9 metadata block by hand (e.g. via
// the upstream-doc verbatim `drbdmeta --force <rd>/<vol> v09 <dev>
// internal create-md <peers>` command), but the satellite-side
// `.md-created` marker is missing — because the operator bypassed
// blockstor entirely and the controller-side desired state never
// changed, so no reconciler-driven create-md ever wrote the marker.
//
// The safety property: when `drbdadm dump-md <rd>` reports parseable
// metadata already on the lower disk (`HasMD=true`), the satellite
// MUST NOT re-run `drbdadm create-md` on this resource. `create-md
// --force` would wipe the operator's freshly-stamped GI + bitmap
// state and orphan the local data from the cluster — the exact
// failure mode the recipe is supposed to recover FROM.
//
// Instead, the satellite must:
//   - adopt the existing metadata (skip create-md)
//   - write the `.md-created` marker so subsequent reconciles see
//     `firstActivation=false` and stay on the steady-state branch
//   - continue to `drbdadm adjust` (the satellite-side analog of
//     `drbdadm attach`, which is the last step of the upstream
//     recipe) so the kernel picks up the new metadata block
//
// This is the property the e2e `disk-replace-internal-metadata.sh`
// indirectly exercises end-to-end ("reconciler picks up state within
// 10s without overwriting `.res`"); this unit test pins the exact
// satellite-side decision branch so a regression at the satellite
// layer surfaces in unit tests, not only in slow e2e runs.
func TestApplyAdoptsExistingMetadataAfterDiskReplace(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// LV present — the LINSTOR-managed teardown didn't run (operator
	// bypassed blockstor), so the lower disk + its metadata block are
	// already in place by the time the next reconcile fires.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-w09-adopt_00000",
		storage.FakeResponse{Stdout: []byte("pvc-w09-adopt_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-w09-adopt_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-w09-adopt_00000|1048576\n")})

	// drbdadm dump-md returns a parseable metadata block — the
	// operator's `drbdmeta create-md` just stamped a fresh one. The
	// real drbdadm dump-md prints a multi-line `version`/`la-size`/
	// `bm-uuid`/... dump; the satellite's HasMD only needs `err == nil
	// && len(out) > 0`, so a minimal canned response suffices.
	fx.Expect("drbdadm dump-md pvc-w09-adopt",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-w09-adopt",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2"},
			DrbdOptions: map[string]string{
				"port":            "7901",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1901",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7901",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	calls := fx.CommandLines()

	// The forbidden verb: `drbdadm create-md` on a resource whose
	// metadata is already present would wipe the operator-stamped
	// GI + bitmap and orphan the local data from the cluster. This
	// is the W09 invariant that makes the upstream recipe safe.
	for _, line := range calls {
		if strings.HasPrefix(line, "drbdadm create-md") {
			t.Errorf("reconciler re-ran create-md despite HasMD=true (would wipe operator-stamped metadata): %s", line)
		}
	}

	// dump-md (HasMD probe) MUST have fired — without it the safety
	// guard is bypassed and the create-md call above would have run.
	if indexOfPrefix(calls, "drbdadm dump-md pvc-w09-adopt") < 0 {
		t.Errorf("HasMD probe (drbdadm dump-md) missing from call sequence: %v", calls)
	}

	// adjust MUST still fire — adopting metadata is only safe if we
	// also hand the kernel control of the now-attached state. Without
	// adjust, the resource stays Diskless on n1 forever despite the
	// metadata being healthy.
	if !slices.Contains(calls, "drbdadm adjust pvc-w09-adopt") {
		t.Errorf("expected drbdadm adjust after adopting existing metadata; got %v", calls)
	}

	// .md-created marker MUST be written — it gates `firstActivation`
	// across satellite restarts. Without the marker, the next reconcile
	// would re-enter the firstActivation=true branch, hit the dump-md
	// probe again (still ok), but more importantly betray the adopt
	// path's "this is a one-shot fixup" intent.
	if _, statErr := os.Stat(filepath.Join(dir, "pvc-w09-adopt.md-created")); statErr != nil {
		t.Errorf(".md-created marker not written after adopting existing metadata: %v", statErr)
	}
}

// TestApplyFirstActivationNoSkipOnLVMThick (Bug 77 negative case)
// pins that on a fresh RD with no peer to seed from AND a backing
// provider that does NOT guarantee zeroes on read (thick LVM hands
// back whatever bytes were on the PV's extents previously), the
// satellite MUST NOT issue a synthetic day0 set-gi. DRBD then
// falls through to the full initial-sync on first connect, which
// is the only safe behaviour: skipping initial-sync on thick LVM
// would let pre-existing garbage on one replica's extents differ
// from the other's and never resync.
//
// SeedFromGi is empty (no UpToDate peer in Status.Peers). The
// existing-peer path is covered by TestApplyFirstActivationSeedsGiBeforeAdjust
// and is unchanged.
func TestApplyFirstActivationNoSkipOnLVMThick(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-noseed_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thick := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thick1": thick},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-noseed",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thick1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdmeta") {
			t.Errorf("drbdmeta ran on thick LVM without SeedFromGi: %s", line)
		}
	}
}

// TestApplyFirstActivationSkipsInitialSyncOnThinOrZFS (Bug 77 fix)
// pins the upstream-LINSTOR `DrbdLayerUtils.skipInitSync` behaviour:
// when a fresh RD activates on a backing provider that is guaranteed
// to hand back zero-initialised storage (thin LVM, thin or thick
// ZFS, sparse file), the satellite stamps a deterministic per-RD,
// per-volume "day 0" GI on each replica's metadata block before
// `drbdadm adjust`. The day0 is the same on every node (derived
// from the RD name + volume number), so DRBD-9's GI handshake on
// first connect matches and skips the full initial-sync.
//
// Two cases via subtests: ProviderKind="ZFS_THIN" (sparse zvol)
// and ProviderKind="LVM_THIN" (dm-thin volume). Both have the
// "read-as-zero on unprovisioned blocks" property the upstream
// short-circuit relies on.
//
// The fix MUST work without SeedFromGi being stamped by the
// controller — that's the difference from the existing-peer path
// already pinned by TestApplyFirstActivationSeedsGiBeforeAdjust.
func TestApplyFirstActivationSkipsInitialSyncOnThinOrZFS(t *testing.T) {
	cases := []struct {
		name        string
		newProvider func(storage.Exec) storage.Provider
		poolName    string
		// listProbe is the per-provider "does the volume exist?" exec
		// line. The satellite issues it via Provider.CreateVolume's
		// idempotency check.
		listProbe string
		// statProbe is the VolumeStatus exec line the satellite issues
		// after CreateVolume to learn the on-disk device path.
		statProbe string
		// statReply is the stdout the FakeExec hands back for statProbe.
		// Carries the device path the satellite then passes to drbdmeta.
		statReply string
		// wantDevice is the device path that must show up inside the
		// drbdmeta set-gi command line.
		wantDevice string
	}{
		{
			name: "ZFS_THIN",
			newProvider: func(ex storage.Exec) storage.Provider {
				return zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, ex)
			},
			poolName:   "zfs-thin1",
			listProbe:  "zfs list -H -o name tank/pvc-zskip_00000",
			statProbe:  "zfs list -H -p -o name,volsize,used tank/pvc-zskip_00000",
			statReply:  "tank/pvc-zskip_00000\t1073741824\t512\n",
			wantDevice: "/dev/zvol/tank/pvc-zskip_00000",
		},
		{
			name: "LVM_THIN",
			newProvider: func(ex storage.Exec) storage.Provider {
				return lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, ex)
			},
			poolName:   "lvm-thin1",
			listProbe:  "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-zskip_00000",
			statProbe:  "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-zskip_00000",
			statReply:  "/dev/vg/pvc-zskip_00000|1048576\n",
			wantDevice: "/dev/vg/pvc-zskip_00000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			fx.Expect(tc.listProbe, storage.FakeResponse{Stdout: []byte("")})
			fx.Expect(tc.statProbe, storage.FakeResponse{Stdout: []byte(tc.statReply)})

			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{tc.poolName: tc.newProvider(fx)},
				Adm:       drbd.NewAdm(fx),
				StateDir:  dir,
				NodeName:  "n1",
			})

			_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
				{
					Name:     "pvc-zskip",
					NodeName: "n1",
					Volumes: []*intent.DesiredVolume{
						{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: tc.poolName},
					},
					DrbdOptions: map[string]string{
						"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
						// Bug 81: per-peer SetGi loop needs a peer wired
						// in so the day0 skip-init-sync seed actually
						// fires. Production never sees a 1-replica RD
						// reach this path (autoplace mandates at least
						// 1 peer or tiebreaker); test mirrors a 2-replica
						// shape.
						"peer.n2.node-id": "1", "peer.n2.address": "10.0.0.2",
					},
					Peers: []string{"n2"},
				},
			})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			calls := fx.CommandLines()

			createMD := indexOfPrefix(calls, fmt.Sprintf("drbdadm create-md --force --max-peers=%d pvc-zskip", drbd.MaxPeers-1))
			setGi := indexOfPrefix(calls, "drbdmeta --force pvc-zskip/0 v09 ")
			adjust := indexOfPrefix(calls, "drbdadm adjust pvc-zskip")

			if createMD < 0 {
				t.Fatalf("missing drbdadm create-md in calls: %v", calls)
			}

			if setGi < 0 {
				t.Fatalf("missing drbdmeta set-gi (day0 skip-init-sync) in calls: %v", calls)
			}

			if adjust < 0 {
				t.Fatalf("missing drbdadm adjust in calls: %v", calls)
			}

			if createMD >= setGi || setGi >= adjust {
				t.Errorf("ordering: create-md@%d → set-gi@%d → adjust@%d (want strictly ascending); calls=%v",
					createMD, setGi, adjust, calls)
			}

			// Pin the exact day0 GI shape: same current_uuid in BOTH
			// current_uuid and bitmap_uuid slots, history zeroed. The
			// satellite derives day0 deterministically from RD name +
			// volume number — keep the expected value in sync with
			// pkg/satellite/providerkind.go's day0GiFor().
			day0 := satellite.Day0GiForTest("pvc-zskip", 0)
			// Bug 81: per-peer set-gi includes --node-id <peer>.
			// Day0 is identical across all peers (deterministic from
			// RD name + volume), so peer n2 (node-id=1) gets the same
			// day0 in BOTH bitmap-uuid slots — exactly what DRBD needs
			// for its skip-init-sync handshake.
			wantSetGi := fmt.Sprintf("drbdmeta --force pvc-zskip/0 v09 %s internal set-gi --node-id 1 %s:%s:0:0",
				tc.wantDevice, day0, day0)
			if !slices.Contains(calls, wantSetGi) {
				t.Errorf("missing exact day0 set-gi command %q in calls: %v", wantSetGi, calls)
			}
		})
	}
}

// TestApplyDefersAdjustDuringSyncTarget (scenario 5.16, Bug 8 unit
// pin) encodes the satellite-side invariant the e2e test
// `tests/e2e/recovery-synctarget-defer.sh` exercises end-to-end:
//
//	`drbdadm adjust` re-renders the kernel's connection config from
//	the .res file. DRBD-9 treats `adjust` against a peer that is
//	currently SyncSource / SyncTarget as a connection-config touch
//	and disconnects, dropping the in-flight bitmap progress and
//	restarting the resync from 0%. On multi-hundred-GiB volumes
//	this loops forever as new prop / spec changes keep landing.
//
// The fix: on a steady-state Apply (not first activation), the
// reconciler MUST consult kernel state for the resource and skip
// the `drbdadm adjust` call when any peer is mid-resync
// (`replication:SyncTarget` or `replication:SyncSource`). The
// .res file rewrite is still safe — DRBD doesn't re-read it
// without an `adjust`. The deferred adjust runs on the next
// reconcile pass once the peer reaches `replication:Established`.
//
// FIRST ACTIVATION IS EXEMPT: a fresh `drbdadm adjust` is what
// actually brings the resource up — there's no in-flight resync
// to clobber, the kernel slot doesn't even exist yet. This test
// gates on the *steady-state* Apply (second pass with .res +
// md-marker persisting); the first-pass adjust is verified by
// the existing TestApplyInvokesDrbdadmAdjust.
//
// PROBE: the natural mechanism is `drbdsetup status <rd>` —
// drbd-utils already lists replication:<state> per peer there.
// The reconciler shells out, parses the replication tokens, and
// gates the adjust on the absence of {SyncSource, SyncTarget,
// PausedSyncS, PausedSyncT, VerifyS, VerifyT}.
//
// Currently t.Skip()'d: the reconciler does NOT yet probe
// kernel state on Apply (see applyDRBD in pkg/satellite/reconciler.go
// — the `r.cfg.Adm.Adjust(ctx, dr.GetName())` call is
// unconditional). Once the probe + gate land, drop the t.Skip
// and the test pins the invariant for regression catches.
func TestApplyDefersAdjustDuringSyncTarget(t *testing.T) {
	t.Skip("Bug 8 fix not yet implemented in pkg/satellite/reconciler.go applyDRBD — " +
		"the reconciler calls drbdadm adjust unconditionally. Once kernel-state probing " +
		"+ defer gate lands (per scenario 5.16 e2e test), drop the Skip and this test " +
		"pins the SyncTarget defer invariant.")

	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// First-pass storage probe: LV absent, lvcreate will run.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-synctgt_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-synctgt",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 2 * 1024 * 1024, StoragePool: "thin1"},
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
		},
	}

	// First Apply: brings the resource up, adjust MUST fire. This
	// pass also writes the .res + md-marker the second pass keys off
	// of for firstActivation=false.
	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (first activation): %v", err)
	}

	if !slices.Contains(fx.CommandLines(), "drbdadm adjust pvc-synctgt") {
		t.Fatalf("first-activation Apply must adjust to bring the resource up; got %v",
			fx.CommandLines())
	}

	// Second Apply: steady-state. .res + md-marker persist →
	// firstActivation=false. Stage `drbdsetup status` to show n2 in
	// SyncTarget — the kernel probe the post-fix reconciler will
	// consult. The exact wire format mirrors what drbdsetup emits
	// on a live mid-sync resource.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-synctgt_00000",
		storage.FakeResponse{Stdout: []byte("pvc-synctgt_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-synctgt_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-synctgt_00000|2097152\n")})
	fx.Expect("drbdsetup status pvc-synctgt",
		storage.FakeResponse{Stdout: []byte(`pvc-synctgt role:Primary
  volume:0 disk:UpToDate
  n2 role:Secondary
    volume:0 replication:SyncTarget peer-disk:Inconsistent done:42.50
`)})

	_, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (steady-state mid-sync): %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm adjust pvc-synctgt") {
		t.Errorf("steady-state Apply during SyncTarget MUST defer adjust (Bug 8): got %v",
			fx.CommandLines())
	}
}

// TestApplyDefersAdjustDuringPausedSyncS (scenario 5.25) extends the
// 5.16 SyncTarget defer to the PausedSyncS variant. DRBD-9 emits
// `replication:PausedSyncS` when the SyncSource side has paused the
// resync — typically because of `resync-suspended:dependency`: the
// kernel detected another peer holds the only UpToDate copy of a
// region the SyncTarget needs, and dropped the resync until that
// dependency clears (the operator runs `drbdadm disconnect <r>:<peer>`
// + `drbdadm connect <r>:<peer>` against the Primary to force a
// fresh handshake, exactly as in the drbd-recovery skill).
//
// The reconciler MUST treat PausedSyncS the same as SyncTarget /
// SyncSource: a `drbdadm adjust` mid-pause would re-render
// connection config, the kernel would drop the (still-armed)
// resync state, and the operator's manual recovery recipe would
// race against the reconciler's adjust on every reconcile pass.
//
// PausedSyncT mirror is asserted in a subtest below — same
// invariant, peer-side perspective. VerifyS / VerifyT have the
// same kernel-level "connection-config-touch tears down state"
// failure mode and are exercised inline so a future regression
// that special-cases PausedSync* but drops Verify* surfaces.
//
// Currently t.Skip()'d: same reason as TestApplyDefersAdjustDuringSyncTarget
// — kernel-state probe + defer gate are not yet wired into applyDRBD.
func TestApplyDefersAdjustDuringPausedSyncS(t *testing.T) {
	cases := []struct {
		name        string
		replication string
		peerDisk    string
	}{
		{
			name:        "PausedSyncS (resync-suspended:dependency)",
			replication: "PausedSyncS",
			peerDisk:    "Inconsistent",
		},
		{
			name:        "PausedSyncT (peer-side paused, same invariant)",
			replication: "PausedSyncT",
			peerDisk:    "Inconsistent",
		},
		{
			name:        "VerifyS (online verify in progress)",
			replication: "VerifyS",
			peerDisk:    "UpToDate",
		},
		{
			name:        "VerifyT (peer-side verify)",
			replication: "VerifyT",
			peerDisk:    "UpToDate",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Skip("Bug 8 fix not yet implemented — reconciler calls drbdadm adjust " +
				"unconditionally; PausedSyncS/PausedSyncT/VerifyS/VerifyT defer " +
				"invariant will activate once the kernel-state probe + gate land.")

			dir := t.TempDir()
			fx := storage.NewFakeExec()
			fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-paused_00000",
				storage.FakeResponse{Stdout: []byte("")})

			thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{"thin1": thin},
				Adm:       drbd.NewAdm(fx),
				StateDir:  dir,
				NodeName:  "n1",
			})

			dr := []*intent.DesiredResource{
				{
					Name:     "pvc-paused",
					NodeName: "n1",
					Volumes: []*intent.DesiredVolume{
						{VolumeNumber: 0, SizeKib: 2 * 1024 * 1024, StoragePool: "thin1"},
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
				},
			}

			// First activation: adjust must fire (no in-flight resync
			// to clobber, kernel slot doesn't exist yet).
			_, err := rec.Apply(t.Context(), dr)
			if err != nil {
				t.Fatalf("Apply (first activation): %v", err)
			}

			if !slices.Contains(fx.CommandLines(), "drbdadm adjust pvc-paused") {
				t.Fatalf("first-activation Apply must adjust; got %v", fx.CommandLines())
			}

			// Steady-state with kernel mid-pause: stage drbdsetup status
			// to report the paused/verify replication state. The
			// reconciler's gate must pick this up and skip adjust.
			//
			// IMPORTANT: the .res + md-marker linger from the first
			// pass, so firstActivation=false on the second pass. The
			// defer only applies on the firstActivation=false branch
			// — see the test rationale on TestApplyDefersAdjustDuringSyncTarget.
			fx.Reset()
			fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-paused_00000",
				storage.FakeResponse{Stdout: []byte("pvc-paused_00000\n")})
			fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-paused_00000",
				storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-paused_00000|2097152\n")})
			fx.Expect("drbdsetup status pvc-paused",
				storage.FakeResponse{Stdout: []byte(fmt.Sprintf(`pvc-paused role:Primary
  volume:0 disk:UpToDate
  n2 role:Secondary
    volume:0 replication:%s peer-disk:%s done:33.00
`, tc.replication, tc.peerDisk))})

			_, err = rec.Apply(t.Context(), dr)
			if err != nil {
				t.Fatalf("Apply (steady-state %s): %v", tc.replication, err)
			}

			if slices.Contains(fx.CommandLines(), "drbdadm adjust pvc-paused") {
				t.Errorf("steady-state Apply during %s MUST defer adjust "+
					"(operator runs disconnect+connect to recover; reconciler must NOT race that): got %v",
					tc.replication, fx.CommandLines())
			}

			// Positive control: .res file MUST still be rewritten —
			// it's the durable record of desired state, the kernel
			// just doesn't re-read it without an `adjust`. Skipping
			// the .res write would lose any prop/peer change the
			// controller pushed while the resync was paused.
			resPath := filepath.Join(dir, "pvc-paused.res")
			if _, statErr := os.Stat(resPath); statErr != nil {
				t.Errorf("steady-state Apply during %s must still rewrite .res "+
					"(adjust is deferred, .res-write is not): %v",
					tc.replication, statErr)
			}
		})
	}
}

// indexOfPrefix returns the index of the first call line that
// begins with the given prefix, or -1.
func indexOfPrefix(lines []string, prefix string) int {
	for i, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return i
		}
	}

	return -1
}

// TestReconcilerDoesNotPropagateDiscardMyData (scenario 5.31) pins the
// safety property that drives operator-misuse defence:
//
//	Setup: 2-replica RD pvc-discard. This satellite (n1) holds the
//	ONLY UpToDate copy; peer n2 is Inconsistent / already-discarded
//	(e.g. the operator just ran `drbdadm connect --discard-my-data`
//	on n2 against the rules, or n2 came back from a failed resync).
//
//	Expectation: when the satellite drives a reconcile pass over a
//	steady DesiredResource (no growth, no flag change, no peer
//	churn) it MUST NOT auto-replicate the discard back onto its
//	own UpToDate replica. Concretely:
//	  - no `drbdadm connect --discard-my-data` on n1
//	  - no `drbdadm disconnect` on the surviving UpToDate peer
//	  - no `drbdadm down` / `drbdadm up` on the surviving replica
//	The reconciler treats the on-the-wire state of the peer as
//	observed-state (not desired-state) and converges by writing
//	the .res file + `drbdadm adjust`; DRBD then negotiates with
//	the peer using the normal handshake, which will refuse a
//	discard-my-data offered against a current-uuid the local node
//	doesn't recognise.
//
// The test drives two Apply passes: first activation (writes .res +
// runs create-md + adjust) and a steady-state second pass with the
// LV already present. The assertions span BOTH passes — proving
// the property holds across the bring-up boundary, since the
// scenario can fire either way in production (operator misuse can
// happen the moment a brand-new replica joins, or long after the
// resource has been live).
func TestReconcilerDoesNotPropagateDiscardMyData(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// First-pass storage probe: LV absent, lvcreate will run.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-discard_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// 2-replica RD: this satellite (n1) is the UpToDate copy, n2 is
	// the peer the operator just ran `drbdadm connect
	// --discard-my-data` against. The DesiredResource shape the
	// satellite sees from the controller is identical to a healthy
	// 2-replica RD — peer disk-state lives in the observer, NOT in
	// the apply payload, so the reconciler cannot use it to make a
	// "discard" decision. That's exactly the property under test.
	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-discard",
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
		},
	}

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (first activation): %v", err)
	}

	first := fx.CommandLines()

	// Steady-state second pass: LV already present, .res + md-marker
	// linger from the first pass → firstActivation=false. This is
	// the path that fires repeatedly while the operator-misuse
	// state persists on n2. Reconciler must remain inert toward
	// connect/disconnect/down/up regardless of how many times it
	// re-runs.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-discard_00000",
		storage.FakeResponse{Stdout: []byte("pvc-discard_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-discard_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-discard_00000|1048576\n")})
	// Steady-state kernel probe (Bug 47 / scenario 5.32): the
	// reconciler now consults `drbdsetup status <rd>` before
	// choosing between `drbdadm adjust` (kernel slot present) and
	// `drbdadm up` (kernel slot absent). Stage the UpToDate kernel
	// view this test asserts — without it the FakeExec returns
	// empty stdout, the probe reads as "not loaded", and the
	// reconciler emits `drbdadm up` instead of the `adjust` the
	// test pins.
	fx.Expect("drbdsetup status pvc-discard", storage.FakeResponse{
		Stdout: []byte("pvc-discard role:Secondary\n  volume:0 disk:UpToDate\n"),
	})

	_, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (steady-state): %v", err)
	}

	second := fx.CommandLines()

	// Forbidden commands. Every one of these, if issued by the
	// reconciler on the surviving UpToDate replica, would either
	// propagate the operator's discard (`connect
	// --discard-my-data`), pre-quiesce the connection so DRBD's
	// handshake never gets to refuse the discard (`disconnect`),
	// or drop the kernel state entirely so the next `up` re-reads
	// metadata and re-handshakes from scratch (`down`/`up`) —
	// undoing the protection DRBD's own handshake gives us.
	forbidden := []string{
		"drbdadm connect --discard-my-data",
		"connect --discard-my-data",
		"drbdadm disconnect pvc-discard",
		"drbdadm down pvc-discard",
		"drbdadm up pvc-discard",
	}

	for _, phase := range []struct {
		label string
		lines []string
	}{
		{"first-activation", first},
		{"steady-state", second},
	} {
		for _, bad := range forbidden {
			for _, line := range phase.lines {
				if strings.Contains(line, bad) {
					t.Errorf("%s phase: reconciler issued forbidden %q on surviving UpToDate replica: %s\nall calls: %v",
						phase.label, bad, line, phase.lines)
				}
			}
		}
	}

	// Positive control: the reconciler MUST still run `drbdadm
	// adjust` — that's its sole convergence verb for live state.
	// Without it the test would degenerate to "reconciler did
	// nothing", which would pass the forbidden-command checks
	// trivially.
	if !slices.Contains(first, "drbdadm adjust pvc-discard") {
		t.Errorf("first-activation phase: expected drbdadm adjust to fire; got %v", first)
	}

	if !slices.Contains(second, "drbdadm adjust pvc-discard") {
		t.Errorf("steady-state phase: expected drbdadm adjust to fire; got %v", second)
	}
}

// TestReconcilerRespectsOperatorDisconnect (scenario 5.29) pins the
// safety property that an operator-initiated `drbdadm disconnect`
// from the satellite shell must survive ≥30s of reconciler activity
// without auto-reconnect. The 30s window is the documented manual
// recovery budget — long enough for the operator to inspect peer
// state, run `drbdcheck`, decide whether to `drbdadm connect` or
// `drbdadm primary --force`, etc. — without the reconciler racing
// them by re-establishing the connection.
//
// Design choice: Option B (rely on structural reconciler behaviour),
// NOT Option A (per-resource `Aux/operator-managed=true` gate).
//
// Why B: at the time of writing, the satellite-side `drbd.Adm`
// wrapper exposes Up / Down / Adjust / CreateMD / Primary /
// PrimaryForce / Secondary / Detach / Resize / SetGi / DelPeer —
// notably NO `Connect` verb. The reconciler's sole live-state
// convergence call is `drbdadm adjust`, which re-reads the .res
// file and reconfigures peers, but in DRBD 9 does NOT force a
// connection on a peer the operator has manually disconnected
// (a disconnected peer stays StandAlone / Disconnecting until
// `drbdadm connect` is run). So "operator disconnect survives
// reconciliation" is enforced by absence-of-verb, not by a prop
// gate.
//
// That makes this test a regression pin: if someone later adds an
// `(*Adm).Connect` and wires it into the apply path (e.g. trying
// to be helpful and auto-reconnect StandAlone peers based on
// observer state), this test will fail and force the author to
// either gate the new code on `Aux/operator-managed=false` or
// justify the change against scenario 5.29.
//
// The test drives 5 reconcile passes (the count chosen to model
// roughly one pass per ~6s of the 30s window — reconcile is event-
// driven not interval-driven, but 5 passes covers the worst-case
// burst from peer-state flapping + heartbeat retries inside the
// window) over a steady DesiredResource and asserts the FakeExec
// transcript contains ZERO `drbdadm connect` lines for the whole
// run.
//
// Open issue tracked separately: if/when the scenarios doc settles
// on requiring an explicit `Aux/operator-managed=true` prop (the
// 5.29 doc still calls it an "open design question"), this test
// should grow a sibling that drives the prop-gated branch.
func TestReconcilerRespectsOperatorDisconnect(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// First-pass storage probe: LV absent, lvcreate will run.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-opdisc_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// 2-replica RD. The observer (out-of-band, not modelled here)
	// reports peer n2 as Disconnecting/StandAlone after the
	// operator ran `drbdadm disconnect pvc-opdisc` on the
	// satellite shell. The DesiredResource shape the controller
	// hands the satellite is unchanged — peer connection-state is
	// observer-only, NOT part of the apply payload. That's the
	// point: the reconciler has no signal that would tempt it to
	// "fix" a manually-disconnected peer, and no verb in its
	// toolbox to do so even if it tried.
	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-opdisc",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2"},
			DrbdOptions: map[string]string{
				"port":            "7100",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1100",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7100",
			},
		},
	}

	// Pass 1: first activation — writes .res, runs create-md +
	// adjust. From pass 2 onward we're in steady state with the
	// LV present, the .res file linger, and create-md already
	// marked done.
	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply pass 1 (first activation): %v", err)
	}

	allLines := []string{}
	allLines = append(allLines, fx.CommandLines()...)

	// Passes 2–5: steady-state reconciles. This is the loop a
	// satellite runs while the operator is still inside their
	// 30s window — DesiredResource hasn't changed, observer
	// keeps reporting peer StandAlone, reconciler keeps firing.
	for pass := 2; pass <= 5; pass++ {
		fx.Reset()
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-opdisc_00000",
			storage.FakeResponse{Stdout: []byte("pvc-opdisc_00000\n")})
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-opdisc_00000",
			storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-opdisc_00000|1048576\n")})

		if _, err := rec.Apply(t.Context(), dr); err != nil {
			t.Fatalf("Apply pass %d (steady-state): %v", pass, err)
		}

		allLines = append(allLines, fx.CommandLines()...)
	}

	// Core assertion: ZERO `drbdadm connect` calls anywhere in
	// the 5-pass window. `disconnect` is also forbidden — the
	// reconciler should never preemptively quiesce a connection
	// either, that's purely the operator's call here.
	forbidden := []string{
		"drbdadm connect",
		"drbdadm disconnect",
	}

	for _, bad := range forbidden {
		for _, line := range allLines {
			if strings.Contains(line, bad) {
				t.Errorf("reconciler issued forbidden %q during operator-disconnect window: %s\nall calls: %v",
					bad, line, allLines)
			}
		}
	}

	// Positive control: `drbdadm adjust` must still fire at
	// least once across the 5 passes — without it the test
	// degenerates to "reconciler did nothing", which would pass
	// the forbidden-verb check trivially. Adjust is the verb
	// that proves the reconciler IS running, just not touching
	// the connection state.
	sawAdjust := false

	for _, line := range allLines {
		if strings.Contains(line, "drbdadm adjust pvc-opdisc") {
			sawAdjust = true

			break
		}
	}

	if !sawAdjust {
		t.Errorf("expected at least one drbdadm adjust across 5 passes; got %v", allLines)
	}
}

// TestOperatorDisconnectSurvives30sWindow (scenario 5.W14, cross-listed
// with wave1 5.29) pins the wave2 P1 regression budget on the operator-
// disconnect window. Where the sibling TestReconcilerRespectsOperator
// Disconnect drives 5 passes as a quick-burst guard, this test stretches
// to a 10-pass steady-state loop — long enough to cover the documented
// ≥30s manual-recovery budget at one-pass-per-~3s plus a comfortable
// margin for reconcile back-pressure / observer-driven re-enqueues
// during the window.
//
// What's pinned beyond the 5.29 baseline:
//
//  1. A broader forbidden-verb set. 5.29 forbids `drbdadm connect` /
//     `drbdadm disconnect` outright. 5.W14 extends to the lower-level
//     `drbdsetup connect` / `drbdsetup disconnect` and the bring-up
//     short-circuit `drbdsetup new-peer ... --connect` that a future
//     "be helpful and re-handshake StandAlone peers" patch might reach
//     for. If any of those forms ever lands on the apply path without
//     a corresponding Aux/operator-managed=false gate, this test fails
//     and forces the author to justify the change against 5.W14's
//     scenario doc + the open design question on Aux/operator-managed.
//
//  2. Per-pass liveness positive control. 5.29 asserts adjust fires
//     "at least once" across the whole 5-pass window. 5.W14 asserts
//     `drbdadm adjust` fires on EVERY steady-state pass (2 through 10):
//     a future regression that silently skipped reconcile passes when
//     it observed a StandAlone peer would still pass 5.29's "at least
//     one" gate but fail this one. The reconciler's contract is "keep
//     converging the convergeable layers (.res render, adjust) even
//     when the peer is operator-quiesced" — adjust on a peer in
//     StandAlone is a no-op for the peer's connection state in DRBD 9
//     and is what makes that contract safe.
//
//  3. Doc-pin for the design choice. The 5.W14 scenarios doc still
//     calls Aux/operator-managed=true an "open design question". This
//     test implicitly pins Option B (rely on the absence of any
//     reconnect verb in `drbd.Adm`) by NOT stamping any operator-
//     managed prop on the DesiredResource. If the project later flips
//     to Option A (explicit prop gate), this test's DesiredResource
//     shape becomes the negative-control baseline (no prop → still no
//     reconnect because the gate defaults closed) and a sibling test
//     should drive the prop-set case.
//
// The test does NOT model wall-clock time directly — the reconciler is
// event-driven, not interval-driven, so wall-clock isn't a meaningful
// invariant to assert against in unit scope. The pass count is the
// chosen proxy: 10 passes models the worst-case burst the satellite
// could fire during a 30s window if every event-source (controller
// push, observer state change, heartbeat retry) flapped concurrently.
func TestOperatorDisconnectSurvives30sWindow(t *testing.T) {
	const (
		// passCount models the ≥30s recovery budget. At the worst-case
		// reconcile burst observed in production (~one pass per 3s
		// under observer flap), 10 passes covers the full window with
		// a 3s margin.
		passCount = 10
		// steadyStartPass is the first pass that's in steady-state
		// (LV present, .res lingering, create-md marker dropped).
		// Pass 1 is the first-activation bring-up.
		steadyStartPass = 2
	)

	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// Pass 1: first activation — LV absent, lvcreate fires.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-opdisc30s_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// 2-replica RD. Observer (out-of-band, not modelled) reports peer
	// n2 StandAlone after operator's `drbdadm disconnect pvc-opdisc30s`
	// — but the DesiredResource the controller pushes to the satellite
	// is unchanged, because peer connection-state is observer-only and
	// not part of the apply payload. That's the property under test:
	// the reconciler has no signal that would tempt it to "fix" a
	// manually-disconnected peer.
	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-opdisc30s",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2"},
			DrbdOptions: map[string]string{
				"port":            "7200",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1200",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7200",
			},
		},
	}

	// Pass 1: first activation. create-md + adjust + bring-up land
	// here; the forbidden-verb scan still applies, but the per-pass
	// adjust-liveness check starts from steadyStartPass.
	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply pass 1 (first activation): %v", err)
	}

	type passCommands struct {
		pass  int
		lines []string
	}

	transcript := []passCommands{{pass: 1, lines: append([]string{}, fx.CommandLines()...)}}

	// Passes 2..passCount: steady-state. .res file + md-marker linger
	// from pass 1, LV reports present. This is the loop the satellite
	// actually runs while the operator is inside their 30s window.
	//
	// Per-pass FakeExec staging:
	//
	//   - `lvs` (twice): the storage layer's existence + path/size
	//     probe. Reports the LV present at the requested size, so the
	//     storage path is a no-op and only the DRBD path runs.
	//   - `drbdsetup status <rd>`: the kernel-state probe gating
	//     `drbdadm up` vs `drbdadm adjust` (Bug 47 / scenario 5.32).
	//     Stage a non-empty role/disk line so IsLoaded reads "loaded"
	//     and the reconciler picks adjust. Without this, the probe
	//     fails → reconciler falls back to `drbdadm up`, and the
	//     per-pass adjust-liveness check below would fire spuriously
	//     even though the steady-state path is structurally correct.
	//     The status payload deliberately reports peer n2 in
	//     `connection:StandAlone` to mirror what the kernel would
	//     actually expose mid-operator-disconnect — a future
	//     reconciler that grew StandAlone-aware behaviour would
	//     trip the forbidden-verb scan above.
	for pass := steadyStartPass; pass <= passCount; pass++ {
		fx.Reset()
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-opdisc30s_00000",
			storage.FakeResponse{Stdout: []byte("pvc-opdisc30s_00000\n")})
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-opdisc30s_00000",
			storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-opdisc30s_00000|1048576\n")})
		fx.Expect("drbdsetup status pvc-opdisc30s",
			storage.FakeResponse{Stdout: []byte(
				"pvc-opdisc30s role:Secondary\n" +
					"  volume:0 disk:UpToDate\n" +
					"  n2 connection:StandAlone\n")})

		if _, err := rec.Apply(t.Context(), dr); err != nil {
			t.Fatalf("Apply pass %d (steady-state): %v", pass, err)
		}

		transcript = append(transcript, passCommands{pass: pass, lines: append([]string{}, fx.CommandLines()...)})
	}

	// Forbidden-verb scan: zero connect/disconnect commands of any
	// shape across the full 10-pass window. Each substring is a
	// distinct verb form a future regression might reach for — the
	// list is deliberately broader than 5.29's two-entry set.
	forbidden := []string{
		"drbdadm connect",
		"drbdadm disconnect",
		"drbdsetup connect",
		"drbdsetup disconnect",
		// Bring-up short-circuit: `drbdadm up` is allowed (it's how
		// the resource enters the kernel on pass 1), but the explicit
		// `drbdsetup new-peer ... --connect` shape some patches inline
		// to force a handshake is not.
		"new-peer --connect",
		// Equivalent shape with the flag in a different position.
		"--connect new-peer",
	}

	for _, entry := range transcript {
		for _, line := range entry.lines {
			for _, bad := range forbidden {
				if strings.Contains(line, bad) {
					t.Errorf("scenario 5.W14: pass %d issued forbidden %q during operator-disconnect window: %s",
						entry.pass, bad, line)
				}
			}
		}
	}

	// Per-pass liveness check: every steady-state pass MUST fire
	// `drbdadm adjust`. Adjust is the reconciler's sole convergence
	// verb and is a no-op for connection state on a StandAlone peer
	// in DRBD 9 — so it stays safe to fire while keeping the rest of
	// the reconcile loop honest. A regression that silently skipped
	// adjust on observed StandAlone peers would fail here even though
	// 5.29's coarser "at least one adjust across all passes" check
	// would still pass.
	for _, entry := range transcript {
		if entry.pass < steadyStartPass {
			continue
		}

		sawAdjust := false

		for _, line := range entry.lines {
			if strings.Contains(line, "drbdadm adjust pvc-opdisc30s") {
				sawAdjust = true

				break
			}
		}

		if !sawAdjust {
			t.Errorf("scenario 5.W14: pass %d (steady-state) did not fire `drbdadm adjust` — reconciler appears to have skipped this pass; got %v",
				entry.pass, entry.lines)
		}
	}

	// Final guard against the lazy-pass regression: at least one
	// `drbdadm adjust` must appear in the LAST pass specifically.
	// This catches the failure mode where a future change defers
	// adjust on the first few StandAlone observations and never
	// re-runs it — a pattern that could read as "fine, eventually
	// consistent" in code review but in fact silently drops the
	// reconciler's convergence contract for the rest of the window.
	lastPass := transcript[len(transcript)-1]
	if lastPass.pass != passCount {
		t.Fatalf("transcript bookkeeping error: last pass = %d, want %d", lastPass.pass, passCount)
	}

	sawAdjustLast := false

	for _, line := range lastPass.lines {
		if strings.Contains(line, "drbdadm adjust pvc-opdisc30s") {
			sawAdjustLast = true

			break
		}
	}

	if !sawAdjustLast {
		t.Errorf("scenario 5.W14: final pass (%d) did not fire `drbdadm adjust` — reconciler stopped converging mid-window; got %v",
			passCount, lastPass.lines)
	}
}

// TestApplyRendersExternalMetaDiskPath: scenario 6.18 spec — when the
// dispatcher stamps DesiredVolume.MetaPool, the .res file the
// reconciler writes carries the matching `meta-disk <path>;` line
// for the local diskful host. Path shape follows the existing
// LVM/ZFS `/dev/<pool>/<rd>_<vol5digits>_meta` convention so a
// follow-up provisioner can drop the LV in place without further
// renaming.
//
// This test ONLY asserts the .res render path is wired up; the
// matching satellite-side provisioning of the meta volume is
// covered by TestApplyProvisionsBothDataAndMeta below (currently
// t.Skip — see that test's comment).
func TestApplyRendersExternalMetaDiskPath(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-meta-0_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-meta-0",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{
					VolumeNumber: 0,
					SizeKib:      1024 * 1024,
					StoragePool:  "thin1",
					// External-metadata routing — the dispatcher
					// would stamp this from a Resource/RD prop
					// `StorPoolNameDrbdMeta=ssd-meta`.
					MetaPool: "ssd-meta",
				},
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
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "pvc-meta-0.res"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	got := string(body)

	// Local diskful host gets the external path verbatim. The path
	// shape (`/dev/<metaPool>/<rd>_<vol5digits>_meta`) is the same
	// /dev/<pool>/<lv> shape the LVM/ZFS providers use for the data
	// volume — keeps the satellite reconciler from having to thread
	// a second devices map.
	wantMeta := "meta-disk /dev/ssd-meta/pvc-meta-0_00000_meta;"
	if !strings.Contains(got, wantMeta) {
		t.Errorf("missing %q in:\n%s", wantMeta, got)
	}

	// Peer host keeps `internal` — drbd never reads peer-side
	// meta-disk and pinning a path here would couple every peer's
	// .res to this satellite's local layout.
	if !strings.Contains(got, "on n2 {") {
		t.Fatalf("missing peer block in:\n%s", got)
	}

	// Expect exactly one `meta-disk internal;` (the peer's) and
	// one external `meta-disk /dev/...;` (the local).
	internal := strings.Count(got, "meta-disk internal;")
	if internal != 1 {
		t.Errorf("want exactly 1 'meta-disk internal;' (peer only); got %d in:\n%s", internal, got)
	}
}

// TestApplyRoutesMetaToSeparatePoolScenario5W05 pins scenario 5.W05
// (wave2-05 external metadata pool, cross-listed with wave1 6.18): the
// `StorPoolNameDrbdMeta=<otherpool>` Resource-level prop must make the
// satellite emit the meta-disk on a pool DIFFERENT from the data pool.
// This is the load-bearing invariant of W05 — without it, the prop
// would still resolve to "internal" or to the same pool as data,
// negating the I/O-isolation purpose UG9 §"Using external DRBD
// metadata" calls out (small random meta-disk writes shouldn't share
// the data pool's spindle/SSD wear pattern).
//
// The dispatcher already resolves the prop into DesiredVolume.MetaPool
// (see TestExternalMetadataRouting in pkg/dispatcher); this test pins
// the satellite-side rendering: given a DesiredResource where
// StoragePool=<data> and MetaPool=<meta>, the .res file MUST carry
//
//   - `disk /dev/<data-pool>/<rd>_<vol5digits>;` and
//   - `meta-disk /dev/<meta-pool>/<rd>_<vol5digits>_meta;`
//
// against the local diskful host, with `<data-pool> != <meta-pool>`.
// Peer hosts keep `meta-disk internal;` (DRBD never reads peer-side
// metadata; pinning a path here would couple every peer .res to this
// satellite's local layout).
func TestApplyRoutesMetaToSeparatePoolScenario5W05(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// Only the data pool is queried for existing LVs; the meta carve
	// is the open follow-up (see TestApplyProvisionsBothDataAndMeta).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name data-vg/pvc-5w05-0_00000",
		storage.FakeResponse{Stdout: []byte("")})

	data := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "data-vg", ThinPool: "data-tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"data-thin": data},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	const (
		dataPool = "data-thin"
		metaPool = "nvme-meta"
	)

	if dataPool == metaPool {
		t.Fatalf("test setup: data and meta pools must differ to exercise 5.W05")
	}

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-5w05-0",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{
					VolumeNumber: 0,
					SizeKib:      1024 * 1024,
					// The dispatcher would have routed these via the
					// `StorPoolName=data-thin` data-pool selection +
					// `StorPoolNameDrbdMeta=nvme-meta` Resource-level
					// prop (most-specific scope wins — see
					// dispatcher.resolveMetaPool, exercised by
					// TestExternalMetadataRouting `resource-overrides-rd`).
					StoragePool: dataPool,
					MetaPool:    metaPool,
				},
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
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "pvc-5w05-0.res"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	got := string(body)

	// Local diskful host: data path on the data pool, meta path on
	// the meta pool. Two different `/dev/<pool>/...` prefixes — the
	// W05 invariant.
	wantData := "disk /dev/data-thin/pvc-5w05-0_00000;"
	if !strings.Contains(got, wantData) {
		t.Errorf("missing data line %q in:\n%s", wantData, got)
	}

	wantMeta := "meta-disk /dev/nvme-meta/pvc-5w05-0_00000_meta;"
	if !strings.Contains(got, wantMeta) {
		t.Errorf("missing meta line %q in:\n%s", wantMeta, got)
	}

	// Anti-regression: the meta line MUST NOT collide with the data
	// pool path. A prior dispatcher bug (effectiveProps strip-through)
	// could quietly fall back to data-pool routing; this check pins
	// the separation.
	collision := "meta-disk /dev/data-thin/"
	if strings.Contains(got, collision) {
		t.Errorf("meta-disk landed on data pool (%q); W05 isolation broken:\n%s",
			collision, got)
	}

	// Peer host n2 keeps `meta-disk internal;` — DRBD never reads
	// peer-side meta-disk, and pinning a path here would couple every
	// peer .res to this satellite's local pool naming.
	if !strings.Contains(got, "on n2 {") {
		t.Fatalf("missing peer block in:\n%s", got)
	}

	internal := strings.Count(got, "meta-disk internal;")
	if internal != 1 {
		t.Errorf("want exactly 1 'meta-disk internal;' (peer only); got %d in:\n%s",
			internal, got)
	}

	// Exactly one external meta-disk line — the local diskful host's.
	// More would indicate the renderer mistakenly stamped the path on
	// peers; fewer would mean the dispatcher's MetaPool stamp got
	// dropped on the floor.
	external := strings.Count(got, "meta-disk /dev/nvme-meta/")
	if external != 1 {
		t.Errorf("want exactly 1 external 'meta-disk /dev/nvme-meta/' line; got %d in:\n%s",
			external, got)
	}
}

// TestApplyProvisionsBothDataAndMeta: scenario 6.18 satellite-side
// follow-up. When DesiredVolume.MetaPool is set the reconciler must
// carve TWO backing volumes — `<rd>_<vol5digits>` on the data pool
// and `<rd>_<vol5digits>_meta` on the meta pool — before drbdadm
// create-md runs against the assembled .res.
//
// Currently t.Skip'd: the data + meta carve requires either:
//
//  1. a new Provider method (CreateMetaVolume) so the satellite can
//     issue the sibling create with a `_meta` LV-name suffix without
//     bending storage.Volume's naming contract; or
//  2. a Volume.NameSuffix-style field that all providers honour.
//
// Both are non-trivial: each provider (LVM thin, LVM thick, ZFS,
// FILE_THIN) carries its own volumeLVName / volumeFSPath helper, so
// the meta-suffix has to thread through every backend. Tracked
// against the same 6.18 backlog entry as `MetaPool` itself.
func TestApplyProvisionsBothDataAndMeta(t *testing.T) {
	t.Skip("6.18 satellite-side wiring follow-up — see test godoc for the open Provider-API design point")
}

// TestReconcilerPassesSkipDiskFlag (scenario 5.11) pins the
// SkipDisk gate: when the observer (or an operator) stamps
// `DrbdOptions/SkipDisk=True` onto Resource.Spec.Props, the
// reconciler MUST invoke `drbdadm adjust --skip-disk <rsc>` rather
// than the bare `drbdadm adjust <rsc>`. Without the flag, drbdadm
// would re-attempt disk attachment on a Failed/Diskless replica
// and bail; with it, only network/peer state reconciles (UG9
// §4428-4460 + upstream's DrbdAdm.adjust skipDisk branch at
// satellite/.../DrbdAdm.java:124).
//
// Two shapes exercised to match the prop's lifecycle:
//
//  1. Prop on the wire-side `Props` map. Mirrors the observer's
//     SSA write onto Resource.Spec.Props verbatim — when the
//     dispatcher hasn't yet split DrbdOptions/... keys out of the
//     props bag, the reconciler must still see the gate.
//  2. Prop folded into DrbdOptions by the dispatcher. The
//     production path runs every `DrbdOptions/...` key through
//     `mergeEffectiveProps` which moves it from Props → DrbdOptions
//     before Apply sees the DesiredResource; the gate must survive
//     that hop.
//
// Negative-control case: no prop, plain `drbdadm adjust` lands —
// guards against the regression where someone always appends the
// flag regardless of state.
//
// Case-insensitive matches "True"/"true"/"TRUE" because upstream
// reads VAL_TRUE via `equalsIgnoreCase` (DrbdRscData:584) so an
// operator who sets the prop via `r sp` with lower-case `true`
// gets the same effect.
func TestReconcilerPassesSkipDiskFlag(t *testing.T) {
	cases := []struct {
		name        string
		props       map[string]string
		drbdOpts    map[string]string
		wantCommand string
	}{
		{
			name:        "no SkipDisk prop -> bare adjust",
			wantCommand: "drbdadm adjust pvc-skipdisk",
		},
		{
			name:        "SkipDisk in Props -> adjust --skip-disk",
			props:       map[string]string{"DrbdOptions/SkipDisk": "True"},
			wantCommand: "drbdadm adjust --skip-disk pvc-skipdisk",
		},
		{
			name:        "SkipDisk in DrbdOptions (dispatcher landing) -> adjust --skip-disk",
			drbdOpts:    map[string]string{"DrbdOptions/SkipDisk": "True"},
			wantCommand: "drbdadm adjust --skip-disk pvc-skipdisk",
		},
		{
			name:        "SkipDisk lowercase 'true' -> adjust --skip-disk (case-insensitive)",
			props:       map[string]string{"DrbdOptions/SkipDisk": "true"},
			wantCommand: "drbdadm adjust --skip-disk pvc-skipdisk",
		},
		{
			name:        "SkipDisk empty value -> bare adjust (operator unset path)",
			props:       map[string]string{"DrbdOptions/SkipDisk": ""},
			wantCommand: "drbdadm adjust pvc-skipdisk",
		},
		{
			name:        "SkipDisk 'False' -> bare adjust",
			props:       map[string]string{"DrbdOptions/SkipDisk": "False"},
			wantCommand: "drbdadm adjust pvc-skipdisk",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-skipdisk_00000",
				storage.FakeResponse{Stdout: []byte("")})

			thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{"thin1": thin},
				Adm:       drbd.NewAdm(fx),
				StateDir:  dir,
				NodeName:  "n1",
			})

			drbdOpts := map[string]string{
				"port":    "7000",
				"node-id": "0",
				"address": "10.0.0.1",
				"minor":   "1000",
			}

			for k, v := range tc.drbdOpts {
				drbdOpts[k] = v
			}

			_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
				{
					Name:     "pvc-skipdisk",
					NodeName: "n1",
					Props:    tc.props,
					Volumes: []*intent.DesiredVolume{
						{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
					},
					DrbdOptions: drbdOpts,
				},
			})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			cmds := fx.CommandLines()

			if !slices.Contains(cmds, tc.wantCommand) {
				t.Errorf("expected command %q; got %v", tc.wantCommand, cmds)
			}

			// Cross-guard: when --skip-disk is expected, the bare
			// `drbdadm adjust pvc-skipdisk` MUST NOT also appear —
			// otherwise we'd be re-attempting disk attachment alongside
			// the skip-disk pass, which defeats the whole point of the
			// gate. (And the reverse: bare adjust must not coexist with
			// the flagged form.)
			forbidden := "drbdadm adjust pvc-skipdisk"
			if tc.wantCommand == "drbdadm adjust --skip-disk pvc-skipdisk" {
				for _, line := range cmds {
					if line == forbidden {
						t.Errorf("got both bare adjust and --skip-disk: %v", cmds)
					}
				}
			} else {
				skipDiskCmd := "drbdadm adjust --skip-disk pvc-skipdisk"
				for _, line := range cmds {
					if line == skipDiskCmd {
						t.Errorf("unexpected --skip-disk without prop set: %v", cmds)
					}
				}
			}
		})
	}
}

// TestApplyDropsPeerWhenRemovedFromDesired pins Bug 67. Reproducer on
// dev-kvaps: a 3-replica RD where worker-2 was diskful and worker-3 a
// TieBreaker. After `linstor r d worker-2 test`, ensureTiebreaker
// correctly retired worker-3, but the surviving worker-1 replica kept
// rendering the old .res — including stale `on worker-2 {}` /
// `on worker-3 {}` blocks — so `linstor r l` showed
// `Conns=Connecting(worker-2, worker-3)` to ghosts indefinitely.
//
// Contract this test pins on the satellite side:
//   - A subsequent Apply call with a SHRUNK Peers slice MUST re-render
//     .res without the dropped peer's `on <peer> {}` block.
//   - The dropped peer's connection-level `connection { host A; host B; }`
//     entry MUST also disappear (otherwise drbdadm parses the file but
//     leaves the peer-slot reservation in place).
//   - `drbdadm adjust` MUST run on the second Apply pass so the kernel
//     calls drbdsetup del-peer and frees the slot — adjust on its own
//     diffs the loaded resource against the .res and emits del-peer
//     when a host block is gone.
//
// The fake-exec assertions cover both halves: the file content (no
// dropped-peer string) AND the command line (adjust invoked at least
// once after the second Apply, post-shrink).
//
// If this test FAILS, the .res file still contains the dropped peer
// or drbdadm adjust isn't being called on a Peers-only delta — which
// is exactly the Bug 67 symptom and the operator's `Connecting(...)`
// ghost on the live cluster.
func TestApplyDropsPeerWhenRemovedFromDesired(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// LVM probe + size readback are issued on every Apply; allow both
	// passes by registering the absent-volume response (empty stdout)
	// and the size-table response — FakeExec returns them per call.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-67_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Pass 1: full 3-peer topology (this node + n2 + n3). The .res
	// reflects all three `on <node> {}` blocks; this is the
	// pre-`linstor r d` state.
	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-67",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2", "n3"},
			DrbdOptions: map[string]string{
				"port":            "7000",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1000",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7000",
				"peer.n3.address": "10.0.0.3",
				"peer.n3.node-id": "2",
				"peer.n3.port":    "7000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply pass 1: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-67.res")

	pass1Body, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile pass 1: %v", err)
	}

	// Sanity-check the pre-state: both peer blocks present.
	if !strings.Contains(string(pass1Body), "on n2 {") {
		t.Fatalf("pass 1 .res missing `on n2 {`; got:\n%s", pass1Body)
	}

	if !strings.Contains(string(pass1Body), "on n3 {") {
		t.Fatalf("pass 1 .res missing `on n3 {`; got:\n%s", pass1Body)
	}

	// Record the adjust call count from pass 1 so the pass 2 assertion
	// can prove a NEW adjust fired, not just the leftover from pass 1.
	pass1Cmds := append([]string{}, fx.CommandLines()...)

	// Pass 2: simulate `linstor r d n2 pvc-67` + tiebreaker retirement
	// — the dispatcher now pushes the same DesiredResource but with
	// Peers=[] (single-replica topology). Satellite MUST re-render
	// .res to drop both `on n2 {}` and `on n3 {}` blocks, and MUST
	// invoke `drbdadm adjust pvc-67` so the kernel runs del-peer for
	// the retired node-ids.
	_, err = rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-67",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: nil, // ← peer list collapsed to "this node only"
			DrbdOptions: map[string]string{
				"port":    "7000",
				"node-id": "0",
				"address": "10.0.0.1",
				"minor":   "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply pass 2: %v", err)
	}

	pass2Body, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile pass 2: %v", err)
	}

	got := string(pass2Body)

	// Hard asserts on .res content. The dropped peers must NOT appear
	// in any form — block header, connection wire, or stale address.
	for _, banned := range []string{
		"on n2 {",
		"on n3 {",
		"10.0.0.2",
		"10.0.0.3",
	} {
		if strings.Contains(got, banned) {
			t.Errorf("pass 2 .res still contains dropped-peer marker %q (Bug 67); got:\n%s", banned, got)
		}
	}

	// Either `drbdadm adjust pvc-67` (which internally calls
	// `drbdsetup del-peer` when a host block is gone) OR explicit
	// per-peer `drbdadm disconnect <peer>:pvc-67` + `drbdadm del-peer
	// <peer>:pvc-67` invocations are valid responses on the second
	// Apply pass. The satellite currently picks the targeted-del-peer
	// path (preferred — narrower scope than adjust); pin that any
	// teardown verb fires for every dropped peer.
	pass2Cmds := fx.CommandLines()

	for _, peer := range []string{"n2", "n3"} {
		want := []string{
			"drbdadm adjust pvc-67",
			"drbdadm del-peer " + peer + ":pvc-67",
			"drbdadm disconnect " + peer + ":pvc-67",
		}

		sawTeardown := false

		for i := len(pass1Cmds); i < len(pass2Cmds); i++ {
			if slices.Contains(want, pass2Cmds[i]) {
				sawTeardown = true

				break
			}
		}

		if !sawTeardown {
			t.Errorf("Bug 67: peer %q dropped from Desired but pass 2 emitted no teardown verb (adjust / disconnect / del-peer); cmds=%v",
				peer, pass2Cmds[len(pass1Cmds):])
		}
	}
}

// TestTemporarySecondaryFailureAutoRecovers (scenario 5.W15 / wave2-05,
// cross-listed with wave1 5.8 and 5.15) pins the auto-recovery
// invariant for a transient secondary-node failure:
//
//	Surviving Primary (this satellite, n1) records changes to the
//	dirty-bitmap while the Secondary peer (n2) is gone. When n2
//	powers back on its satellite re-renders the same .res, the
//	kernel re-handshakes from the bitmap, and DRBD walks the peer
//	through Outdated → Inconsistent → SyncTarget → UpToDate
//	without any operator action.
//
// The reconciler's job during this lifecycle is to STAY OUT OF THE
// WAY. Concretely, across the four observed peer states it MUST NOT:
//
//   - Issue `drbdadm disconnect` / `drbdadm connect` on n1 (would
//     pre-empt DRBD's own re-handshake and drop the bitmap-based
//     delta sync in favour of a full resync, or worse race the
//     handshake into split-brain).
//   - Issue `drbdadm down` / `drbdadm up` on n1 (re-reads metadata
//     and forces a fresh handshake from zero — defeats bitmap delta).
//   - Issue `drbdadm primary --force` on n1 (already Primary; would
//     bump current-uuid and trigger a full resync to the recovering
//     peer instead of bitmap delta).
//   - Re-run `drbdadm create-md` on n1 (wipes metadata — would
//     orphan the local replica from the cluster's GI history).
//   - Issue `drbdadm adjust` during the SyncTarget phase on n2
//     (Bug 8 / scenario 5.16: adjust mid-resync re-renders the
//     kernel's connection config, kernel drops in-flight bitmap
//     progress, resync restarts at 0% — the bitmap delta this whole
//     scenario relies on dies). The reconciler's kernel-state probe
//     must catch SyncTarget and defer adjust to the next pass.
//
// Cross-listed with the 5.W14 (`TestReconcilerRespectsOperatorDisconnect`)
// and 5.31 (`TestReconcilerDoesNotPropagateDiscardMyData`) patterns:
// same "reconciler must not fight DRBD's own recovery" property, just
// over a different failure shape (transient peer disappearance vs
// operator disconnect vs operator misuse).
//
// Lifecycle drive: four reconcile passes, one per documented peer
// state. .res + md-marker linger from pass 1, so passes 2–4 hit the
// firstActivation=false branch (which is where the kernel-state probe
// + adjust gate live).
//
// NOTE on the SyncTarget pass: the adjust-defer behaviour is itself
// tracked by TestApplyDefersAdjustDuringSyncTarget (currently
// t.Skip'd until Bug 8 is implemented). This test exercises the
// SyncTarget step ONLY to confirm the broader "no disruptive verbs"
// invariant — it does NOT pin the adjust-defer (that's 5.16's job).
// Once 5.16 lands, the SyncTarget pass here will exercise the gate
// end-to-end as a natural side effect.
func TestTemporarySecondaryFailureAutoRecovers(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// Pass 1: first activation — LV absent, lvcreate fires. Establishes
	// the steady-state files (.res + md-marker) so subsequent passes
	// hit firstActivation=false.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-tempsec_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// 2-replica RD. The observer (out-of-band, not modelled here)
	// reports peer n2's disk transitioning through Outdated →
	// Inconsistent → SyncTarget → UpToDate. The DesiredResource shape
	// the controller pushes the satellite NEVER changes through this
	// lifecycle — that's the property under test: peer recovery is
	// observable-state, not desired-state, so a steady Apply payload
	// must produce a steady-and-inert reconciler.
	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-tempsec",
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
		},
	}

	// Pass 1: first activation, bring-up. adjust MUST fire here —
	// that's how the resource enters the kernel in the first place,
	// and is the verb every other 5.W15-style test pins as the
	// positive control. create-md also fires on pass 1 (one-shot
	// metadata write) — this is fine and expected; the forbidden-verb
	// assertion below only spans the steady-state passes (2 through
	// 5) where the recovery lifecycle actually plays out.
	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply pass 1 (first activation): %v", err)
	}

	pass1Cmds := append([]string{}, fx.CommandLines()...)

	if !slices.Contains(pass1Cmds, "drbdadm adjust pvc-tempsec") {
		t.Fatalf("first-activation Apply must adjust to bring the resource up; got %v",
			pass1Cmds)
	}

	// Steady-state passes: walk through the four peer states DRBD
	// emits during a temporary-secondary recovery. Each pass restages
	// the storage probes (LV present, size readback) and the kernel
	// status probe with the matching wire-format snippet. The
	// reconciler's firstActivation=false branch consults
	// `drbdsetup status <rd>` via IsLoaded — staging a real status
	// block keeps it on the adjust path (kernel slot present), which
	// is the path 5.W15 lives on. An empty / missing status response
	// would push the reconciler onto the `drbdadm up` fallback (Bug
	// 47 / scenario 5.32) and bypass this test's invariant.
	cases := []struct {
		name     string
		status   string
		expectN1 string // n1's local disk
	}{
		{
			name: "n2 powered off (Outdated, Connecting)",
			// n2 is gone. Surviving Primary records changes to the
			// dirty bitmap and stays Primary/UpToDate; the connection
			// flaps through Connecting (or StandAlone, depending on
			// DRBD timing — Connecting is the canonical "peer
			// disappeared but I'm waiting for it to come back" shape).
			status: `pvc-tempsec role:Primary
  volume:0 disk:UpToDate
  n2 connection:Connecting role:Unknown
    volume:0 peer-disk:Outdated
`,
			expectN1: "UpToDate",
		},
		{
			name: "n2 back, pre-handshake (Inconsistent)",
			// n2's satellite booted, kernel module loaded, .res
			// re-rendered, drbdadm up ran on n2. The peer slot
			// reappears on the wire but the GI handshake hasn't yet
			// converged — n2's disk reads Inconsistent until DRBD
			// computes the bitmap diff and starts the resync.
			status: `pvc-tempsec role:Primary
  volume:0 disk:UpToDate
  n2 connection:Connected role:Secondary
    volume:0 peer-disk:Inconsistent
`,
			expectN1: "UpToDate",
		},
		{
			name: "n2 catching up (SyncTarget, mid-resync)",
			// Bitmap delta in flight. n1 is SyncSource, n2 is
			// SyncTarget. This is the danger pass: a `drbdadm
			// adjust` here re-renders kernel connection config and
			// drops the in-flight bitmap progress (Bug 8 / scenario
			// 5.16). The kernel-state probe in runBringUpOrAdjust
			// must catch SyncTarget and defer.
			status: `pvc-tempsec role:Primary
  volume:0 disk:UpToDate
  n2 connection:Connected role:Secondary
    volume:0 replication:SyncSource peer-disk:Inconsistent done:42.50
`,
			expectN1: "UpToDate",
		},
		{
			name: "n2 caught up (UpToDate, Established)",
			// Resync complete. DRBD has flipped n2 to UpToDate and
			// the connection is Established. The reconciler can
			// resume normal adjust passes from here — but ALL the
			// disruptive verbs are still forbidden because nothing
			// in the DesiredResource has changed.
			status: `pvc-tempsec role:Primary
  volume:0 disk:UpToDate
  n2 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`,
			expectN1: "UpToDate",
		},
	}

	// steadyCmds accumulates the FakeExec transcript across the four
	// steady-state passes. Kept separate from pass1Cmds so the
	// forbidden-verb checks below can exclude pass 1's expected
	// `drbdadm create-md` (one-shot metadata write — fine on first
	// activation, forbidden anywhere else).
	steadyCmds := []string{}

	for i, tc := range cases {
		fx.Reset()
		// Steady-state storage probes: LV already present (carve done
		// in pass 1) + size readback. These match every other
		// firstActivation=false test in this file.
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-tempsec_00000",
			storage.FakeResponse{Stdout: []byte("pvc-tempsec_00000\n")})
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-tempsec_00000",
			storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-tempsec_00000|1048576\n")})
		// Kernel-state probe: stage the per-state drbdsetup status
		// block. IsLoaded reads it as "loaded" (exit zero + non-empty
		// stdout), keeping the reconciler on the adjust path.
		fx.Expect("drbdsetup status pvc-tempsec",
			storage.FakeResponse{Stdout: []byte(tc.status)})

		if _, err := rec.Apply(t.Context(), dr); err != nil {
			t.Fatalf("Apply pass %d (%s): %v", i+2, tc.name, err)
		}

		steadyCmds = append(steadyCmds, fx.CommandLines()...)
	}

	// Core assertion: across the FOUR steady-state passes (peer
	// Outdated → Inconsistent → SyncTarget → UpToDate), the
	// reconciler must have issued ZERO disruptive verbs on the local
	// replica. DRBD's own re-handshake + bitmap-based delta sync is
	// doing the recovery — the reconciler must not race it.
	//
	// Pass 1 (first activation) is excluded: `drbdadm create-md` is
	// the one-shot metadata write that brings the resource up the
	// very first time, and `drbdadm adjust` is the bring-up verb.
	// Neither is allowed on any subsequent pass.
	forbidden := []string{
		"drbdadm disconnect pvc-tempsec",
		"drbdadm connect pvc-tempsec",
		"drbdadm down pvc-tempsec",
		"drbdadm up pvc-tempsec",
		"drbdadm primary --force pvc-tempsec",
		"drbdadm create-md",
		"drbdadm invalidate",
		"drbdadm invalidate-remote",
		"drbdadm del-peer",
	}

	for _, bad := range forbidden {
		for _, line := range steadyCmds {
			if strings.Contains(line, bad) {
				t.Errorf("scenario 5.W15: reconciler issued forbidden %q during temp-secondary auto-recovery; "+
					"DRBD must drive the bitmap delta unaided.\nline: %s\nsteady-state calls: %v",
					bad, line, steadyCmds)
			}
		}
	}

	// Positive control 1: pass 1 (first activation) MUST have run
	// `drbdadm adjust pvc-tempsec`. Already asserted above; restate
	// here so a refactor that loses the pass-1 adjust surfaces in
	// the same test that pins the lifecycle.
	if !slices.Contains(pass1Cmds, "drbdadm adjust pvc-tempsec") {
		t.Errorf("expected drbdadm adjust on first activation; got %v", pass1Cmds)
	}

	// Positive control 2: the .res file MUST be present and stable —
	// the reconciler keeps rewriting it on every pass (cheap, durable
	// record of desired state), but its content must not flap.
	resPath := filepath.Join(dir, "pvc-tempsec.res")
	body, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile .res: %v", err)
	}

	// Peer block must still be present in every pass — losing it
	// would teach DRBD the peer is gone-for-good and trigger
	// del-peer on the next adjust, defeating the bitmap delta.
	if !strings.Contains(string(body), "on n2 {") {
		t.Errorf("scenario 5.W15: .res must keep `on n2 {` across the recovery; got:\n%s", body)
	}
}

// TestApplyAutoMkfsXfsFiresOnceOnFirstActivation pins scenario 9.W14
// (P1, unit) — RG `FileSystem/Type=xfs` inherited via effective props
// onto a spawned RD with `auto-primary=true`: on the first reconcile
// the satellite must (a) promote the replica via `drbdadm primary
// --force`, (b) run `mkfs.xfs /dev/drbd<minor>` on every diskful
// volume so the consumer sees a usable filesystem, then (c) demote
// via `drbdadm secondary`. Idempotency is keyed off a per-RD marker
// file under StateDir (`<rd>.mkfs.done`): the SECOND Apply pass
// (firstActivation=false because both `.md-created` and `.mkfs.done`
// persist) must NOT re-run mkfs — repeating it would silently wipe a
// populated filesystem and is unrecoverable from the operator's side.
//
// Cross-listed with the wave1 4.x lifecycle: a toggle disk / migrate
// shouldn't ever cross the mkfs gate, hence the marker-file probe and
// the second-pass assertion that no `mkfs.*` line appears.
func TestApplyAutoMkfsXfsFiresOnceOnFirstActivation(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-mkfs_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-mkfs",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Props: map[string]string{
				"FileSystem/Type": "xfs",
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
				"auto-primary": "true",
			},
		},
	}

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (1st): %v", err)
	}

	first := fx.CommandLines()

	count := func(lines []string, needle string) int {
		n := 0
		for _, line := range lines {
			if strings.Contains(line, needle) {
				n++
			}
		}

		return n
	}

	if count(first, "mkfs.xfs /dev/drbd1000") != 1 {
		t.Errorf("first Apply must run mkfs.xfs on /dev/drbd1000 exactly once; got %v", first)
	}

	// mkfs must land BETWEEN primary --force and secondary so the
	// kernel actually accepts the write (Secondary is read-only).
	posPrim, posMkfs, posSec := -1, -1, -1

	for i, line := range first {
		switch {
		case posPrim < 0 && strings.Contains(line, "drbdadm primary --force pvc-mkfs"):
			posPrim = i
		case posMkfs < 0 && strings.Contains(line, "mkfs.xfs /dev/drbd1000"):
			posMkfs = i
		case posSec < 0 && strings.Contains(line, "drbdadm secondary pvc-mkfs"):
			posSec = i
		}
	}

	if posPrim < 0 || posMkfs <= posPrim || posSec <= posMkfs {
		t.Errorf("ordering: want primary --force < mkfs.xfs < secondary; got prim=%d mkfs=%d sec=%d in %v",
			posPrim, posMkfs, posSec, first)
	}

	markerPath := filepath.Join(dir, "pvc-mkfs.mkfs.done")
	if _, statErr := os.Stat(markerPath); statErr != nil {
		t.Errorf("mkfs.done marker: want present after first apply, got stat err %v", statErr)
	}

	// Second pass: marker persists → mkfs MUST NOT re-run. Wiping a
	// populated filesystem on every reconcile is a data-loss bug.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-mkfs_00000",
		storage.FakeResponse{Stdout: []byte("pvc-mkfs_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-mkfs_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-mkfs_00000|1048576\n")})

	_, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (2nd): %v", err)
	}

	if got := count(fx.CommandLines(), "mkfs."); got != 0 {
		t.Errorf("idempotent reconcile must NOT re-run mkfs; got %d mkfs.* call(s) in %v",
			got, fx.CommandLines())
	}
}

// TestApplyAutoMkfsHonoursMkfsParams pins the optional `FileSystem/MkfsParams`
// path of scenario 9.W14: extra mkfs flags set on the RG (e.g.
// `-K -L data`) must be forwarded verbatim BEFORE the device path on
// the mkfs command line. Operators rely on this to disable lazy
// discard / set labels at create time; silently dropping the params
// would re-introduce defaults the operator explicitly overrode.
func TestApplyAutoMkfsHonoursMkfsParams(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-mkfsparams_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-mkfsparams",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Props: map[string]string{
				"FileSystem/Type":       "xfs",
				"FileSystem/MkfsParams": "-K -L data",
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "2000",
				"auto-primary": "true",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	want := "mkfs.xfs -K -L data /dev/drbd2000"

	found := false

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, want) {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("expected mkfs line %q; got %v", want, fx.CommandLines())
	}
}

// TestApplyAutoMkfsSkipsWithoutFileSystemType pins the "no
// FileSystem/Type prop → no mkfs" half of scenario 9.W14: a vanilla
// auto-primary seed without the RG-inherited FS prop must not touch
// mkfs at all. Otherwise we'd format every freshly-seeded replica
// with a default filesystem the consumer never asked for, breaking
// raw-block PVCs (the LINSTOR-CSI default).
func TestApplyAutoMkfsSkipsWithoutFileSystemType(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-nomkfs_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-nomkfs",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "3000",
				"auto-primary": "true",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "mkfs.") || strings.Contains(line, " mkfs.") {
			t.Errorf("no FileSystem/Type prop → no mkfs expected; saw %q in %v", line, fx.CommandLines())
		}
	}

	markerPath := filepath.Join(dir, "pvc-nomkfs.mkfs.done")
	if _, statErr := os.Stat(markerPath); statErr == nil {
		t.Errorf("mkfs.done marker: want absent when no FS configured, got present at %s", markerPath)
	}
}

// TestApplyAutoMkfsSkipsDisklessReplica pins scenario 9.W14's
// DISKLESS guard: a DISKLESS replica has no lower disk to format
// (DRBD reads/writes via the network from a peer), so the satellite
// must NOT issue `mkfs.<type>` on it even when the RG props carry
// FileSystem/Type. mkfs on a Diskless DRBD device would either
// deadlock waiting for the network round-trip or — worse — succeed
// and silently overwrite the Primary peer's data.
func TestApplyAutoMkfsSkipsDisklessReplica(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-diskless",
			NodeName: "n1",
			Flags:    []string{"DISKLESS"},
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Props: map[string]string{
				"FileSystem/Type": "xfs",
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "4000",
				"auto-primary": "true",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "mkfs.") || strings.Contains(line, " mkfs.") {
			t.Errorf("DISKLESS replica must skip mkfs; saw %q in %v", line, fx.CommandLines())
		}
	}
}

// TestApplyAutoMkfsSkipsWhenNotPrimary pins the "non-primary replica
// does not mkfs" guard of scenario 9.W14: the FS is written ONCE,
// on the primary replica only (DRBD then mirrors the bytes to peers
// via initial-sync). A peer that activates without `auto-primary=true`
// stays Secondary and must NOT issue mkfs — DRBD rejects writes from
// Secondary anyway, but a Secondary-side mkfs would still race the
// initial-sync stream and produce diverging history. Mirrors the
// scenario's "primary replica AFTER creation" phrasing.
func TestApplyAutoMkfsSkipsWhenNotPrimary(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-peer_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  dir,
		NodeName:  "n2",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-peer",
			NodeName: "n2",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Props: map[string]string{
				"FileSystem/Type": "xfs",
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "1", "address": "10.0.0.2", "minor": "5000",
				// auto-primary NOT set → this replica must stay Secondary
				// and skip mkfs.
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "mkfs.") || strings.Contains(line, " mkfs.") {
			t.Errorf("non-primary replica must skip mkfs; saw %q in %v", line, fx.CommandLines())
		}
	}
}

// TestApplyResFileUsesKernelHostnameNotLINSTORName pins scenario 4.W02
// (wave2-04-lifecycle.md): when the LINSTOR node name decouples from
// the host's kernel hostname (UG9 §"Naming LINSTOR nodes" — operators
// may register a satellite under any identifier while the OS-level
// `uname -n` stays at the cloud-init / DNS default), the rendered
// `.res` file MUST use the KERNEL hostname in the `on <host> { ... }`
// block, NOT the LINSTOR name.
//
// Contract:
//
//   - `cfg.NodeName` on the satellite is the kernel hostname, sourced
//     from $NODE_NAME (DaemonSet downward API: `spec.nodeName` ==
//     kubelet hostname == kernel `uname -n`). Stays constant for the
//     life of the pod.
//   - `DesiredResource.NodeName` is the LINSTOR identifier the
//     controller filtered Resource CRDs against; arbitrary, set by
//     the operator via `linstor node create <name>`.
//   - When the two differ, `buildResFile` MUST emit
//     `on <kernel-hostname> {` (matches what drbd-9 / drbdadm see on
//     this host) — `drbdadm adjust` resolves the local `on { }` block
//     by matching against the kernel hostname, NOT the controller's
//     symbolic name.
//
// The REST half of the same scenario (handler accepting the
// mismatch, Props["NodeUname"] preserved verbatim) lives in
// pkg/rest/nodes_test.go::TestNodeCreateArbitraryNameVsKernelHostname.
func TestApplyResFileUsesKernelHostnameNotLINSTORName(t *testing.T) {
	const (
		// LINSTOR-side identifier the operator typed into
		// `linstor node create <name>` — symbolic, controller-only.
		linstorName = "worker-fra-az1"
		// Kernel `uname -n` value — what drbdadm sees on the host.
		// Notably different shape from linstorName so a regression
		// that conflated the two surfaces an obvious mismatch.
		kernelHostname = "ip-10-0-0-7.eu-central-1.compute.internal"
		peerLinstor    = "worker-fra-az2"
	)

	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		// cfg.NodeName == kernel hostname (downward-API contract).
		// LINSTOR name lives ONLY in the DesiredResource below.
		NodeName: kernelHostname,
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name: "pvc-1",
			// LINSTOR-side identifier — what the controller selected
			// this satellite by. Intentionally != kernelHostname.
			NodeName: linstorName,
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{peerLinstor},
			DrbdOptions: map[string]string{
				"port":    "7000",
				"node-id": "0",
				"address": "10.0.0.7",
				"minor":   "1000",
				// Peer wire payload also uses LINSTOR-side names —
				// they're symbolic on the peer's host too. The peer's
				// `on { }` block is rendered on the PEER satellite
				// with its own kernel hostname; here we only assert
				// the LOCAL `on { }` line.
				"peer." + peerLinstor + ".address": "10.0.0.8",
				"peer." + peerLinstor + ".node-id": "1",
				"peer." + peerLinstor + ".port":    "7000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-1.res")

	body, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	got := string(body)

	// Positive: kernel hostname appears as the local `on { }` host.
	wantOn := "on " + kernelHostname + " {"
	if !strings.Contains(got, wantOn) {
		t.Errorf("missing local %q in:\n%s", wantOn, got)
	}

	// Negative: LINSTOR name MUST NOT leak into a local `on { }`
	// header. (Matching `on <name> {` exactly rather than the bare
	// name avoids false positives — `linstorName` could conceivably
	// appear in a comment header or a connection-block path.)
	badOn := "on " + linstorName + " {"
	if strings.Contains(got, badOn) {
		t.Errorf("LINSTOR name leaked into local `on { }` header (%q); .res must use kernel hostname:\n%s",
			badOn, got)
	}
}
