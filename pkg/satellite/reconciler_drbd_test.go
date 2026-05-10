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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

var (
	errNotALUKSDevice    = errors.New("not a luks device")
	errLUKSOpenAlready   = errors.New("device pvc-luks-only-0-luks already exists")
	errDrbdadmAdjustFail = errors.New("drbdadm: simulated mid-Apply abort")
	errDrbdadmResizeFail = errors.New("drbdadm: resize failed (peer disconnected)")
)

// TestApplyWritesResFile: Apply leaves a /etc/drbd.d/<name>.res file
// (here under StateDir) reflecting the DesiredResource. The reconciler
// owns this file — controller never touches it directly.
func TestApplyWritesResFile(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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

// TestApplyInvokesDrbdadmAdjust: writing the .res isn't enough — DRBD
// needs `adjust` to pick up changes. Apply must call it.
func TestApplyInvokesDrbdadmAdjust(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-grow_00000",
		storage.FakeResponse{Stdout: []byte("pvc-grow_00000\n")})
	// VolumeStatus: 1 GiB on disk (1024*1024 KiB).
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-grow_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-grow_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Desired: 2 GiB.
	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-grow",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
		"lvextend --size 2048MiB vg/pvc-grow_00000",
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-new_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-new",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
		if strings.HasPrefix(line, "lvextend ") {
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-rwx_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-rwx",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-noeviction_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-noeviction",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-no-drbd_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-no-drbd",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-luks_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// VolumeStatus query → reports the LV at a known path so the LUKS
	// layer has a non-empty device to format/open.
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks_00000",
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

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-luks",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-luks-empty_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-luks-empty",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-luks-only_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-only_00000",
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

	dr := []*satellitepb.DesiredResource{
		{
			Name:     "pvc-luks-only",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-luks-only_00000",
		storage.FakeResponse{Stdout: []byte("pvc-luks-only_00000\n")})
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-only_00000",
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-stack_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-stack_00000",
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

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-stack",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-resize-fail_00000",
		storage.FakeResponse{Stdout: []byte("pvc-resize-fail_00000\n")})
	// VolumeStatus reports current size 1 GiB; desired is 2 GiB.
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-resize-fail_00000",
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

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-resize-fail",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-bad-pool",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-inactive",
			NodeName: "n1",
			Flags:    []string{"INACTIVE"},
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-no-cs_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
		// Cryptsetup intentionally nil.
	})

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-no-cs",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-luks-grow_00000",
		storage.FakeResponse{Stdout: []byte("pvc-luks-grow_00000\n")})
	// VolumeStatus reports current size (1 GiB) — desired is 2 GiB.
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-grow_00000",
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

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-luks-grow",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-seed_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := []*satellitepb.DesiredResource{
		{
			Name:     "pvc-seed",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-seed_00000",
		storage.FakeResponse{Stdout: []byte("pvc-seed_00000\n")})
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-seed_00000",
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-luks-del_00000",
		storage.FakeResponse{Stdout: []byte("pvc-luks-del_00000\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	resp, err := rec.DeleteResource(t.Context(), &satellitepb.DeleteResourceRequest{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-abort_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// VolumeStatus query for the .res builder.
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-abort_00000",
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

	dr := []*satellitepb.DesiredResource{
		{
			Name:     "pvc-abort",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-abort_00000",
		storage.FakeResponse{Stdout: []byte("pvc-abort_00000\n")})
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-abort_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-abort_00000|1048576\n")})
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

	fx.Expect("lvs --noheadings -o lv_name vg/pvc-luks-format_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-luks-format_00000",
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

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-luks-format",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
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
