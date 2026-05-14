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
			},
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
	wantSetGi := "drbdmeta --force pvc-seed/0 v09 /dev/vg/pvc-seed_00000 internal set-gi 78A0DDDABCDEF000:78A0DDDABCDEF000:0:0"
	if !slices.Contains(calls, wantSetGi) {
		t.Errorf("missing exact set-gi command %q in calls: %v", wantSetGi, calls)
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
					},
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
			wantSetGi := fmt.Sprintf("drbdmeta --force pvc-zskip/0 v09 %s internal set-gi %s:%s:0:0",
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
