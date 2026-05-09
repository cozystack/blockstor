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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
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
