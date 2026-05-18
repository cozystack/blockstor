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
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Bug 315 (P0-5): the satellite's `.res` writer must be
// content-idempotent — when buildResFile produces a body that
// matches what's already on disk, the file must NOT be rewritten.
// Rewriting on every reconcile churns mtime and feeds spurious
// "config changed" signal to downstream observers (drbdadm
// config-file watchers, debugging diffs) even when nothing
// material changed.

// TestApplyDRBDSkipsWriteWhenResUnchanged: two back-to-back Applies
// with identical DesiredResources must leave the .res file's mtime
// untouched on the second pass. The first Apply creates the file,
// the second must skip the os.WriteFile because the rendered body
// bytewise-equals the on-disk content.
func TestApplyDRBDSkipsWriteWhenResUnchanged(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// LV already exists for both passes — so buildResFile resolves
	// the same /dev/vg/... disk path each time and the rendered
	// body is bytewise identical across reconciles.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-idem_00000",
		storage.FakeResponse{Stdout: []byte("pvc-idem_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-idem_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-idem_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-idem",
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
		t.Fatalf("Apply (1st): %v", err)
	}

	resPath := filepath.Join(dir, "pvc-idem.res")

	info1, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("Stat (1st): %v", err)
	}

	body1, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile (1st): %v", err)
	}

	// Sleep past coarse filesystem mtime granularity (HFS+/ext4
	// truncate to 1s) so a rewrite would be observable.
	time.Sleep(1100 * time.Millisecond)

	// Second Apply: identical DesiredResource → identical body →
	// the writer must short-circuit on bytes.Equal.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-idem_00000",
		storage.FakeResponse{Stdout: []byte("pvc-idem_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-idem_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-idem_00000|1048576\n")})

	_, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (2nd): %v", err)
	}

	info2, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("Stat (2nd): %v", err)
	}

	body2, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile (2nd): %v", err)
	}

	if string(body1) != string(body2) {
		t.Fatalf("body changed unexpectedly:\nbefore: %s\nafter: %s", body1, body2)
	}

	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Errorf("mtime advanced on identical-content reconcile: before=%v after=%v — writer is not content-idempotent",
			info1.ModTime(), info2.ModTime())
	}
}

// TestApplyDRBDWritesWhenContentDiffers: bookend the idempotency
// gate — when the DesiredResource changes (here we flip a peer
// address) the rendered body differs, and the writer MUST rewrite
// the .res file. mtime advances and the new body is on disk.
func TestApplyDRBDWritesWhenContentDiffers(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-diff_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr1 := []*intent.DesiredResource{
		{
			Name:     "pvc-diff",
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

	_, err := rec.Apply(t.Context(), dr1)
	if err != nil {
		t.Fatalf("Apply (1st): %v", err)
	}

	resPath := filepath.Join(dir, "pvc-diff.res")

	info1, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("Stat (1st): %v", err)
	}

	body1, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile (1st): %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	// Second Apply with a different peer address — body must change.
	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-diff_00000",
		storage.FakeResponse{Stdout: []byte("pvc-diff_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-diff_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-diff_00000|1048576\n")})

	dr2 := []*intent.DesiredResource{
		{
			Name:     "pvc-diff",
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
				"peer.n2.address": "10.0.0.99", // changed
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7000",
			},
		},
	}

	_, err = rec.Apply(t.Context(), dr2)
	if err != nil {
		t.Fatalf("Apply (2nd): %v", err)
	}

	info2, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("Stat (2nd): %v", err)
	}

	body2, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile (2nd): %v", err)
	}

	if string(body1) == string(body2) {
		t.Fatalf("body unchanged despite DesiredResource diff:\n%s", body1)
	}

	if !info2.ModTime().After(info1.ModTime()) {
		t.Errorf("mtime did not advance despite content diff: before=%v after=%v",
			info1.ModTime(), info2.ModTime())
	}
}
