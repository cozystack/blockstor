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

// TestApplyDRBDSkipsWriteWhenResUnchanged pins the P0-5 idempotent-
// .res-write contract: a second Apply with identical inputs MUST NOT
// touch the on-disk .res file. The disk-replace-internal-metadata
// scenario asserts the .res sha256 stays pinned across an operator's
// `drbdadm detach ; drbdmeta create-md ; drbdadm attach` recipe; if
// the satellite re-renders + re-writes on every reconcile, the file's
// mtime drifts even though the rendered text is identical and the
// scenario's checksum check trips.
//
// We measure idempotency by mtime: the first Apply writes the file,
// we capture its ModTime, sleep just enough for the filesystem's
// timestamp resolution to advance, run a second Apply with the same
// DesiredResource, then assert ModTime is unchanged. A re-write on
// the second pass would necessarily bump mtime.
func TestApplyDRBDSkipsWriteWhenResUnchanged(t *testing.T) {
	t.Parallel()

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

	desired := []*intent.DesiredResource{
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
	}

	if _, err := rec.Apply(t.Context(), desired); err != nil {
		t.Fatalf("first Apply: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-1.res")

	first, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("stat after first Apply: %v", err)
	}

	bodyFirst, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read after first Apply: %v", err)
	}

	// Sleep just enough for the host filesystem's mtime resolution to
	// advance — APFS is nanosecond, ext4 is millisecond, tmpfs is
	// effectively single-tick. 50ms covers every reasonable backend so
	// a re-write would necessarily produce a strictly-later ModTime.
	// No retry / poll loop: the test asserts equality, not eventual
	// equality.
	time.Sleep(50 * time.Millisecond)

	// Re-seed the FakeExec response for the second Apply pass — the
	// lvs lookup runs again as part of applyStorage's pickup pre-flight.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_00000")})

	if _, err := rec.Apply(t.Context(), desired); err != nil {
		t.Fatalf("second Apply: %v", err)
	}

	second, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("stat after second Apply: %v", err)
	}

	bodyAfter, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read after second Apply: %v", err)
	}

	if string(bodyFirst) != string(bodyAfter) {
		t.Fatalf("rendered body changed across identical Apply passes:\nbefore:\n%s\nafter:\n%s",
			bodyFirst, bodyAfter)
	}

	if !second.ModTime().Equal(first.ModTime()) {
		t.Errorf(".res mtime moved across identical Apply passes (P0-5 idempotency broken): "+
			"first=%s second=%s", first.ModTime(), second.ModTime())
	}
}
