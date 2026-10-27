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

package satellite_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// A replica that already went to disk under [DRBD,STORAGE] and is now
// asked for LUKS keeps its data: the apply fails on that volume with
// the reason in the message, and luksFormat never runs.
func TestApplyLUKSOverAPlaintextVolumeIsRefused(t *testing.T) {
	t.Parallel()

	const dev = "/dev/vg/pvc-plain_00000"

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-plain_00000",
		storage.FakeResponse{Stdout: []byte("pvc-plain_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-plain_00000",
		storage.FakeResponse{Stdout: []byte(dev + "|1048576\n")})
	fx.Expect("cryptsetup isLuks "+dev, storage.FakeResponse{Err: errNotALUKSDevice})
	fx.Expect("blkid -p -o export "+dev, storage.FakeResponse{Stdout: []byte("DEVNAME=" + dev + "\nTYPE=drbd\n")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		Adm:        drbd.NewAdm(fx),
		StateDir:   t.TempDir(),
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
		Name:       "pvc-plain",
		NodeName:   "n1",
		Volumes:    []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"}},
		LayerStack: []string{"LUKS", "STORAGE"},
		Props:      map[string]string{"LuksPassphrase": "topsecret"},
	}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || results[0].GetOk() {
		t.Fatalf("results %v, want one failed apply", results)
	}

	if !strings.Contains(results[0].GetMessage(), "already holds data") || !strings.Contains(results[0].GetMessage(), "TYPE=drbd") {
		t.Errorf("message %q does not say the device holds DRBD data", results[0].GetMessage())
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "luksFormat") {
			t.Fatalf("luksFormat ran over a plaintext volume: %s", line)
		}
	}
}
