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
	"errors"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

var errVGBusy = errors.New("exit status 5: Volume group vg is locked by another process")

// DeleteResource answers Ok only once the storage is gone, because Ok is what
// releases the satellite finalizer and nothing deletes the volume after that.
// A pool that is not registered yet (a satellite that just started) cannot
// delete anything, so the answer is not Ok.
func TestDeleteResourceWithAnUnregisteredPoolIsNotOk(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		StateDir:  t.TempDir(),
		NodeName:  "n1",
	})

	resp, err := rec.DeleteResource(t.Context(), &intent.DeleteResourceRequest{
		Name:          "pvc-a",
		StoragePool:   "thin1",
		VolumeNumbers: []int32{0},
	})
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	if resp.GetOk() {
		t.Fatalf("DeleteResource Ok=true with pool thin1 unregistered; the finalizer would go over a volume still on disk")
	}

	if !strings.Contains(resp.GetMessage(), "thin1") {
		t.Errorf("message %q does not name the pool", resp.GetMessage())
	}

	if got := fx.CommandLines(); len(got) != 0 {
		t.Errorf("commands ran: %v", got)
	}
}

// A provider probe that fails is not "volume gone" either.
func TestDeleteResourceWithAFailingProbeIsNotOk(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-a_00000",
		storage.FakeResponse{Err: errVGBusy})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		StateDir:  t.TempDir(),
		NodeName:  "n1",
	})

	resp, err := rec.DeleteResource(t.Context(), &intent.DeleteResourceRequest{
		Name:          "pvc-a",
		StoragePool:   "thin1",
		VolumeNumbers: []int32{0},
	})
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	if resp.GetOk() {
		t.Fatalf("DeleteResource Ok=true on a failed probe")
	}
}

// A diskless replica has no pool and nothing to delete: still Ok.
func TestDeleteResourceDisklessIsOk(t *testing.T) {
	t.Parallel()

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		StateDir:  t.TempDir(),
		NodeName:  "n1",
	})

	resp, err := rec.DeleteResource(t.Context(), &intent.DeleteResourceRequest{Name: "pvc-a"})
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	if !resp.GetOk() {
		t.Fatalf("diskless DeleteResource Ok=false: %s", resp.GetMessage())
	}
}
