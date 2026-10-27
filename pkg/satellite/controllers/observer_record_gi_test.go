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

package controllers

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The satellite keeps its own record of the GIs its volumes carried, since
// the status field is dropped when a volume leaves a data-bearing state. The
// observer feeds it every GI it reads, backfilled or not, and nothing for a
// volume without data.
func TestObserverRecordsTheGIOfDataBearingVolumes(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdmeta --force pvc-r/0 v09 /dev/vg/pvc-r_00000 internal get-gi --node-id 0",
		storage.FakeResponse{Stdout: []byte("6E3A5D0B2C1F4A96:0:0:0\n")})

	recorded := map[int32]string{}
	o := &ObserverRunnable{RecordGI: func(_ string, vol int32, gi string) { recorded[vol] = gi }}

	ev := &observation{ResourceName: "pvc-r", Volumes: []volumeObservation{
		{VolumeNumber: 0, DiskState: "UpToDate", BackingDev: "/dev/vg/pvc-r_00000"},
		{VolumeNumber: 1, DiskState: "UpToDate", CurrentUUID: "1111111111111110"},
		{VolumeNumber: 2, DiskState: "Inconsistent", CurrentUUID: "2222222222222220"},
	}}

	o.backfillCurrentGI(context.Background(), drbd.NewAdm(fx), ev)

	if recorded[0] != "6E3A5D0B2C1F4A96" || recorded[1] != "1111111111111110" {
		t.Errorf("recorded %v, want the backfilled and the reported GI", recorded)
	}

	if _, ok := recorded[2]; ok {
		t.Error("a volume without data had its GI recorded")
	}
}

// The observer the manager runs feeds the reconciler's GI record. Without
// that wiring the record stays empty and every crashed replica is
// initialised again, with no test or log line to say so.
func TestManagerObserverRecordsIntoTheReconciler(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := &Config{Apply: satellite.NewReconciler(satellite.ReconcilerConfig{StateDir: dir})}

	o := newObserverRunnableFromConfig(nil, cfg)
	if o.RecordGI == nil {
		t.Fatal("the observer records no GIs")
	}

	o.RecordGI("pvc-w", 0, "6e3a5d0b2c1f4a96")

	data, err := os.ReadFile(filepath.Join(dir, "pvc-w.gi-history"))
	if err != nil || string(data) != "0 6E3A5D0B2C1F4A96\n" {
		t.Fatalf("record = %q, %v; want the GI in the reconciler's history", data, err)
	}
}
