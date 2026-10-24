package controllers

import (
	"context"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
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
