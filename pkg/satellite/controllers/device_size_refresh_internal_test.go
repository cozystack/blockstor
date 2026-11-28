// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestObserverRefreshesDeviceSizeAfterResize: an online resize grows
// the DRBD device with no frame carrying the new size, so the resync
// reads it from sysfs by the minor the device frame named, and the
// status the next write builds carries the grown size. A minor sysfs
// cannot read keeps the size the frames reported.
func TestObserverRefreshesDeviceSizeAfterResize(t *testing.T) {
	sizes := map[string]int64{"1047": 2097160}

	o := &ObserverRunnable{readDeviceSize: func(minor string) (int64, error) {
		size, ok := sizes[minor]
		if !ok {
			return 0, errors.New("no such device")
		}

		return size, nil
	}}

	for _, fields := range []map[string]string{
		{"name": "pvc-grown", "volume": "0", "minor": "1047", "disk": "UpToDate", "size": "1048584"},
		{"name": "pvc-gone", "volume": "0", "minor": "1048", "disk": "UpToDate", "size": "1048584"},
	} {
		obs, ok := translateDeviceEvent(drbd.Event{Action: "exists", Kind: eventKindDevice, Fields: fields})
		if !ok {
			t.Fatalf("device frame %v not translated", fields)
		}

		o.mergeVolumes(&obs)
	}

	// A later frame without minor or size must not lose the minor.
	later := observation{
		ResourceName: "pvc-grown",
		Volumes:      []volumeObservation{{VolumeNumber: 0, DiskState: "UpToDate"}},
	}
	o.mergeVolumes(&later)

	o.refreshDeviceSizes()

	grown := o.snapshotFor("pvc-grown")
	if status := buildObserverVolumeStatus(&grown, "data"); len(status) != 1 || status[0].DeviceSizeKib != 2097160 {
		t.Fatalf("status after the resize: got %+v, want deviceSizeKib 2097160", status)
	}

	gone := o.snapshotFor("pvc-gone")
	if status := buildObserverVolumeStatus(&gone, "data"); len(status) != 1 || status[0].DeviceSizeKib != 1048584 {
		t.Fatalf("status of a device sysfs cannot read: got %+v, want the frame's 1048584", status)
	}
}

// TestSysfsDrbdSizeKibRefusesAMinorThatIsNotANumber: the minor comes
// from a frame and becomes part of a path, so anything but a number is
// refused before a file is opened.
func TestSysfsDrbdSizeKibRefusesAMinorThatIsNotANumber(t *testing.T) {
	if _, err := sysfsDrbdSizeKib("../sda"); err == nil {
		t.Fatal("a minor with a path in it was read")
	}
}
