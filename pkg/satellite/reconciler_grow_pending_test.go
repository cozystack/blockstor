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
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

const (
	growPendingRD     = "pvc-grow-pending"
	growPendingOldKib = int64(1024 * 1024)
	growPendingNewKib = int64(2 * 1024 * 1024)
)

// errNoDeviceSize is a device whose size cannot be read.
var errNoDeviceSize = errors.New("no size")

// growGrossKib is the backing a grow to growPendingNewKib extends to:
// the size plus DRBD's internal metadata, in whole MiB.
func growGrossKib() int64 {
	return (satellite.DrbdGrossKibForTest(growPendingNewKib) + 1023) / 1024 * 1024
}

// growFixture is one replica of a 2 GiB volume whose LV holds lvKib,
// with appliedKib as the size it last reported applied. deviceKib is
// what the DRBD device reads after the pass; 0 means it cannot be read.
type growFixture struct {
	fx  *storage.FakeExec
	rec *satellite.Reconciler

	appliedKib int64
	deviceKib  int64
}

func newGrowFixture(t *testing.T, lvKib, appliedKib int64) *growFixture {
	t.Helper()

	gf := &growFixture{fx: storage.NewFakeExec(), appliedKib: appliedKib}

	gf.fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/"+growPendingRD+"_00000",
		storage.FakeResponse{Stdout: []byte(growPendingRD + "_00000\n")})
	gf.fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/"+growPendingRD+"_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/" + growPendingRD + "_00000|" + strconv.FormatInt(lvKib, 10) + "\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, gf.fx)
	gf.rec = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(gf.fx),
		StateDir:  t.TempDir(),
		NodeName:  "n1",
		DeviceSizeKib: func(_ context.Context, device string) (int64, error) {
			if device != "/dev/drbd1000" {
				t.Errorf("measured %s, want the DRBD device", device)
			}

			if gf.deviceKib == 0 {
				return 0, errNoDeviceSize
			}

			return gf.deviceKib, nil
		},
	})

	return gf
}

// pass runs one apply pass and returns the commands it ran and its result.
func (gf *growFixture) pass(t *testing.T) ([]string, *intent.ResourceApplyResult) {
	t.Helper()

	before := len(gf.fx.CommandLines())

	results, err := gf.rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     growPendingRD,
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: growPendingNewKib, StoragePool: "thin1", AppliedSizeKib: gf.appliedKib},
			},
			SkipInitialSync: skipInitTrue(),
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() || len(results[0].GetVolumes()) != 1 {
		t.Fatalf("unexpected results: %+v", results)
	}

	return gf.fx.CommandLines()[before:], results[0]
}

func linesWithPrefix(lines []string, prefix string) []string {
	var out []string

	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			out = append(out, line)
		}
	}

	return out
}

// TestApplyResizesDRBDWhileGrowPending: a replica whose backing grew on
// an earlier pass, but whose drbdadm resize failed, must resize DRBD on
// the next pass. Before, the resize ran only on the pass that grew the
// backing, so a failed one was never retried and the device stayed at
// the old size.
func TestApplyResizesDRBDWhileGrowPending(t *testing.T) {
	gf := newGrowFixture(t, growGrossKib(), growPendingOldKib)

	lines, _ := gf.pass(t)

	if got := linesWithPrefix(lines, "drbdadm resize"); !slices.Equal(got, []string{"drbdadm resize --assume-clean " + growPendingRD}) {
		t.Fatalf("drbdadm resize calls: %v, want one", got)
	}

	if got := linesWithPrefix(lines, "lvextend "); len(got) != 0 {
		t.Errorf("the backing was at size and still got extended: %v", got)
	}
}

// TestApplyGrowsBackingThatHeldOnlyTheNetSize: an adopted LINSTOR volume
// carries DRBD's metadata on top of its definition size, so a new size
// can fit in the old backing while the device it leaves is still short
// of it. Comparing the definition size with the backing skipped the
// lvextend, drbdadm resize had nothing to grow, and resize2fs grew
// nothing. The backing must reach the size plus the metadata.
func TestApplyGrowsBackingThatHeldOnlyTheNetSize(t *testing.T) {
	gf := newGrowFixture(t, growPendingNewKib+512, growPendingOldKib)

	lines, _ := gf.pass(t)

	want := "lvextend --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } activation { udev_sync=0 udev_rules=0 } --size " +
		strconv.FormatInt(growGrossKib()/1024, 10) + "MiB vg/" + growPendingRD + "_00000"
	if got := linesWithPrefix(lines, "lvextend "); !slices.Equal(got, []string{want}) {
		t.Fatalf("lvextend calls: %v, want %q", got, want)
	}
}

// TestApplyNoResizeOnceApplied: a replica that applied the size, or one
// that never reported (every replica right after an upgrade), does not
// resize DRBD on a pass with nothing to grow, and reports the size.
func TestApplyNoResizeOnceApplied(t *testing.T) {
	for _, applied := range []int64{growPendingNewKib, 0} {
		gf := newGrowFixture(t, growGrossKib(), applied)

		lines, result := gf.pass(t)

		if got := linesWithPrefix(lines, "drbdadm resize"); len(got) != 0 {
			t.Errorf("applied=%d: unexpected drbdadm resize: %v", applied, got)
		}

		if got := result.GetVolumes()[0].GetAppliedSizeKib(); got != growPendingNewKib || result.GetGrowPending() {
			t.Errorf("applied=%d: reported %d (pending %v), want %d", applied, got, result.GetGrowPending(), growPendingNewKib)
		}
	}
}

// TestApplyReportsAppliedSizeOnlyOnceTheDeviceGrew: the REST layer
// answers a resize once every replica reports the new size, so a pass
// may report it only when the device shows it. Before, any pass that
// succeeded reported the definition size, whether or not the device had
// grown, and the resize was acknowledged before resize2fs could grow
// anything.
func TestApplyReportsAppliedSizeOnlyOnceTheDeviceGrew(t *testing.T) {
	gf := newGrowFixture(t, growGrossKib(), growPendingOldKib)

	for _, deviceKib := range []int64{0, growPendingOldKib, growPendingNewKib - 4} {
		gf.deviceKib = deviceKib

		_, result := gf.pass(t)

		if got := result.GetVolumes()[0].GetAppliedSizeKib(); got != growPendingOldKib || !result.GetGrowPending() {
			t.Fatalf("device at %d: reported %d (pending %v), want the old size, pending", deviceKib, got, result.GetGrowPending())
		}
	}

	gf.deviceKib = growPendingNewKib

	_, result := gf.pass(t)

	if got := result.GetVolumes()[0].GetAppliedSizeKib(); got != growPendingNewKib || result.GetGrowPending() {
		t.Fatalf("device grown: reported %d (pending %v), want %d", got, result.GetGrowPending(), growPendingNewKib)
	}
}

// TestApplyStopsRetryingAResizeThatDoesNotGrowTheDevice: a device held
// back by a peer whose backing has not grown does not grow from a local
// resize, and every pass that resizes keeps the late-add self-heals off.
// The retries stop after a few, while the size is still measured, so a
// peer's resize that grows the device is still reported.
func TestApplyStopsRetryingAResizeThatDoesNotGrowTheDevice(t *testing.T) {
	gf := newGrowFixture(t, growGrossKib(), growPendingOldKib)
	gf.deviceKib = growPendingOldKib

	resizes := 0

	for range 6 {
		lines, _ := gf.pass(t)
		resizes += len(linesWithPrefix(lines, "drbdadm resize"))
	}

	if resizes != 3 {
		t.Fatalf("drbdadm resize ran on %d of 6 passes, want 3", resizes)
	}

	gf.deviceKib = growPendingNewKib

	_, result := gf.pass(t)

	if got := result.GetVolumes()[0].GetAppliedSizeKib(); got != growPendingNewKib {
		t.Fatalf("after a peer grew the device: reported %d, want %d", got, growPendingNewKib)
	}
}
