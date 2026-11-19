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
	"slices"
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

// applyAtFullBacking runs one pass of a 2 GiB volume whose LV is already
// 2 GiB, with appliedKib as the size the replica last reported applied.
func applyAtFullBacking(t *testing.T, appliedKib int64) (*storage.FakeExec, []*intent.ResourceApplyResult) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/"+growPendingRD+"_00000",
		storage.FakeResponse{Stdout: []byte(growPendingRD + "_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/"+growPendingRD+"_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/" + growPendingRD + "_00000|2097152\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  t.TempDir(),
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     growPendingRD,
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: growPendingNewKib, StoragePool: "thin1", AppliedSizeKib: appliedKib},
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

	return fx, results
}

func resizeLines(fx *storage.FakeExec) []string {
	var out []string

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm resize") {
			out = append(out, line)
		}
	}

	return out
}

// TestApplyResizesDRBDWhileGrowPending: a replica whose backing grew on
// an earlier pass, but which never finished the grow (its drbdadm resize
// failed, or its backing was large enough already), must resize DRBD on
// the next pass. Before, the resize ran only on the pass that grew the
// backing, so a failed one was never retried and the device stayed at
// the old size.
func TestApplyResizesDRBDWhileGrowPending(t *testing.T) {
	fx, _ := applyAtFullBacking(t, growPendingOldKib)

	if got := resizeLines(fx); !slices.Equal(got, []string{"drbdadm resize --assume-clean " + growPendingRD}) {
		t.Fatalf("drbdadm resize calls: %v, want one", got)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvextend ") {
			t.Errorf("the backing was at size and still got extended: %s", line)
		}
	}
}

// TestApplyNoResizeOnceApplied: a replica that applied the size, or one
// that never reported (every replica right after an upgrade), does not
// resize DRBD on a pass with nothing to grow.
func TestApplyNoResizeOnceApplied(t *testing.T) {
	for _, applied := range []int64{growPendingNewKib, 0} {
		fx, _ := applyAtFullBacking(t, applied)

		if got := resizeLines(fx); len(got) != 0 {
			t.Errorf("applied=%d: unexpected drbdadm resize: %v", applied, got)
		}
	}
}

// TestApplyReportsAppliedSize: a successful pass reports the size it
// applied, which is what the REST layer waits for before it answers a
// resize.
func TestApplyReportsAppliedSize(t *testing.T) {
	_, results := applyAtFullBacking(t, growPendingOldKib)

	if len(results) != 1 || !results[0].GetOk() || len(results[0].GetVolumes()) != 1 {
		t.Fatalf("unexpected results: %+v", results)
	}

	if got := results[0].GetVolumes()[0].GetAppliedSizeKib(); got != growPendingNewKib {
		t.Fatalf("applied size %d, want %d", got, growPendingNewKib)
	}
}
