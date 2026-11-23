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

package satellite

import (
	"context"
	"strings"
	"testing"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// zfsReconciler serves one ZFS pool "zfs1" whose devices report the
// given discard granularity.
func zfsReconciler(t *testing.T, gran string, devices ...string) *Reconciler {
	t.Helper()

	fx := storage.NewFakeExec()
	for _, device := range devices {
		discGranResponse(fx, device, gran)
	}

	return NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfs1": zfs.NewProvider(zfs.Config{Pool: "data"}, fx)},
		Exec:      fx,
	})
}

func zfsResource(name string, vols ...*intent.DesiredVolume) *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     name,
		NodeName: "n1",
		Volumes:  vols,
		Peers:    []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"peer.n2.address": "10.0.0.2", "peer.n2.node-id": "1", "peer.n2.port": "7000",
		},
	}
}

func renderWithDerivedOptions(t *testing.T, rec *Reconciler, dr *intent.DesiredResource, devices map[int32]string) string {
	t.Helper()

	ctx := context.Background()

	body, err := buildResFile(dr, "n1", "10.0.0.1", devices,
		rec.autoDiskOptionsForResource(ctx, dr, devices), rec.autoDiskOptionsForVolumes(ctx, dr, devices))
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	return body
}

// A clone LINSTOR made carries discard-zeroes-if-aligned on its volume
// definition but no rs-discard-granularity: LINSTOR's controller never
// computed one for it, and its ConfFileBuilder renders that option for a
// volume from the volume definition alone, so the clone ran without it.
// Adopted, it must render the same way, however fine the zvol's discard
// granularity is; deriving one from the device changed the kernel
// configuration of exactly those volumes on adoption.
func TestAnAdoptedCloneGetsNoDerivedDiscardGranularity(t *testing.T) {
	t.Parallel()

	device := "/dev/zvol/data/pvc-clone_00000"
	rec := zfsReconciler(t, "16384", device)

	dr := zfsResource("pvc-clone", &intent.DesiredVolume{
		VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "zfs1",
		DiskOptions: map[string]string{"block-size": "4096", "discard-zeroes-if-aligned": "yes"},
	})

	body := renderWithDerivedOptions(t, rec, dr, map[int32]string{0: device})

	if strings.Contains(body, "rs-discard-granularity") {
		t.Errorf("a volume whose definition decides its discard options got a derived granularity:\n%s", body)
	}

	if strings.Count(body, "discard-zeroes-if-aligned") != 1 {
		t.Errorf("discard-zeroes-if-aligned must come once, from the definition:\n%s", body)
	}
}

// An adopted volume whose definition carries LINSTOR's computed
// granularity renders that value, in the volume, and no derived
// resource-scope one beside it.
func TestADefinitionsDiscardGranularityIsTheOneRendered(t *testing.T) {
	t.Parallel()

	device := "/dev/zvol/data/pvc-1_00000"
	rec := zfsReconciler(t, "65536", device)

	dr := zfsResource("pvc-1", &intent.DesiredVolume{
		VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "zfs1",
		DiskOptions: map[string]string{"discard-zeroes-if-aligned": "yes", "rs-discard-granularity": "16384"},
	})

	body := renderWithDerivedOptions(t, rec, dr, map[int32]string{0: device})

	if !strings.Contains(body, "        rs-discard-granularity 16384;\n") || strings.Contains(body, "65536") {
		t.Errorf("want the definition's 16384 inside the volume and nothing derived:\n%s", body)
	}
}

// In a resource where one volume's definition decides and another's
// does not, the other still gets what its device reports, but in its own
// disk section: at resource scope it would reach the deciding volume
// wherever that definition is silent.
func TestDerivedOptionsMoveIntoTheVolumesThatNeedThem(t *testing.T) {
	t.Parallel()

	adopted, added := "/dev/zvol/data/pvc-2_00000", "/dev/zvol/data/pvc-2_00001"
	rec := zfsReconciler(t, "65536", adopted, added)

	dr := zfsResource("pvc-2",
		&intent.DesiredVolume{
			VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "zfs1",
			DiskOptions: map[string]string{"discard-zeroes-if-aligned": "yes"},
		},
		&intent.DesiredVolume{
			VolumeNumber: 1, SizeKib: 1024 * 1024, StoragePool: "zfs1",
			DiskOptions: map[string]string{"block-size": "4096"},
		},
	)

	body := renderWithDerivedOptions(t, rec, dr, map[int32]string{0: adopted, 1: added})

	want := "      device /dev/drbd1001 minor 1001;\n" +
		"      disk " + added + ";\n" +
		"      meta-disk internal;\n" +
		"      disk {\n" +
		"        block-size 4096;\n" +
		"        discard-zeroes-if-aligned yes;\n" +
		"        rs-discard-granularity 65536;\n" +
		"      }\n"
	if !strings.Contains(body, want) {
		t.Errorf("volume 1 must carry its derived options in its own disk section:\n%s", body)
	}

	if strings.Count(body, "rs-discard-granularity") != 1 {
		t.Errorf("rs-discard-granularity must reach volume 1 only:\n%s", body)
	}

	if len(dr.Volumes[1].DiskOptions) != 1 {
		t.Errorf("the render edited the intent's volume options: %v", dr.Volumes[1].DiskOptions)
	}
}

// An operator's resource-scope value still beats a derived one when the
// derived one goes into a volume's own section: there it would override
// the operator for that volume, where at resource scope it never did.
func TestAnOperatorsResourceScopeDiscardOptionStillWins(t *testing.T) {
	t.Parallel()

	adopted, added := "/dev/zvol/data/pvc-3_00000", "/dev/zvol/data/pvc-3_00001"
	rec := zfsReconciler(t, "65536", adopted, added)

	dr := zfsResource("pvc-3",
		&intent.DesiredVolume{
			VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "zfs1",
			DiskOptions: map[string]string{"discard-zeroes-if-aligned": "yes"},
		},
		&intent.DesiredVolume{VolumeNumber: 1, SizeKib: 1024 * 1024, StoragePool: "zfs1"},
	)
	dr.DrbdOptions["DrbdOptions/Disk/rs-discard-granularity"] = "8192"

	body := renderWithDerivedOptions(t, rec, dr, map[int32]string{0: adopted, 1: added})

	if strings.Contains(body, "65536") {
		t.Errorf("a derived granularity overrode the operator's resource-scope 8192:\n%s", body)
	}
}
