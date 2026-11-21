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

package zfs_test

import (
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

const gib = int64(1) << 30

// A thick pool holding about 133 GiB of zvols: the zpool still counts
// the unwritten part of their refreservation as free, the dataset does
// not. Shape of a production node where the two disagreed by ~130 GiB.
const (
	thickZpoolSize      = 260 * gib
	thickZpoolFree      = 246 * gib
	thickDatasetAvail   = 113 * gib
	thickDatasetAvailKi = thickDatasetAvail / 1024
	thickZpoolSizeKi    = thickZpoolSize / 1024
)

func expectPool(fx *storage.FakeExec, pool, root string, avail, quota, size int64) {
	fx.Expect("zfs get -Hp -o property,value available,quota "+pool,
		storage.FakeResponse{Stdout: fmtProps(avail, quota)})
	fx.Expect("zpool get -Hp -o value size "+root,
		storage.FakeResponse{Stdout: []byte(itoa(size) + "\n")})
	// What `zpool list` would say, so a provider that still reads the
	// zpool's free space gets a plausible answer and fails on value.
	fx.Expect("zpool list -H -p -o size,free "+pool,
		storage.FakeResponse{Stdout: []byte(itoa(size) + "\t" + itoa(thickZpoolFree) + "\n")})
}

func fmtProps(avail, quota int64) []byte {
	return []byte("available\t" + itoa(avail) + "\nquota\t" + itoa(quota) + "\n")
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}

// TestPoolStatusFreeIsDatasetAvailable pins that both ZFS kinds report
// the pool dataset's `available` as free capacity, matching LINSTOR.
// With the zpool's `free` a thick pool overstated its headroom by the
// unwritten refreservation of every zvol on it, and the autoplacer,
// which reads FreeCapacity as-is for thick pools, overcommitted it.
func TestPoolStatusFreeIsDatasetAvailable(t *testing.T) {
	for _, thin := range []bool{false, true} {
		fx := storage.NewFakeExec()
		expectPool(fx, "data", "data", thickDatasetAvail, 0, thickZpoolSize)

		p := zfs.NewProvider(zfs.Config{Pool: "data", Thin: thin}, fx)

		got, err := p.PoolStatus(t.Context())
		if err != nil {
			t.Fatalf("%s PoolStatus: %v", p.Kind(), err)
		}

		if got.FreeCapacityKib != thickDatasetAvailKi {
			t.Errorf("%s FreeCapacityKib: got %d, want %d (dataset available, not zpool free)",
				p.Kind(), got.FreeCapacityKib, thickDatasetAvailKi)
		}

		if got.TotalCapacityKib != thickZpoolSizeKi {
			t.Errorf("%s TotalCapacityKib: got %d, want %d (zpool size)",
				p.Kind(), got.TotalCapacityKib, thickZpoolSizeKi)
		}
	}
}

// TestPoolStatusTotalIsQuotaWhenSet pins that a quota on the pool
// dataset caps the reported total, as LINSTOR does, and that the zpool
// size is then not consulted.
func TestPoolStatusTotalIsQuotaWhenSet(t *testing.T) {
	const quota = 50 * gib

	fx := storage.NewFakeExec()
	expectPool(fx, "tank/linstor", "tank", 40*gib, quota, thickZpoolSize)

	p := zfs.NewProvider(zfs.Config{Pool: "tank/linstor"}, fx)

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib != quota/1024 {
		t.Errorf("TotalCapacityKib: got %d, want %d (dataset quota)", got.TotalCapacityKib, quota/1024)
	}

	if got.FreeCapacityKib != 40*gib/1024 {
		t.Errorf("FreeCapacityKib: got %d, want %d", got.FreeCapacityKib, 40*gib/1024)
	}

	for _, line := range fx.CommandLines() {
		if line == "zpool get -Hp -o value size tank" {
			t.Errorf("zpool size must not be read when a quota is set; calls %v", fx.CommandLines())
		}
	}
}

// TestPoolStatusChildDatasetUsesRootZpoolSize pins that a pool named
// as a child dataset reads the size of the zpool that holds it, since
// `zpool get` does not accept a dataset path.
func TestPoolStatusChildDatasetUsesRootZpoolSize(t *testing.T) {
	fx := storage.NewFakeExec()
	expectPool(fx, "tank/linstor", "tank", 40*gib, 0, thickZpoolSize)

	p := zfs.NewProvider(zfs.Config{Pool: "tank/linstor", Thin: true}, fx)

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib != thickZpoolSizeKi {
		t.Errorf("TotalCapacityKib: got %d, want %d", got.TotalCapacityKib, thickZpoolSizeKi)
	}

	if !slices.Contains(fx.CommandLines(), "zpool get -Hp -o value size tank") {
		t.Errorf("expected the size of zpool tank to be read; calls %v", fx.CommandLines())
	}
}

// TestPoolStatusMissingZpoolSize pins that a zpool that vanished
// between the two probes is reported as gone, not as a zero total.
func TestPoolStatusMissingZpoolSize(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs get -Hp -o property,value available,quota tank",
		storage.FakeResponse{Stdout: fmtProps(gib, 0)})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.PoolStatus(t.Context())
	if !errors.Is(err, storage.ErrPoolGone) {
		t.Errorf("want ErrPoolGone, got %v", err)
	}
}

// TestPoolStatusIncompleteProps pins that output missing one of the
// two properties is an error rather than a zero value.
func TestPoolStatusIncompleteProps(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs get -Hp -o property,value available,quota tank",
		storage.FakeResponse{Stdout: []byte("available\t1024\n")})
	fx.Expect("zpool get -Hp -o value size tank",
		storage.FakeResponse{Stdout: []byte("4096\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	if _, err := p.PoolStatus(t.Context()); err == nil {
		t.Errorf("expected an error when quota is missing from zfs get output")
	}
}
