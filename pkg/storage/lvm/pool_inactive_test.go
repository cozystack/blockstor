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

package lvm_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// A thin pool survives a reboot as metadata rather than as a running
// device, and until something activates it lvs reports its lv_size but
// leaves data_percent empty. Read as a destroyed pool, that zeroes the
// StoragePool's capacity, the placer then skips the node, and volume
// creation fails across the cluster with "Add more nodes or storage
// pools" while every node is online and every pool object still
// present. These tests pin the recovery: an inactive pool is activated
// and re-probed, and an active one costs a single lvs.

const (
	poolKey = "lvs --config " + lvm.ConfigFilter +
		" --noheadings --separator | -o lv_size,data_percent,lv_attr --units k --nosuffix vg/thinpool"
	activateKey = "lvchange --config " + lvm.ConfigFilter + " --activate y vg/thinpool"
)

// scriptedExec answers each command from a script and records the order
// in which they were called.
type scriptedExec struct {
	mu    sync.Mutex
	calls []string
	// replies are keyed by command line; a key may hold several
	// answers, returned in turn, so a re-probe can differ.
	replies map[string][][]byte
}

func newScriptedExec() *scriptedExec {
	return &scriptedExec{replies: map[string][][]byte{}}
}

func (f *scriptedExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return f.RunWithStdin(ctx, nil, name, args...)
}

func (f *scriptedExec) RunWithStdin(
	_ context.Context,
	_ io.Reader,
	name string,
	args ...string,
) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := name
	if len(args) > 0 {
		key += " " + strings.Join(args, " ")
	}
	f.calls = append(f.calls, key)

	queued := f.replies[key]
	if len(queued) == 0 {
		return nil, nil
	}
	out := queued[0]
	if len(queued) > 1 {
		f.replies[key] = queued[1:]
	}

	return out, nil
}

func (f *scriptedExec) ran(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	var n int
	for _, c := range f.calls {
		if c == key {
			n++
		}
	}

	return n
}

func TestPoolStatusActivatesAnInactivePool(t *testing.T) {
	t.Parallel()

	fx := newScriptedExec()
	// First probe: the pool is there, with no usage reported and the
	// state character of lv_attr unset. Second: active, as lvchange
	// leaves it.
	fx.replies[poolKey] = [][]byte{
		[]byte("104857600.00||twi---tz--\n"),
		[]byte("104857600.00|10.00|twi-aotz--\n"),
	}

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	st, err := p.PoolStatus(context.Background())
	if err != nil {
		t.Fatalf("PoolStatus on an inactive pool: %v", err)
	}

	if fx.ran(activateKey) != 1 {
		t.Errorf("the pool must be activated exactly once, ran %d times", fx.ran(activateKey))
	}

	if fx.ran(poolKey) != 2 {
		t.Errorf("the pool must be probed again after activation, probed %d times", fx.ran(poolKey))
	}

	if st.TotalCapacityKib != 104857600 {
		t.Errorf("TotalCapacityKib %d, want 104857600", st.TotalCapacityKib)
	}

	// 10% of 104857600 used.
	if st.FreeCapacityKib != 94371840 {
		t.Errorf("FreeCapacityKib %d, want 94371840", st.FreeCapacityKib)
	}
}

func TestPoolStatusLeavesAnActivePoolAlone(t *testing.T) {
	t.Parallel()

	fx := newScriptedExec()
	fx.replies[poolKey] = [][]byte{[]byte("104857600.00|10.00|twi-aotz--\n")}

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	if _, err := p.PoolStatus(context.Background()); err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if fx.ran(activateKey) != 0 {
		t.Errorf("an active pool must not be activated; lvchange ran %d times", fx.ran(activateKey))
	}

	if fx.ran(poolKey) != 1 {
		t.Errorf("an active pool costs one probe, took %d", fx.ran(poolKey))
	}
}

// A pool that reports nothing at all is gone, not cold: that distinction
// is what keeps "the operator removed the backing pool" working, so it
// must not be swallowed by the activation path.
func TestPoolStatusStillReportsAMissingPool(t *testing.T) {
	t.Parallel()

	fx := newScriptedExec()
	fx.replies[poolKey] = [][]byte{[]byte("\n")}

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	if _, err := p.PoolStatus(context.Background()); err == nil {
		t.Fatal("a missing pool must be an error")
	}

	if fx.ran(activateKey) != 0 {
		t.Errorf("a missing pool must not be activated; lvchange ran %d times", fx.ran(activateKey))
	}
}
