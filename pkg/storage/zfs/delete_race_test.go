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
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// racePause is how long the gated command waits for the other delete to
// interleave. With deletes serialised the other one cannot run meanwhile, so
// the wait runs out and the gated delete finishes first.
const racePause = 300 * time.Millisecond

// gatedPool holds one command back until a rename of another dataset ran, so
// a test can put a second delete between a delete's read of a name and its
// use of it, the way two satellite workers interleave.
type gatedPool struct {
	*pool

	// gate is the command that waits; reached is closed when it is issued.
	gate    string
	reached chan struct{}

	// renameOf is the dataset whose rename releases the gate.
	renameOf string
	renamed  chan struct{}
	once     sync.Once
}

func (g *gatedPool) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if name+" "+strings.Join(args, " ") == g.gate {
		close(g.reached)

		select {
		case <-g.renamed:
		case <-time.After(racePause):
		}
	}

	out, err := g.pool.Run(ctx, name, args...)

	if err == nil && len(args) > 1 && args[0] == "rename" && args[1] == g.renameOf {
		g.once.Do(func() { close(g.renamed) })
	}

	return out, err
}

// The source zvol, its snapshot and the snapshot's last clone are deleted
// together. The clone's delete reads its origin, then the source's delete
// renames the zvol (and the snapshot with it) and finds the clone still
// there, then the clone is destroyed. Neither delete may leave the marked
// zvol and snapshot behind: nothing else would ever destroy them.
func TestLastCloneDeleteRacingTheSourceRenameLeavesNothing(t *testing.T) {
	t.Parallel()

	inner := newPool(t, "tank/pvc-src_00000", "tank/pvc-src_00000@base")
	inner.clone("tank/pvc-src_00000@base", "tank/pvc-restore_00000")

	gated := &gatedPool{
		pool:     inner,
		gate:     "zfs destroy tank/pvc-restore_00000",
		reached:  make(chan struct{}),
		renameOf: "tank/pvc-src_00000",
		renamed:  make(chan struct{}),
	}

	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, gated)

	err := prov.DeleteSnapshot(t.Context(), snap("pvc-src", "base"))
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	cloneDone := make(chan error, 1)

	go func() { cloneDone <- prov.DeleteVolume(t.Context(), vol("pvc-restore")) }()

	<-gated.reached

	err = prov.DeleteVolume(t.Context(), vol("pvc-src"))
	if err != nil {
		t.Fatalf("DeleteVolume pvc-src: %v", err)
	}

	err = <-cloneDone
	if err != nil {
		t.Fatalf("DeleteVolume pvc-restore: %v", err)
	}

	if got := inner.names(); len(got) != 0 {
		t.Errorf("pool after all three deletes: %v, want empty", got)
	}
}
