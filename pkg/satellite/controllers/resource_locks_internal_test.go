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
	"io"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// isHeld reports whether name is held or waited for.
func (l *ResourceLocks) isHeld(name string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, ok := l.held[name]

	return ok
}

// lockWitnessExec answers like FakeExec and counts the dump-md calls
// made while name was not locked.
type lockWitnessExec struct {
	*storage.FakeExec

	locks    *ResourceLocks
	name     string
	unlocked atomic.Int32
}

func (e *lockWitnessExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if name == "drbdadm" && len(args) > 0 && args[0] == "dump-md" && !e.locks.isHeld(e.name) {
		e.unlocked.Add(1)
	}

	return e.FakeExec.Run(ctx, name, args...) //nolint:wrapcheck // test double
}

func (e *lockWitnessExec) RunWithStdin(ctx context.Context, stdin io.Reader, name string, args ...string) ([]byte, error) {
	return e.FakeExec.RunWithStdin(ctx, stdin, name, args...) //nolint:wrapcheck // test double
}

// The backfill probes a resource only while it holds that resource's
// lock, which the reconciler holds for a whole apply pass. A probe run
// beside a pass that is creating the metadata could read the busy
// lower disk as metadata present and stamp an empty volume.
func TestTheBackfillProbesUnderTheResourceLock(t *testing.T) {
	t.Parallel()

	f := newBackfillFixture("pvc-6", 0, 1)
	locks := &ResourceLocks{}
	fx := &lockWitnessExec{FakeExec: newDumpMdExec("pvc-6/0", "pvc-6/1"), locks: locks, name: "pvc-6"}

	cli := fake.NewClientBuilder().WithScheme(crNameScheme(t)).WithObjects(f.rd, f.replica).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	backfill := &MetadataCreatedBackfillRunnable{
		Client:   cli,
		Adm:      drbd.NewAdm(fx),
		Stamper:  &MetadataCreatedStamper{Client: cli},
		NodeName: "n1",
		Locks:    locks,
	}

	if err := backfill.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got := dumpMdTargets(fx.FakeExec); len(got) != 2 {
		t.Fatalf("dump-md targets = %v, want both volumes probed", got)
	}

	if n := fx.unlocked.Load(); n != 0 {
		t.Errorf("%d dump-md probes ran without the resource lock, want none", n)
	}

	if locks.isHeld("pvc-6") {
		t.Error("the backfill left the resource locked")
	}
}

// lockWitnessProvider records whether a volume was created while the
// resource's lock was held.
type lockWitnessProvider struct {
	flakyCreateProvider

	locks    *ResourceLocks
	name     string
	unlocked atomic.Int32
}

func (p *lockWitnessProvider) CreateVolume(ctx context.Context, vol storage.Volume) error {
	if !p.locks.isHeld(p.name) {
		p.unlocked.Add(1)
	}

	return p.flakyCreateProvider.CreateVolume(ctx, vol)
}

// The reconciler holds the resource's lock for the whole apply pass,
// so the backfill cannot probe in the middle of it.
func TestRunApplyHoldsTheResourceLock(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("STORAGE")
	res := memberReplica()
	r := newPassphraseTestReconciler(t, rd, res)

	locks := &ResourceLocks{}
	provider := &lockWitnessProvider{locks: locks, name: naming.Key(rd.Name)}
	r.Config.ApplyLocks = locks
	r.Config.Exec = storage.NewFakeExec()
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": provider},
		NodeName:  "node-a",
	})

	if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	if n := atomic.LoadInt32(&provider.created); n == 0 {
		t.Fatal("no volume created, the apply pass did not run")
	}

	if n := provider.unlocked.Load(); n != 0 {
		t.Errorf("%d volumes created without the resource lock, want none", n)
	}

	if locks.isHeld(naming.Key(rd.Name)) {
		t.Error("the apply pass left the resource locked")
	}
}

// A nil lock set locks nothing, so a reconciler built without one runs.
func TestANilResourceLocksLocksNothing(t *testing.T) {
	t.Parallel()

	var locks *ResourceLocks

	locks.Lock("pvc-7")()
	locks.Lock("pvc-7")()
}

// NewManager hands the backfill the lock set the reconciler holds; two
// separate sets would each lock nothing the other sees.
func TestTheBackfillSharesTheReconcilersLocks(t *testing.T) {
	t.Parallel()

	cfg := &Config{NodeName: "n1", Exec: storage.NewFakeExec()}
	ensureWiredDefaults(cfg)

	if cfg.ApplyLocks == nil {
		t.Fatal("no ApplyLocks wired")
	}

	if got := newMetadataCreatedBackfillFromConfig(nil, cfg).Locks; got != cfg.ApplyLocks {
		t.Errorf("backfill locks = %p, want the reconciler's %p", got, cfg.ApplyLocks)
	}
}
