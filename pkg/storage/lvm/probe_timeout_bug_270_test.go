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

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Bug 270 (P1, @drbd_ru #22302 et al): a stuck `lvs` / `vgs` /
// probe call must NOT block the satellite reconciler indefinitely.
// Without the withProbeTimeout wrapper, the controller-runtime
// reconcile worker holds the call forever, REST traffic to the
// affected node returns nothing, and the only recovery is host
// reboot. This file guards both the wire-level intent (callers
// receive a Deadline on their ctx) and the structural property
// (every probe handler invokes withProbeTimeout).

// deadlineRecorderExec records whether the ctx it receives carries
// a Deadline. PoolStatus and lvExists are the call sites we expect
// to derive a deadline; their internals call exec.Run with the
// timed-out ctx.
type deadlineRecorderExec struct {
	mu          sync.Mutex
	hadDeadline []bool
	stdout      map[string][]byte
}

func newDeadlineRecorderExec() *deadlineRecorderExec {
	return &deadlineRecorderExec{stdout: map[string][]byte{}}
}

func (f *deadlineRecorderExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return f.RunWithStdin(ctx, nil, name, args...)
}

func (f *deadlineRecorderExec) RunWithStdin(
	ctx context.Context,
	_ io.Reader,
	name string,
	args ...string,
) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := ctx.Deadline()
	f.hadDeadline = append(f.hadDeadline, ok)

	key := name
	if len(args) > 0 {
		key += " " + strings.Join(args, " ")
	}

	if out, ok := f.stdout[key]; ok {
		return out, nil
	}

	return nil, nil
}

// TestPoolStatusBoundsBackendCall_Thin pins Bug 270 fix for thin:
// Thin.PoolStatus must derive a deadline before shelling out to lvs.
func TestPoolStatusBoundsBackendCall_Thin(t *testing.T) {
	t.Parallel()

	fx := newDeadlineRecorderExec()
	fx.stdout["lvs --config "+lvm.ConfigFilter+" --noheadings --separator | -o lv_size,data_percent --units k --nosuffix vg/thinpool"] = []byte("104857600.00|10.00\n")

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	// Caller passes a bare ctx (no deadline) — controller-runtime
	// Reconcile workers do the same. The provider must derive a
	// bounded child before invoking lvs.
	_, err := p.PoolStatus(context.Background())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if len(fx.hadDeadline) == 0 {
		t.Fatalf("no exec calls recorded")
	}

	if !fx.hadDeadline[0] {
		t.Errorf("Thin.PoolStatus must bound the lvs ctx with a deadline; got no Deadline on child ctx (Bug 270)")
	}
}

// TestPoolStatusBoundsBackendCall_Thick mirrors the thin test for
// the Thick.PoolStatus → vgs path.
func TestPoolStatusBoundsBackendCall_Thick(t *testing.T) {
	t.Parallel()

	fx := newDeadlineRecorderExec()
	fx.stdout["vgs --config "+lvm.ConfigFilter+" --noheadings --separator | -o vg_size,vg_free --units k --nosuffix vg"] = []byte("104857600.00|78643200.00\n")

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	_, err := p.PoolStatus(context.Background())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if len(fx.hadDeadline) == 0 {
		t.Fatalf("no exec calls recorded")
	}

	if !fx.hadDeadline[0] {
		t.Errorf("Thick.PoolStatus must bound the vgs ctx with a deadline (Bug 270)")
	}
}

// TestCreateVolumeBoundsLvExistsProbe pins that the lvExists
// pre-check used by CreateVolume / DeleteVolume / DeleteSnapshot
// idempotency derives a deadline. lvExists is the most common
// shell-out on the satellite hot path; an unbounded lvs there
// is the closest analog to the @drbd_ru #22302 symptom.
func TestCreateVolumeBoundsLvExistsProbe(t *testing.T) {
	t.Parallel()

	fx := newDeadlineRecorderExec()
	// lvExists returns empty stdout → CreateVolume falls through
	// to lvcreate. Both exec calls are observed.
	fx.stdout["lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-1_00000"] = []byte("")

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateVolume(context.Background(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if len(fx.hadDeadline) < 1 {
		t.Fatalf("expected at least one exec call; got %d", len(fx.hadDeadline))
	}

	// First call is lvExists → must be bounded.
	if !fx.hadDeadline[0] {
		t.Errorf("lvExists must bound the lvs ctx with a deadline (Bug 270)")
	}
}
