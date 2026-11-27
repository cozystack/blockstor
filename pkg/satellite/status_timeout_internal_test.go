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
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

const wedgedRes = "pvc-wedged"

// errStatusKilled is what a drbdsetup killed by its context's end returns.
var errStatusKilled = errors.New("signal: killed")

// wedgedStatusExec answers every command from its FakeExec but
// `drbdsetup status`, which blocks until its context ends, as a status
// that reaches a resource whose receivers deadlocked does.
type wedgedStatusExec struct{ *storage.FakeExec }

func (w wedgedStatusExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return w.RunWithStdin(ctx, nil, name, args...)
}

func (w wedgedStatusExec) RunWithStdin(ctx context.Context, stdin io.Reader, name string, args ...string) ([]byte, error) {
	if name == "drbdsetup" && len(args) > 0 && args[0] == "status" {
		<-ctx.Done()

		return nil, errStatusKilled
	}

	return w.FakeExec.RunWithStdin(ctx, stdin, name, args...) //nolint:wrapcheck // test double
}

func wedgedReconciler(t *testing.T) (*Reconciler, *storage.FakeExec) {
	t.Helper()

	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(wedgedStatusExec{fx})
	adm.SetStatusTimeout(20 * time.Millisecond)

	return NewReconciler(ReconcilerConfig{Adm: adm, StateDir: t.TempDir(), NodeName: "n1"}), fx
}

type recordingClearer struct {
	mu    sync.Mutex
	calls []string
}

func (c *recordingClearer) ClearSkipDisk(_ context.Context, resourceName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, resourceName)

	return nil
}

func (c *recordingClearer) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.calls)
}

// A diskless toggle whose kernel probe did not answer must not go on to
// close LUKS and reclaim the backing volume: the slot may still hold it.
func TestDetachStopsWhenTheKernelDoesNotAnswer(t *testing.T) {
	t.Parallel()

	rec, fx := wedgedReconciler(t)

	err := rec.detachIfStillAttached(t.Context(), &intent.DesiredResource{Name: wedgedRes, NodeName: "n1"})
	if !errors.Is(err, drbd.ErrStatusTimeout) {
		t.Fatalf("got %v, want ErrStatusTimeout so the toggle stops before the reclaim", err)
	}

	if cmds := fx.CommandLines(); len(cmds) != 0 {
		t.Errorf("nothing may run after the probe timed out, got %v", cmds)
	}
}

// A replica whose peers' state cannot be read must not be forced
// primary: a connected peer may hold the data.
func TestNoForcePromoteWhenTheKernelDoesNotAnswer(t *testing.T) {
	t.Parallel()

	rec, _ := wedgedReconciler(t)
	dr := &intent.DesiredResource{
		Name:     wedgedRes,
		NodeName: "n1",
		Peers:    []intent.DesiredPeer{{Name: "n2"}},
	}

	if rec.shouldForcePromote(t.Context(), dr) {
		t.Fatal("forced primary while every peer probe timed out")
	}
}

// A SkipDisk stamp stays while the kernel does not answer: a timed-out
// probe is not a disk that came back attached.
func TestSkipDiskKeptWhenTheKernelDoesNotAnswer(t *testing.T) {
	t.Parallel()

	rec, _ := wedgedReconciler(t)
	clearer := &recordingClearer{}
	rec.SetSkipDiskClearer(clearer)

	dr := &intent.DesiredResource{
		Name:     wedgedRes,
		NodeName: "n1",
		Props:    map[string]string{skipDiskPropKey: skipDiskPropValue},
	}

	_ = rec.runAdjust(t.Context(), dr, false)

	if calls := clearer.Calls(); len(calls) != 0 {
		t.Errorf("SkipDisk released on a probe that timed out: %v", calls)
	}
}

// Without SkipDisk, a timed-out disk probe stops the adjust: a plain
// adjust could reattach a disk the probe would have kept off.
func TestAdjustStopsWhenTheDiskProbeDoesNotAnswer(t *testing.T) {
	t.Parallel()

	rec, fx := wedgedReconciler(t)
	dr := &intent.DesiredResource{Name: wedgedRes, NodeName: "n1"}

	err := rec.runAdjust(t.Context(), dr, false)
	if !errors.Is(err, drbd.ErrStatusTimeout) {
		t.Fatalf("got %v, want ErrStatusTimeout", err)
	}

	for _, cmd := range fx.CommandLines() {
		if strings.HasPrefix(cmd, "drbdadm adjust") {
			t.Errorf("adjusted with the disk state unknown: %s", cmd)
		}
	}
}
