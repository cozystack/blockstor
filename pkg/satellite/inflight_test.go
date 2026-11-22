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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// probeExec runs probe before every command, so a test sees what the
// reconciler reports while an apply or delete is running.
type probeExec struct {
	*storage.FakeExec

	probe func()
}

func (p *probeExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	p.probe()

	return p.FakeExec.Run(ctx, name, args...) //nolint:wrapcheck // test double
}

// Busy holds for exactly as long as an apply or a delete of the resource
// runs, so the DRBD workarounds stay off it meanwhile.
func TestBusyWhileApplyingOrDeleting(t *testing.T) {
	t.Parallel()

	var busyInside, otherBusy bool

	var rec *satellite.Reconciler

	exec := &probeExec{FakeExec: storage.NewFakeExec()}
	exec.probe = func() {
		busyInside = busyInside || rec.Busy("pvc-busy")
		otherBusy = otherBusy || rec.Busy("pvc-other")
	}

	rec = satellite.NewReconciler(satellite.ReconcilerConfig{
		Adm:      drbd.NewAdm(exec),
		Exec:     exec,
		StateDir: t.TempDir(),
		NodeName: "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
		Name: "pvc-busy", NodeName: "n1", Flags: []string{"DISKLESS"},
		Volumes:     []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024}},
		DrbdOptions: map[string]string{"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000"},
	}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !busyInside || otherBusy {
		t.Errorf("during the apply: Busy(pvc-busy)=%v Busy(pvc-other)=%v, want true, false", busyInside, otherBusy)
	}

	if rec.Busy("pvc-busy") {
		t.Error("still busy after the apply returned")
	}

	busyInside = false

	_, err = rec.DeleteResource(t.Context(), &intent.DeleteResourceRequest{Name: "pvc-busy"})
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	if !busyInside || rec.Busy("pvc-busy") {
		t.Errorf("delete: busy inside=%v, after=%v; want true, false", busyInside, rec.Busy("pvc-busy"))
	}
}
