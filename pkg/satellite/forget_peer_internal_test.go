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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// A peer removed from a resource with several volumes is forgotten once,
// through the kernel, after del-peer, and drbdmeta is never run against the
// attached disks: it would rewrite the superblock under the kernel.
func TestRemovedPeerIsForgottenThroughTheKernelOnce(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	resPath := filepath.Join(dir, "pvc-fp.res")

	res := "resource pvc-fp {\n  on n1 {\n    node-id 0;\n  }\n  on n2 {\n    node-id 1;\n  }\n}\n"
	if err := os.WriteFile(resPath, []byte(res), 0o600); err != nil {
		t.Fatalf("write .res: %v", err)
	}

	fx := storage.NewFakeExec()
	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: dir, NodeName: "n1"})

	dr := &intent.DesiredResource{Name: "pvc-fp", NodeName: "n1"}
	devices := map[int32]string{0: "/dev/vg/pvc-fp_00000", 1: "/dev/vg/pvc-fp_00001"}

	if err := rec.tearDownRemovedPeers(context.Background(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	calls := fx.CommandLines()

	del := slices.Index(calls, "drbdadm del-peer n2:pvc-fp")
	forget := slices.Index(calls, "drbdsetup forget-peer pvc-fp 1")

	if del < 0 || forget < del {
		t.Fatalf("want del-peer, then drbdsetup forget-peer; got %v", calls)
	}

	forgets := 0

	for _, c := range calls {
		if strings.Contains(c, "forget-peer") {
			forgets++
		}

		if strings.HasPrefix(c, "drbdmeta") {
			t.Errorf("drbdmeta ran against an attached disk: %s", c)
		}
	}

	if forgets != 1 {
		t.Errorf("forget-peer ran %d times, want once for the resource; got %v", forgets, calls)
	}
}

// The prune path forgets a stale slot after del-peer too: the kernel refuses
// forget-peer while the connection to that node exists.
func TestPrunedSlotIsForgottenAfterDelPeer(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})

	slot := &drbd.KernelSlot{Name: "n2", NodeID: 1, ConnectionState: "StandAlone"}
	devices := map[int32]string{0: "/dev/vg/pvc-fp_00000"}

	if err := rec.teardownKernelSlot(context.Background(), "pvc-fp", slot, devices, logr.Discard()); err != nil {
		t.Fatalf("teardownKernelSlot: %v", err)
	}

	calls := fx.CommandLines()

	del := slices.Index(calls, "drbdadm del-peer n2:pvc-fp")
	forget := slices.Index(calls, "drbdsetup forget-peer pvc-fp 1")

	if del < 0 || forget < del {
		t.Fatalf("want del-peer, then drbdsetup forget-peer; got %v", calls)
	}
}

// A diskless replica has no metadata to clear a slot in, so neither path
// asks the kernel to forget the peer.
func TestADisklessReplicaForgetsNoPeer(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	resPath := filepath.Join(dir, "pvc-fp.res")

	res := "resource pvc-fp {\n  on n1 {\n    node-id 0;\n  }\n  on n2 {\n    node-id 1;\n  }\n}\n"
	if err := os.WriteFile(resPath, []byte(res), 0o600); err != nil {
		t.Fatalf("write .res: %v", err)
	}

	fx := storage.NewFakeExec()
	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: dir, NodeName: "n1"})
	diskless := map[int32]string{0: ""}

	err := rec.tearDownRemovedPeers(context.Background(), &intent.DesiredResource{Name: "pvc-fp", NodeName: "n1"}, resPath, diskless)
	if err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	slot := &drbd.KernelSlot{Name: "n3", NodeID: 2, ConnectionState: "StandAlone"}
	if err := rec.teardownKernelSlot(context.Background(), "pvc-fp", slot, diskless, logr.Discard()); err != nil {
		t.Fatalf("teardownKernelSlot: %v", err)
	}

	for _, c := range fx.CommandLines() {
		if strings.Contains(c, "forget-peer") {
			t.Errorf("a diskless replica ran %s", c)
		}
	}
}
