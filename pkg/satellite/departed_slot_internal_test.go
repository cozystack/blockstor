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
	"encoding/json"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Exit codes the fake answers with: drbdadm's for a config lookup that
// failed, drbdsetup's for a request the kernel refused.
const (
	drbdadmConfigExit   = 1
	drbdsetupRefuseExit = 10
)

// kernelPeerFake models the DRBD 9 kernel and drbd-utils for one
// resource, as far as removing a peer goes:
//
//   - drbdadm resolves a peer through the .res: with no `on <peer>` block
//     it prints "not defined in your config" and leaves the kernel alone;
//   - drbdsetup del-peer works from the kernel, by node-id;
//   - forget-peer is refused with ERR_NET_CONFIGURED (125) while the
//     kernel still holds a connection under that node-id.
type kernelPeerFake struct {
	mu        sync.Mutex
	resource  string
	resPeers  map[string]int32 // what the .res names
	conns     map[int32]string // kernel connections: node-id → peer name
	forgotten []int32
	calls     []string

	// afterFirstStatus, when set, changes the kernel after the first
	// status read, as a concurrent change between two reads would.
	afterFirstStatus func(conns map[int32]string)
	statusReads      int
}

func (f *kernelPeerFake) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	line := strings.Join(append([]string{name}, args...), " ")
	f.calls = append(f.calls, line)

	exit := func(out string, code int) ([]byte, error) {
		return []byte(out), errors.Wrap(storage.FakeExitError{Code: code}, out)
	}

	switch {
	case name == "drbdsetup" && len(args) == 3 && args[0] == "status":
		return f.status()
	case name == "drbdadm" && len(args) == 2 && (args[0] == "disconnect" || args[0] == "del-peer"):
		peer, _, _ := strings.Cut(args[1], ":")

		id, named := f.resPeers[peer]
		if !named {
			return exit("'"+args[1]+"' not defined in your config (for this host).", drbdadmConfigExit)
		}

		if args[0] == "del-peer" {
			delete(f.conns, id)
		}

		return nil, nil
	case name == "drbdsetup" && len(args) == 3 && (args[0] == "del-peer" || args[0] == "disconnect"):
		id := parseNodeID(args[2])
		if _, ok := f.conns[id]; !ok {
			return exit(args[1]+": Failure: (158) Unknown connection", drbdsetupRefuseExit)
		}

		if args[0] == "del-peer" {
			delete(f.conns, id)
		}

		return nil, nil
	case name == "drbdsetup" && len(args) == 3 && args[0] == "forget-peer":
		id := parseNodeID(args[2])
		if _, ok := f.conns[id]; ok {
			return exit(args[1]+": Failure: (125) Device has a net-config (use disconnect first)", drbdsetupRefuseExit)
		}

		f.forgotten = append(f.forgotten, id)

		return nil, nil
	case len(args) > 0 && (args[0] == "down" || args[0] == "detach"):
		return exit("refusing "+line+" in this fake", drbdadmConfigExit)
	}

	return exit("unexpected command: "+line, drbdadmConfigExit)
}

func (f *kernelPeerFake) RunWithStdin(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	return f.Run(ctx, name, args...)
}

func parseNodeID(s string) int32 {
	n, err := strconv.Atoi(s)
	if err != nil {
		return -1
	}

	return int32(n)
}

func (f *kernelPeerFake) status() ([]byte, error) {
	conns := make([]map[string]any, 0, len(f.conns))

	for _, id := range slices.Sorted(maps.Keys(f.conns)) {
		conns = append(conns, map[string]any{
			"peer-node-id":     id,
			"name":             f.conns[id],
			"connection-state": "Connecting",
			"peer_devices":     []any{},
		})
	}

	out, err := json.Marshal([]map[string]any{{"name": f.resource, "connections": conns}})

	f.statusReads++
	if f.statusReads == 1 && f.afterFirstStatus != nil {
		f.afterFirstStatus(f.conns)
	}

	return out, errors.Wrap(err, "marshal status")
}

// newDepartedPeerFixture is a diskful replica on worker-0 whose .res has
// already been rewritten without worker-2 (node-id 2), deleted, while
// the kernel still holds worker-2's connection.
func newDepartedPeerFixture(t *testing.T) (*Reconciler, *kernelPeerFake) {
	t.Helper()

	fake := &kernelPeerFake{
		resource: "pvc-1",
		resPeers: map[string]int32{"worker-1": 1},
		conns:    map[int32]string{1: "worker-1", 2: "worker-2"},
	}

	stateDir := t.TempDir()
	writeOwnedSlot(t, stateDir, "pvc-1", fake.resPeers)

	r := NewReconciler(ReconcilerConfig{NodeName: "worker-0", Adm: drbd.NewAdm(fake), StateDir: stateDir})

	return r, fake
}

// departedPeerExpectation is the observed set once worker-2's Resource
// is gone: worker-1 alone, allocated.
func departedPeerExpectation() PeerExpectation {
	return PeerExpectation{
		PeerNames:        []string{"worker-1"},
		AllocatedNodeIDs: []int32{0, 1},
		Allocated:        true,
	}
}

// A deleted peer's slot is forgotten once the .res no longer names it:
// the connection goes by node-id first, which drbdadm cannot do any more.
// Before, del-peer through drbdadm reported success without removing
// anything and forget-peer failed with (125) on every deletion.
func TestDepartedPeerSlotIsForgottenAfterTheResStopsNamingIt(t *testing.T) {
	t.Parallel()

	r, fake := newDepartedPeerFixture(t)

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1", departedPeerExpectation(), []int32{0}, nil)
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}

	if _, ok := fake.conns[2]; ok {
		t.Errorf("worker-2's connection survived; calls: %v", fake.calls)
	}

	if !slices.Equal(fake.forgotten, []int32{2}) {
		t.Errorf("forgotten node-ids = %v, want [2]; calls: %v", fake.forgotten, fake.calls)
	}

	if fake.conns[1] != "worker-1" {
		t.Errorf("worker-1's connection was touched; calls: %v", fake.calls)
	}

	for _, c := range fake.calls {
		touchesWorker1 := strings.HasSuffix(c, "pvc-1 1") || strings.Contains(c, "worker-1:")
		if touchesWorker1 || strings.Contains(c, "down") || strings.Contains(c, "detach") {
			t.Errorf("unexpected call %q", c)
		}
	}
}

// A node-id another peer holds by the time of the del-peer is not cut:
// the kernel is read again before anything is removed by node-id.
func TestDepartedPeerNodeIDTakenOverIsLeftAlone(t *testing.T) {
	t.Parallel()

	r, fake := newDepartedPeerFixture(t)
	fake.afterFirstStatus = func(conns map[int32]string) {
		conns[2] = "worker-1"
		delete(conns, 1)
	}

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1", departedPeerExpectation(), []int32{0}, nil)
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}

	if fake.conns[2] != "worker-1" || len(fake.forgotten) != 0 {
		t.Errorf("node-id 2, now worker-1's, was touched: conns %v, forgotten %v; calls: %v", fake.conns, fake.forgotten, fake.calls)
	}
}

// A connection already gone by the second read still has its slot
// forgotten.
func TestDepartedPeerAlreadyDisconnectedIsForgotten(t *testing.T) {
	t.Parallel()

	r, fake := newDepartedPeerFixture(t)
	fake.afterFirstStatus = func(conns map[int32]string) {
		delete(conns, 2)
	}

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1", departedPeerExpectation(), []int32{0}, nil)
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}

	if !slices.Equal(fake.forgotten, []int32{2}) {
		t.Errorf("forgotten node-ids = %v, want [2]; calls: %v", fake.forgotten, fake.calls)
	}

	if slices.Contains(fake.calls, "drbdsetup del-peer pvc-1 2") {
		t.Errorf("del-peer ran for a connection already gone; calls: %v", fake.calls)
	}
}
