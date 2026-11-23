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

package drbd_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

const kernelStatusKey = "drbdsetup status --json"

// errExit10 is drbdsetup's exit on a kernel with no resource.
var errExit10 = errors.New("exit status 10")

// kernelStatusSample is `drbdsetup status --json` of three resources in
// the shapes the workarounds look for, field spelling as drbd-utils 9.36
// prints it.
const kernelStatusSample = `[
{"name":"pvc-idle","node-id":0,"role":"Primary","suspended":false,"force-io-failures":false,
 "devices":[{"volume":0,"minor":1000,"disk-state":"UpToDate","client":false,"open":false,"quorum":true}],
 "connections":[]},
{"name":"pvc-stall","node-id":2,"role":"Secondary","suspended":false,"force-io-failures":false,
 "devices":[{"volume":0,"minor":1001,"disk-state":"Inconsistent","client":false,"open":false,"quorum":true}],
 "connections":[
  {"peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
   "peer_devices":[{"volume":0,"replication-state":"SyncTarget","peer-disk-state":"UpToDate","resync-suspended":"no",
     "received":113448,"sent":0,"out-of-sync":14592,"pending":0,"unacked":542,"percent-in-sync":42.17}]},
  {"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
   "peer_devices":[{"volume":0,"replication-state":"PausedSyncT","peer-disk-state":"UpToDate","resync-suspended":"dependency",
     "received":11840,"sent":0,"out-of-sync":2122972,"pending":0,"unacked":70,"percent-in-sync":0.58}]}]},
{"name":"pvc-failio","node-id":1,"role":"Secondary","suspended":false,"force-io-failures":true,
 "devices":[{"volume":0,"minor":1002,"disk-state":"Outdated","client":false,"open":false,"quorum":true}],
 "connections":[]}
]`

func kernelStatus(t *testing.T, stdout string) []drbd.KernelResource {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect(kernelStatusKey, storage.FakeResponse{Stdout: []byte(stdout)})

	resources, err := drbd.NewAdm(fx).KernelStatus(t.Context())
	if err != nil {
		t.Fatalf("KernelStatus: %v", err)
	}

	return resources
}

func TestKernelStatusReadsTheWorkaroundShapes(t *testing.T) {
	t.Parallel()

	resources := kernelStatus(t, kernelStatusSample)
	if len(resources) != 3 {
		t.Fatalf("%d resources, want 3", len(resources))
	}

	idle, stall, failio := &resources[0], &resources[1], &resources[2]

	if !idle.IdlePrimary() || stall.IdlePrimary() || failio.IdlePrimary() {
		t.Errorf("IdlePrimary: %v, %v, %v, want idle true, stall false, failio false",
			idle.IdlePrimary(), stall.IdlePrimary(), failio.IdlePrimary())
	}

	if !failio.UnusedSecondaryFailingIO() || idle.UnusedSecondaryFailingIO() || stall.UnusedSecondaryFailingIO() {
		t.Errorf("UnusedSecondaryFailingIO: %v, %v, %v, want idle false, stall false, failio true",
			idle.UnusedSecondaryFailingIO(), stall.UnusedSecondaryFailingIO(), failio.UnusedSecondaryFailingIO())
	}

	dep, ok := stall.ResyncDependency()

	want := drbd.ResyncDependency{
		Volume: 0, Active: "n1", Paused: "n2", PercentInSync: 42.17,
		OutOfSync: 14592, Received: 113448, Outstanding: 542,
	}
	if !ok || dep != want {
		t.Errorf("ResyncDependency = %+v, %v; want %+v", dep, ok, want)
	}

	if _, ok := idle.ResyncDependency(); ok {
		t.Error("an UpToDate resource has a resync dependency")
	}

	if !stall.Inconsistent() || idle.Inconsistent() {
		t.Errorf("Inconsistent: stall %v, idle %v; want true, false", stall.Inconsistent(), idle.Inconsistent())
	}
}

// drbd-utils releases that print flags as "yes"/"no" read the same.
func TestKernelStatusReadsYesNoFlags(t *testing.T) {
	t.Parallel()

	resources := kernelStatus(t, `[{"name":"r","role":"Secondary","force-io-failures":"yes",
 "devices":[{"volume":0,"disk-state":"UpToDate","open":"no"}]},
{"name":"s","role":"Primary","force-io-failures":"no","devices":[{"volume":0,"disk-state":"UpToDate","open":"yes"}]}]`)

	if !resources[0].UnusedSecondaryFailingIO() {
		t.Error(`"yes"/"no" flags: the Secondary failing I/O is not seen`)
	}

	if resources[1].IdlePrimary() {
		t.Error(`"yes"/"no" flags: an open Primary reads as idle`)
	}
}

// A resource in use is never idle, never a Secondary to take down, and a
// resync nothing waits on, or one drbd-utils reports no statistics of, is
// not the stall shape.
func TestKernelStatusLeavesResourcesInUseAlone(t *testing.T) {
	t.Parallel()

	resources := kernelStatus(t, `[
{"name":"open-primary","role":"Primary","devices":[{"volume":0,"disk-state":"UpToDate","open":false},{"volume":1,"disk-state":"UpToDate","open":true}]},
{"name":"open-failio","role":"Secondary","force-io-failures":true,"devices":[{"volume":0,"disk-state":"Outdated","open":true}]},
{"name":"lone-resync","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent","open":false}],
 "connections":[{"name":"n1","peer_devices":[{"volume":0,"replication-state":"SyncTarget","resync-suspended":"no","out-of-sync":8,"percent-in-sync":10}]}]},
{"name":"no-statistics","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent","open":false}],
 "connections":[{"name":"n1","peer_devices":[{"volume":0,"replication-state":"SyncTarget","resync-suspended":"no"}]},
  {"name":"n2","peer_devices":[{"volume":0,"replication-state":"PausedSyncT","resync-suspended":"peer,dependency"}]}]}
]`)

	if resources[0].IdlePrimary() {
		t.Error("a Primary with one open volume reads as idle")
	}

	if resources[1].UnusedSecondaryFailingIO() {
		t.Error("a Secondary failing I/O to an opener reads as unused")
	}

	if _, ok := resources[2].ResyncDependency(); ok {
		t.Error("a resync no other peer waits on reads as the stall shape")
	}

	if _, ok := resources[3].ResyncDependency(); ok {
		t.Error("a SyncTarget without out-of-sync reads as the stall shape")
	}
}

func TestKernelStatusEmptyKernel(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect(kernelStatusKey, storage.FakeResponse{
		Stdout: []byte("No currently configured DRBD found.\n"),
		Err:    errExit10,
	})

	resources, err := drbd.NewAdm(fx).KernelStatus(t.Context())
	if err != nil || len(resources) != 0 {
		t.Errorf("KernelStatus on an empty kernel = %v, %v; want nothing", resources, err)
	}
}

func TestPeerConnectionVerbs(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.DisconnectPeer(t.Context(), "pvc-stall", "n1"); err != nil {
		t.Fatal(err)
	}

	if err := adm.ConnectPeer(t.Context(), "pvc-stall", "n1"); err != nil {
		t.Fatal(err)
	}

	got := fx.CommandLines()
	if len(got) != 2 || got[0] != "drbdadm disconnect pvc-stall:n1" || got[1] != "drbdadm connect pvc-stall:n1" {
		t.Errorf("commands = %q", got)
	}
}

// A Primary peer that reaches no other UpToDate disk depends on this
// replica; one with an UpToDate disk of its own, or another UpToDate
// peer, or one not connected, does not.
func TestKernelStatusSoleUpToDateOfAPrimaryPeer(t *testing.T) {
	t.Parallel()

	resources := kernelStatus(t, `[
{"name":"sole","role":"Secondary","devices":[{"volume":0,"disk-state":"UpToDate"}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Primary",
  "peer_devices":[{"volume":0,"peer-disk-state":"Diskless"}]},
  {"name":"n2","connection-state":"Connecting","peer-role":"Unknown",
  "peer_devices":[{"volume":0,"peer-disk-state":"DUnknown"}]}]},
{"name":"primary-has-disk","role":"Secondary","devices":[{"volume":0,"disk-state":"UpToDate"}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Primary",
  "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]}]},
{"name":"other-source","role":"Secondary","devices":[{"volume":0,"disk-state":"UpToDate"}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Primary",
  "peer_devices":[{"volume":0,"peer-disk-state":"Diskless"}]},
  {"name":"n2","connection-state":"Connected","peer-role":"Secondary",
  "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]}]},
{"name":"stale-here","role":"Secondary","devices":[{"volume":0,"disk-state":"Outdated"}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Primary",
  "peer_devices":[{"volume":0,"peer-disk-state":"Diskless"}]}]},
{"name":"no-primary","role":"Secondary","devices":[{"volume":0,"disk-state":"UpToDate"}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Secondary",
  "peer_devices":[{"volume":0,"peer-disk-state":"Diskless"}]}]}
]`)

	want := []bool{true, false, false, false, false}
	for i := range resources {
		if got := resources[i].SoleUpToDateOfAPrimaryPeer(); got != want[i] {
			t.Errorf("%s: SoleUpToDateOfAPrimaryPeer = %v, want %v", resources[i].Name, got, want[i])
		}
	}
}

// The resync dependency is read per volume: a SyncTarget of one volume
// and a dependency pause of another are not the shape.
func TestKernelStatusResyncDependencyIsPerVolume(t *testing.T) {
	t.Parallel()

	resources := kernelStatus(t, `[{"name":"r","role":"Secondary",
 "devices":[{"volume":0,"disk-state":"Inconsistent"},{"volume":1,"disk-state":"Inconsistent"}],
 "connections":[
  {"name":"n1","peer_devices":[{"volume":0,"replication-state":"SyncTarget","resync-suspended":"no","out-of-sync":8},
   {"volume":1,"replication-state":"Established","resync-suspended":"no","out-of-sync":0}]},
  {"name":"n2","peer_devices":[{"volume":0,"replication-state":"Established","resync-suspended":"no","out-of-sync":0},
   {"volume":1,"replication-state":"PausedSyncT","resync-suspended":"dependency","out-of-sync":8}]}]}]`)

	if dep, ok := resources[0].ResyncDependency(); ok {
		t.Errorf("volumes mixed into one dependency: %+v", dep)
	}
}

// Dropping a peer is refused only where a Primary, here or the peer,
// would be left with no UpToDate disk among this node and its other
// connected peers.
func TestKernelStatusMayDisconnect(t *testing.T) {
	t.Parallel()

	// Shapes of the field stalls: this node Inconsistent, SyncTarget of
	// a and paused toward b.
	resources := kernelStatus(t, `[
{"name":"secondaries","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent"}],
 "connections":[{"name":"a","connection-state":"Connected","peer-role":"Secondary","peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]},
  {"name":"b","connection-state":"Connected","peer-role":"Secondary","peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]}]},
{"name":"primary-peer-with-another","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent"}],
 "connections":[{"name":"a","connection-state":"Connected","peer-role":"Primary","peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]},
  {"name":"b","connection-state":"Connected","peer-role":"Secondary","peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]}]},
{"name":"primary-peer-alone","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent"}],
 "connections":[{"name":"a","connection-state":"Connected","peer-role":"Primary","peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]},
  {"name":"b","connection-state":"Connecting","peer-role":"Unknown","peer_devices":[{"volume":0,"peer-disk-state":"DUnknown"}]}]},
{"name":"local-primary","role":"Primary","devices":[{"volume":0,"disk-state":"Inconsistent"}],
 "connections":[{"name":"a","connection-state":"Connected","peer-role":"Secondary","peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]},
  {"name":"b","connection-state":"Connected","peer-role":"Secondary","peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent"}]}]}
]`)

	cases := []struct {
		res  int
		peer string
		want bool
	}{
		{0, "a", true},
		{0, "b", true},
		// The Primary keeps b's copy.
		{1, "a", true},
		// b is no Primary, and this node is none either.
		{1, "b", true},
		// The Primary has no other UpToDate disk this node sees.
		{2, "a", false},
		// A local Inconsistent Primary reads from a alone.
		{3, "a", false},
		{3, "b", true},
	}

	for _, c := range cases {
		if got := resources[c.res].MayDisconnect(c.peer); got != c.want {
			t.Errorf("%s: MayDisconnect(%s) = %v, want %v", resources[c.res].Name, c.peer, got, c.want)
		}
	}
}
