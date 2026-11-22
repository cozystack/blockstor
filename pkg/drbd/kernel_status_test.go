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
   "peer_devices":[{"volume":0,"replication-state":"SyncTarget","peer-disk-state":"UpToDate","resync-suspended":"no","percent-in-sync":42.17}]},
  {"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
   "peer_devices":[{"volume":0,"replication-state":"PausedSyncT","peer-disk-state":"UpToDate","resync-suspended":"dependency","percent-in-sync":42.17}]}]},
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

	from, ok := stall.ResyncSource()
	if !ok || from.Peer != "n1" || from.PercentInSync != 42.17 {
		t.Errorf("ResyncSource = %+v, %v; want n1 at 42.17", from, ok)
	}

	if _, ok := idle.ResyncSource(); ok {
		t.Error("an UpToDate resource has a resync source")
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
// resync nothing waits on is not the stall.
func TestKernelStatusLeavesResourcesInUseAlone(t *testing.T) {
	t.Parallel()

	resources := kernelStatus(t, `[
{"name":"open-primary","role":"Primary","devices":[{"volume":0,"disk-state":"UpToDate","open":false},{"volume":1,"disk-state":"UpToDate","open":true}]},
{"name":"open-failio","role":"Secondary","force-io-failures":true,"devices":[{"volume":0,"disk-state":"Outdated","open":true}]},
{"name":"lone-resync","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent","open":false}],
 "connections":[{"name":"n1","peer_devices":[{"volume":0,"replication-state":"SyncTarget","resync-suspended":"no","percent-in-sync":10}]}]},
{"name":"no-percent","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent","open":false}],
 "connections":[{"name":"n1","peer_devices":[{"volume":0,"replication-state":"SyncTarget","resync-suspended":"no"}]},
  {"name":"n2","peer_devices":[{"volume":0,"replication-state":"PausedSyncT","resync-suspended":"peer,dependency"}]}]}
]`)

	if resources[0].IdlePrimary() {
		t.Error("a Primary with one open volume reads as idle")
	}

	if resources[1].UnusedSecondaryFailingIO() {
		t.Error("a Secondary failing I/O to an opener reads as unused")
	}

	if _, ok := resources[2].ResyncSource(); ok {
		t.Error("a resync no other peer waits on reads as the stall")
	}

	if _, ok := resources[3].ResyncSource(); ok {
		t.Error("a SyncTarget without percent-in-sync reads as the stall")
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
