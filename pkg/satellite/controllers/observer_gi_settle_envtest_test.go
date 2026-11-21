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
	"strings"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// giReads counts the get-gi runs fx has seen.
func giReads(fx *storage.FakeExec) int {
	n := 0

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, " get-gi ") {
			n++
		}
	}

	return n
}

// A replica whose kernel state holds still has its GI read from disk
// only until the read has outlasted DRBD's metadata sync, and again
// once a frame says the state moved. Re-reading every replica on every
// resync opened each backing device read-write every 5 s; with a
// hundred replicas that was the satellite's whole CPU limit, with the
// disk rescans udev's change uevents set off, and a new replica built
// at a fraction of a core.
func TestObserverStopsReadingASettledGI(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	fx := statusTestExec()
	clock := time.Now()
	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode, clock: func() time.Time { return clock }}
	adm := drbd.NewAdm(fx)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())

	reads := giReads(fx)

	o.resyncOnce(ctx)

	if got := giReads(fx) - reads; got != 1 {
		t.Fatalf("resync read a GI that may still be settling %d times, want once", got)
	}

	clock = clock.Add(giSettleWindow)
	reads = giReads(fx)

	for range 3 {
		o.resyncOnce(ctx)
	}

	if got := giReads(fx) - reads; got != 0 {
		t.Fatalf("resync read a settled GI %d times", got)
	}

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != statusTestGI {
		t.Fatalf("settled GI not published: %q", got)
	}

	o.handleObservation(ctx, adm, peerConnected("n2"))

	reads = giReads(fx)

	o.resyncOnce(ctx)

	if got := giReads(fx) - reads; got != 1 {
		t.Fatalf("resync read the GI of a replica whose state moved %d times, want once", got)
	}
}

// A GI that could not be read is tried again on every resync, however
// long the replica has held still: until it is read, a new replica's
// seed decision has nothing to go on.
func TestObserverKeepsReadingAGIItHasNot(t *testing.T) {
	cli := startStatusEnvtest(t)
	createStatusReplica(t, cli)

	fx := storage.NewFakeExec()
	fx.Expect("drbdmeta --force - v09 "+statusTestBacking+" internal get-gi --node-id 0",
		storage.FakeResponse{Err: storage.FakeExitError{Code: 1}})

	clock := time.Now()
	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode, clock: func() time.Time { return clock }}
	adm := drbd.NewAdm(fx)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())

	clock = clock.Add(giSettleWindow)
	reads := giReads(fx)

	o.resyncOnce(ctx)

	if got := giReads(fx) - reads; got != 1 {
		t.Fatalf("resync read a GI it never had %d times, want once", got)
	}
}
