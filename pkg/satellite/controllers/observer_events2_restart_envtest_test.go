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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The replica mid-resync from n2 and cut off from n3, as the first
// stream last saw it before it ended.
const events2BeforeTheStreamEnded = `exists resource name:pvc-status role:Secondary suspended:no
exists device name:pvc-status volume:0 minor:1000 disk:Inconsistent client:no quorum:yes
exists connection name:pvc-status peer-node-id:1 conn-name:n2 connection:Connected role:Secondary
exists connection name:pvc-status peer-node-id:2 conn-name:n3 connection:StandAlone role:Unknown
exists peer-device name:pvc-status peer-node-id:1 conn-name:n2 volume:0 replication:SyncTarget peer-disk:UpToDate done:12.50
exists -
`

// What the kernel holds by the time a new stream dumps it: the resync
// is over and n3 is gone.
const events2AfterTheRestart = `exists resource name:pvc-status role:Secondary suspended:no
exists device name:pvc-status volume:0 minor:1000 disk:UpToDate client:no quorum:yes
exists connection name:pvc-status peer-node-id:1 conn-name:n2 connection:Connected role:Secondary
exists peer-device name:pvc-status peer-node-id:1 conn-name:n2 volume:0 replication:Established peer-disk:UpToDate
exists -
`

// drbdsetup events2 exits when the kernel drops notifications it had
// no room for. The observer then stopped reading while its resync kept
// writing the last state it had: after a node's reboot, replicas that
// were mid-resync or still attaching at that moment stayed SyncTarget
// or StandAlone ... Attaching in `r l` with the kernel long UpToDate.
// It must start a new stream, as LINSTOR does, and forget what the old
// one said.
func TestObserverRestartsAnEndedEvents2Stream(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	live, feed := io.Pipe()
	t.Cleanup(func() { _ = feed.Close() })

	var starts atomic.Int64

	o := &ObserverRunnable{
		Client:       cli,
		Exec:         storage.NewFakeExec(),
		NodeName:     statusTestNode,
		restartDelay: 10 * time.Millisecond,
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			if starts.Add(1) == 1 {
				return drbd.NewWatcher(strings.NewReader(events2BeforeTheStreamEnded)), func() {}, nil
			}

			go func() { _, _ = io.WriteString(feed, events2AfterTheRestart) }()

			return drbd.NewWatcher(live), func() {}, nil
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- o.Start(ctx) }()

	t.Cleanup(func() {
		cancel()
		_ = feed.Close()
		<-done
	})

	deadline := time.Now().Add(10 * time.Second)

	for {
		status := getStatusReplica(t, cli, name)
		if restartedView(status) {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("status still shows the ended stream's state (starts=%d): volumes=%+v connections=%+v",
				starts.Load(), status.Volumes, status.Connections)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func restartedView(status blockstoriov1alpha1.ResourceStatus) bool {
	if len(status.Volumes) != 1 || status.Volumes[0].DiskState != "UpToDate" {
		return false
	}

	if len(status.Connections) != 1 || status.Connections[0].PeerNodeName != "n2" {
		return false
	}

	conn := status.Connections[0]

	return conn.ReplicationState == "Established" && len(conn.PeerVolumes) == 1 &&
		conn.PeerVolumes[0].PeerDiskState == "UpToDate"
}
