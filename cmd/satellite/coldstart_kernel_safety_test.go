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

package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// Cold-start kernel-safety unit tests (P0).
//
// The classifier resourceHealthyFromStatus is the gate that protects a
// healthy replica from being torn down when the satellite pod restarts.
// The fixtures below are captured verbatim from a real satellite on the
// QEMU stand via `kubectl exec ds/blockstor-satellite -- drbdsetup
// status -j <res>` (drbd-utils 9.x), with the SyncTarget / StandAlone /
// Diskless variants synthesised by editing only the relevant state
// fields — the surrounding JSON shape is real.

// statusUpToDateConnected is the steady-state 2-replica case captured
// from worker-1: local UpToDate, peer Connected + Established.
const statusUpToDateConnected = `[
{
  "name": "pvc-1",
  "node-id": 0,
  "role": "Secondary",
  "devices": [
    { "volume": 0, "minor": 20000, "disk-state": "UpToDate" }
  ],
  "connections": [
    {
      "peer-node-id": 1,
      "name": "worker-2",
      "connection-state": "Connected",
      "peer-role": "Secondary",
      "peer_devices": [
        { "volume": 0, "replication-state": "Established", "peer-disk-state": "UpToDate" }
      ]
    } ]
}
]`

// statusSyncTargetMidSync is the P0 scenario: this replica is mid
// initial-sync as SyncTarget. Its local disk is Inconsistent (it has
// not finished receiving) but it is actively replicating — tearing it
// down at cold start loses sync progress and races the reconciler.
const statusSyncTargetMidSync = `[
{
  "name": "pvc-1",
  "node-id": 0,
  "role": "Secondary",
  "devices": [
    { "volume": 0, "minor": 20000, "disk-state": "Inconsistent" }
  ],
  "connections": [
    {
      "peer-node-id": 1,
      "name": "worker-2",
      "connection-state": "Connected",
      "peer-role": "Secondary",
      "peer_devices": [
        { "volume": 0, "replication-state": "SyncTarget", "peer-disk-state": "UpToDate" }
      ]
    } ]
}
]`

// statusSyncSourceMidSync — the opposite end of an initial sync: local
// UpToDate, feeding a SyncTarget peer. Must survive (the data is here).
const statusSyncSourceMidSync = `[
{
  "name": "pvc-1",
  "node-id": 0,
  "role": "Primary",
  "devices": [
    { "volume": 0, "minor": 20000, "disk-state": "UpToDate" }
  ],
  "connections": [
    {
      "peer-node-id": 1,
      "name": "worker-2",
      "connection-state": "Connected",
      "peer-role": "Secondary",
      "peer_devices": [
        { "volume": 0, "replication-state": "SyncSource", "peer-disk-state": "Inconsistent" }
      ]
    } ]
}
]`

// statusDisklessStandAlone is the orphan signature: no local disk and a
// StandAlone connection with no active replication. Nothing here for
// the reconciler to recover; safe to down at cold start.
const statusDisklessStandAlone = `[
{
  "name": "pvc-1",
  "node-id": 0,
  "role": "Secondary",
  "devices": [
    { "volume": 0, "minor": 20000, "disk-state": "Diskless" }
  ],
  "connections": [
    {
      "peer-node-id": 1,
      "name": "worker-2",
      "connection-state": "StandAlone",
      "peer-role": "Unknown",
      "peer_devices": [
        { "volume": 0, "replication-state": "Off", "peer-disk-state": "DUnknown" }
      ]
    } ]
}
]`

// statusOutdatedNoPeer — local Outdated disk, no connection at all
// (peer never came back). Outdated still holds recoverable data the
// reconciler can bring back via resync, so it must survive.
const statusOutdatedNoPeer = `[
{
  "name": "pvc-1",
  "node-id": 0,
  "role": "Secondary",
  "devices": [
    { "volume": 0, "minor": 20000, "disk-state": "Outdated" }
  ],
  "connections": []
}
]`

// statusDisklessConnecting — diskless local, peer only Connecting (no
// active session). True orphan from this node's perspective: no data,
// no live peer.
const statusDisklessConnecting = `[
{
  "name": "pvc-1",
  "node-id": 0,
  "role": "Secondary",
  "devices": [
    { "volume": 0, "minor": 20000, "disk-state": "Diskless" }
  ],
  "connections": [
    {
      "peer-node-id": 1,
      "name": "worker-2",
      "connection-state": "Connecting",
      "peer_devices": []
    } ]
}
]`

// statusFailedNoPeer — local backing device Failed, no peer. The disk
// is gone and there is no live peer; reaping is correct (the reconciler
// will re-create via new-minor once the device returns).
const statusFailedNoPeer = `[
{
  "name": "pvc-1",
  "node-id": 0,
  "role": "Secondary",
  "devices": [
    { "volume": 0, "minor": 20000, "disk-state": "Failed" }
  ],
  "connections": []
}
]`

func mustParseStatus(t *testing.T, raw string) *statusResource {
	t.Helper()

	var root []statusResource
	if err := json.Unmarshal([]byte(raw), &root); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}

	if len(root) == 0 {
		t.Fatalf("fixture parsed to empty array")
	}

	return &root[0]
}

func TestResourceHealthyFromStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		json string
		want bool
	}{
		{"uptodate+connected → healthy", statusUpToDateConnected, true},
		// THE P0 case: a SyncTarget mid initial-sync (local Inconsistent)
		// MUST be classified healthy so cold-start leaves it alone.
		{"synctarget mid-sync (Inconsistent) → healthy", statusSyncTargetMidSync, true},
		{"syncsource mid-sync → healthy", statusSyncSourceMidSync, true},
		{"outdated, no peer → healthy (recoverable data)", statusOutdatedNoPeer, true},
		{"diskless + StandAlone, no session → orphan", statusDisklessStandAlone, false},
		{"diskless + Connecting, no session → orphan", statusDisklessConnecting, false},
		{"failed disk, no peer → orphan", statusFailedNoPeer, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := mustParseStatus(t, tc.json)
			if got := resourceHealthyFromStatus(res); got != tc.want {
				t.Errorf("resourceHealthyFromStatus = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResourceHealthyFromStatus_EmptyResource — a resource with no
// devices and no connections (kernel object created but nothing
// attached) is not healthy; nothing to protect.
func TestResourceHealthyFromStatus_EmptyResource(t *testing.T) {
	t.Parallel()

	res := &statusResource{}
	if resourceHealthyFromStatus(res) {
		t.Errorf("empty resource must be classified orphan")
	}
}

// TestResourceHealthyFromStatus_MultiVolumePartial — a multi-volume
// resource where ONE volume still holds data must be protected as a
// whole; we never down a resource that has any recoverable volume.
func TestResourceHealthyFromStatus_MultiVolumePartial(t *testing.T) {
	t.Parallel()

	res := &statusResource{
		Devices: []statusVolume{
			{DiskState: "Diskless"},
			{DiskState: "UpToDate"},
		},
	}
	if !resourceHealthyFromStatus(res) {
		t.Errorf("resource with one UpToDate volume must be healthy")
	}
}

func TestReplicationStateIsLive(t *testing.T) {
	t.Parallel()

	live := []string{"Established", "SyncSource", "SyncTarget", "PausedSyncS", "PausedSyncT", "VerifyS", "VerifyT", "WFBitMapS"}
	for _, s := range live {
		if !replicationStateIsLive(s) {
			t.Errorf("replicationStateIsLive(%q) = false, want true", s)
		}
	}

	dead := []string{"", "Off"}
	for _, s := range dead {
		if replicationStateIsLive(s) {
			t.Errorf("replicationStateIsLive(%q) = true, want false", s)
		}
	}
}

// TestCleanStateDir_PreservesHealthyRes is the .res half of the P0 fix:
// the .res file backing a healthy kernel resource must survive the
// cold-start wipe so an interim `drbdadm adjust` has something to read,
// while orphan .res files are still removed.
func TestCleanStateDir_PreservesHealthyRes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := []string{"pvc-healthy.res", "pvc-orphan.res", "global_common.conf"}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatalf("seed %s: %v", f, err)
		}
	}

	healthy := map[string]struct{}{"pvc-healthy": {}}

	cleanStateDir(dir, healthy, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	assertExists := func(name string, want bool) {
		_, err := os.Stat(filepath.Join(dir, name))
		got := err == nil
		if got != want {
			t.Errorf("%s exists=%v, want %v", name, got, want)
		}
	}

	assertExists("pvc-healthy.res", true)    // preserved: backs a healthy resource
	assertExists("pvc-orphan.res", false)    // wiped: no matching live kernel resource
	assertExists("global_common.conf", true) // never touched: not a .res file
}

// TestCleanStateDir_EmptyHealthyWipesAll — when nothing is healthy
// (fresh kernel / all orphans), every .res is wiped, matching the
// pre-fix behaviour for the orphan-only case.
func TestCleanStateDir_EmptyHealthyWipesAll(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for _, f := range []string{"a.res", "b.res"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	cleanStateDir(dir, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	for _, f := range []string{"a.res", "b.res"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			t.Errorf("%s should have been wiped", f)
		}
	}
}
