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

package view_test

import (
	"regexp"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/view"
)

func ptrBool(b bool) *bool { return &b }

// diskful builds a replica with one volume in the given disk state.
func diskful(node, state string, outOfSync int64) apiv1.Resource {
	return apiv1.Resource{
		Name:     "pvc-x",
		NodeName: node,
		State:    apiv1.ResourceState{InUse: ptrBool(false)},
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: state, OutOfSyncKib: outOfSync},
		}},
		LayerObject: &apiv1.ResourceLayer{
			Type: "DRBD",
			Drbd: &apiv1.DrbdResourceLayer{
				TCPPorts:    []int32{7001},
				Connections: map[string]apiv1.DrbdConnection{"other": {Connected: true, Message: "Connected"}},
			},
		},
		CreateTimestamp: 1_784_000_000_000,
	}
}

// The column set and its ORDER are a contract with this repository's
// shell harnesses, which read `awk -F'|'` fields by index. Changing
// either silently makes those scripts read the wrong cell.
func TestResourceListColumns(t *testing.T) {
	t.Parallel()

	tbl := view.ResourceList(view.ResourceListInput{
		Resources: []apiv1.Resource{diskful("node-1", "UpToDate", 0)},
	})

	want := []string{"ResourceName", "Node", "Port", "Usage", "Conns", "State", "CreatedOn"}
	if len(tbl.ColumnDefinitions) != len(want) {
		t.Fatalf("columns = %d, want %d", len(tbl.ColumnDefinitions), len(want))
	}

	for i, name := range want {
		if tbl.ColumnDefinitions[i].Name != name {
			t.Errorf("column[%d] = %q, want %q", i, tbl.ColumnDefinitions[i].Name, name)
		}
	}
}

// Usage is a tri-state: the satellite may not have reported yet, and
// an unreported replica must render blank rather than claiming to be
// unused.
func TestUsageTriState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		inUse *bool
		want  string
	}{
		{ptrBool(true), "InUse"},
		{ptrBool(false), "Unused"},
		{nil, ""},
	}

	for _, tc := range cases {
		res := diskful("node-1", "UpToDate", 0)
		res.State.InUse = tc.inUse

		got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{res}}), 0, "Usage")
		if got != tc.want {
			t.Errorf("Usage for InUse=%v = %q, want %q", tc.inUse, got, tc.want)
		}
	}
}

// The State column carries several hard contracts, each asserted by a
// script in this repo:
//   - a tie-breaker renders the literal `TieBreaker` (case-sensitive);
//   - a replica under deletion renders `DELETING`;
//   - a converged replica renders `UpToDate` with NO percentage —
//     the harness greps for the absence of `UpToDate(NN%)`;
//   - a replica in SyncTarget carries DRBD's `done` for that resync,
//     and no other state carries a percentage.
func TestStateColumnContracts(t *testing.T) {
	t.Parallel()

	t.Run("tiebreaker renders the literal token", func(t *testing.T) {
		t.Parallel()

		res := diskful("node-3", "", 0)
		res.Volumes = nil
		res.Flags = []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}

		got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{res}}), 0, "State")
		if got != "TieBreaker" {
			t.Errorf("State = %q, want exactly %q", got, "TieBreaker")
		}
	})

	t.Run("deleting replica", func(t *testing.T) {
		t.Parallel()

		res := diskful("node-1", "UpToDate", 0)
		res.Flags = []string{"DELETE"}

		got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{res}}), 0, "State")
		if got != "DELETING" {
			t.Errorf("State = %q, want %q", got, "DELETING")
		}
	})

	t.Run("converged replica carries no percentage", func(t *testing.T) {
		t.Parallel()

		got := cellAt(t, view.ResourceList(view.ResourceListInput{
			Resources: []apiv1.Resource{diskful("node-1", "UpToDate", 0)},
		}), 0, "State")

		if got != "UpToDate" {
			t.Errorf("State = %q, want bare %q", got, "UpToDate")
		}

		if regexp.MustCompile(`\([0-9]+%\)`).MatchString(got) {
			t.Errorf("State %q carries a sync percentage on a converged replica", got)
		}
	})

	t.Run("syncing replica carries the resync's done", func(t *testing.T) {
		t.Parallel()

		res := diskful("node-2", "Inconsistent", 512)
		done := 50.31
		res.Volumes[0].State.ReplicationStates = map[string]apiv1.ReplicationState{
			"node-1": {ReplicationState: "SyncTarget", DonePercentage: &done},
		}

		got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{res}}), 0, "State")

		if got != "SyncTarget(50.31%)" {
			t.Errorf("State = %q, want %q", got, "SyncTarget(50.31%)")
		}
	})

	// The live report: a replica finished a small resync from one peer
	// and then ran a full one from the Primary. The out-of-sync count
	// is the last peer frame's (here 1% of the volume), the kernel says
	// done:50.31 for the running resync. The column read
	// `Inconsistent(99%)`; LINSTOR shows `SyncTarget(50.31%)`.
	t.Run("resync from a second peer shows that resync, not the out-of-sync count", func(t *testing.T) {
		t.Parallel()

		res := diskful("node-3", "Inconsistent", 1)
		done := 50.31
		res.Volumes[0].State.ReplicationStates = map[string]apiv1.ReplicationState{
			"node-1": {ReplicationState: "SyncTarget", DonePercentage: &done},
			"node-2": {ReplicationState: "Established"},
		}

		got := cellAt(t, view.ResourceList(view.ResourceListInput{
			Resources:  []apiv1.Resource{res},
			FaultyOnly: true,
		}), 0, "State")

		if got != "SyncTarget(50.31%)" {
			t.Errorf("State = %q, want %q", got, "SyncTarget(50.31%)")
		}
	})

	t.Run("inconsistent replica with no running resync carries no percentage", func(t *testing.T) {
		t.Parallel()

		res := diskful("node-2", "Inconsistent", 512)

		got := cellAt(t, view.ResourceList(view.ResourceListInput{
			Resources: []apiv1.Resource{res},
		}), 0, "State")

		if got != "Inconsistent" {
			t.Errorf("State = %q, want bare %q", got, "Inconsistent")
		}
	})

	t.Run("diskless replica", func(t *testing.T) {
		t.Parallel()

		res := diskful("node-3", "", 0)
		res.Volumes = nil
		res.Flags = []string{apiv1.ResourceFlagDiskless}

		got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{res}}), 0, "State")
		if got != "Diskless" {
			t.Errorf("State = %q, want %q", got, "Diskless")
		}
	})

	t.Run("unreported replica is Unknown, never blank", func(t *testing.T) {
		t.Parallel()

		res := diskful("node-1", "", 0)
		res.Volumes = nil

		got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{res}}), 0, "State")
		if got != "Unknown" {
			t.Errorf("State = %q, want %q", got, "Unknown")
		}
	})
}

// `--faulty` keeps only replicas whose observed disk state is present
// and not converged; a healthy cluster prints an empty table rather
// than every replica.
func TestFaultyFilter(t *testing.T) {
	t.Parallel()

	in := view.ResourceListInput{
		Resources: []apiv1.Resource{
			diskful("node-1", "UpToDate", 0),
			diskful("node-2", "Inconsistent", 0),
		},
		FaultyOnly: true,
	}

	tbl := view.ResourceList(in)
	if len(tbl.Rows) != 1 {
		t.Fatalf("faulty rows = %d, want 1", len(tbl.Rows))
	}

	if got := cellAt(t, tbl, 0, "Node"); got != "node-2" {
		t.Errorf("faulty row is %q, want node-2", got)
	}

	in.FaultyOnly = false
	if got := len(view.ResourceList(in).Rows); got != 2 {
		t.Errorf("unfiltered rows = %d, want 2", got)
	}
}

// The Conns column summarises the peer connections; a fully-connected
// mesh reads `Ok`, and a broken peer surfaces its state rather than
// being hidden behind a healthy-looking summary.
func TestConnsColumn(t *testing.T) {
	t.Parallel()

	ok := diskful("node-1", "UpToDate", 0)
	if got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{ok}}), 0, "Conns"); got != "Ok" {
		t.Errorf("Conns = %q, want Ok", got)
	}

	broken := diskful("node-1", "UpToDate", 0)
	broken.LayerObject.Drbd.Connections = map[string]apiv1.DrbdConnection{
		"node-2": {Connected: false, Message: "StandAlone"},
	}

	if got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{broken}}), 0, "Conns"); got != "StandAlone(node-2)" {
		t.Errorf("Conns = %q, want StandAlone(node-2)", got)
	}
}

// CreatedOn renders the wall-clock format a harness greps for with
// `[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}`, and an
// unreported timestamp stays blank instead of printing the epoch.
func TestCreatedOnFormat(t *testing.T) {
	t.Parallel()

	got := cellAt(t, view.ResourceList(view.ResourceListInput{
		Resources: []apiv1.Resource{diskful("node-1", "UpToDate", 0)},
	}), 0, "CreatedOn")

	if !regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$`).MatchString(got) {
		t.Errorf("CreatedOn = %q, want YYYY-MM-DD HH:MM:SS", got)
	}

	zero := diskful("node-1", "UpToDate", 0)
	zero.CreateTimestamp = 0

	if got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{zero}}), 0, "CreatedOn"); got != "" {
		t.Errorf("CreatedOn for an unreported timestamp = %q, want blank", got)
	}
}

// Port comes from the replica's own DRBD layer — per-node since the
// allocator became per-replica.
func TestPortColumn(t *testing.T) {
	t.Parallel()

	got := cellAt(t, view.ResourceList(view.ResourceListInput{
		Resources: []apiv1.Resource{diskful("node-1", "UpToDate", 0)},
	}), 0, "Port")

	if got != "7001" {
		t.Errorf("Port = %q, want 7001", got)
	}
}

// cellAt returns the rendered cell text of a row by column name.
func cellAt(t *testing.T, tbl *metav1.Table, row int, column string) string {
	t.Helper()

	idx := -1

	for i := range tbl.ColumnDefinitions {
		if tbl.ColumnDefinitions[i].Name == column {
			idx = i

			break
		}
	}

	if idx < 0 {
		t.Fatalf("no column %q", column)
	}

	if row >= len(tbl.Rows) {
		t.Fatalf("row %d out of range (%d rows)", row, len(tbl.Rows))
	}

	cell := tbl.Rows[row].Cells[idx]
	if cell == nil {
		return ""
	}

	s, ok := cell.(string)
	if !ok {
		t.Fatalf("cell %q is %T, want string", column, cell)
	}

	return s
}

// A replica toggled to diskful whose satellite cannot attach the new
// disk keeps reporting Diskless with every connection up. It read as a
// healthy diskless replica, and a conversion stuck on a refused attach
// went unnoticed for hours. LINSTOR's client paints a Diskless replica
// without the DISKLESS flag red; here it is faulty, and it reads
// AttachFailed once the satellite has failed to attach.
func TestDisklessReplicaMeantToHaveDisk(t *testing.T) {
	t.Parallel()

	converting := diskful("node-2", "Diskless", 0)

	tbl := view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{converting}, FaultyOnly: true})
	if len(tbl.Rows) != 1 {
		t.Fatalf("a diskful replica reporting Diskless is not listed by --faulty")
	}

	if got := cellAt(t, tbl, 0, "State"); got != "Diskless" {
		t.Errorf("before a failed attach: State = %q, want %q", got, "Diskless")
	}

	stuck := converting
	stuck.State.ToggleDiskRetries = 3

	tbl = view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{stuck}, FaultyOnly: true})
	if len(tbl.Rows) != 1 {
		t.Fatalf("a replica failing to attach is not listed by --faulty")
	}

	if got := cellAt(t, tbl, 0, "State"); got != "AttachFailed" {
		t.Errorf("after failed attaches: State = %q, want %q", got, "AttachFailed")
	}

	twoVolumes := stuck
	twoVolumes.Volumes = []apiv1.Volume{
		{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}},
		{VolumeNumber: 1, State: apiv1.VolumeState{DiskState: "Diskless"}},
	}

	if got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{twoVolumes}}), 0, "State"); got != "AttachFailed" {
		t.Errorf("one volume failing to attach: State = %q, want %q", got, "AttachFailed")
	}

	intended := diskful("node-3", "Diskless", 0)
	intended.Flags = []string{apiv1.ResourceFlagDiskless}
	intended.State.ToggleDiskRetries = 3

	if view.IsFaulty(&intended) {
		t.Errorf("a replica meant to be diskless is reported faulty")
	}

	if got := cellAt(t, view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{intended}}), 0, "State"); got != "Diskless" {
		t.Errorf("intended diskless: State = %q, want %q", got, "Diskless")
	}
}
