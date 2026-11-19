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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/color"
	"github.com/cozystack/blockstor/internal/cli/view"
)

// The stacks a definition can carry, top layer first.
func stackStorage() []string     { return []string{"STORAGE"} }
func stackLuksStorage() []string { return []string{"LUKS", "STORAGE"} }
func stackDrbd() []string        { return []string{"DRBD", "STORAGE"} }
func stackDrbdLuks() []string    { return []string{"DRBD", "LUKS", "STORAGE"} }

// stacked builds a replica the way a listing hands it to the view: the
// store's default DRBD chain, re-stamped with the definition's stack.
// Each disk state becomes one volume; a nil slice is a replica no
// volume has been reported for.
func stacked(stack []string, inUse *bool, disks ...string) apiv1.Resource {
	res := apiv1.Resource{
		Name:     "pvc-x",
		NodeName: "node-1",
		State:    apiv1.ResourceState{InUse: inUse},
		LayerObject: &apiv1.ResourceLayer{
			Type: "DRBD",
			Drbd: &apiv1.DrbdResourceLayer{TCPPorts: []int32{7001}},
		},
	}

	for i, disk := range disks {
		res.Volumes = append(res.Volumes, apiv1.Volume{
			VolumeNumber: int32(i),
			State:        apiv1.VolumeState{DiskState: disk},
		})
	}

	apiv1.ApplyLayerStack(&res, stack)
	apiv1.ApplyLayerStackToVolumes(res.Volumes, stack)

	return res
}

func withFlags(res apiv1.Resource, flags ...string) apiv1.Resource {
	res.Flags = append(res.Flags, flags...)

	return res
}

func withPeers(res apiv1.Resource, conns map[string]apiv1.DrbdConnection) apiv1.Resource {
	res.LayerObject.Drbd.Connections = conns

	return res
}

// reporting is a replica the satellite has seen: it wrote the role.
func reporting(res apiv1.Resource) apiv1.Resource {
	res.State.DrbdState = "Secondary"

	return res
}

type cellWant struct {
	text  string
	class color.Class
}

// The State, Usage and Conns cells of `resource list`, per layer stack
// and per state, as `linstor r l` renders the same replica. Colours
// map the upstream client's palette: its default green is Healthy,
// yellow is Transitional, red is Broken, and an uncoloured Usage cell
// is Neutral.
func TestResourceListStateParity(t *testing.T) {
	t.Parallel()

	connected := map[string]apiv1.DrbdConnection{"node-2": {Connected: true, Message: "Connected"}}

	cases := []struct {
		name  string
		res   apiv1.Resource
		state cellWant
		usage cellWant
		conns cellWant
	}{
		{
			name:  "storage only reads Created, with no usage to report",
			res:   stacked(stackStorage(), nil, ""),
			state: cellWant{"Created", color.Healthy},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "luks over storage reads Created",
			res:   stacked(stackLuksStorage(), ptrBool(false), ""),
			state: cellWant{"Created", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "storage only with no volume reported yet",
			res:   stacked(stackStorage(), nil),
			state: cellWant{"Unknown", color.Transitional},
		},
		{
			name:  "drbd over storage up to date",
			res:   withPeers(stacked(stackDrbd(), ptrBool(false), "UpToDate"), connected),
			state: cellWant{"UpToDate", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "drbd over luks over storage up to date",
			res:   withPeers(stacked(stackDrbdLuks(), ptrBool(false), "UpToDate"), connected),
			state: cellWant{"UpToDate", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "single drbd replica has no peers and reads Ok",
			res:   stacked(stackDrbd(), ptrBool(false), "UpToDate"),
			state: cellWant{"UpToDate", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "drbd with no disk state yet",
			res:   stacked(stackDrbd(), ptrBool(false), ""),
			state: cellWant{"Unknown", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "drbd DUnknown reads Unknown",
			res:   stacked(stackDrbd(), ptrBool(false), "DUnknown"),
			state: cellWant{"Unknown", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "drbd with no volume reported yet",
			res:   stacked(stackDrbd(), ptrBool(false)),
			state: cellWant{"Unknown", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "drbd inconsistent",
			res:   stacked(stackDrbd(), ptrBool(false), "Inconsistent"),
			state: cellWant{"Inconsistent", color.Broken},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "drbd failed",
			res:   stacked(stackDrbd(), ptrBool(false), "Failed"),
			state: cellWant{"Failed", color.Broken},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "drbd attaching",
			res:   stacked(stackDrbd(), ptrBool(false), "To: Attaching"),
			state: cellWant{"To: Attaching", color.Broken},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "drbd outdated",
			res:   stacked(stackDrbd(), ptrBool(false), "Outdated"),
			state: cellWant{"Outdated", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "drbd attached",
			res:   stacked(stackDrbd(), ptrBool(false), "Attached"),
			state: cellWant{"Attached", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "a diskful replica reporting Diskless has lost its disk",
			res:   stacked(stackDrbd(), ptrBool(false), "Diskless"),
			state: cellWant{"Diskless", color.Broken},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "a diskless replica reporting Diskless is healthy",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "Diskless"), apiv1.ResourceFlagDiskless),
			state: cellWant{"Diskless", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "a diskless replica the satellite has seen but no volume reports on",
			res:   reporting(withFlags(stacked(stackDrbd(), ptrBool(false)), apiv1.ResourceFlagDiskless)),
			state: cellWant{"Diskless", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "a diskless replica nothing has reported on is not known to be healthy",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false)), apiv1.ResourceFlagDiskless),
			state: cellWant{"Unknown", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "a diskless replica seen only through its peer links",
			res:   withPeers(withFlags(stacked(stackDrbd(), ptrBool(false)), apiv1.ResourceFlagDiskless), connected),
			state: cellWant{"Diskless", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name: "tiebreaker",
			res: reporting(withFlags(stacked(stackDrbd(), ptrBool(false)),
				apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker)),
			state: cellWant{"TieBreaker", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name: "a tiebreaker nothing has reported on keeps its token but turns yellow",
			res: withFlags(stacked(stackDrbd(), ptrBool(false)),
				apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker),
			state: cellWant{"TieBreaker", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "deleting storage replica reports no usage",
			res:   withFlags(stacked(stackStorage(), ptrBool(true), ""), "DELETE"),
			state: cellWant{"DELETING", color.Broken},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "drbd delete reads DELETING",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), "DRBD_DELETE"),
			state: cellWant{"DELETING", color.Broken},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "inactive drbd replica",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), apiv1.ResourceFlagInactive),
			state: cellWant{"INACTIVE", color.Transitional},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "inactive storage replica",
			res:   withFlags(stacked(stackStorage(), nil, ""), apiv1.ResourceFlagInactive),
			state: cellWant{"INACTIVE", color.Transitional},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "evacuating",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), "EVACUATE"),
			state: cellWant{"UpToDate, Evacuating", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "adding a disk turns a healthy state yellow",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), "DISK_ADDING"),
			state: cellWant{"UpToDate, Adding Disk", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "a pending disk removal",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), "DISK_REMOVE_REQUESTED"),
			state: cellWant{"UpToDate, Removing Disk (pending)", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name: "broken links are grouped by message and turn the state red",
			res: withPeers(stacked(stackDrbd(), ptrBool(false), "UpToDate"), map[string]apiv1.DrbdConnection{
				"node-2": {Connected: false, Message: "StandAlone"},
				"node-3": {Connected: false, Message: "Connecting"},
				"node-4": {Connected: false, Message: "StandAlone"},
				"node-5": {Connected: true, Message: "Connected"},
			}),
			state: cellWant{"UpToDate", color.Broken},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"StandAlone(node-2,node-4),Connecting(node-3)", color.Broken},
		},
		{
			name: "links are not judged while the state is Unknown",
			res: withPeers(stacked(stackDrbd(), ptrBool(false), ""), map[string]apiv1.DrbdConnection{
				"node-2": {Connected: false, Message: "StandAlone"},
			}),
			state: cellWant{"Unknown", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "the first volume that is not healthy speaks for the row",
			res:   stacked(stackDrbd(), ptrBool(false), "UpToDate", "Inconsistent", "Outdated"),
			state: cellWant{"Inconsistent", color.Broken},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
		{
			name:  "every volume healthy",
			res:   stacked(stackDrbd(), ptrBool(false), "UpToDate", "UpToDate"),
			state: cellWant{"UpToDate", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
			conns: cellWant{"Ok", color.Healthy},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tbl := view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{tc.res}})

			for column, want := range map[string]cellWant{"State": tc.state, "Usage": tc.usage, "Conns": tc.conns} {
				if got := cellAt(t, tbl, 0, column); got != want.text {
					t.Errorf("%s = %q, want %q", column, got, want.text)
				}

				if got := classAt(t, tbl, 0, column); got != want.class {
					t.Errorf("%s class = %v, want %v", column, got, want.class)
				}
			}
		})
	}
}

// Two replicas in use at once is a live migration or a split brain;
// either way the Usage cell stops reading as healthy.
func TestResourceListTwoPrimariesAreYellow(t *testing.T) {
	t.Parallel()

	first := stacked(stackDrbd(), ptrBool(true), "UpToDate")
	second := stacked(stackDrbd(), ptrBool(true), "UpToDate")
	second.NodeName = "node-2"

	other := stacked(stackDrbd(), ptrBool(true), "UpToDate")
	other.Name = "pvc-y"

	tbl := view.ResourceList(view.ResourceListInput{Resources: []apiv1.Resource{first, second, other}})

	for row, want := range []color.Class{color.Transitional, color.Transitional, color.Healthy} {
		if got := classAt(t, tbl, row, "Usage"); got != want {
			t.Errorf("row %d Usage class = %v, want %v", row, got, want)
		}
	}
}

// `volume list` and `resource list-volumes` read the volume state the
// same way the State column does.
func TestVolumeListStateParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		res   apiv1.Resource
		rows  int
		state cellWant
		usage cellWant
	}{
		{
			name:  "storage only",
			res:   stacked(stackStorage(), nil, ""),
			rows:  1,
			state: cellWant{"Created", color.Healthy},
		},
		{
			name:  "luks over storage",
			res:   stacked(stackLuksStorage(), ptrBool(false), ""),
			rows:  1,
			state: cellWant{"Created", color.Healthy},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "drbd up to date",
			res:   stacked(stackDrbd(), ptrBool(true), "UpToDate"),
			rows:  1,
			state: cellWant{"UpToDate", color.Healthy},
			usage: cellWant{"InUse", color.Healthy},
		},
		{
			name:  "drbd with no disk state",
			res:   stacked(stackDrbd(), ptrBool(false), ""),
			rows:  1,
			state: cellWant{"Unknown", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name: "a broken link turns the state red",
			res: withPeers(stacked(stackDrbd(), ptrBool(false), "UpToDate"), map[string]apiv1.DrbdConnection{
				"node-2": {Connected: false, Message: "StandAlone"},
			}),
			rows:  1,
			state: cellWant{"UpToDate", color.Broken},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name:  "adding a disk",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), "DISK_ADD_REQUESTED"),
			rows:  1,
			state: cellWant{"UpToDate, Adding Disk (pending)", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
		{
			name: "an inactive replica has no volumes to show",
			res:  withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), apiv1.ResourceFlagInactive),
			rows: 0,
		},
		{
			name:  "an inactive replica on an evicted node still shows",
			res:   withFlags(stacked(stackDrbd(), ptrBool(false), "UpToDate"), apiv1.ResourceFlagInactive, "EVICTED"),
			rows:  1,
			state: cellWant{"INACTIVE", color.Transitional},
			usage: cellWant{"Unused", color.Neutral},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tbl := view.VolumeList([]apiv1.Resource{tc.res})
			if len(tbl.Rows) != tc.rows {
				t.Fatalf("rows = %d, want %d", len(tbl.Rows), tc.rows)
			}

			if tc.rows == 0 {
				return
			}

			for column, want := range map[string]cellWant{"State": tc.state, "InUse": tc.usage} {
				if got := cellAt(t, tbl, 0, column); got != want.text {
					t.Errorf("%s = %q, want %q", column, got, want.text)
				}

				if got := classAt(t, tbl, 0, column); got != want.class {
					t.Errorf("%s class = %v, want %v", column, got, want.class)
				}
			}
		})
	}
}
