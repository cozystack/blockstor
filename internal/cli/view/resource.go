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

// Package view assembles `metav1.Table`s from store DTOs.
//
// These are the cross-kind, derived-column views the Kubernetes API
// cannot serve from a single CRD's printer columns — a resource row
// joins the replica, its DRBD layer and its volumes, and computes the
// usage, connection and state cells an operator actually reads.
//
// Column names and order are a contract: shell in this repository
// parses these tables at fixed `awk -F'|'` indexes, so the tests pin
// both. The derivations reproduce the upstream client's observable
// behaviour, the cells and colours `linstor r l` shows for each layer
// stack and state, and this repository's own assertions. No upstream
// code or structure is carried over.
package view

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"

	"github.com/cozystack/blockstor/internal/cli/color"
)

// Well-known state cells shared across views.
const (
	stateOk       = "Ok"
	stateDeleting = "DELETING"
	stateUnknown  = "Unknown"
	stateCreated  = "Created"
	stateDiskless = "Diskless"
	flagDelete    = "DELETE"
)

// Flags the State column reads beyond the ones apiv1 names.
const (
	flagDrbdDelete          = "DRBD_DELETE"
	flagEvacuate            = "EVACUATE"
	flagEvicted             = "EVICTED"
	flagDiskAdding          = "DISK_ADDING"
	flagDiskAddRequested    = "DISK_ADD_REQUESTED"
	flagDiskRemoving        = "DISK_REMOVING"
	flagDiskRemoveRequested = "DISK_REMOVE_REQUESTED"
)

// State cells that are terminal — a converged replica never carries a
// sync percentage, which a harness asserts by grepping for the absence
// of `UpToDate(NN%)`.
var terminalStates = map[string]struct{}{ //nolint:gochecknoglobals // static classification table
	"uptodate":   {},
	"diskless":   {},
	"tiebreaker": {},
	"created":    {},
}

// ResourceListInput is everything `resource list` renders from.
type ResourceListInput struct {
	// Resources are the replicas, already filtered by node/resource if
	// the caller passed those flags.
	Resources []apiv1.Resource

	// VolumeSizesKib maps a resource-definition name to its volume
	// sizes, used to turn the observed out-of-sync KiB into a progress
	// percentage. Absent sizes simply omit the percentage.
	//
	// Keyed by definition, not by volume number alone: a listing spans
	// several definitions, and volume 0 of one is not volume 0 of
	// another.
	VolumeSizesKib map[string]map[int32]int64

	// FaultyOnly keeps only replicas whose observed disk state is
	// present and not converged (`--faulty`).
	FaultyOnly bool
}

// ResourceList builds the `resource list` table.
func ResourceList(in ResourceListInput) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("ResourceName", "Node", "Port", "Usage", "Conns", "State", "CreatedOn"),
	}

	primaries := primariesByDefinition(in.Resources)

	for i := range in.Resources {
		res := &in.Resources[i]

		if in.FaultyOnly && !IsFaulty(res) {
			continue
		}

		row := describeReplica(res, in.VolumeSizesKib[res.Name], primaries[naming.Key(res.Name)])

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			res.Name,
			res.NodeName,
			drbdPort(res),
			row.usage,
			row.conns,
			row.state,
			createdOn(res.CreateTimestamp),
		}})
	}

	return tbl
}

// columns is the shared column-definition builder.
func columns(names ...string) []metav1.TableColumnDefinition {
	out := make([]metav1.TableColumnDefinition, 0, len(names))
	for _, name := range names {
		out = append(out, metav1.TableColumnDefinition{Name: name, Type: "string"})
	}

	return out
}

// replicaRow is the derived cells of one `resource list` row.
type replicaRow struct {
	usage color.Cell
	conns color.Cell
	state color.Cell
}

// describeReplica derives the Usage, Conns and State cells together,
// because they depend on each other the way `linstor r l` shows them:
// a replica being deleted or deactivated reports no usage, Conns stays
// blank while the state is Unknown, and a broken peer link turns the
// State red whatever the disk says.
func describeReplica(res *apiv1.Resource, sizes map[int32]int64, primaries int) replicaRow {
	var row replicaRow

	switch {
	case slices.Contains(res.Flags, flagDelete) || slices.Contains(res.Flags, flagDrbdDelete):
		row.state = color.Cell{Text: stateDeleting, Class: color.Broken}
	case slices.Contains(res.Flags, apiv1.ResourceFlagInactive):
		row.state = color.Cell{Text: apiv1.ResourceFlagInactive, Class: color.Transitional}
	default:
		row.usage = usageCell(res, primaries)
		row.state = replicaState(res, sizes)
	}

	row.state = withToggleDisk(row.state, res.Flags)

	var broken bool

	row.conns, broken = connsCell(res, row.state.Text)
	if broken {
		row.state.Class = color.Broken
	}

	return row
}

// primariesByDefinition counts the in-use replicas per definition. Two
// at once is either a live migration or a split brain, so the Usage
// cell turns yellow instead of claiming a healthy InUse.
func primariesByDefinition(resources []apiv1.Resource) map[string]int {
	out := make(map[string]int, len(resources))

	for i := range resources {
		inUse := resources[i].State.InUse
		if inUse != nil && *inUse {
			out[naming.Key(resources[i].Name)]++
		}
	}

	return out
}

// usageCell renders the tri-state usage. A satellite that has not
// reported yet, and a replica whose top layer has no notion of being
// open (a bare STORAGE volume), leave the cell blank rather than
// claiming the replica is unused.
func usageCell(res *apiv1.Resource, primaries int) color.Cell {
	if res.State.InUse == nil {
		return color.Cell{}
	}

	if !*res.State.InUse {
		return color.Cell{Text: "Unused", Class: color.Neutral}
	}

	if primaries > 1 {
		return color.Cell{Text: "InUse", Class: color.Transitional}
	}

	return color.Cell{Text: "InUse", Class: color.Healthy}
}

// drbdPort renders the replica's own DRBD listen port; the allocator
// is per-replica, so this is read from the replica's layer rather than
// from the definition.
func drbdPort(res *apiv1.Resource) string {
	if res.LayerObject == nil || res.LayerObject.Drbd == nil || len(res.LayerObject.Drbd.TCPPorts) == 0 {
		return ""
	}

	return strconv.Itoa(int(res.LayerObject.Drbd.TCPPorts[0]))
}

// connsCell summarises the peer connections: a fully-connected mesh
// reads `Ok`, and broken peers are named, grouped by what is wrong
// with them, so a partition is visible in the table instead of hiding
// behind a healthy-looking summary.
//
// A replica with no DRBD layer has no peers to lose and reads `Ok`; a
// replica whose state is still Unknown says nothing about its links
// yet, so the cell stays blank. It reports whether any link is broken.
func connsCell(res *apiv1.Resource, state string) (color.Cell, bool) {
	if state == stateUnknown {
		return color.Cell{}, false
	}

	allConnected := color.Cell{Text: stateOk, Class: color.Healthy}

	if !topLayerIsDRBD(res) || len(res.LayerObject.Drbd.Connections) == 0 {
		return allConnected, false
	}

	peers := make([]string, 0, len(res.LayerObject.Drbd.Connections))
	for peer := range res.LayerObject.Drbd.Connections {
		peers = append(peers, peer)
	}

	sort.Strings(peers)

	var messages []string

	byMessage := map[string][]string{}

	for _, peer := range peers {
		conn := res.LayerObject.Drbd.Connections[peer]
		if conn.Connected {
			continue
		}

		message := conn.Message
		if message == "" {
			message = "Unconnected"
		}

		if _, seen := byMessage[message]; !seen {
			messages = append(messages, message)
		}

		byMessage[message] = append(byMessage[message], peer)
	}

	if len(messages) == 0 {
		return allConnected, false
	}

	broken := make([]string, 0, len(messages))
	for _, message := range messages {
		broken = append(broken, fmt.Sprintf("%s(%s)", message, strings.Join(byMessage[message], ",")))
	}

	return color.Cell{Text: strings.Join(broken, ","), Class: color.Broken}, true
}

// topLayerIsDRBD reports whether the replica's own layer chain starts
// with DRBD and carries its payload.
func topLayerIsDRBD(res *apiv1.Resource) bool {
	if res.LayerObject == nil || res.LayerObject.Drbd == nil {
		return false
	}

	return res.LayerObject.Type == "" || strings.EqualFold(res.LayerObject.Type, apiv1.LayerKindDRBD)
}

// replicaState derives the State column from the replica's volumes.
//
// A tie-breaker reads the literal `TieBreaker` (a harness greps for
// that exact token, case included) rather than the Diskless it also
// technically is: this table has no Flags column to carry the
// difference. A diskless replica no volume reports on reads Diskless.
// Otherwise the first volume that is not healthy speaks for the row,
// and the last one when all are.
func replicaState(res *apiv1.Resource, sizes map[int32]int64) color.Cell {
	if slices.Contains(res.Flags, apiv1.ResourceFlagTieBreaker) {
		return color.Cell{Text: "TieBreaker", Class: color.Healthy}
	}

	if slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) && !anyDiskState(res) {
		return color.Cell{Text: stateDiskless, Class: color.Healthy}
	}

	state := color.Cell{Text: stateUnknown, Class: color.Transitional}

	for i := range res.Volumes {
		state = volumeState(res, &res.Volumes[i], sizes)

		if slices.Contains(res.Flags, flagEvacuate) {
			state.Text += ", Evacuating"
		}

		if state.Class != color.Healthy {
			break
		}
	}

	return state
}

// anyDiskState reports whether any volume of the replica has an
// observed disk state.
func anyDiskState(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		if res.Volumes[i].State.DiskState != "" {
			return true
		}
	}

	return false
}

// volumeState derives one volume's state cell.
//
// Only a volume whose top layer is DRBD has a disk state to report: a
// bare STORAGE volume, or LUKS over STORAGE, is either there or not,
// and once it is listed it reads Created. Reading such a volume's
// missing disk state as Unknown is what made a healthy local volume
// look like it was never reported on.
//
// There is no `Resizing` marker: nothing in this store flags a volume
// while it grows, so a branch for one would only ever fire in a test.
func volumeState(res *apiv1.Resource, vol *apiv1.Volume, sizes map[int32]int64) color.Cell {
	if !volumeTopLayerIsDRBD(res, vol) {
		return color.Cell{Text: stateCreated, Class: color.Healthy}
	}

	disk := vol.State.DiskState

	class := color.Transitional

	switch strings.ToLower(disk) {
	case "", "dunknown":
		return color.Cell{Text: stateUnknown, Class: color.Transitional}
	case "diskless":
		// Diskless on purpose is healthy; a replica that was given a
		// disk and reports none has lost it.
		class = color.Broken
		if slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) {
			class = color.Healthy
		}
	case "inconsistent", "failed", "to: creating", "to: attachable", "to: attaching":
		class = color.Broken
	case "uptodate", "created", "attached":
		class = color.Healthy
	}

	text := disk

	// A converged replica never carries a percentage; a syncing one
	// does, computed from what the satellite reports as still
	// out-of-sync.
	if _, terminal := terminalStates[strings.ToLower(disk)]; !terminal {
		if pct, ok := syncPercent(vol, sizes); ok {
			text = fmt.Sprintf("%s(%d%%)", text, pct)
		}
	}

	return color.Cell{Text: text, Class: class}
}

// volumeTopLayerIsDRBD reads the volume's own layer list first and the
// replica's layer chain after it. A volume with neither was never
// stamped with a stack, which in this store means the default one, and
// that starts with DRBD.
func volumeTopLayerIsDRBD(res *apiv1.Resource, vol *apiv1.Volume) bool {
	if len(vol.LayerDataList) > 0 {
		return strings.EqualFold(vol.LayerDataList[0].Type, apiv1.LayerKindDRBD)
	}

	if res.LayerObject != nil && res.LayerObject.Type != "" {
		return strings.EqualFold(res.LayerObject.Type, apiv1.LayerKindDRBD)
	}

	return true
}

// withToggleDisk appends an in-flight toggle-disk to the state. The
// conversion is work in progress, so a state that read healthy turns
// yellow until it finishes.
func withToggleDisk(state color.Cell, flags []string) color.Cell {
	var suffix string

	switch {
	case slices.Contains(flags, flagDiskAdding):
		suffix = ", Adding Disk"
	case slices.Contains(flags, flagDiskAddRequested):
		suffix = ", Adding Disk (pending)"
	case slices.Contains(flags, flagDiskRemoving):
		suffix = ", Removing Disk"
	case slices.Contains(flags, flagDiskRemoveRequested):
		suffix = ", Removing Disk (pending)"
	default:
		return state
	}

	state.Text += suffix
	if state.Class == color.Healthy || state.Class == color.Neutral {
		state.Class = color.Transitional
	}

	return state
}

// syncPercent converts the observed out-of-sync KiB into progress.
func syncPercent(vol *apiv1.Volume, sizes map[int32]int64) (int, bool) {
	if vol.State.OutOfSyncKib <= 0 {
		return 0, false
	}

	size, ok := sizes[vol.VolumeNumber]
	if !ok || size <= 0 {
		return 0, false
	}

	done := float64(size-vol.State.OutOfSyncKib) / float64(size) * 100
	if done < 0 {
		done = 0
	}

	return int(done), true
}

// IsFaulty backs `--faulty`: a replica counts as faulty when a volume
// has an observed disk state that has not converged, or when a peer
// link is down. A replica the satellite has not reported on is NOT
// faulty — absence of data is not evidence of breakage, and reporting
// it as such would drown a real fault.
func IsFaulty(res *apiv1.Resource) bool {
	// A replica whose local disk is UpToDate but whose peer link is
	// StandAlone or failed is exactly the split-brain the
	// troubleshooting runbooks tell an operator to find with
	// `--faulty`. Looking only at DiskState hides it.
	if hasBrokenConnection(res) {
		return true
	}

	for i := range res.Volumes {
		state := strings.ToLower(res.Volumes[i].State.DiskState)
		if state == "" {
			continue
		}

		if _, terminal := terminalStates[state]; !terminal {
			return true
		}
	}

	return false
}

// hasBrokenConnection reports whether any DRBD peer link is in a state
// other than Connected.
func hasBrokenConnection(res *apiv1.Resource) bool {
	if res.LayerObject == nil || res.LayerObject.Drbd == nil {
		return false
	}

	for _, conn := range res.LayerObject.Drbd.Connections {
		if !conn.Connected {
			return true
		}
	}

	return false
}

// createdOn renders the replica creation time. The timestamp arrives
// in unix milliseconds; an unreported one stays blank rather than
// printing the epoch.
func createdOn(unixMillis int64) string {
	if unixMillis <= 0 {
		return ""
	}

	return time.UnixMilli(unixMillis).UTC().Format("2006-01-02 15:04:05")
}
