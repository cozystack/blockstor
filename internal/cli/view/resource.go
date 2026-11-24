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

	replicationStateSyncTarget = "SyncTarget"
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

// propSkipDisk is the property the satellite stamps on a replica whose
// backing disk failed with an I/O error; DRBD keeps that disk detached
// while it is true.
const propSkipDisk = "DrbdOptions/SkipDisk"

// ResourceListInput is everything `resource list` renders from.
type ResourceListInput struct {
	// Resources are the replicas, already filtered by node/resource if
	// the caller passed those flags.
	Resources []apiv1.Resource

	// DefinitionProps maps a resource-definition name (naming.Key) to
	// its own properties, so a SkipDisk set on the definition shows on
	// every replica of it.
	DefinitionProps map[string]map[string]string

	// Primaries maps a resource-definition name (naming.Key) to how
	// many of its replicas are in use. Nil counts them from Resources;
	// a caller that has already narrowed Resources passes the count
	// from the rows it read, so filtering out a healthy primary does
	// not hide a second one.
	Primaries map[string]int

	// FaultyOnly keeps only the rows IsFaulty reports (`--faulty`).
	FaultyOnly bool
}

// ResourceList builds the `resource list` table.
func ResourceList(in ResourceListInput) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("ResourceName", "Node", "Port", "Usage", "Conns", "State", "CreatedOn"),
	}

	primaries := in.Primaries
	if primaries == nil {
		primaries = PrimariesByDefinition(in.Resources)
	}

	for i := range in.Resources {
		res := &in.Resources[i]
		key := naming.Key(res.Name)

		row := describeReplica(res, primaries[key], in.DefinitionProps[key])
		if in.FaultyOnly && row.state.Class == color.Healthy {
			continue
		}

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
// blank while nothing is known about the replica, and a broken peer
// link turns the State red whatever the disk says.
func describeReplica(res *apiv1.Resource, primaries int, defProps map[string]string) replicaRow {
	var row replicaRow

	reported := true

	switch {
	case slices.Contains(res.Flags, flagDelete) || slices.Contains(res.Flags, flagDrbdDelete):
		row.state = color.Cell{Text: stateDeleting, Class: color.Broken}
	case slices.Contains(res.Flags, apiv1.ResourceFlagInactive):
		row.state = color.Cell{Text: apiv1.ResourceFlagInactive, Class: color.Transitional}
	default:
		row.usage = usageCell(res, primaries)
		row.state, reported = replicaState(res)
	}

	row.state = withSkipDisk(row.state, res, defProps)
	row.state = withToggleDisk(row.state, res.Flags)

	if !reported || row.state.Text == stateUnknown {
		return row
	}

	var broken bool

	row.conns, broken = connsCell(res)
	if broken {
		row.state.Class = color.Broken
	}

	return row
}

// PrimariesByDefinition counts the in-use replicas per definition,
// keyed by naming.Key. Two at once is either a live migration or a
// split brain, so the Usage cell turns yellow instead of claiming a
// healthy InUse.
func PrimariesByDefinition(resources []apiv1.Resource) map[string]int {
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
// A replica with no DRBD layer has no peers to lose and reads `Ok`.
// The caller leaves the cell blank for a replica nothing has reported
// on, whose links are not known yet. It reports whether any link is
// broken.
func connsCell(res *apiv1.Resource) (color.Cell, bool) {
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

// replicaState derives the State column from the replica's volumes,
// and whether the satellite has said anything about the replica yet.
//
// A tie-breaker reads the literal `TieBreaker` (a harness greps for
// that exact token, case included) rather than the Diskless it also
// technically is: this table has no Flags column to carry the
// difference. A diskless replica no volume reports on, but whose
// device the satellite has seen, reads Diskless.
//
// A diskless replica or tie-breaker the satellite has never reported
// on is not known to be healthy: its device may never have come up.
// It reads yellow, as the upstream client shows it, the tie-breaker
// keeping its token and a plain diskless one reading Unknown.
//
// Otherwise the first volume that is not healthy speaks for the row,
// and the last one when all are.
func replicaState(res *apiv1.Resource) (color.Cell, bool) {
	tieBreaker := slices.Contains(res.Flags, apiv1.ResourceFlagTieBreaker)
	diskless := slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) && !anyDiskState(res)

	if !tieBreaker && !diskless {
		return volumesState(res), true
	}

	reported := satelliteReported(res)

	switch {
	case tieBreaker && reported:
		return color.Cell{Text: "TieBreaker", Class: color.Healthy}, true
	case tieBreaker:
		return color.Cell{Text: "TieBreaker", Class: color.Transitional}, false
	case reported:
		return color.Cell{Text: stateDiskless, Class: color.Healthy}, true
	default:
		return color.Cell{Text: stateUnknown, Class: color.Transitional}, false
	}
}

// volumesState is the state of the first volume that is not healthy,
// or of the last one when all are.
func volumesState(res *apiv1.Resource) color.Cell {
	state := color.Cell{Text: stateUnknown, Class: color.Transitional}

	for i := range res.Volumes {
		state = volumeState(res, &res.Volumes[i])

		if slices.Contains(res.Flags, flagEvacuate) {
			state.Text += ", Evacuating"
		}

		if state.Class != color.Healthy {
			break
		}
	}

	return state
}

// satelliteReported reports whether the satellite has written anything
// it observed about the replica's device: a role, a volume's disk
// state or a peer link. The store fills none of these on its own.
func satelliteReported(res *apiv1.Resource) bool {
	if res.State.DrbdState != "" || anyDiskState(res) {
		return true
	}

	return res.LayerObject != nil && res.LayerObject.Drbd != nil && len(res.LayerObject.Drbd.Connections) > 0
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
func volumeState(res *apiv1.Resource, vol *apiv1.Volume) color.Cell {
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

	// A volume resyncing from a peer reads `SyncTarget(50.31%)`, DRBD's
	// own `done` for that resync, which is what `linstor r l` shows
	// against LINSTOR. No other state carries a percentage. One derived
	// from the volume size and the out-of-sync count is wrong once a
	// replica resyncs from more than one peer: the count is the last
	// peer's, so a replica half way through a full resync read
	// `Inconsistent(99%)` after a small resync from another peer.
	if done, ok := vol.State.SyncProgress(replicationStateSyncTarget); ok {
		return color.Cell{Text: apiv1.SyncProgressState(replicationStateSyncTarget, done), Class: color.Transitional}
	}

	return color.Cell{Text: disk, Class: class}
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

// IsFaulty backs `--faulty`: a replica is faulty when its State cell
// is anything but green, which already covers a broken peer link. So
// the filter lists exactly the rows the table flags: a diskful replica
// that lost its disk, one being deleted or deactivated, one nothing
// has reported on yet, one with SkipDisk set or a toggle-disk in
// flight. defProps are the properties of the replica's definition.
func IsFaulty(res *apiv1.Resource, defProps map[string]string) bool {
	return describeReplica(res, 0, defProps).state.Class != color.Healthy
}

// SkipDisk reports whether the replica's State carries the SkipDisk
// marker, so a caller can explain it under the table.
func SkipDisk(res *apiv1.Resource, defProps map[string]string) bool {
	return skipDiskSuffix(res, defProps) != ""
}

// SkipDiskHelp explains the SkipDisk marker. It goes to stderr, under a
// table that showed one, so the table itself stays parseable.
const SkipDiskHelp = `SkipDisk: DrbdOptions/SkipDisk is set on at least one replica above.
  The satellite sets it after an I/O error on the backing disk, and DRBD
  keeps that disk detached while it stays set. Once the disk is repaired
  or replaced, remove it with
  'r sp <node> <resource> DrbdOptions/SkipDisk ""' (or 'rd sp' where the
  marker reads RD) so the volume is attached, and recreated if needed.
`

// withSkipDisk marks a replica whose disk is held detached. It is not
// healthy whatever the volumes say, so a green state turns yellow.
func withSkipDisk(state color.Cell, res *apiv1.Resource, defProps map[string]string) color.Cell {
	suffix := skipDiskSuffix(res, defProps)
	if suffix == "" {
		return state
	}

	state.Text += suffix
	if state.Class == color.Healthy || state.Class == color.Neutral {
		state.Class = color.Transitional
	}

	return state
}

// skipDiskSuffix renders `, SkipDisk (R, RD)`: the replica's own value
// wins over its definition's, as it does for the satellite, and every
// scope that sets the key is named, the winning one first. Empty when
// the effective value is not true.
func skipDiskSuffix(res *apiv1.Resource, defProps map[string]string) string {
	var scopes []string

	value, set := res.Props[propSkipDisk]
	if set {
		scopes = append(scopes, "R")
	}

	if defValue, ok := defProps[propSkipDisk]; ok {
		if !set {
			value = defValue
		}

		scopes = append(scopes, "RD")
	}

	if !strings.EqualFold(value, "true") {
		return ""
	}

	return ", SkipDisk (" + strings.Join(scopes, ", ") + ")"
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
