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
// both. The derivations were reproduced from observed output and from
// this repository's own assertions, not from the upstream client's
// (GPL) source.
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
)

// Well-known state cells shared across views.
const (
	stateOk       = "Ok"
	stateDeleting = "DELETING"
	stateUnknown  = "Unknown"
	flagDelete    = "DELETE"
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

	// VolumeSizesKib maps a volume number to its defined size, used to
	// turn the observed out-of-sync KiB into a progress percentage.
	// Absent sizes simply omit the percentage.
	VolumeSizesKib map[int32]int64

	// FaultyOnly keeps only replicas whose observed disk state is
	// present and not converged (`--faulty`).
	FaultyOnly bool
}

// ResourceList builds the `resource list` table.
func ResourceList(in ResourceListInput) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("ResourceName", "Node", "Port", "Usage", "Conns", "State", "CreatedOn"),
	}

	for i := range in.Resources {
		res := &in.Resources[i]

		if in.FaultyOnly && !isFaulty(res) {
			continue
		}

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			res.Name,
			res.NodeName,
			drbdPort(res),
			usageCell(res),
			connsCell(res),
			stateCell(res, in.VolumeSizesKib),
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

// usageCell renders the tri-state usage. A satellite that has not
// reported yet leaves the cell blank rather than claiming the replica
// is unused.
func usageCell(res *apiv1.Resource) string {
	if res.State.InUse == nil {
		return ""
	}

	if *res.State.InUse {
		return "InUse"
	}

	return "Unused"
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
// reads `Ok`, and any broken peer is named so a partition is visible
// in the table instead of hiding behind a healthy-looking summary.
func connsCell(res *apiv1.Resource) string {
	if res.LayerObject == nil || res.LayerObject.Drbd == nil || len(res.LayerObject.Drbd.Connections) == 0 {
		return ""
	}

	peers := make([]string, 0, len(res.LayerObject.Drbd.Connections))
	for peer := range res.LayerObject.Drbd.Connections {
		peers = append(peers, peer)
	}

	sort.Strings(peers)

	broken := make([]string, 0, len(peers))

	for _, peer := range peers {
		conn := res.LayerObject.Drbd.Connections[peer]
		if conn.Connected {
			continue
		}

		message := conn.Message
		if message == "" {
			message = "Unconnected"
		}

		broken = append(broken, fmt.Sprintf("%s(%s)", message, peer))
	}

	if len(broken) == 0 {
		return "Ok"
	}

	return strings.Join(broken, ",")
}

// stateCell derives the State column.
//
// Order matters: a replica being deleted reads DELETING whatever its
// disk says, and a tie-breaker reads the literal `TieBreaker` (a
// harness greps for that exact token, case included) rather than the
// Diskless it also technically is.
func stateCell(res *apiv1.Resource, sizes map[int32]int64) string {
	if slices.Contains(res.Flags, flagDelete) {
		return stateDeleting
	}

	if slices.Contains(res.Flags, apiv1.ResourceFlagTieBreaker) {
		return "TieBreaker"
	}

	if slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) {
		return "Diskless"
	}

	vol := worstVolume(res)
	if vol == nil || vol.State.DiskState == "" {
		return stateUnknown
	}

	state := vol.State.DiskState

	// A converged replica never carries a percentage; a syncing one
	// does, computed from what the satellite reports as still
	// out-of-sync.
	if _, terminal := terminalStates[strings.ToLower(state)]; terminal {
		return state
	}

	if pct, ok := syncPercent(vol, sizes); ok {
		return fmt.Sprintf("%s(%d%%)", state, pct)
	}

	return state
}

// worstVolume picks the volume whose state the row should report: the
// first non-converged one, else the first.
func worstVolume(res *apiv1.Resource) *apiv1.Volume {
	if len(res.Volumes) == 0 {
		return nil
	}

	for i := range res.Volumes {
		state := strings.ToLower(res.Volumes[i].State.DiskState)
		if state == "" {
			continue
		}

		if _, terminal := terminalStates[state]; !terminal {
			return &res.Volumes[i]
		}
	}

	return &res.Volumes[0]
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

// isFaulty backs `--faulty`: a replica counts as faulty when it has an
// observed disk state that has not converged. A replica the satellite
// has not reported on is NOT faulty — absence of data is not evidence
// of breakage, and reporting it as such would drown a real fault.
func isFaulty(res *apiv1.Resource) bool {
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

// createdOn renders the replica creation time. The timestamp arrives
// in unix milliseconds; an unreported one stays blank rather than
// printing the epoch.
func createdOn(unixMillis int64) string {
	if unixMillis <= 0 {
		return ""
	}

	return time.UnixMilli(unixMillis).UTC().Format("2006-01-02 15:04:05")
}
