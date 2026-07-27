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

package view

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// PropertyColumns is the column set every `list-properties` view uses.
func PropertyColumns() []metav1.TableColumnDefinition {
	return columns("Key", "Value")
}

// StoragePoolList builds the `storage-pool list` table.
//
// The State cell is the reason this view exists rather than a printer
// column: a pool whose backing store vanished out-of-band still has a
// perfectly healthy-looking CRD, and reporting `Ok` there is precisely
// the regression this repo's recovery test watches for.
func StoragePoolList(pools []apiv1.StoragePool) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns(
			"StoragePool", "Node", "Driver", "PoolName",
			"FreeCapacity", "TotalCapacity", "CanSnapshots", "State",
		),
	}

	for i := range pools {
		pool := &pools[i]

		state := stateOk
		if pool.PoolMissing {
			state = "Faulty"
		}

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			pool.StoragePoolName,
			pool.NodeName,
			pool.ProviderKind,
			backingPoolName(pool),
			capacity(pool.FreeCapacity),
			capacity(pool.TotalCapacity),
			boolCell(pool.SupportsSnapshot),
			state,
		}})
	}

	return tbl
}

// backingPoolName surfaces the backing device the driver was pointed
// at, reading the same generic property a real database records it
// under before falling back to the kind-specific keys.
func backingPoolName(pool *apiv1.StoragePool) string {
	for _, key := range []string{
		"StorDriver/StorPoolName",
		"StorDriver/ZPoolThin", "StorDriver/ZPool",
		"StorDriver/LvmVg", "StorDriver/FileDir",
	} {
		if v, ok := pool.Props[key]; ok && v != "" {
			return v
		}
	}

	return ""
}

// ResourceDefinitionList builds the `resource-definition list` table.
func ResourceDefinitionList(defs []apiv1.ResourceDefinition) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("ResourceName", "Port", "ResourceGroup", "Layers", "State"),
	}

	for i := range defs {
		def := &defs[i]

		state := stateOk
		if slices.Contains(def.Flags, flagDelete) {
			state = stateDeleting
		}

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			def.Name,
			definitionPort(def),
			def.ResourceGroupName,
			strings.Join(def.LayerStack, ","),
			state,
		}})
	}

	return tbl
}

// definitionPort reads the definition-scoped DRBD port when the layer
// data carries one.
func definitionPort(def *apiv1.ResourceDefinition) string {
	for i := range def.LayerData {
		if def.LayerData[i].Drbd != nil && len(def.LayerData[i].Drbd.TCPPorts) > 0 {
			return strconv.Itoa(int(def.LayerData[i].Drbd.TCPPorts[0]))
		}
	}

	return ""
}

// VolumeDefinitionList builds the `volume-definition list` table.
func VolumeDefinitionList(rdName string, vds []apiv1.VolumeDefinition) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("ResourceName", "VolumeNr", "VolumeMinor", "Size", "Gross", "State"),
	}

	for i := range vds {
		vd := &vds[i]

		state := stateOk
		if slices.Contains(vd.Flags, flagDelete) {
			state = stateDeleting
		}

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			rdName,
			strconv.Itoa(int(vd.VolumeNumber)),
			"",
			capacity(vd.SizeKib),
			boolCell(slices.Contains(vd.Flags, "GROSS_SIZE")),
			state,
		}})
	}

	return tbl
}

// SnapshotList builds the `snapshot list` table.
func SnapshotList(snaps []apiv1.Snapshot) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("ResourceName", "SnapshotName", "NodeNames", "Volumes", "Created", "State"),
	}

	for i := range snaps {
		snap := &snaps[i]

		volumes := make([]string, 0, len(snap.VolumeDefinitions))
		for j := range snap.VolumeDefinitions {
			volumes = append(volumes, strconv.Itoa(int(snap.VolumeDefinitions[j].VolumeNumber)))
		}

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			snap.ResourceName,
			snap.Name,
			strings.Join(snap.Nodes, ","),
			strings.Join(volumes, ","),
			snapshotCreated(snap),
			snapshotState(snap),
		}})
	}

	return tbl
}

// snapshotState reports the terminal marker the satellite or the
// controller stamped, defaulting to Successful for a snapshot that
// carries no failure flag.
func snapshotState(snap *apiv1.Snapshot) string {
	for _, flag := range snap.Flags {
		switch flag {
		case "FAILED":
			return "Failed"
		case "FAILED_DISCONNECT":
			return "Satellite disconnected"
		case flagDelete:
			return stateDeleting
		}
	}

	return "Successful"
}

// snapshotCreated renders the newest per-node creation time.
func snapshotCreated(snap *apiv1.Snapshot) string {
	var newest int64

	for i := range snap.Snapshots {
		if ts := snap.Snapshots[i].CreateTimestamp; ts > newest {
			newest = ts
		}
	}

	return createdOn(newest)
}

// ResourceGroupList builds the `resource-group list` table.
func ResourceGroupList(groups []apiv1.ResourceGroup) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("ResourceGroup", "SelectFilter", "VlmNrs", "Description"),
	}

	for i := range groups {
		group := &groups[i]

		volumes := make([]string, 0, len(group.VolumeGroups))
		for j := range group.VolumeGroups {
			volumes = append(volumes, strconv.Itoa(int(group.VolumeGroups[j].VolumeNumber)))
		}

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			group.Name,
			selectFilterCell(&group.SelectFilter),
			strings.Join(volumes, ","),
			group.Description,
		}})
	}

	return tbl
}

// selectFilterCell summarises the placement policy in the multi-line
// shape the parity tooling normalises.
func selectFilterCell(filter *apiv1.AutoSelectFilter) string {
	const filterParts = 3

	parts := make([]string, 0, filterParts)

	parts = append(parts, "PlaceCount: "+strconv.Itoa(int(filter.PlaceCount)))

	pools := filter.StoragePoolList
	if filter.StoragePool != "" {
		pools = append([]string{filter.StoragePool}, pools...)
	}

	if len(pools) > 0 {
		parts = append(parts, "StoragePool(s): "+strings.Join(pools, ","))
	}

	if len(filter.LayerStack) > 0 {
		parts = append(parts, "LayerStack: "+strings.Join(filter.LayerStack, ","))
	}

	return strings.Join(parts, "\n")
}

// VolumeList builds the `volume list` table: one row per replica
// volume.
func VolumeList(resources []apiv1.Resource) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns(
			"Node", "Resource", "StoragePool", "VolumeNr",
			"MinorNr", "DeviceName", "Allocated", "InUse", "State",
		),
	}

	for i := range resources {
		res := &resources[i]

		for j := range res.Volumes {
			vol := &res.Volumes[j]

			state := vol.State.DiskState
			if state == "" {
				state = stateUnknown
			}

			tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
				res.NodeName,
				res.Name,
				vol.StoragePool,
				strconv.Itoa(int(vol.VolumeNumber)),
				minorFromDevice(vol.DevicePath),
				vol.DevicePath,
				capacity(vol.AllocatedKib),
				usageCell(res),
				state,
			}})
		}
	}

	return tbl
}

// minorFromDevice extracts the DRBD minor from `/dev/drbdNNNN`.
func minorFromDevice(device string) string {
	const prefix = "/dev/drbd"

	if !strings.HasPrefix(device, prefix) {
		return ""
	}

	return strings.TrimPrefix(device, prefix)
}

// capacity renders KiB in the binary units the scripts grep for. Zero
// stays blank rather than printing "0 KiB" for an unreported value.
func capacity(kib int64) string {
	if kib <= 0 {
		return ""
	}

	const step = 1024

	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}

	value := float64(kib)
	idx := 0

	for value >= step && idx < len(units)-1 {
		value /= step
		idx++
	}

	if value == float64(int64(value)) {
		return fmt.Sprintf("%d %s", int64(value), units[idx])
	}

	return fmt.Sprintf("%.2f %s", value, units[idx])
}

// boolCell renders the True/False spelling the scripts match.
func boolCell(v bool) string {
	if v {
		return "True"
	}

	return "False"
}

// VolumeGroupColumns is the `volume-group list` header.
func VolumeGroupColumns() []metav1.TableColumnDefinition {
	return columns("ResourceGroup", "VolumeNr", "Flags")
}

// VolumeGroupRows renders one resource group's per-volume templates.
func VolumeGroupRows(group *apiv1.ResourceGroup) []metav1.TableRow {
	rows := make([]metav1.TableRow, 0, len(group.VolumeGroups))

	for i := range group.VolumeGroups {
		volume := &group.VolumeGroups[i]

		rows = append(rows, metav1.TableRow{Cells: []any{
			group.Name,
			strconv.Itoa(int(volume.VolumeNumber)),
			strings.Join(volume.Flags, ","),
		}})
	}

	return rows
}
