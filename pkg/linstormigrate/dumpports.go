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

package linstormigrate

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrDumpNotAtStorageVersion is returned for a dump whose tables were
// read at a CRD version other than the storage version.
//
// LINSTOR's CRDs serve several versions with no conversion, and
// discovery prefers the alphabetically first one, so a plain
// `kubectl get <crd>` reads every table at the oldest schema. The API
// server then prunes each column that schema lacks, so the dump is
// missing data without any error.
var ErrDumpNotAtStorageVersion = errors.New("the LINSTOR dump was not read at its CRDs' storage version")

// indexDumpPort records the port list of one live DRBD replica row,
// and counts the row toward the storage-version check.
func (c *converter) indexDumpPort(lri *LayerResourceIDRow, row *LayerDrbdResourceRow) {
	key := replicaKey{node: lri.NodeName, rd: lri.ResourceName}

	c.drbdReplicaRows++

	if row.TCPPortList == nil {
		if def, ok := c.drbdRD[lri.ResourceName]; !ok || def.TCPPort == nil {
			c.portlessDRBDRows++
		}

		return
	}

	var ports []int32

	err := json.Unmarshal([]byte(*row.TCPPortList), &ports)
	if err != nil || slices.ContainsFunc(ports, func(port int32) bool { return port <= 0 || port > maxTCPPort }) {
		c.warnf("resource %s.%s: tcp_port_list %q is not a list of ports; its DRBD port is not carried from the database",
			c.displayRD(lri.ResourceName), c.displayNode(lri.NodeName), *row.TCPPortList)

		ports = nil
	}

	c.dumpPorts[key] = ports
}

// checkStorageVersion refuses a dump read at a non-storage version.
//
// LINSTOR writes the DRBD port of every replica into tcp_port_list
// since it moved the port off the definition, and before that kept it
// in the definition's tcp_port. A replica row with neither is a row
// the API server pruned, and the same read dropped other columns that
// cannot be detected from the data, such as a resource group's
// x_replicas_on_different and peer_slots. Converting such a dump would
// carry the loss into blockstor, so nothing is converted.
func (c *converter) checkStorageVersion() error {
	if c.portlessDRBDRows == 0 {
		return nil
	}

	readAt := ""
	if versions := c.dump.APIVersions["layerdrbdresources"]; len(versions) > 0 {
		readAt = " (rows read at " + strings.Join(versions, ", ") + ")"
	}

	return fmt.Errorf("%w: %d of %d DRBD replica rows in layerdrbdresources have no tcp_port_list, and their definitions no tcp_port%s. "+
		"LINSTOR stores a port for every replica, so the API server pruned the column, together with the other columns that version lacks. "+
		"Take the dump again with every table read at its CRD's storage version, as the migration runbook shows (pre-flight step 2)",
		ErrDumpNotAtStorageVersion, c.portlessDRBDRows, c.drbdReplicaRows, readAt)
}

// dumpReplicaPort is the port the database holds for one replica: the
// single port of its tcp_port_list, or on a schema from before LINSTOR
// moved the port to the replica, its definition's tcp_port.
func (c *converter) dumpReplicaPort(row *ResourceRow, defPort *int32) (int32, bool) {
	ports, ok := c.dumpPorts[replicaKey{node: row.NodeName, rd: row.ResourceName}]
	if !ok {
		if defPort != nil && *defPort > 0 {
			return *defPort, true
		}

		return 0, false
	}

	if len(ports) > 1 {
		c.warnOncef("drbdports:"+row.ResourceName+":"+row.NodeName,
			"resource %s.%s: the database gives this replica %d DRBD ports, and blockstor carries one per replica; no port is taken from the database for it",
			c.displayRD(row.ResourceName), c.displayNode(row.NodeName), len(ports))
	}

	if len(ports) != 1 {
		return 0, false
	}

	return ports[0], true
}

// livePort is the port the live map captured for one replica: its own
// line when the definition has per-replica lines, the definition-wide
// line otherwise.
func (c *converter) livePort(row *ResourceRow) (int32, bool) {
	rd := strings.ToLower(row.ResourceName)

	if perReplica := c.opts.ReplicaDRBDPorts[rd]; len(perReplica) > 0 {
		return c.mappedReplicaPort(perReplica, row.NodeName)
	}

	port, ok := c.opts.DRBDPorts[rd]

	return port, ok
}

// replicaPort is the port one replica is preset to: the kernel's,
// captured in the live map, over the database's. It reports no gap;
// resolveReplicaDRBDPort does, once per replica.
func (c *converter) replicaPort(row *ResourceRow, defPort *int32) *int32 {
	if port, ok := c.livePort(row); ok {
		return ptr(port)
	}

	if port, ok := c.dumpReplicaPort(row, defPort); ok {
		return ptr(port)
	}

	return nil
}
