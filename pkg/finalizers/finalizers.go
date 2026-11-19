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

// Package finalizers names the finalizers satellites hold on the objects
// whose on-disk state they own, for the satellite that holds them and for
// the one caller allowed to release them without the satellite: `node
// lost`, for a node that will never run its satellite again.
package finalizers

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/cozystack/blockstor/pkg/naming"
)

const (
	// SatelliteResource is held by a replica's satellite until it has
	// deleted the replica's storage.
	SatelliteResource = "blockstor.org/satellite-resource"

	// LegacyResource is the controller-side finalizer older controllers
	// stamped on replicas; the controller strips it on sight.
	LegacyResource = "blockstor.org/resource"

	// SatelliteStoragePool is held by a pool's satellite until it has
	// deregistered the pool. Deleting a pool never touches the disk.
	SatelliteStoragePool = "blockstor.org/satellite-storagepool"

	// SharedSatelliteSnapshot is the snapshot finalizer older satellites
	// shared: the first node to delete its copy released it for all. It is
	// kept only to migrate Snapshots that still carry it; see SnapshotNode.
	SharedSatelliteSnapshot = "blockstor.org/satellite-snapshot"

	// StorageAbandoned marks a replica whose storage is left on its node:
	// `node lost` removed it because the node is gone. A satellite that
	// still sees it tears down DRBD and does not delete the storage.
	StorageAbandoned = "blockstor.org/storage-abandoned"
)

const (
	snapshotNodePrefix  = "satellite-snapshot.blockstor.org/"
	qualifiedNameMaxLen = 63
	hashedKeyLen        = 40
)

// SnapshotNode is the finalizer one satellite holds on a Snapshot while its
// own copy may still be on disk. One key per node lets the Snapshot go only
// once every node has deleted its copy, as each replica's own finalizer does
// for its Resource.
//
// The key is the node's naming.Key when that fits the name part of a
// qualified name, and a hash of it otherwise, so a long node name still gets
// a valid, stable key and every spelling of a node gets the same one.
func SnapshotNode(node string) string {
	key := naming.Key(node)
	if len(key) <= qualifiedNameMaxLen && len(validation.IsQualifiedName(snapshotNodePrefix+key)) == 0 {
		return snapshotNodePrefix + key
	}

	sum := sha256.Sum256([]byte(key))

	return snapshotNodePrefix + "node-" + hex.EncodeToString(sum[:])[:hashedKeyLen]
}

// OthersHoldTheirOwn reports whether every node in nodes other than node
// holds its own Snapshot finalizer in held, so the shared one protects no
// other node's copy any more.
func OthersHoldTheirOwn(nodes, held []string, node string) bool {
	for _, n := range nodes {
		if !naming.Same(n, node) && !slices.Contains(held, SnapshotNode(n)) {
			return false
		}
	}

	return true
}
