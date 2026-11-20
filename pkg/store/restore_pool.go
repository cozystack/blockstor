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

package store

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// ErrNoRestorePool refuses a restored replica that would have no storage
// pool to be created in. The satellite cannot build a replica without one:
// it fails on every pass, and its peers wait on it for good.
var ErrNoRestorePool = errors.New("no storage pool to restore into")

// storPoolNameProp names a replica's pool on the wire.
const storPoolNameProp = "StorPoolName"

// RestorePools names, per node (keyed by naming.Key), the storage pool a
// replica restored from snap is created in, or refuses with
// ErrNoRestorePool before anything is written.
//
// The pool is the one that holds the snapshot on that node, which is what
// LINSTOR restores into: it reads the pool off the snapshot volume, never off
// the source's replicas, so the restore works the same once the source is
// gone. blockstor's satellite records it on the node's entry once the copy is
// taken. A snapshot from before that record falls back to the source's live
// replica on the node, then to the pool recorded or used on another node,
// since pool names are cluster-wide; a source with no replica left gives
// nothing to fall back to.
func RestorePools(ctx context.Context, st Store, snap *apiv1.Snapshot, nodes []string) (map[string]string, error) {
	recorded, live, err := knownRestorePools(ctx, st, snap)
	if err != nil {
		return nil, err
	}

	fallback := firstPool(snap.Nodes, recorded, live)
	pools := make(map[string]string, len(nodes))

	for _, node := range nodes {
		key := naming.Key(node)
		if _, done := pools[key]; done || node == "" {
			continue
		}

		pool := recorded[key]
		if pool == "" {
			pool = live[key]
		}

		if pool == "" {
			pool = fallback
		}

		if pool == "" {
			return nil, fmt.Errorf("%w: snapshot %q of %q does not record the storage pool that holds it on %q, "+
				"and %q has no replica left to read it from", ErrNoRestorePool, snap.Name, snap.ResourceName,
				node, snap.ResourceName)
		}

		pools[key] = pool
	}

	return pools, nil
}

// knownRestorePools reads the pools snap recorded per node, and the pools of
// the source's live diskful replicas per node.
func knownRestorePools(ctx context.Context, st Store, snap *apiv1.Snapshot) (map[string]string, map[string]string, error) {
	recorded := make(map[string]string, len(snap.Snapshots))

	for i := range snap.Snapshots {
		if pool := snap.Snapshots[i].StoragePool; pool != "" {
			recorded[naming.Key(snap.Snapshots[i].NodeName)] = pool
		}
	}

	replicas, err := st.Resources().ListByDefinition(ctx, snap.ResourceName)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, nil, fmt.Errorf("list the replicas of %q: %w", snap.ResourceName, err)
	}

	live := make(map[string]string, len(replicas))

	for i := range replicas {
		if pool := replicas[i].Props[storPoolNameProp]; pool != "" && HoldsData(&replicas[i]) {
			live[naming.Key(replicas[i].NodeName)] = pool
		}
	}

	return recorded, live, nil
}

// firstPool is the fallback for a node neither map knows: the pool recorded
// on the first snapshot node that has one, else the first live replica's.
func firstPool(nodes []string, recorded, live map[string]string) string {
	for _, byNode := range []map[string]string{recorded, live} {
		for _, node := range nodes {
			if pool := byNode[naming.Key(node)]; pool != "" {
				return pool
			}
		}
	}

	if keys := slices.Sorted(maps.Keys(live)); len(keys) > 0 {
		return live[keys[0]]
	}

	return ""
}
