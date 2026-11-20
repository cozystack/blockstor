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

package controllers

import (
	"context"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// recordSnapshotPool records on this node's entry of snap the pool that
// holds the copy taken here, which is the pool of the replica it was taken
// from. A restore places its replica in that pool, and it has to find it
// after the replica is gone: the copy outlives the replica, and nothing
// else names the pool by then. LINSTOR keeps the pool on each snapshot
// volume for the same reason.
//
// Recorded once and never rewritten: a replica moved to another pool later
// leaves its earlier copies where they were. A snapshot taken before the
// field existed, or adopted, is recorded on the first pass that finds its
// replica, which is every pass until it does. With no replica left there is
// nothing to read the pool from, and the entry stays without one.
func (r *SnapshotReconciler) recordSnapshotPool(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) error {
	if snapshotPoolRecorded(snap.Status.NodeStatus, r.Config.NodeName) {
		return nil
	}

	pool, err := r.localReplicaPool(ctx, snap.Spec.ResourceDefinitionName)
	if err != nil || pool == "" {
		return err
	}

	return r.upsertPerNodeStatusField(ctx, snap, func(entry *blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		if entry.StoragePool != "" {
			return false
		}

		entry.StoragePool = pool

		return true
	})
}

// snapshotPoolRecorded reports whether nodeName's entry names its pool.
func snapshotPoolRecorded(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string) bool {
	collapsed, mine, _ := collapseNodeStatus(entries, nodeName)

	return mine >= 0 && collapsed[mine].StoragePool != ""
}

// localReplicaPool is the pool of this node's diskful replica of rdName, or
// "" when there is none.
func (r *SnapshotReconciler) localReplicaPool(ctx context.Context, rdName string) (string, error) {
	name, err := replicaCRName(ctx, r.Client, rdName, r.Config.NodeName)
	if err != nil {
		return "", err
	}

	var res blockstoriov1alpha1.Resource

	err = r.Get(ctx, client.ObjectKey{Name: name}, &res)
	if apierrors.IsNotFound(err) {
		return "", nil
	}

	if err != nil {
		return "", errors.Wrapf(err, "get replica %s", name)
	}

	return resolveDeleteStoragePool(&res), nil
}
