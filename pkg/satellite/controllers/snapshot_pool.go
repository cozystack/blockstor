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
	"slices"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// recordPoolOnEntry sets pool on nodeName's entry unless the entry already
// names one. A record is never rewritten: a replica moved to another pool
// later leaves its earlier copies where they were.
func recordPoolOnEntry(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName, pool string) {
	if pool == "" {
		return
	}

	for i := range entries {
		if naming.Same(entries[i].NodeName, nodeName) && entries[i].StoragePool == "" {
			entries[i].StoragePool = pool
		}
	}
}

// recordSnapshotPool records on this node's entry of a taken snapshot the
// pool that holds the copy, for a copy whose take did not record it: one
// taken or adopted before the record existed. A restore places its replica
// in that pool, and it has to find it after the replica is gone; LINSTOR
// keeps the pool on each snapshot volume for the same reason.
//
// The pool is asked of the local pools themselves, not read off the
// replica: the replica may have been re-created in another pool since,
// leaving the older copy where it was. The replica's pool only breaks a tie
// between pools that both report the copy (two LVM pools on one volume
// group). With no pool reporting it, nothing is recorded.
//
// The recorded check runs on the fresh object inside the upsert, so the
// pools are only probed while the entry names none.
func (r *SnapshotReconciler) recordSnapshotPool(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) error {
	return r.upsertPerNodeStatusField(ctx, snap, func(entry *blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		if entry.StoragePool != "" {
			return false
		}

		pool := r.poolHoldingCopy(ctx, snap)
		if pool == "" {
			return false
		}

		entry.StoragePool = pool

		return true
	})
}

// poolHoldingCopy is the local pool that holds this node's copy of snap, or
// "" when none, or more than one with no way to tell them apart, does.
func (r *SnapshotReconciler) poolHoldingCopy(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) string {
	if r.Config.Apply == nil {
		return ""
	}

	pools := r.Config.Apply.SnapshotPools(ctx, naming.Key(snap.Spec.ResourceDefinitionName), snap.Spec.SnapshotName)

	switch len(pools) {
	case 0:
		return ""
	case 1:
		return pools[0]
	}

	replicaPool, err := r.localReplicaPool(ctx, snap.Spec.ResourceDefinitionName)
	if err != nil || !slices.Contains(pools, replicaPool) {
		return ""
	}

	return replicaPool
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
