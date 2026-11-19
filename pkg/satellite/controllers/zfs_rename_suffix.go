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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// ZFSRenameSuffixProp is zfs.RenameSuffixProp. blockstor records it the
// way LINSTOR's controller does, from the satellite that is about to do the
// rename, so every path that deletes a replica gets it.
const ZFSRenameSuffixProp = zfs.RenameSuffixProp

// snapshotRenameSuffixProp is zfs.SnapshotRenameSuffixProp.
func snapshotRenameSuffixProp(node string) string { return zfs.SnapshotRenameSuffixProp(node) }

// recordRenameSuffix returns the replica's rename suffix, recording it first
// on the Resource and then on this node's Snapshots of the definition that do
// not carry one yet. The Resource is written first so a retry after a crash
// reuses the suffix the Snapshots may already hold. Only a replica on a ZFS
// pool is renamed, so only it gets one.
func (r *ResourceReconciler) recordRenameSuffix(ctx context.Context, res *blockstoriov1alpha1.Resource) (string, error) {
	pool := resolveDeleteStoragePool(res)
	if pool == "" {
		return "", nil
	}

	isZFS, err := r.poolIsZFS(ctx, pool)
	if err != nil || !isZFS {
		return "", err
	}

	suffix := res.Spec.Props[ZFSRenameSuffixProp]
	if suffix == "" {
		suffix = zfs.RenameSuffix(time.Now())

		base := res.DeepCopy()
		if res.Spec.Props == nil {
			res.Spec.Props = map[string]string{}
		}

		res.Spec.Props[ZFSRenameSuffixProp] = suffix

		err = r.Patch(ctx, res, client.MergeFrom(base))
		if err != nil {
			return "", errors.Wrap(err, "record the ZFS rename suffix on the Resource")
		}
	}

	return suffix, r.recordSnapshotRenameSuffix(ctx, res.Spec.ResourceDefinitionName, suffix)
}

func (r *ResourceReconciler) recordSnapshotRenameSuffix(ctx context.Context, rdName, suffix string) error {
	var list blockstoriov1alpha1.SnapshotList

	err := r.List(ctx, &list)
	if err != nil {
		return errors.Wrap(err, "list snapshots")
	}

	key := snapshotRenameSuffixProp(r.Config.NodeName)

	for i := range list.Items {
		snap := &list.Items[i]

		if !naming.Same(snap.Spec.ResourceDefinitionName, rdName) ||
			!slices.ContainsFunc(snap.Spec.Nodes, func(n string) bool { return naming.Same(n, r.Config.NodeName) }) ||
			snap.Spec.Props[key] != "" {
			continue
		}

		base := snap.DeepCopy()
		if snap.Spec.Props == nil {
			snap.Spec.Props = map[string]string{}
		}

		snap.Spec.Props[key] = suffix

		err = r.Patch(ctx, snap, client.MergeFrom(base))
		if err != nil {
			return errors.Wrapf(err, "record the ZFS rename suffix on Snapshot %s", snap.Name)
		}
	}

	return nil
}

// poolIsZFS reports whether this node's pool of that name is a ZFS pool.
func (r *ResourceReconciler) poolIsZFS(ctx context.Context, pool string) (bool, error) {
	var list blockstoriov1alpha1.StoragePoolList

	err := r.List(ctx, &list)
	if err != nil {
		return false, errors.Wrap(err, "list storage pools")
	}

	for i := range list.Items {
		sp := &list.Items[i]
		if naming.Same(sp.Spec.NodeName, r.Config.NodeName) && sp.Spec.PoolName == pool {
			return strings.HasPrefix(strings.ToUpper(sp.Spec.ProviderKind), "ZFS"), nil
		}
	}

	return false, nil
}
