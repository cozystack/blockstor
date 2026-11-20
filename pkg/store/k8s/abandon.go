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

package k8s

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/finalizers"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

var (
	_ store.ReplicaAbandoner     = (*resources)(nil)
	_ store.PoolAbandoner        = (*storagePools)(nil)
	_ store.SnapshotNodeReleaser = (*snapshots)(nil)
)

// AbandonReplica is store.ReplicaAbandoner.
func (s *resources) AbandonReplica(ctx context.Context, rdName, node string) error {
	key := types.NamespacedName{Name: resourceCRDName(rdName, node)}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var crd crdv1alpha1.Resource

		err := s.c.Get(ctx, key, &crd)
		if err != nil {
			return err //nolint:wrapcheck // classified below
		}

		if crd.Annotations[finalizers.StorageAbandoned] != "" {
			return nil
		}

		base := crd.DeepCopy()
		if crd.Annotations == nil {
			crd.Annotations = map[string]string{}
		}

		crd.Annotations[finalizers.StorageAbandoned] = "node-lost"

		return s.c.Patch(ctx, &crd, ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return errors.Wrapf(err, "mark Resource %s/%s abandoned", rdName, node)
	}

	err = s.Delete(ctx, rdName, node)
	if err != nil {
		return err
	}

	return releaseFinalizers(ctx, s.c, key, &crdv1alpha1.Resource{},
		finalizers.SatelliteResource, finalizers.LegacyResource)
}

// AbandonPool is store.PoolAbandoner.
func (s *storagePools) AbandonPool(ctx context.Context, node, pool string) error {
	name, err := s.resolveCRDName(ctx, node, pool)
	if err != nil {
		return err
	}

	err = s.Delete(ctx, node, pool)
	if err != nil {
		return err
	}

	return releaseFinalizers(ctx, s.c, types.NamespacedName{Name: name}, &crdv1alpha1.StoragePool{},
		finalizers.SatelliteStoragePool)
}

// ReleaseSnapshotsOnNode is store.SnapshotNodeReleaser.
func (s *snapshots) ReleaseSnapshotsOnNode(ctx context.Context, node string) error {
	var list crdv1alpha1.SnapshotList

	err := s.c.List(ctx, &list)
	if err != nil {
		return errors.Wrap(err, "list Snapshots")
	}

	released := finalizers.SnapshotNode(node)

	for i := range list.Items {
		snap := &list.Items[i]
		if !slices.ContainsFunc(snap.Spec.Nodes, func(n string) bool { return naming.Same(n, node) }) {
			continue
		}

		drop := []string{released}

		if finalizers.OthersHoldTheirOwn(snap.Spec.Nodes, snap.Finalizers, node) {
			drop = append(drop, finalizers.SharedSatelliteSnapshot)
		}

		err = releaseFinalizers(ctx, s.c, types.NamespacedName{Name: snap.Name}, &crdv1alpha1.Snapshot{}, drop...)
		if err != nil {
			return err
		}
	}

	return nil
}

// releaseFinalizers removes the named finalizers from the object, re-reading
// it on conflict. An object that is already gone is released.
func releaseFinalizers(ctx context.Context, c ctrlclient.Client, key types.NamespacedName, obj ctrlclient.Object, names ...string) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		err := c.Get(ctx, key, obj)
		if err != nil {
			return err //nolint:wrapcheck // classified below
		}

		kept := slices.DeleteFunc(slices.Clone(obj.GetFinalizers()), func(f string) bool { return slices.Contains(names, f) })
		if len(kept) == len(obj.GetFinalizers()) {
			return nil
		}

		obj.SetFinalizers(kept)

		return c.Update(ctx, obj)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "release finalizers of %s", key.Name)
	}

	return nil
}
