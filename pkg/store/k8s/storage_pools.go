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
	"sort"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// storagePools implements store.StoragePoolStore against the StoragePool CRD.
type storagePools struct {
	c ctrlclient.Client
}

// crdName encodes the (node, pool) composite key into a single CRD name.
// LINSTOR accepts mixed-case names that k8s would reject, so the result
// is run through Name() to slugify when needed.
func crdName(node, pool string) string {
	return Name(node + "." + pool)
}

// List returns every StoragePool CRD as a wire-shape value, sorted by
// (node, pool).
func (s *storagePools) List(ctx context.Context) ([]apiv1.StoragePool, error) {
	var crdList crdv1alpha1.StoragePoolList

	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list StoragePool CRDs")
	}

	out := make([]apiv1.StoragePool, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireStoragePool(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeName != out[j].NodeName {
			return out[i].NodeName < out[j].NodeName
		}

		return out[i].StoragePoolName < out[j].StoragePoolName
	})

	return out, nil
}

// ListByNode returns pools on the named node. We use a label selector so
// k8s narrows the list server-side rather than us filtering after the fact.
func (s *storagePools) ListByNode(ctx context.Context, node string) ([]apiv1.StoragePool, error) {
	var crdList crdv1alpha1.StoragePoolList

	err := s.c.List(ctx, &crdList,
		ctrlclient.MatchingLabels{LabelNodeName: node})
	if err != nil {
		return nil, errors.Wrapf(err, "list StoragePool CRDs on node %q", node)
	}

	out := make([]apiv1.StoragePool, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireStoragePool(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].StoragePoolName < out[j].StoragePoolName
	})

	return out, nil
}

// Get returns the named pool on the named node, or ErrNotFound.
func (s *storagePools) Get(ctx context.Context, node, pool string) (apiv1.StoragePool, error) {
	var crd crdv1alpha1.StoragePool

	err := s.c.Get(ctx, types.NamespacedName{Name: crdName(node, pool)}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.StoragePool{}, errors.Wrapf(store.ErrNotFound, "storage pool %q on node %q", pool, node)
		}

		return apiv1.StoragePool{}, errors.Wrapf(err, "get StoragePool %s/%s", node, pool)
	}

	return crdToWireStoragePool(&crd), nil
}

// Create persists a new StoragePool CRD.
func (s *storagePools) Create(ctx context.Context, in *apiv1.StoragePool) error {
	if in == nil {
		return errors.New("nil StoragePool")
	}

	crd := wireToCRDStoragePool(in)

	err := s.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "storage pool %q on node %q", in.StoragePoolName, in.NodeName)
		}

		return errors.Wrapf(err, "create StoragePool %s/%s", in.NodeName, in.StoragePoolName)
	}

	return nil
}

// Update overwrites the spec of an existing pool.
func (s *storagePools) Update(ctx context.Context, in *apiv1.StoragePool) error {
	if in == nil {
		return errors.New("nil StoragePool")
	}

	var existing crdv1alpha1.StoragePool

	key := types.NamespacedName{Name: crdName(in.NodeName, in.StoragePoolName)}

	err := s.c.Get(ctx, key, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "storage pool %q on node %q", in.StoragePoolName, in.NodeName)
		}

		return errors.Wrapf(err, "get StoragePool %s/%s", in.NodeName, in.StoragePoolName)
	}

	existing.Spec = wireToCRDStoragePoolSpec(in)

	err = s.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update StoragePool %s/%s", in.NodeName, in.StoragePoolName)
	}

	return nil
}

// Delete removes the named pool.
func (s *storagePools) Delete(ctx context.Context, node, pool string) error {
	crd := &crdv1alpha1.StoragePool{ObjectMeta: metav1.ObjectMeta{Name: crdName(node, pool)}}

	err := s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "storage pool %q on node %q", pool, node)
		}

		return errors.Wrapf(err, "delete StoragePool %s/%s", node, pool)
	}

	return nil
}

// crdToWireStoragePool flattens a StoragePool CRD into the LINSTOR REST shape.
func crdToWireStoragePool(crd *crdv1alpha1.StoragePool) apiv1.StoragePool {
	poolName := crd.Spec.PoolName
	if poolName == "" {
		// Recover pool name from labels (set on Create), fall back to the
		// portion of metadata.name after the first '.'.
		if l, ok := crd.Labels[LabelPoolName]; ok {
			poolName = l
		}
	}

	return apiv1.StoragePool{
		StoragePoolName:  poolName,
		NodeName:         crd.Spec.NodeName,
		ProviderKind:     crd.Spec.ProviderKind,
		SharedSpaceID:    crd.Spec.SharedSpaceID,
		Props:            crd.Spec.Props,
		FreeCapacity:     crd.Status.FreeCapacity,
		TotalCapacity:    crd.Status.TotalCapacity,
		SupportsSnapshot: crd.Status.SupportsSnapshots,
		StaticTraits:     crd.Status.StaticTraits,
		UUID:             string(crd.UID),
	}
}

// SetCapacity updates the pool's Status subresource with live free /
// total / supports-snapshots numbers. Avoids a Spec rewrite so a
// Hello racing with a periodic capacity push doesn't lose either side.
func (s *storagePools) SetCapacity(ctx context.Context, node, pool string, freeKib, totalKib int64, supportsSnap bool) error {
	var existing crdv1alpha1.StoragePool

	err := s.c.Get(ctx, types.NamespacedName{Name: crdName(node, pool)}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "storage pool %q on node %q", pool, node)
		}

		return errors.Wrapf(err, "get StoragePool %s/%s", node, pool)
	}

	existing.Status.FreeCapacity = freeKib * bytesPerKib
	existing.Status.TotalCapacity = totalKib * bytesPerKib
	existing.Status.SupportsSnapshots = supportsSnap

	err = s.c.Status().Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "status update StoragePool %s/%s", node, pool)
	}

	return nil
}

// bytesPerKib so the satellite-side `kib` numbers convert cleanly
// into the CRD's bytes-shaped Status fields without a magic literal.
const bytesPerKib = 1024

// wireToCRDStoragePool builds a fresh CRD from an apiv1.StoragePool.
func wireToCRDStoragePool(in *apiv1.StoragePool) *crdv1alpha1.StoragePool {
	return &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdName(in.NodeName, in.StoragePoolName),
			Labels: map[string]string{
				LabelNodeName: in.NodeName,
				LabelPoolName: in.StoragePoolName,
			},
		},
		Spec: wireToCRDStoragePoolSpec(in),
	}
}

// wireToCRDStoragePoolSpec is the spec-only converter for Update.
func wireToCRDStoragePoolSpec(in *apiv1.StoragePool) crdv1alpha1.StoragePoolSpec {
	return crdv1alpha1.StoragePoolSpec{
		NodeName:      in.NodeName,
		PoolName:      in.StoragePoolName,
		ProviderKind:  in.ProviderKind,
		SharedSpaceID: in.SharedSpaceID,
		Props:         in.Props,
	}
}
