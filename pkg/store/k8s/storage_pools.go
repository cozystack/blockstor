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

// crdName encodes the (pool, node) composite key into a single CRD name.
// LINSTOR accepts mixed-case names that k8s would rejected, so the result
// is run through Name() to slugify when needed.
//
// The order is `<pool>.<node>` — matches Resource (`<rd>.<node>`) and
// the CRD-level CEL rule `self.metadata.name == self.spec.poolName +
// '.' + self.spec.nodeName`. Any helper or test that builds the name
// by hand must use the same order; mismatched orders will be rejected
// by the apiserver on Create with a CEL validation error.
//
// IMPORTANT: blockstor's StoragePool CRDs may still carry an operator-
// chosen metadata.name (e.g. piraeus's "zfs-thin-w3" produced via
// `kubectl apply -f`) that does NOT follow this convention. The (node,
// pool) identity is anchored in Spec.NodeName + Spec.PoolName, not the
// CRD name. crdName() is authoritative for OUR own Create path (so
// newly-created pools land predictably AND satisfy CEL), but per-key
// reads/writes (Get/Delete/Update/SetCapacity) must resolve through
// resolveCRDName() to honour operator-managed CRDs. See Bug 55.
func crdName(node, pool string) string {
	return Name(pool + "." + node)
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
//
// Resolves the underlying CRD by Spec.NodeName / Spec.PoolName rather
// than assuming the canonical crdName() shape, so operator-managed
// pools (e.g. piraeus's `zfs-thin-w3` created via `kubectl apply -f`
// with Spec.NodeName=worker-3 + Spec.PoolName=zfs-thin) are reachable.
// See Bug 55.
func (s *storagePools) Get(ctx context.Context, node, pool string) (apiv1.StoragePool, error) {
	name, err := s.resolveCRDName(ctx, node, pool)
	if err != nil {
		return apiv1.StoragePool{}, err
	}

	var crd crdv1alpha1.StoragePool

	err = s.c.Get(ctx, types.NamespacedName{Name: name}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Race: matched in resolveCRDName, deleted before
			// we re-fetched. Treat as not-found.
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

// Delete removes the named pool. Registry-only: the REST DELETE
// handler folds ErrNotFound into a 200 success envelope, and the
// satellite-side finalizer (handlePoolDelete in pkg/satellite/
// controllers/storagepool.go) only deregisters the in-memory
// provider — on-disk pool teardown (vgremove/zpool destroy) is an
// out-of-band operator concern.
//
// Resolves the underlying CRD by Spec.NodeName / Spec.PoolName so
// operator-managed CRDs whose metadata.name doesn't follow blockstor's
// canonical "node.pool" shape are still deletable. See Bug 55.
func (s *storagePools) Delete(ctx context.Context, node, pool string) error {
	name, err := s.resolveCRDName(ctx, node, pool)
	if err != nil {
		return err
	}

	crd := &crdv1alpha1.StoragePool{ObjectMeta: metav1.ObjectMeta{Name: name}}

	err = s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Race: matched in resolveCRDName, deleted before
			// we issued our own DELETE. Treat as already-gone.
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

	// Upstream LINSTOR populates FreeSpaceMgrName with `<Node>:<Pool>`
	// for local pools. The Python CLI does `':' not in
	// free_space_mgr_name` and crashes with TypeError when the field
	// is null. Mirror upstream — for shared pools, upstream uses the
	// shared_space identifier; we follow the same convention.
	fsmName := crd.Spec.NodeName + ":" + poolName
	if crd.Spec.SharedSpaceID != "" {
		fsmName = crd.Spec.SharedSpaceID
	}

	// Surface the satellite's PoolMissing signal as the wire `state`
	// field. Upstream LINSTOR uses "Ok" / "Faulty" / "Error" — we map
	// PoolMissing=true → "Faulty", everything else → "Ok".
	state := "Ok"
	if crd.Status.PoolMissing {
		state = "Faulty"
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
		FreeSpaceMgrName: fsmName,
		UUID:             string(crd.UID),
		State:            state,
		PoolMissing:      crd.Status.PoolMissing,
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

	// Store raw KiB. The CRD field was originally documented as
	// "bytes" but the wire shape (gRPC SatellitePool.FreeCapacityKib,
	// REST `free_capacity_kib`) is KiB end-to-end; multiplying by
	// 1024 here introduced a 1024x reporting error for /v1/view/
	// storage-pools and the autoplacer's capacity comparisons. The
	// shared storetest suite catches the divergence between the
	// InMemory store (raw KiB) and this implementation.
	existing.Status.FreeCapacity = freeKib
	existing.Status.TotalCapacity = totalKib
	existing.Status.SupportsSnapshots = supportsSnap

	err = s.c.Status().Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "status update StoragePool %s/%s", node, pool)
	}

	return nil
}

// resolveCRDName finds the actual metadata.name of the StoragePool CRD
// whose Spec.NodeName / Spec.PoolName match the given pair. Returns
// store.ErrNotFound when no CRD matches, or a non-sentinel error when
// multiple CRDs claim the same (node, pool) identity (a data-integrity
// violation that the operator must resolve — we refuse to auto-pick a
// winner since deleting/updating the wrong one would silently corrupt
// state).
//
// We try the canonical crdName() shape first as a fast-path: the vast
// majority of clusters use blockstor's Create() path and the direct
// Get is O(1). On miss, we fall back to a full List + Spec filter,
// which is the only correct way to find operator-named CRDs (e.g.
// piraeus's `zfs-thin-w3` from `kubectl apply -f`). Bug 55.
func (s *storagePools) resolveCRDName(ctx context.Context, node, pool string) (string, error) {
	// Fast path: assume the CRD was created by our Create() and
	// follows the canonical name shape. A single Get avoids the
	// full-List cost on every Delete/Get.
	canonical := crdName(node, pool)

	var fast crdv1alpha1.StoragePool

	err := s.c.Get(ctx, types.NamespacedName{Name: canonical}, &fast)
	if err == nil {
		// Defence in depth: if the CRD's Spec disagrees with the
		// name (someone renamed the spec inside an operator-named
		// CRD that happens to collide with our canonical shape),
		// fall through to the List+filter pass. Mismatch is rare
		// enough that the cost is acceptable.
		if fast.Spec.NodeName == node && fast.Spec.PoolName == pool {
			return canonical, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return "", errors.Wrapf(err, "get StoragePool by canonical name %q", canonical)
	}

	// Slow path: List + Spec filter. This is the same shape List()
	// uses, so operator-named CRDs (Spec.NodeName=N, Spec.PoolName=P,
	// metadata.name=anything) are found here.
	var crdList crdv1alpha1.StoragePoolList

	listErr := s.c.List(ctx, &crdList)
	if listErr != nil {
		return "", errors.Wrap(listErr, "list StoragePool CRDs")
	}

	var matches []string

	for i := range crdList.Items {
		item := &crdList.Items[i]
		if item.Spec.NodeName == node && item.Spec.PoolName == pool {
			matches = append(matches, item.Name)
		}
	}

	switch len(matches) {
	case 0:
		return "", errors.Wrapf(store.ErrNotFound, "storage pool %q on node %q", pool, node)
	case 1:
		return matches[0], nil
	default:
		// Data-integrity violation. Surface loudly with all
		// colliding CRD names so the operator can pick one and
		// delete the rest. Auto-picking would risk deleting the
		// wrong CRD (and the satellite finalizer would then
		// unregister the live provider).
		return "", errors.Wrap(
			errors.Newf("multiple StoragePool CRDs claim node=%q pool=%q: %v (operator must resolve duplicates)",
				node, pool, matches),
			"resolve StoragePool by spec")
	}
}

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
