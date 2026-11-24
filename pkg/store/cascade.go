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
	"slices"
	"sort"
	"strconv"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// The teardown a delete has to perform, shared by the two write doors.
//
// Deleting a parent without reaping its children does not fail: the objects
// simply stay, pointing at something that is gone. A Resource whose RD
// vanished never gets a DeletionTimestamp, so the satellite's finalizer never
// runs and `drbdadm down` never happens — the DRBD minor, port and peer
// entries stay live on every satellite, and the next create with the same
// name collides with them. A StoragePool whose Node vanished is the same
// shape, one level up.
//
// The REST door has done this since it was written. The CLI writes the same
// objects directly, so it has to do the same thing, and doing it from one
// implementation is what keeps the two answers identical.

// CascadeDeleteMaxPasses bounds the retry-until-empty loop below.
//
// A single enumerate-then-delete pass races the RD reconciler's
// auto-tiebreaker path: it takes its own snapshot of the children and, on
// seeing two diskful and no witness, creates a fresh TIE_BREAKER Resource. A
// witness landing between the list and the delete loop — or after the loop
// but before the RD itself goes — is a phantom, because its parent vanishes
// from under it and the reconciler's own cleanup bails on a NotFound parent.
//
// The cap is low on purpose: each pass costs an apiserver round-trip and a
// healthy controller stamps at most one witness per RD.
const CascadeDeleteMaxPasses = 5

// CascadeDeleteResources deletes every Resource replica under the named
// resource definition, so each satellite's finalizer can drain its own DRBD
// state before the parent goes.
//
// A child that has already vanished is not an error: it races another
// controller or a previous partial cascade. A NotFound on the parent means
// there is nothing to cascade, and the caller's own delete decides what that
// means.
func CascadeDeleteResources(ctx context.Context, st Store, rdName string) error {
	for range CascadeDeleteMaxPasses {
		children, err := st.Resources().ListByDefinition(ctx, rdName)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}

			return fmt.Errorf("list replicas of %s: %w", rdName, err)
		}

		if len(children) == 0 {
			return nil
		}

		for i := range children {
			err = st.Resources().Delete(ctx, rdName, children[i].NodeName)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("delete replica %s on %s: %w", rdName, children[i].NodeName, err)
			}
		}
	}

	return nil
}

// ReplicasOnNode is the node-scoped replica read a node's fate is decided on,
// asked in every spelling the node's objects can carry. ReferencesOnNode and
// CascadeOrphansForLostNode ask the storage pools the same way.
//
// Nodes().Get and Delete fold the name, while spec.nodeName is stored
// verbatim, so a node addressed in a different case than its replicas were
// written with resolved for the delete and returned nothing for the gate: the
// refusal passed, the cascade reaped nothing, the node went, and the replicas
// still pointed at it.
//
// Three spellings are asked, see NodeSpellings. The one that remains out of
// reach is a spelling that is neither the caller's, nor the folded one, nor
// the node's registered one: a replica created by hand under yet another case.
// A field selector compares the stored spelling verbatim, which is the
// boundary pkg/store/k8s/spellings.go documents. The merge base asked only the
// caller's spelling, verbatim.
func ReplicasOnNode(ctx context.Context, st Store, node string) ([]apiv1.Resource, error) {
	spellings, err := NodeSpellings(ctx, st, node)
	if err != nil {
		return nil, err
	}

	return replicasUnder(ctx, st, spellings)
}

// NodeSpellings is the set of spellings a node-scoped read has to be asked in:
// the caller's, the folded one, and the one the node is registered under.
//
// The registered spelling is the one that matters most and the one the other
// two missed. Adoption from LINSTOR and linstor-csi write a replica's node
// under the name the node was registered with, so a node registered as
// `NODE-1` carries replicas on `NODE-1`, and an operator typing the canonical
// `node-1` asked in one spelling, since that one is already folded, and found
// nothing.
//
// It is read from the node listing and compared with naming.Key, rather than
// taken from Nodes().Get: the Kubernetes store's Get folds through the CRD
// slug, the in-memory one does not, and the listing gives both the same
// answer. A node that is not registered (already gone, or never was)
// contributes nothing, and the two remaining spellings are still asked.
//
// That makes the answer depend on whether the node row still exists, so a
// caller that re-asks after deleting the node resolves the spellings before the
// delete and passes them to ReferencesUnderSpellings for both walks.
func NodeSpellings(ctx context.Context, st Store, node string) ([]string, error) {
	spellings := []string{node}

	add := func(spelling string) {
		if !slices.Contains(spellings, spelling) {
			spellings = append(spellings, spelling)
		}
	}

	add(naming.Key(node))

	nodes, err := st.Nodes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes to resolve %s: %w", node, err)
	}

	for i := range nodes {
		if naming.Key(nodes[i].Name) == naming.Key(node) {
			add(nodes[i].Name)
		}
	}

	return spellings, nil
}

func replicasUnder(ctx context.Context, st Store, spellings []string) ([]apiv1.Resource, error) {
	return inEverySpelling(ctx, spellings, "replicas", st.Resources().ListByNode,
		func(r *apiv1.Resource) string { return r.Name })
}

func poolsUnder(ctx context.Context, st Store, spellings []string) ([]apiv1.StoragePool, error) {
	return inEverySpelling(ctx, spellings, "storage pools", st.StoragePools().ListByNode,
		func(p *apiv1.StoragePool) string { return p.StoragePoolName })
}

// inEverySpelling runs a node-scoped read under each spelling, and returns
// every object once.
func inEverySpelling[T any](
	ctx context.Context, spellings []string, what string,
	read func(context.Context, string) ([]T, error), name func(*T) string,
) ([]T, error) {
	var out []T

	seen := map[string]struct{}{}

	for _, spelling := range spellings {
		found, err := read(ctx, spelling)
		if err != nil {
			return nil, fmt.Errorf("list %s on %s: %w", what, spelling, err)
		}

		for i := range found {
			key := naming.Key(name(&found[i]))
			if _, dup := seen[key]; dup {
				continue
			}

			seen[key] = struct{}{}

			out = append(out, found[i])
		}
	}

	return out, nil
}

// CascadeOrphansForLostNode deletes every Resource replica and StoragePool
// that references the named node, which is what makes a forced node delete
// leave nothing pointing at an object that is gone.
//
// The node is gone, so its satellite never deletes anything: as LINSTOR's
// `node lost`, the objects are removed and the storage stays on the node.
// A store whose objects carry satellite finalizers releases them here (see
// ReplicaAbandoner), since nothing else ever would, and drops the node's hold
// on the Snapshots it is listed on.
func CascadeOrphansForLostNode(ctx context.Context, st Store, node string) error {
	spellings, err := NodeSpellings(ctx, st, node)
	if err != nil {
		return err
	}

	resources, err := replicasUnder(ctx, st, spellings)
	if err != nil {
		return err
	}

	replicas := st.Resources()
	abandoner, abandons := replicas.(ReplicaAbandoner)

	for i := range resources {
		if abandons {
			err = abandoner.AbandonReplica(ctx, resources[i].Name, resources[i].NodeName)
		} else {
			err = replicas.Delete(ctx, resources[i].Name, resources[i].NodeName)
		}

		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("delete replica %s on %s: %w", resources[i].Name, node, err)
		}
	}

	if releaser, ok := st.Snapshots().(SnapshotNodeReleaser); ok {
		for _, spelling := range spellings {
			err = releaser.ReleaseSnapshotsOnNode(ctx, spelling)
			if err != nil {
				return fmt.Errorf("release snapshots on %s: %w", node, err)
			}
		}
	}

	pools, err := poolsUnder(ctx, st, spellings)
	if err != nil {
		return err
	}

	poolStore := st.StoragePools()
	poolAbandoner, abandonsPools := poolStore.(PoolAbandoner)

	for i := range pools {
		if abandonsPools {
			err = poolAbandoner.AbandonPool(ctx, pools[i].NodeName, pools[i].StoragePoolName)
		} else {
			err = poolStore.Delete(ctx, pools[i].NodeName, pools[i].StoragePoolName)
		}

		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("delete storage pool %s on %s: %w", pools[i].StoragePoolName, node, err)
		}
	}

	return nil
}

// ReferencesOnNode lists the resource definitions with a replica on the node
// and the storage pools registered against it, sorted so the refusal names
// them in a stable order.
//
// This is what a plain node delete is refused on: the operator either clears
// the references or says explicitly that the node is gone.
func ReferencesOnNode(ctx context.Context, st Store, node string) ([]string, []string, error) {
	spellings, err := NodeSpellings(ctx, st, node)
	if err != nil {
		return nil, nil, err
	}

	return ReferencesUnderSpellings(ctx, st, spellings)
}

// ReferencesUnderSpellings is ReferencesOnNode over spellings the caller
// resolved with NodeSpellings. A caller that walks the references again after
// deleting the node has to use it: by then the node row is gone and a fresh
// NodeSpellings no longer knows the registered spelling.
func ReferencesUnderSpellings(ctx context.Context, st Store, spellings []string) ([]string, []string, error) {
	resources, err := replicasUnder(ctx, st, spellings)
	if err != nil {
		return nil, nil, err
	}

	seen := map[string]struct{}{}
	rscRefs := make([]string, 0, len(resources))

	for i := range resources {
		if _, dup := seen[naming.Key(resources[i].Name)]; dup {
			continue
		}

		seen[naming.Key(resources[i].Name)] = struct{}{}

		rscRefs = append(rscRefs, resources[i].Name)
	}

	pools, err := poolsUnder(ctx, st, spellings)
	if err != nil {
		return nil, nil, err
	}

	poolRefs := make([]string, 0, len(pools))

	for i := range pools {
		// The default diskless pool is created on every node create, so
		// counting it makes the refusal fire on a freshly registered idle
		// node — and it names a pool the operator cannot remove, so the
		// node becomes undeletable. The REST refusal has always skipped
		// it; the shared implementation has to as well.
		if naming.Same(pools[i].StoragePoolName, apiv1.DfltDisklessStorPoolName) {
			continue
		}

		poolRefs = append(poolRefs, pools[i].StoragePoolName)
	}

	sort.Strings(rscRefs)
	sort.Strings(poolRefs)

	return rscRefs, poolRefs, nil
}

// SnapshotsOnNode lists, as `<rd>/<snapshot>` and sorted, the snapshots that
// keep a copy on a node under any of the spellings. Each such node holds the
// Snapshot until its satellite has deleted that copy, so the node, and the
// pool the copy lives in, have to stay until then: with the pool gone the
// copy cannot be deleted, and with the node gone nothing ever runs there.
func SnapshotsOnNode(ctx context.Context, st Store, spellings []string) ([]string, error) {
	snapshots, err := st.Snapshots().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}

	var refs []string

	for i := range snapshots {
		onNode := slices.ContainsFunc(snapshots[i].Nodes, func(n string) bool {
			return slices.ContainsFunc(spellings, func(s string) bool { return naming.Same(n, s) })
		})
		if onNode {
			refs = append(refs, snapshots[i].ResourceName+"/"+snapshots[i].Name)
		}
	}

	sort.Strings(refs)

	return refs, nil
}

// SnapshotsInPool is SnapshotsOnNode for a pool delete: the snapshots whose
// copy on the node is in the pool, as LINSTOR refuses a pool delete over the
// snapshot-volumes in it. The satellite records the pool of its copy once the
// copy exists; a copy with no pool recorded yet may be in any diskful pool of
// the node, so it counts for each of them. A diskless pool, or one that does
// not exist, holds none.
func SnapshotsInPool(ctx context.Context, st Store, node, pool string) ([]string, error) {
	sp, err := st.StoragePools().Get(ctx, node, pool)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("get storage pool %s on %s: %w", pool, node, err)
	}

	if sp.ProviderKind == apiv1.StoragePoolKindDiskless {
		return nil, nil
	}

	spellings, err := NodeSpellings(ctx, st, node)
	if err != nil {
		return nil, err
	}

	snapshots, err := st.Snapshots().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}

	isNode := func(n string) bool {
		return slices.ContainsFunc(spellings, func(s string) bool { return naming.Same(n, s) })
	}

	var refs []string

	for i := range snapshots {
		if !slices.ContainsFunc(snapshots[i].Nodes, isNode) {
			continue
		}

		if copyPool := recordedCopyPool(&snapshots[i], isNode); copyPool != "" && !naming.Same(copyPool, pool) {
			continue
		}

		refs = append(refs, snapshots[i].ResourceName+"/"+snapshots[i].Name)
	}

	sort.Strings(refs)

	return refs, nil
}

// recordedCopyPool is the pool the satellite recorded for the snapshot's copy
// on the node, empty while none is recorded.
func recordedCopyPool(snap *apiv1.Snapshot, isNode func(string) bool) string {
	for i := range snap.Snapshots {
		if isNode(snap.Snapshots[i].NodeName) {
			return snap.Snapshots[i].StoragePool
		}
	}

	return ""
}

// ReplicasInPool returns the `<rd>/<vol_nr>` keys of every volume on the node
// whose pool is the one named, and `<rd>/spec` for a replica that has not
// reported volumes yet but already pins the pool in `StorPoolName`. A pool
// delete is refused on them: a replica whose pool is gone cannot have its
// storage deleted, so its delete never finishes.
func ReplicasInPool(ctx context.Context, st Store, node, pool string) ([]string, error) {
	all, err := st.Resources().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list resources: %w", err)
	}

	var refs []string

	for i := range all {
		if !naming.Same(all[i].NodeName, node) {
			continue
		}

		matched := false

		for j := range all[i].Volumes {
			vol := &all[i].Volumes[j]
			if naming.Same(vol.StoragePool, pool) {
				refs = append(refs, all[i].Name+"/"+strconv.FormatInt(int64(vol.VolumeNumber), 10))
				matched = true

				break
			}
		}

		if !matched && naming.Same(all[i].Props["StorPoolName"], pool) {
			refs = append(refs, all[i].Name+"/spec")
		}
	}

	return refs, nil
}
