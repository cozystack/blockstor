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
	"sort"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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

// ReplicasOnNode and PoolsOnNode are the node-scoped reads a node's fate is
// decided on, asked in the spelling the caller used and in the folded one.
//
// Nodes().Get and Delete fold the name, while spec.nodeName is stored
// verbatim, so a node addressed in a different case than its replicas were
// written with resolved for the delete and returned nothing for the gate: the
// refusal passed, the cascade reaped nothing, the node went, and the replicas
// still pointed at it. Both spellings are legal LINSTOR input. Asking in both
// covers the case where the operator's spelling differs from a lowercase
// write; a replica written in a third spelling is the boundary FoldName
// documents.
func ReplicasOnNode(ctx context.Context, st Store, node string) ([]apiv1.Resource, error) {
	return inBothSpellings(ctx, node, "replicas", st.Resources().ListByNode,
		func(r *apiv1.Resource) string { return r.Name })
}

// PoolsOnNode is ReplicasOnNode for storage pools.
func PoolsOnNode(ctx context.Context, st Store, node string) ([]apiv1.StoragePool, error) {
	return inBothSpellings(ctx, node, "storage pools", st.StoragePools().ListByNode,
		func(p *apiv1.StoragePool) string { return p.StoragePoolName })
}

// inBothSpellings runs a node-scoped read under the caller's spelling and the
// folded one, and returns every object once.
func inBothSpellings[T any](
	ctx context.Context, node, what string,
	read func(context.Context, string) ([]T, error), name func(*T) string,
) ([]T, error) {
	out, err := read(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("list %s on %s: %w", what, node, err)
	}

	folded := FoldName(node)
	if folded == node {
		return out, nil
	}

	more, err := read(ctx, folded)
	if err != nil {
		return nil, fmt.Errorf("list %s on %s: %w", what, folded, err)
	}

	seen := make(map[string]struct{}, len(out))
	for i := range out {
		seen[FoldName(name(&out[i]))] = struct{}{}
	}

	for i := range more {
		if _, dup := seen[FoldName(name(&more[i]))]; !dup {
			out = append(out, more[i])
		}
	}

	return out, nil
}

// CascadeOrphansForLostNode deletes every Resource replica and StoragePool
// that references the named node, which is what makes a forced node delete
// leave nothing pointing at an object that is gone.
func CascadeOrphansForLostNode(ctx context.Context, st Store, node string) error {
	resources, err := ReplicasOnNode(ctx, st, node)
	if err != nil {
		return err
	}

	for i := range resources {
		err = st.Resources().Delete(ctx, resources[i].Name, resources[i].NodeName)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("delete replica %s on %s: %w", resources[i].Name, node, err)
		}
	}

	pools, err := PoolsOnNode(ctx, st, node)
	if err != nil {
		return err
	}

	for i := range pools {
		err = st.StoragePools().Delete(ctx, pools[i].NodeName, pools[i].StoragePoolName)
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
	resources, err := ReplicasOnNode(ctx, st, node)
	if err != nil {
		return nil, nil, err
	}

	seen := map[string]struct{}{}
	rscRefs := make([]string, 0, len(resources))

	for i := range resources {
		if _, dup := seen[resources[i].Name]; dup {
			continue
		}

		seen[resources[i].Name] = struct{}{}

		rscRefs = append(rscRefs, resources[i].Name)
	}

	pools, err := PoolsOnNode(ctx, st, node)
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
		if pools[i].StoragePoolName == apiv1.DfltDisklessStorPoolName {
			continue
		}

		poolRefs = append(poolRefs, pools[i].StoragePoolName)
	}

	sort.Strings(rscRefs)
	sort.Strings(poolRefs)

	return rscRefs, poolRefs, nil
}
