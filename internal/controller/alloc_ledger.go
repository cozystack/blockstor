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

package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// allocLedger holds the DRBD identities this controller has handed
// out and its cache may not show yet, the way LINSTOR keeps its number
// pools in the controller's memory.
//
// The allocator reads the taken node ids, ports and minors from the
// cache. A cache trails the writes this process just made, so two
// allocations in a row could both see a value as free and both take
// it; the ledger closes that window. A value is reserved here as soon
// as it is chosen, before the write that persists it: a write whose
// answer is lost (a timeout, a 5xx) may still have landed, and the
// retry that finds the value already persisted has nothing left to
// record. Every read of a taken set adds what is reserved.
//
// A reservation is released once the cache shows the object, under the
// reserved UID, carrying the reserved fields, or once the API server
// says the object is gone or replaced. A cache that does not list the
// object at all says nothing: definitions and replicas come through
// separate informers, and the replica that triggers an allocation is
// routinely listed before its freshly created definition is.
//
// Reading the cache instead of the apiserver is what keeps a burst of
// new replicas from queueing: each allocation used to list every
// Resource in the cluster from the apiserver several times while
// holding clusterAllocMu, which made the allocations of a burst run
// strictly one after another at the speed of those lists. The point
// reads past the cache here happen only while the cache trails.
//
// Every access happens under clusterAllocMu, so the ledger needs no
// lock of its own. The writes that persist a reservation run after the
// mutex is released: the reservation is what keeps the value taken
// while they are in flight.
type allocLedger struct {
	replicas map[string]replicaGrant
	minors   map[string]minorGrant
}

// replicaGrant is the node id and port reserved for one Resource.
type replicaGrant struct {
	name   string
	uid    types.UID
	rd     string
	node   string
	nodeID *int32
	port   *int32
}

// minorGrant is the minor of every volume reserved for one definition.
type minorGrant struct {
	name   string
	uid    types.UID
	minors map[int32]int32
}

// recordReplica reserves the identities res carries.
func (l *allocLedger) recordReplica(res *blockstoriov1alpha1.Resource) {
	if res.Spec.DRBDNodeID == nil && res.Spec.DRBDPort == nil {
		return
	}

	if l.replicas == nil {
		l.replicas = map[string]replicaGrant{}
	}

	l.replicas[res.Name] = replicaGrant{
		name:   res.Name,
		uid:    res.UID,
		rd:     res.Spec.ResourceDefinitionName,
		node:   res.Spec.NodeName,
		nodeID: copyI32(res.Spec.DRBDNodeID),
		port:   copyI32(res.Spec.DRBDPort),
	}
}

// recordMinors reserves the minors rd carries.
func (l *allocLedger) recordMinors(rd *blockstoriov1alpha1.ResourceDefinition) {
	grant := minorGrant{name: rd.Name, uid: rd.UID, minors: map[int32]int32{}}

	for i := range rd.Spec.VolumeDefinitions {
		if m := rd.Spec.VolumeDefinitions[i].DRBDMinor; m != nil {
			grant.minors[rd.Spec.VolumeDefinitions[i].VolumeNumber] = *m
		}
	}

	if len(grant.minors) == 0 {
		return
	}

	if l.minors == nil {
		l.minors = map[string]minorGrant{}
	}

	l.minors[naming.Key(rd.Name)] = grant
}

// pendingReplicas releases every reservation that is settled and
// returns the rest: the ones the cache still trails.
func (l *allocLedger) pendingReplicas(
	ctx context.Context, reader client.Reader, cached []blockstoriov1alpha1.Resource,
) ([]replicaGrant, error) {
	if len(l.replicas) == 0 {
		return nil, nil
	}

	byName := make(map[string]*blockstoriov1alpha1.Resource, len(cached))
	for i := range cached {
		byName[cached[i].Name] = &cached[i]
	}

	out := make([]replicaGrant, 0, len(l.replicas))

	for name, grant := range l.replicas {
		if res, ok := byName[name]; ok && res.UID == grant.uid {
			if replicaShows(res, &grant) {
				delete(l.replicas, name)

				continue
			}

			out = append(out, grant)

			continue
		}

		var live blockstoriov1alpha1.Resource

		held, err := reservationHeld(ctx, reader, client.ObjectKey{Name: grant.name}, &live, grant.uid)
		if err != nil {
			return nil, err
		}

		if !held {
			delete(l.replicas, name)

			continue
		}

		out = append(out, grant)
	}

	return out, nil
}

// pendingMinors releases every reservation that is settled and returns
// the rest keyed by definition.
func (l *allocLedger) pendingMinors(
	ctx context.Context, reader client.Reader, cached []blockstoriov1alpha1.ResourceDefinition,
) (map[string]minorGrant, error) {
	if len(l.minors) == 0 {
		return map[string]minorGrant{}, nil
	}

	byName := make(map[string]*blockstoriov1alpha1.ResourceDefinition, len(cached))
	for i := range cached {
		byName[naming.Key(cached[i].Name)] = &cached[i]
	}

	out := make(map[string]minorGrant, len(l.minors))

	for key, grant := range l.minors {
		if rd, ok := byName[key]; ok && rd.UID == grant.uid {
			if definitionShows(rd, grant) {
				delete(l.minors, key)

				continue
			}

			out[key] = grant

			continue
		}

		var live blockstoriov1alpha1.ResourceDefinition

		held, err := reservationHeld(ctx, reader, client.ObjectKey{Name: naming.Key(grant.name)}, &live, grant.uid)
		if err != nil {
			return nil, err
		}

		if !held {
			delete(l.minors, key)

			continue
		}

		out[key] = grant
	}

	return out, nil
}

// reservationHeld asks the API server whether the object a reservation
// was made for still exists under the reserved UID. It is asked only
// when the cache does not list that object, or lists another one under
// its name: a cache that has not seen the object yet, or still shows
// the one it replaced, cannot tell either way.
func reservationHeld(ctx context.Context, reader client.Reader, key client.ObjectKey, obj client.Object, uid types.UID) (bool, error) {
	err := reader.Get(ctx, key, obj)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}

		return false, err
	}

	return obj.GetUID() == uid, nil
}

// replicaShows reports whether the cached res carries every reserved
// field. Spec identities are written once, so a cached value that is
// set is the final one even when it is not the reserved value.
func replicaShows(res *blockstoriov1alpha1.Resource, grant *replicaGrant) bool {
	return (grant.nodeID == nil || res.Spec.DRBDNodeID != nil) &&
		(grant.port == nil || res.Spec.DRBDPort != nil)
}

// definitionShows reports whether the cached rd carries a minor on
// every volume a minor was reserved for.
func definitionShows(rd *blockstoriov1alpha1.ResourceDefinition, grant minorGrant) bool {
	for vn := range grant.minors {
		found := false

		for i := range rd.Spec.VolumeDefinitions {
			vd := &rd.Spec.VolumeDefinitions[i]
			if vd.VolumeNumber == vn && vd.DRBDMinor != nil {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// cachedResources lists every Resource from the cache without copying
// them; the caller only reads.
func (r *ResourceReconciler) cachedResources(ctx context.Context) ([]blockstoriov1alpha1.Resource, error) {
	var list blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &list, client.UnsafeDisableDeepCopy)
	if err != nil {
		return nil, err
	}

	return list.Items, nil
}

// cachedDefinitions lists every ResourceDefinition from the cache
// without copying them; the caller only reads.
func (r *ResourceReconciler) cachedDefinitions(ctx context.Context) ([]blockstoriov1alpha1.ResourceDefinition, error) {
	var list blockstoriov1alpha1.ResourceDefinitionList

	err := r.List(ctx, &list, client.UnsafeDisableDeepCopy)
	if err != nil {
		return nil, err
	}

	return list.Items, nil
}

func copyI32(p *int32) *int32 {
	if p == nil {
		return nil
	}

	v := *p

	return &v
}
