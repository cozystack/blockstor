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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// allocLedger holds the DRBD identities this controller has written and
// its cache may not show yet, the way LINSTOR keeps its number pools in
// the controller's memory.
//
// The allocator reads the taken node ids, ports and minors from the
// cache. A cache trails the writes this process just made, so two
// allocations in a row could both see a value as free and both take
// it; the ledger closes that window. Every write of a new identity is
// recorded here after it succeeds, and every read of a taken set adds
// what is recorded. An entry goes away once the cache shows the object
// with the recorded values, or shows the object gone.
//
// Reading the cache instead of the apiserver is what keeps a burst of
// new replicas from queueing: each allocation used to list every
// Resource in the cluster from the apiserver several times while
// holding clusterAllocMu, which made the allocations of a burst run
// strictly one after another at the speed of those lists.
//
// Every access happens under clusterAllocMu, which also covers the
// writes, so the ledger needs no lock of its own.
type allocLedger struct {
	replicas map[string]replicaGrant
	minors   map[string]minorGrant
}

// replicaGrant is the node id and port written onto one Resource.
type replicaGrant struct {
	uid    types.UID
	rd     string
	node   string
	nodeID *int32
	port   *int32
}

// minorGrant is the minor of every volume written onto one definition.
type minorGrant struct {
	uid    types.UID
	minors map[int32]int32
}

// recordReplica remembers the identities just written onto res.
func (l *allocLedger) recordReplica(res *blockstoriov1alpha1.Resource) {
	if l.replicas == nil {
		l.replicas = map[string]replicaGrant{}
	}

	l.replicas[res.Name] = replicaGrant{
		uid:    res.UID,
		rd:     res.Spec.ResourceDefinitionName,
		node:   res.Spec.NodeName,
		nodeID: copyI32(res.Spec.DRBDNodeID),
		port:   copyI32(res.Spec.DRBDPort),
	}
}

// recordMinors remembers the minors just written onto rd.
func (l *allocLedger) recordMinors(rd *blockstoriov1alpha1.ResourceDefinition) {
	if l.minors == nil {
		l.minors = map[string]minorGrant{}
	}

	grant := minorGrant{uid: rd.UID, minors: map[int32]int32{}}

	for i := range rd.Spec.VolumeDefinitions {
		if m := rd.Spec.VolumeDefinitions[i].DRBDMinor; m != nil {
			grant.minors[rd.Spec.VolumeDefinitions[i].VolumeNumber] = *m
		}
	}

	l.minors[naming.Key(rd.Name)] = grant
}

// pendingReplicas drops every grant the cached Resources already show,
// or show gone, and returns the rest: the grants the cache still lags.
func (l *allocLedger) pendingReplicas(cached []blockstoriov1alpha1.Resource) []replicaGrant {
	if len(l.replicas) == 0 {
		return nil
	}

	byName := make(map[string]*blockstoriov1alpha1.Resource, len(cached))
	for i := range cached {
		byName[cached[i].Name] = &cached[i]
	}

	out := make([]replicaGrant, 0, len(l.replicas))

	for name, grant := range l.replicas {
		res, ok := byName[name]
		if !ok || res.UID != grant.uid || replicaShows(res, grant) {
			delete(l.replicas, name)

			continue
		}

		out = append(out, grant)
	}

	return out
}

// pendingMinors drops every grant the cached definitions already show,
// or show gone, and returns the rest keyed by definition.
func (l *allocLedger) pendingMinors(cached []blockstoriov1alpha1.ResourceDefinition) map[string]minorGrant {
	if len(l.minors) == 0 {
		return nil
	}

	byName := make(map[string]*blockstoriov1alpha1.ResourceDefinition, len(cached))
	for i := range cached {
		byName[naming.Key(cached[i].Name)] = &cached[i]
	}

	out := make(map[string]minorGrant, len(l.minors))

	for key, grant := range l.minors {
		rd, ok := byName[key]
		if !ok || rd.UID != grant.uid || definitionShows(rd, grant) {
			delete(l.minors, key)

			continue
		}

		out[key] = grant
	}

	return out
}

// replicaShows reports whether the cached res carries the granted
// identities.
func replicaShows(res *blockstoriov1alpha1.Resource, grant replicaGrant) bool {
	return (grant.nodeID == nil || ptrEqI32(res.Spec.DRBDNodeID, grant.nodeID)) &&
		(grant.port == nil || ptrEqI32(res.Spec.DRBDPort, grant.port))
}

// definitionShows reports whether the cached rd carries every granted
// minor.
func definitionShows(rd *blockstoriov1alpha1.ResourceDefinition, grant minorGrant) bool {
	for vn, minor := range grant.minors {
		found := false

		for i := range rd.Spec.VolumeDefinitions {
			vd := &rd.Spec.VolumeDefinitions[i]
			if vd.VolumeNumber == vn && vd.DRBDMinor != nil && *vd.DRBDMinor == minor {
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
