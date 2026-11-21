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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// FieldResourceDefinitionKey is a cache index over Resources by the key
// (naming.Key) of the definition name in spec. It exists in a manager's
// cache only: the API server has no such field, and nothing sends it
// there. ReplicasOf asks it which spellings the cached replicas of a
// definition carry, in time proportional to that definition's replicas
// rather than to the cluster's.
const FieldResourceDefinitionKey = "blockstor.org/resourceDefinitionKey"

// replicaKeyIndex is the FieldResourceDefinitionKey index.
func replicaKeyIndex() fieldIndex {
	return fieldIndex{
		object: &crdv1alpha1.Resource{}, field: FieldResourceDefinitionKey,
		extract: func(obj ctrlclient.Object) []string {
			res, ok := obj.(*crdv1alpha1.Resource)
			if !ok || res.Spec.ResourceDefinitionName == "" {
				return nil
			}

			return []string{naming.Key(res.Spec.ResourceDefinitionName)}
		},
	}
}

// RegisterReplicaKeyIndex registers the FieldResourceDefinitionKey index
// on a manager that NewManager did not build: the satellite's. NewManager
// registers it with the rest of the store's indexes.
func RegisterReplicaKeyIndex(indexer ctrlclient.FieldIndexer) error {
	return registerIndexesWithin(indexRegistrationBudget, indexer, []fieldIndex{replicaKeyIndex()})
}

// ReplicasOf lists into list the Resource objects of definition rdName
// through reader, whichever spelling of the name they carry.
//
// It asks for the definition's replicas only, one field-selected read
// per spelling, instead of reading every Resource in the cluster and
// keeping a handful. The readers that call it are uncached and run on
// every pass of a replica or a definition: a whole-cluster read there
// is a few megabytes to decode per pass on a cluster with a few hundred
// replicas, and with a pass per definition every few seconds that alone
// kept the controller and the satellites at their CPU limit, so a burst
// of new volumes waited behind it.
//
// A field selector matches a spelling exactly, so the spellings asked
// for are those rdSpellings knows and every one a replica in cache
// carries: a replica the read missed would be a peer left out of a
// rendered resource file. cache may be nil.
//
// A reader that cannot serve the selector (a CRD that predates the
// selectable field, a cache without the index) fails the read rather
// than answering short, and ReplicasOf then reads every Resource and
// keeps the definition's, as before.
func ReplicasOf(ctx context.Context, reader, cache ctrlclient.Reader, rdName string, list *crdv1alpha1.ResourceList) error {
	spellings, err := definitionSpellings(ctx, reader, cache, rdName)
	if err != nil {
		return err
	}

	for _, spelling := range cachedSpellings(ctx, cache, rdName) {
		if !slices.Contains(spellings, spelling) {
			spellings = append(spellings, spelling)
		}
	}

	var out []crdv1alpha1.Resource

	seen := map[string]struct{}{}

	for _, spelling := range spellings {
		var found crdv1alpha1.ResourceList

		err := reader.List(ctx, &found, ctrlclient.MatchingFields{FieldResourceDefinitionName: spelling})
		if SelectorUnsupported(err) {
			log.FromContext(ctx).V(1).Info("scoped Resource read unavailable; reading every replica instead",
				"resourceDefinition", rdName, "reason", err.Error())

			return allReplicasOf(ctx, reader, rdName, list)
		}

		if err != nil {
			return errors.Wrapf(err, "list replicas of %q", rdName)
		}

		for i := range found.Items {
			if _, dup := seen[found.Items[i].Name]; dup {
				continue
			}

			seen[found.Items[i].Name] = struct{}{}

			out = append(out, found.Items[i])
		}
	}

	list.Items = out

	return nil
}

// definitionSpellings is rdSpellings answered from cache when the
// definition is there. Its spellings are fixed when it is created, so
// the cache's copy names them as well as the API server's does, and the
// read on every pass costs no request. A definition the cache has not
// seen yet, or a cache that cannot answer, is asked of reader.
func definitionSpellings(ctx context.Context, reader, cache ctrlclient.Reader, rdName string) ([]string, error) {
	if cache != nil {
		found, ok, err := lookupRD(ctx, cache, rdName)
		if err == nil && ok {
			return spellingSet(rdName, found, true), nil
		}
	}

	return rdSpellings(ctx, reader, rdName)
}

// cachedSpellings lists the spellings of rdName the replicas in cache
// carry in spec. It reads the FieldResourceDefinitionKey index, which
// holds the definition's replicas only, without copying them. A cache
// without the index is walked whole instead, which is correct but
// copies every Resource in it.
func cachedSpellings(ctx context.Context, cache ctrlclient.Reader, rdName string) []string {
	if cache == nil {
		return nil
	}

	var list crdv1alpha1.ResourceList

	err := cache.List(ctx, &list,
		ctrlclient.MatchingFields{FieldResourceDefinitionKey: naming.Key(rdName)}, ctrlclient.UnsafeDisableDeepCopy)
	if SelectorUnsupported(err) {
		log.FromContext(ctx).V(1).Info("cache has no definition-key index; walking every cached replica",
			"resourceDefinition", rdName, "reason", err.Error())

		err = cache.List(ctx, &list, ctrlclient.UnsafeDisableDeepCopy)
	}

	if err != nil {
		return nil
	}

	var out []string

	for i := range list.Items {
		spelling := list.Items[i].Spec.ResourceDefinitionName
		if naming.Same(spelling, rdName) && !slices.Contains(out, spelling) {
			out = append(out, spelling)
		}
	}

	return out
}

// allReplicasOf is ReplicasOf without the selector: every Resource,
// filtered to the definition's.
func allReplicasOf(ctx context.Context, reader ctrlclient.Reader, rdName string, list *crdv1alpha1.ResourceList) error {
	var all crdv1alpha1.ResourceList

	err := reader.List(ctx, &all)
	if err != nil {
		return errors.Wrapf(err, "list replicas of %q", rdName)
	}

	out := make([]crdv1alpha1.Resource, 0, len(all.Items))

	for i := range all.Items {
		if naming.Same(all.Items[i].Spec.ResourceDefinitionName, rdName) {
			out = append(out, all.Items[i])
		}
	}

	list.Items = out

	return nil
}
