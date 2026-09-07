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

	"github.com/cockroachdb/errors"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// FieldResourceNodeName is the field a node-scoped Resource query selects on.
// The CRD declares it selectable, so an uncached client turns it into a
// fieldSelector the API server answers; a cached client needs the matching
// index registered below, or the same query comes back as an error.
const FieldResourceNodeName = "spec.nodeName"

// FieldResourceDefinitionName is the field a definition-scoped Resource query
// selects on. Unlike the label the objects usually carry, it is the spec value
// itself, so a replica applied by hand is found by it too — the Bug 038 shape,
// where a label selector answered with a partial-but-correct subset and the
// unlabelled replicas were invisible rather than an error.
const FieldResourceDefinitionName = "spec.resourceDefinitionName"

// FieldStoragePoolNodeName is the same node field on StoragePool.
const FieldStoragePoolNodeName = "spec.nodeName"

// RegisterFieldIndexes teaches a manager's cache the fields the store selects
// on. Call it on every manager whose client backs a Store.
//
// A field selector has two implementations behind one call. Against an
// uncached client — the CLI's — it becomes a fieldSelector on the wire and the
// API server does the filtering, which is why the CRDs declare the fields
// selectable. Against a manager's cached client it is served from a local
// index, and a field with no index registered is not a slow query but a failed
// one: "Index with name field:spec.nodeName does not exist".
//
// So without this the store's node-scoped reads fell back to listing every
// object and filtering in process on both server binaries — the exhaustive
// read they were written to replace, taken silently on every call.
func RegisterFieldIndexes(ctx context.Context, indexer ctrlclient.FieldIndexer) error {
	err := indexer.IndexField(ctx, &crdv1alpha1.Resource{}, FieldResourceNodeName,
		func(obj ctrlclient.Object) []string {
			res, ok := obj.(*crdv1alpha1.Resource)
			if !ok || res.Spec.NodeName == "" {
				return nil
			}

			return []string{res.Spec.NodeName}
		})
	if err != nil {
		return errors.Wrap(err, "index Resource by "+FieldResourceNodeName)
	}

	err = indexer.IndexField(ctx, &crdv1alpha1.Resource{}, FieldResourceDefinitionName,
		func(obj ctrlclient.Object) []string {
			res, ok := obj.(*crdv1alpha1.Resource)
			if !ok || res.Spec.ResourceDefinitionName == "" {
				return nil
			}

			return []string{res.Spec.ResourceDefinitionName}
		})
	if err != nil {
		return errors.Wrap(err, "index Resource by "+FieldResourceDefinitionName)
	}

	err = indexer.IndexField(ctx, &crdv1alpha1.StoragePool{}, FieldStoragePoolNodeName,
		func(obj ctrlclient.Object) []string {
			pool, ok := obj.(*crdv1alpha1.StoragePool)
			if !ok || pool.Spec.NodeName == "" {
				return nil
			}

			return []string{pool.Spec.NodeName}
		})
	if err != nil {
		return errors.Wrap(err, "index StoragePool by "+FieldStoragePoolNodeName)
	}

	return nil
}
