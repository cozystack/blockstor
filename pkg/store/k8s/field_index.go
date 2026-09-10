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
	"strings"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// NewManager builds a manager whose cache can answer the reads this store
// issues.
//
// The two halves are one call because they are one decision. A manager whose
// client backs a Store, and whose cache has no index for the fields the store
// selects on, does not fail loudly — it answers every scoped read by listing
// the whole collection and filtering in process, which is the read the scoped
// one exists to replace. Registering separately is how that came to be true of
// both server binaries at once, and of the integration harness that claimed to
// mirror them.
//
//nolint:gocritic // ctrl.Options by value mirrors ctrl.NewManager, which this wraps
func NewManager(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error) {
	mgr, err := ctrl.NewManager(cfg, opts)
	if err != nil {
		return nil, errors.Wrap(err, "new manager")
	}

	err = RegisterFieldIndexes(context.Background(), mgr.GetFieldIndexer())
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

// SelectorUnsupported reports whether an error means the server cannot answer
// that selector, as opposed to the read having failed.
//
// Only the first is safe to answer by reading everything instead. A timeout,
// an RBAC refusal or a cancelled context are not statements about the
// selector, and taking the whole-cluster read for them answers a failure that
// ran out of time or permission with a larger request against the same
// exhausted budget — and discards the error that said so.
//
// Three producers say it, two of them without a type to check.
//
//   - the API server rejects a fieldSelector over an undeclared field with
//     400 "field label not supported", which is typed;
//   - a manager's cache answers an unindexed field with `Index with name
//     field:<f> does not exist`;
//   - controller-runtime's fake client, which the unit suites run on, words
//     the same condition as `... no index with name <f> has been registered
//     for GroupVersionKind ...`.
//
// Both untyped wordings name the index, so that is what is matched. Matching
// either wording exactly is how the fake client's went unrecognised: a store
// built on it turned every scoped read into a 500 rather than the fallback.
func SelectorUnsupported(err error) bool {
	if err == nil {
		return false
	}

	if apierrors.IsBadRequest(err) {
		return true
	}

	return strings.Contains(strings.ToLower(err.Error()), "index with name")
}

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

// FieldSnapshotDefinitionName is the definition field a snapshot listing
// selects on, for the same reason its Resource sibling does not use a label:
// a Snapshot adopted from LINSTOR by pkg/linstormigrate carries none.
const FieldSnapshotDefinitionName = "spec.resourceDefinitionName"

// RegisterFieldIndexes teaches a manager's cache the fields the store selects
// on. Call it on every manager whose client backs a Store.
//
// Selectable fields and indexes are two halves of the same capability, and
// which one answers depends on the reader. An UNCACHED reader — the CLI's
// client, and the manager's own API reader — sends a fieldSelector to the API
// server, which answers it from the selectable field the CRD declares. A
// CACHED reader is served from an index here. The controller binary builds its
// store on the cached client alone, so its node-scoped reads need these; the
// apiserver hands the store an API reader as well and its node-scoped reads
// bypass the cache deliberately (see resources.nodeScopedReader).
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

	err = indexer.IndexField(ctx, &crdv1alpha1.Snapshot{}, FieldSnapshotDefinitionName,
		func(obj ctrlclient.Object) []string {
			snap, ok := obj.(*crdv1alpha1.Snapshot)
			if !ok || snap.Spec.ResourceDefinitionName == "" {
				return nil
			}

			return []string{snap.Spec.ResourceDefinitionName}
		})
	if err != nil {
		return errors.Wrap(err, "index Snapshot by "+FieldSnapshotDefinitionName)
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
