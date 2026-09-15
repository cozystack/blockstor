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
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// NewManager builds a manager and the store that serves from it, as one call.
//
// Three things have to hold together for a manager-backed store to answer the
// reads it issues, and each used to be a separate step a binary could skip.
// The cache needs the field indexes the store selects on, or every scoped read
// quietly lists the whole collection and filters in process — the read the
// scoped one exists to replace, and neither step fails loudly. And the store
// needs the manager's direct reader, or the reads that decide a node's fate
// answer from a cache that trails the API server.
//
// Registering the indexes separately is how both server binaries and the
// integration harness came to run on the fallback at once. Threading the
// reader separately is how the controller binary came to serve the LINSTOR
// surface from a cache-only store while the apiserver's had the reader. So
// there is no second constructor to leave out: the store comes back from the
// same call that built the manager, from that manager's own client and reader.
//
// Registering an index resolves its kind's REST mapping, which asks the API
// server, so construction waits for one that is briefly unreachable rather
// than failing on the first refused connection: both binaries exit when this
// returns an error, and an API server restarting while the pod starts would
// otherwise be a crash loop. The wait is bounded by indexRegistrationBudget,
// see there for why the bound is what it is.
//
//nolint:gocritic // ctrl.Options by value mirrors ctrl.NewManager, which this wraps
func NewManager(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, *Store, error) {
	mgr, err := newIndexedManager(cfg, opts, indexRegistrationBudget)
	if err != nil {
		return nil, nil, err
	}

	return mgr, NewWithAPIReader(mgr.GetClient(), mgr.GetAPIReader()), nil
}

// indexRegistrationBudget is how long NewManager keeps retrying the index
// registration before it gives up.
//
// Nothing serves /healthz while it waits: the health endpoint comes up in
// mgr.Start, after construction returns. So every liveness probe fired during
// the wait fails, and the wait has to end before the kubelet's kill does, or
// the outage it rides out ends in a restart with nothing in the log saying
// why. The kubelet kills at initialDelaySeconds + (failureThreshold-1) *
// periodSeconds. The two deployments that ship leave the period and the
// threshold at their defaults of 10 and 3 after a 15s delay, which is 35s;
// config/manager/manager.yaml sets a 20s period, which is 55s. 20s leaves the
// earliest of those 15s for the process to start and build the manager before
// registration begins. TestIndexRegistrationBudgetEndsBeforeLivenessKills
// holds this against the manifests themselves.
//
// The bound is on the wait, not on an attempt: an attempt still in flight when
// the budget runs out is abandoned rather than awaited, since a connection
// into a dropped route can take client-go's 30s dial timeout to fail.
const indexRegistrationBudget = 20 * time.Second

//nolint:gocritic // ctrl.Options by value mirrors ctrl.NewManager, which this wraps
func newIndexedManager(cfg *rest.Config, opts ctrl.Options, budget time.Duration) (ctrl.Manager, error) {
	mgr, err := ctrl.NewManager(cfg, opts)
	if err != nil {
		return nil, errors.Wrap(err, "new manager")
	}

	err = registerFieldIndexesWithin(budget, mgr.GetFieldIndexer())
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

// registerFieldIndexesWithin registers every index, retrying the ones that
// failed until the budget runs out. An index that registered is never asked
// again: the informer refuses a second indexer under the same name, so
// retrying the whole set after a partial success would fail on the part that
// worked.
func registerFieldIndexesWithin(budget time.Duration, indexer ctrlclient.FieldIndexer) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	pending := fieldIndexes()
	delay := indexRetryFirstDelay

	var lastErr error

	for {
		attempt := make(chan registrationAttempt, 1)

		go func(pending []fieldIndex) {
			left, err := registerPending(ctx, indexer, pending)
			attempt <- registrationAttempt{left: left, err: err}
		}(pending)

		select {
		case <-ctx.Done():
			return gaveUpRegistering(budget, lastErr, ctx.Err())
		case got := <-attempt:
			if got.err == nil {
				return nil
			}

			pending, lastErr = got.left, got.err
		}

		select {
		case <-ctx.Done():
			return gaveUpRegistering(budget, lastErr, ctx.Err())
		case <-time.After(delay):
		}

		delay = min(delay*2, indexRetryMaxDelay)
	}
}

type registrationAttempt struct {
	left []fieldIndex
	err  error
}

// gaveUpRegistering names the budget and the most telling error: the last
// attempt's when one finished, the deadline's when the first never did.
func gaveUpRegistering(budget time.Duration, lastErr, deadline error) error {
	if lastErr == nil {
		lastErr = deadline
	}

	return errors.Wrapf(lastErr, "gave up registering the field indexes after %s", budget)
}

const (
	indexRetryFirstDelay = 250 * time.Millisecond
	indexRetryMaxDelay   = 5 * time.Second
)

// registerPending registers what it can and returns what is still left, with
// the first error it met.
func registerPending(ctx context.Context, indexer ctrlclient.FieldIndexer, pending []fieldIndex) ([]fieldIndex, error) {
	var (
		left     []fieldIndex
		firstErr error
	)

	for _, index := range pending {
		err := indexer.IndexField(ctx, index.object, index.field, index.extract)
		if err != nil {
			left = append(left, index)

			if firstErr == nil {
				firstErr = errors.Wrapf(err, "index %s by %s", reflect.TypeOf(index.object).Elem().Name(), index.field)
			}
		}
	}

	return left, firstErr
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
	_, err := registerPending(ctx, indexer, fieldIndexes())

	return err
}

// fieldIndex is one index RegisterFieldIndexes installs.
type fieldIndex struct {
	object  ctrlclient.Object
	field   string
	extract ctrlclient.IndexerFunc
}

func fieldIndexes() []fieldIndex {
	return []fieldIndex{
		{
			object: &crdv1alpha1.Resource{}, field: FieldResourceNodeName,
			extract: func(obj ctrlclient.Object) []string {
				res, ok := obj.(*crdv1alpha1.Resource)
				if !ok || res.Spec.NodeName == "" {
					return nil
				}

				return []string{res.Spec.NodeName}
			},
		},
		{
			object: &crdv1alpha1.Resource{}, field: FieldResourceDefinitionName,
			extract: func(obj ctrlclient.Object) []string {
				res, ok := obj.(*crdv1alpha1.Resource)
				if !ok || res.Spec.ResourceDefinitionName == "" {
					return nil
				}

				return []string{res.Spec.ResourceDefinitionName}
			},
		},
		{
			object: &crdv1alpha1.Snapshot{}, field: FieldSnapshotDefinitionName,
			extract: func(obj ctrlclient.Object) []string {
				snap, ok := obj.(*crdv1alpha1.Snapshot)
				if !ok || snap.Spec.ResourceDefinitionName == "" {
					return nil
				}

				return []string{snap.Spec.ResourceDefinitionName}
			},
		},
		{
			object: &crdv1alpha1.StoragePool{}, field: FieldStoragePoolNodeName,
			extract: func(obj ctrlclient.Object) []string {
				pool, ok := obj.(*crdv1alpha1.StoragePool)
				if !ok || pool.Spec.NodeName == "" {
					return nil
				}

				return []string{pool.Spec.NodeName}
			},
		},
	}
}
