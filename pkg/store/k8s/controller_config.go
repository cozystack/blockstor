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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Patch helpers for the singleton ControllerConfig CRD.
//
// `ControllerConfig.Spec.ExtraProps` and
// `ControllerConfig.Spec.NodeConnections` were previously mutated via
// wholesale read-modify-write paths in pkg/rest/controller_props.go
// and pkg/rest/node_connections.go: Get to mutate in-place to Update.
// Two writers racing on the same singleton (e.g. `linstor c sp` and
// `linstor node-connection sp` issued concurrently) silently lose one
// mutation: the second writer's stale Spec wholesale-overwrites the
// first.
//
// These helpers mirror the Patch approach already in use for the
// Node/RG stores: re-derive the mutation against the freshly-fetched
// CRD on every `RetryOnConflict` cycle and persist via
// `MergeFromWithOptimisticLock`, so disjoint concurrent mutations
// all converge. Auto-create on NotFound matches the pre-existing
// helper behaviour so a fresh cluster doesn't need an explicit
// `kubectl apply` of ControllerConfig before
// `linstor controller set-property` works.

// PatchControllerExtraProps fetches the singleton ControllerConfig
// (creating it on NotFound), invokes `mutate` against
// `Spec.ExtraProps` (initialised to a non-nil map if the CRD's was
// nil), and persists via JSON-merge-patch with optimistic
// concurrency. On 409 the entire Get to mutate to Patch cycle is
// re-run against the fresh state via the tuned conflict backoff;
// disjoint concurrent edits converge instead of clobbering one
// another via stale wire snapshots.
//
// `mutate` runs against the live map by reference so in-place
// edits (set / delete) land directly; the closure may be invoked
// multiple times under contention so it MUST be idempotent.
func PatchControllerExtraProps(
	ctx context.Context,
	c ctrlclient.Client,
	mutate func(map[string]string) error,
) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrap(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		return patchExtraPropsOnce(ctx, c, mutate)
	}), "patch ControllerConfig.ExtraProps")
}

// patchExtraPropsOnce runs one Get-mutate-Patch (or Create) attempt.
// Hoisted out of the RetryOnConflict body so the loop stays under
// the funlen budget and the auto-create branch can return a Conflict
// error directly to trigger the next retry.
func patchExtraPropsOnce(
	ctx context.Context,
	c ctrlclient.Client,
	mutate func(map[string]string) error,
) error {
	var existing crdv1alpha1.ControllerConfig

	err := c.Get(ctx,
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&existing,
	)
	if apierrors.IsNotFound(err) {
		return createControllerConfigWithExtraProps(ctx, c, mutate)
	}

	if err != nil {
		return errors.Wrap(err, "get ControllerConfig")
	}

	base := existing.DeepCopy()

	props := existing.Spec.ExtraProps
	if props == nil {
		props = map[string]string{}
	}

	mErr := mutate(props)
	if mErr != nil {
		return mErr
	}

	existing.Spec.ExtraProps = props

	pErr := c.Patch(ctx, &existing,
		ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}),
	)
	if pErr != nil {
		return errors.Wrap(pErr, "patch ControllerConfig")
	}

	return nil
}

// createControllerConfigWithExtraProps handles the NotFound branch:
// build a fresh singleton with an empty ExtraProps map, let mutate
// populate it, and persist via Create. A racing peer that wins the
// Create() race surfaces as AlreadyExists; we convert that to a
// Conflict so RetryOnConflict re-runs against the now-present CRD
// via the patch branch.
func createControllerConfigWithExtraProps(
	ctx context.Context,
	c ctrlclient.Client,
	mutate func(map[string]string) error,
) error {
	fresh := crdv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: crdv1alpha1.ControllerConfigName},
		Spec: crdv1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{},
		},
	}

	mErr := mutate(fresh.Spec.ExtraProps)
	if mErr != nil {
		return mErr
	}

	cErr := c.Create(ctx, &fresh)
	if apierrors.IsAlreadyExists(cErr) {
		return newControllerConfigConflict(cErr)
	}

	if cErr != nil {
		return errors.Wrap(cErr, "create ControllerConfig")
	}

	return nil
}

// PatchControllerNodeConnections fetches the singleton
// ControllerConfig (creating it on NotFound), invokes `mutate`
// against `Spec.NodeConnections` (initialised to a non-nil map if
// the CRD's was nil), and persists via JSON-merge-patch with
// optimistic concurrency. Same retry-on-conflict semantics as
// PatchControllerExtraProps; mutate may be invoked multiple times.
//
// The closure receives the live map-of-maps by reference: outer key
// is the canonical pair-id (`<lo>::<hi>`), inner map is the per-pair
// props bag. In-place edits (insert / delete pair, set / delete key)
// land directly. To drop a pair entry entirely, `delete(conn, pair)`
// — leaving an empty inner map preserves the entry, which is the
// shape upstream LINSTOR emits for `set-property` immediately
// followed by `drop-property` of the only key.
func PatchControllerNodeConnections(
	ctx context.Context,
	c ctrlclient.Client,
	mutate func(map[string]map[string]string) error,
) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrap(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		return patchNodeConnectionsOnce(ctx, c, mutate)
	}), "patch ControllerConfig.NodeConnections")
}

// patchNodeConnectionsOnce runs one Get-mutate-Patch (or Create)
// attempt. Hoisted out of the RetryOnConflict body for symmetry with
// patchExtraPropsOnce and to stay under funlen.
func patchNodeConnectionsOnce(
	ctx context.Context,
	c ctrlclient.Client,
	mutate func(map[string]map[string]string) error,
) error {
	var existing crdv1alpha1.ControllerConfig

	err := c.Get(ctx,
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&existing,
	)
	if apierrors.IsNotFound(err) {
		return createControllerConfigWithNodeConnections(ctx, c, mutate)
	}

	if err != nil {
		return errors.Wrap(err, "get ControllerConfig")
	}

	base := existing.DeepCopy()

	conn := existing.Spec.NodeConnections
	if conn == nil {
		conn = map[string]map[string]string{}
	}

	mErr := mutate(conn)
	if mErr != nil {
		return mErr
	}

	existing.Spec.NodeConnections = conn

	pErr := c.Patch(ctx, &existing,
		ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}),
	)
	if pErr != nil {
		return errors.Wrap(pErr, "patch ControllerConfig")
	}

	return nil
}

// createControllerConfigWithNodeConnections is the NodeConnections
// twin of createControllerConfigWithExtraProps; see that function
// for the rationale of the AlreadyExists to Conflict mapping.
func createControllerConfigWithNodeConnections(
	ctx context.Context,
	c ctrlclient.Client,
	mutate func(map[string]map[string]string) error,
) error {
	fresh := crdv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: crdv1alpha1.ControllerConfigName},
		Spec: crdv1alpha1.ControllerConfigSpec{
			NodeConnections: map[string]map[string]string{},
		},
	}

	mErr := mutate(fresh.Spec.NodeConnections)
	if mErr != nil {
		return mErr
	}

	cErr := c.Create(ctx, &fresh)
	if apierrors.IsAlreadyExists(cErr) {
		return newControllerConfigConflict(cErr)
	}

	if cErr != nil {
		return errors.Wrap(cErr, "create ControllerConfig")
	}

	return nil
}

// newControllerConfigConflict wraps an AlreadyExists-from-Create
// error into a typed Conflict status so `RetryOnConflict` re-runs
// the loop body and the next iteration falls through to the patch
// branch against the peer-created CRD.
func newControllerConfigConflict(cause error) error {
	return apierrors.NewConflict(
		crdv1alpha1.GroupVersion.WithResource("controllerconfigs").GroupResource(),
		crdv1alpha1.ControllerConfigName,
		cause,
	)
}
