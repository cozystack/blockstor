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

package rest

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// CreateVolume hot-path cache-race mitigation.
//
// The apiserver runs N replicas behind a ClusterIP. linstor-csi's
// CreateVolume sequence is `POST /v1/resource-groups`, then
// `GET /v1/resource-groups/{rg}`, then `POST /v1/resource-groups/{rg}/spawn`.
// Round-robin balances the GET / POST follow-up onto a sibling replica
// whose controller-runtime informer cache has not yet observed the
// just-written object, surfacing a spurious 404 that fails the whole
// CreateVolume.
//
// The reconciler-side fix for the same class (commit a01f6ce) used
// APIReader-direct reads to bypass the cache; on the REST hot path
// the equivalent surgery would require threading mgr.GetAPIReader()
// through the `store.Store` interface — a much wider refactor than
// is justified for three csi-sanity specs.
//
// Instead we wrap RG/RD reads on the hot path with a tight retry-on-
// NotFound loop. The cache trails the apiserver by tens to hundreds
// of milliseconds in practice; a 3-attempt, 200 ms-spaced retry
// covers the worst observed lag with budget to spare. Steady-state
// (cache warm, object exists everywhere) pays zero extra latency.
// A real NotFound (object never existed) costs one extra 600 ms wait
// before the caller's 404 surfaces — acceptable on the CreateVolume
// path, which already runs in the 1-3 s range.
const (
	cacheRetryAttempts = 3
	cacheRetryDelay    = 200 * time.Millisecond
)

// getWithCacheRetry is the shared retry-on-NotFound loop behind every
// get*WithCacheRetry helper. `read` is re-invoked up to
// cacheRetryAttempts times while it keeps returning store.ErrNotFound,
// with cacheRetryDelay between attempts; the first success or
// non-NotFound error (transport, decode, …) returns immediately.
// Context cancellation aborts the retry loop. `what` names the read
// for error wrapping (e.g. `get resource group "rg-1"`).
//
// A real NotFound (object never existed) still surfaces after the
// budget — callers keep their 404 contract, just (attempts-1)*delay
// later on the miss path. Steady-state hits pay zero extra latency.
func getWithCacheRetry[T any](ctx context.Context, what string, read func(context.Context) (T, error)) (T, error) {
	var (
		out  T
		zero T
		err  error
	)

	for attempt := range cacheRetryAttempts {
		out, err = read(ctx)
		if err == nil {
			return out, nil
		}

		if !errors.Is(err, store.ErrNotFound) {
			return zero, errors.Wrap(err, what)
		}

		if attempt == cacheRetryAttempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return zero, errors.Wrap(ctx.Err(), what+": context cancelled")
		case <-time.After(cacheRetryDelay):
		}
	}

	return zero, errors.Wrapf(err, "%s after %d retries", what, cacheRetryAttempts)
}

// getRGWithCacheRetry returns the ResourceGroup `name`, retrying on
// store.ErrNotFound to absorb informer-cache lag after a fresh
// write that landed on a sibling apiserver replica. Any non-NotFound
// error (transport, decode, …) is returned immediately. Context
// cancellation aborts the retry loop.
func getRGWithCacheRetry(ctx context.Context, st store.Store, name string) (apiv1.ResourceGroup, error) {
	return getWithCacheRetry(ctx, fmt.Sprintf("get resource group %q", name),
		func(ctx context.Context) (apiv1.ResourceGroup, error) {
			return st.ResourceGroups().Get(ctx, name)
		})
}

// getRDWithCacheRetry returns the ResourceDefinition `name`, retrying
// on store.ErrNotFound to absorb informer-cache lag after a fresh
// write that landed on a sibling apiserver replica. Same semantics as
// getRGWithCacheRetry.
func getRDWithCacheRetry(ctx context.Context, st store.Store, name string) (apiv1.ResourceDefinition, error) {
	return getWithCacheRetry(ctx, fmt.Sprintf("get resource definition %q", name),
		func(ctx context.Context) (apiv1.ResourceDefinition, error) {
			return st.ResourceDefinitions().Get(ctx, name)
		})
}

// getSnapshotWithCacheRetry returns the Snapshot `snapName` on RD
// `rdName`, retrying on store.ErrNotFound to absorb informer-cache
// lag after a fresh write that landed on a sibling apiserver replica.
// Same semantics as getRGWithCacheRetry.
//
// linstor-csi's CreateVolume-from-snapshot sequence is `POST
// /v1/resource-definitions/{rd}/snapshots`, then an immediate `GET
// /v1/resource-definitions/{rd}/snapshots/{snap}` (size guard), then
// `POST .../snapshot-restore-resource/{snap}`. The follow-up reads
// race the informer cache exactly like the RG/RD reads above — the
// create wrote straight to the apiserver, the read is served from a
// cache that may not have observed the write yet — and a spurious
// 404 fails the whole CreateVolume.
func getSnapshotWithCacheRetry(ctx context.Context, st store.Store, rdName, snapName string) (apiv1.Snapshot, error) {
	return getWithCacheRetry(ctx, fmt.Sprintf("get snapshot %s/%s", rdName, snapName),
		func(ctx context.Context) (apiv1.Snapshot, error) {
			return st.Snapshots().Get(ctx, rdName, snapName)
		})
}

// getNodeWithCacheRetry returns the Node `name`, retrying on
// store.ErrNotFound to absorb informer-cache lag after a fresh
// write that landed on a sibling apiserver replica. Same semantics
// as getRGWithCacheRetry.
//
// Node registration (`POST /v1/nodes` — piraeus-operator's reconcile,
// satellite re-register on restart) is immediately followed by reads
// of the same node: `GET /v1/nodes/{node}` and `POST
// /v1/nodes/{node}/storage-pools` in the same reconcile pass. The
// register wrote straight to the apiserver; the follow-up read is
// served from a cache that may not have observed the write yet.
func getNodeWithCacheRetry(ctx context.Context, st store.Store, name string) (apiv1.Node, error) {
	return getWithCacheRetry(ctx, fmt.Sprintf("get node %q", name),
		func(ctx context.Context) (apiv1.Node, error) {
			return st.Nodes().Get(ctx, name)
		})
}

// getStoragePoolWithCacheRetry returns the StoragePool (node, pool),
// retrying on store.ErrNotFound to absorb informer-cache lag after a
// fresh write that landed on a sibling apiserver replica. Same
// semantics as getRGWithCacheRetry.
//
// `linstor sp c` / the CDP one-shot create the pool, and piraeus-
// operator / linstor-csi GetCapacity read it back immediately via
// `GET /v1/nodes/{node}/storage-pools/{pool}`.
func getStoragePoolWithCacheRetry(ctx context.Context, st store.Store, node, pool string) (apiv1.StoragePool, error) {
	return getWithCacheRetry(ctx, fmt.Sprintf("get storage pool %s/%s", node, pool),
		func(ctx context.Context) (apiv1.StoragePool, error) {
			return st.StoragePools().Get(ctx, node, pool)
		})
}

// getVDWithCacheRetry returns VolumeDefinition (rdName, vn), retrying
// on store.ErrNotFound to absorb informer-cache lag after a fresh
// write that landed on a sibling apiserver replica. Same semantics as
// getRGWithCacheRetry.
//
// VolumeDefinitions live inline on the parent RD CRD, so the lag has
// two faces: the parent RD itself not yet observed, or an old RD
// revision without the just-created VD. Both surface as ErrNotFound
// from the store and both are absorbed here.
func getVDWithCacheRetry(ctx context.Context, st store.Store, rdName string, vn int32) (apiv1.VolumeDefinition, error) {
	return getWithCacheRetry(ctx, fmt.Sprintf("get volume definition %s/%d", rdName, vn),
		func(ctx context.Context) (apiv1.VolumeDefinition, error) {
			return st.VolumeDefinitions().Get(ctx, rdName, vn)
		})
}

// getResourceWithCacheRetry returns the Resource (rdName, node),
// retrying on store.ErrNotFound to absorb informer-cache lag after a
// fresh write that landed on a sibling apiserver replica. Same
// semantics as getRGWithCacheRetry.
//
// Autoplace / spawn / make-available create replicas server-side and
// linstor-csi (ControllerPublishVolume) plus the CLI (`linstor r l
// <rd> <node>`, `linstor v l`) read the replica back immediately via
// `GET /v1/resource-definitions/{rd}/resources/{node}` and the
// per-resource volumes endpoints.
func getResourceWithCacheRetry(ctx context.Context, st store.Store, rdName, node string) (apiv1.Resource, error) {
	return getWithCacheRetry(ctx, fmt.Sprintf("get resource %s/%s", rdName, node),
		func(ctx context.Context) (apiv1.Resource, error) {
			return st.Resources().Get(ctx, rdName, node)
		})
}

// patchNodeSpecWithCacheRetry runs PatchNodeSpec, retrying on
// store.ErrNotFound to absorb informer-cache lag. The only caller is
// the node-register upsert fall-through: `Nodes().Create` just came
// back with ErrAlreadyExists — an authoritative apiserver-side proof
// the Node exists — so an ErrNotFound from the patch helper's
// cache-served read is definitionally lag, not a missing object.
// Without the retry a satellite re-register racing its own original
// registration surfaces a spurious 404 into the registration loop.
func patchNodeSpecWithCacheRetry(ctx context.Context, st store.Store, name string, mutate func(*apiv1.Node) error) error {
	_, err := getWithCacheRetry(ctx, fmt.Sprintf("patch node %q", name),
		func(ctx context.Context) (struct{}, error) {
			return struct{}{}, st.Nodes().PatchNodeSpec(ctx, name, mutate)
		})

	return err
}

// pickCDPDevicesWithCacheRetry resolves the `linstor physical-storage
// create-device-pool` target set, retrying while the device list has
// no match for the requested paths.
//
// The satellite discovery loop creates PhysicalDevice CRDs (and
// stamps Status.DevicePath via the Status subresource) straight on
// the apiserver; the operator sees the device in `ps l` (possibly via
// a sibling replica or kubectl) and fires `ps cdp` immediately. The
// REST replica serving the POST reads PhysicalDevices().ListForNode
// through its informer cache, which may not have observed the device
// — or its Status update — yet, so the match comes back empty and the
// pre-fix handler 404'd a perfectly valid request
// (TestGroupB/PhysicalStorageCDPPropsPerKind regression).
//
// An empty match (no busy device) is retried under the standard
// 3×200 ms budget; a genuinely-bogus device path still surfaces the
// 404 after the budget. A busy device (Bug 89) returns immediately —
// the device IS observable, the cache cannot be trailing on it.
// Non-NotFound list errors return immediately.
func pickCDPDevicesWithCacheRetry(ctx context.Context, st store.Store, node string, paths []string) ([]apiv1.PhysicalDevice, *apiv1.PhysicalDevice, error) {
	var (
		targets []apiv1.PhysicalDevice
		busy    *apiv1.PhysicalDevice
	)

	for attempt := range cacheRetryAttempts {
		devs, err := st.PhysicalDevices().ListForNode(ctx, node)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "list physical devices on node %q", node)
		}

		targets, busy = pickFreeDeviceForAttach(devs, paths)
		if busy != nil || len(targets) > 0 {
			return targets, busy, nil
		}

		if attempt == cacheRetryAttempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return nil, nil, errors.Wrapf(ctx.Err(), "list physical devices on node %q: context cancelled", node)
		case <-time.After(cacheRetryDelay):
		}
	}

	return nil, nil, nil
}
