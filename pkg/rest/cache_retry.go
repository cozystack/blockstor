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

// getRGWithCacheRetry returns the ResourceGroup `name`, retrying on
// store.ErrNotFound to absorb informer-cache lag after a fresh
// write that landed on a sibling apiserver replica. Any non-NotFound
// error (transport, decode, …) is returned immediately. Context
// cancellation aborts the retry loop.
func getRGWithCacheRetry(ctx context.Context, st store.Store, name string) (apiv1.ResourceGroup, error) {
	var (
		rg  apiv1.ResourceGroup
		err error
	)

	for attempt := range cacheRetryAttempts {
		rg, err = st.ResourceGroups().Get(ctx, name)
		if err == nil {
			return rg, nil
		}

		if !errors.Is(err, store.ErrNotFound) {
			return apiv1.ResourceGroup{}, errors.Wrapf(err, "get resource group %q", name)
		}

		if attempt == cacheRetryAttempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return apiv1.ResourceGroup{}, errors.Wrap(ctx.Err(), "get resource group: context cancelled")
		case <-time.After(cacheRetryDelay):
		}
	}

	return apiv1.ResourceGroup{}, errors.Wrapf(err, "get resource group %q after %d retries", name, cacheRetryAttempts)
}

// getRDWithCacheRetry returns the ResourceDefinition `name`, retrying
// on store.ErrNotFound to absorb informer-cache lag after a fresh
// write that landed on a sibling apiserver replica. Same semantics as
// getRGWithCacheRetry.
func getRDWithCacheRetry(ctx context.Context, st store.Store, name string) (apiv1.ResourceDefinition, error) {
	var (
		rd  apiv1.ResourceDefinition
		err error
	)

	for attempt := range cacheRetryAttempts {
		rd, err = st.ResourceDefinitions().Get(ctx, name)
		if err == nil {
			return rd, nil
		}

		if !errors.Is(err, store.ErrNotFound) {
			return apiv1.ResourceDefinition{}, errors.Wrapf(err, "get resource definition %q", name)
		}

		if attempt == cacheRetryAttempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return apiv1.ResourceDefinition{}, errors.Wrap(ctx.Err(), "get resource definition: context cancelled")
		case <-time.After(cacheRetryDelay):
		}
	}

	return apiv1.ResourceDefinition{}, errors.Wrapf(err, "get resource definition %q after %d retries", name, cacheRetryAttempts)
}
