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

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/store"
)

// parentRGSurvived re-reads the resource group a freshly materialised
// definition was parented to, and reports whether it is still there.
//
// This is the post-write half of the Bug 174 guard. `POST
// /v1/resource-definitions` runs it twice — once before the write and once
// after, rolling the definition back when a concurrent `rg d` won the race —
// because a definition left pointing at a group that no longer exists lists
// fine and places badly: the placer's Controller→RG→RD prop-inheritance walk
// drops the RG tier without a word, taking auto-place, auto-diskful,
// place_count observability and rebalance scheduling with it.
//
// Clone and snapshot-restore create definitions the same way, inherit the
// group the same way, and had only the pre-write half.
//
// The read carries the standard cache-retry budget for the same reason
// refuseRDCreateOnRGDeletedRace does: on the CreateVolume hot path the group
// may have been created moments ago and the informer cache may still trail
// it, and mistaking that lag for a delete race would roll back a perfectly
// good clone (see pkg/rest/cache_retry.go). A real `rg d` still trips it once
// the budget is spent.
func (s *Server) parentRGSurvived(ctx context.Context, rgName string) (bool, error) {
	if rgName == "" {
		return true, nil
	}

	_, err := getRGWithCacheRetry(ctx, s.Store, rgName)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}

	return false, err
}

// rollBackMaterialisedRD undoes a clone or restore whose parent group was
// deleted underneath it.
//
// RD-create can compensate with a single Delete because the definition it
// rolls back is bare. These paths cannot: by the time the group can vanish the
// target has volumes hydrated from the snapshot and replicas stamped on the
// nodes that hold it, so the compensation is the cascade `rd d` performs —
// replicas first, then the definition, which carries its inline volumes with
// it.
//
// The internal snapshot a clone took is deliberately left behind. It may be
// the only copy of something, and deleting one is the operator's decision, not
// this endpoint's — the same stance the clone's snapshot reuse takes.
//
// Best-effort, and it says so: if the rollback itself fails the caller still
// gets the refusal, and the dangling-RG error surfaces on the next access to
// the definition rather than being swallowed here.
func (s *Server) rollBackMaterialisedRD(ctx context.Context, rdName string) {
	_ = store.CascadeDeleteResources(ctx, s.Store, rdName)
	_ = s.Store.ResourceDefinitions().Delete(ctx, rdName)
}

// rgDeletedRaceCorrection is the one wording for the refusal, so an operator
// reads the same correction whichever endpoint lost the race.
func rgDeletedRaceCorrection(rgName string) string {
	return "resource group '" + rgName + "' was deleted concurrently with the " +
		"operation (Bug 174): retry after re-creating the resource group"
}
