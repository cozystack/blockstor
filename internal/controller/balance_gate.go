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
	"fmt"
	"slices"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Reasons the scheduled rebalance tick leaves a definition alone. They
// only reach the debug log; the strings name the condition so an
// operator reading it knows which one held.
const (
	balanceSkipDeleting   = "definition is being deleted"
	balanceSkipNoUpToDate = "no diskful replica is UpToDate"
)

// balanceSkipReason reports why the scheduled rebalance tick must not
// add replicas to rd, or "" when it may.
//
// LINSTOR's balance task tops a definition up only when it still holds
// data: a definition being deleted is skipped, and so is one with no
// diskful replica that is UpToDate. The second rule is what keeps a
// definition whose volume was deleted, and which lives on only for its
// snapshots, at zero replicas until the last snapshot goes. Without it
// the pass reads that definition as short of its place count and gives
// it a full set of fresh, empty replicas.
//
// Replicas are read past the cache: the window that matters is the one
// right after the last replica went, which a stale cache still shows.
func balanceSkipReason(ctx context.Context, st store.Store, rd *apiv1.ResourceDefinition) (string, error) {
	if slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return balanceSkipDeleting, nil
	}

	replicas, err := store.LiveReplicas(ctx, st, rd.Name)
	if err != nil {
		return "", fmt.Errorf("list the replicas of %q: %w", rd.Name, err)
	}

	if !slices.ContainsFunc(replicas, func(res apiv1.Resource) bool { return upToDateDiskful(&res) }) {
		return balanceSkipNoUpToDate, nil
	}

	return "", nil
}

// upToDateDiskful reports whether a replica holds a complete copy of
// the data: it has a disk, is not being deleted, and its satellite
// reports every volume UpToDate.
func upToDateDiskful(res *apiv1.Resource) bool {
	if !store.HoldsData(res) || slices.Contains(res.Flags, apiv1.ResourceFlagDelete) ||
		slices.Contains(res.Flags, apiv1.ResourceFlagDeleting) {
		return false
	}

	if len(res.Volumes) == 0 {
		return false
	}

	for i := range res.Volumes {
		if res.Volumes[i].State.DiskState != drbdDiskStateUpToDate {
			return false
		}
	}

	return true
}
