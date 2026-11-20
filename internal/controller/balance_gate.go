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
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// Reasons an implicit placement pass leaves a definition alone. They
// only reach the debug log; the strings name the condition so an
// operator reading it knows which one held.
const (
	balanceSkipDeleting   = "definition is being deleted"
	balanceSkipDisabled   = "BalanceResourcesEnabled is false"
	balanceSkipNoUpToDate = "no diskful replica on an online node is UpToDate"
)

// balanceGate holds what one placement pass nobody asked for (the
// scheduled rebalance tick, an RG watch event, an `rg modify` that did
// not name a place count) reads once and applies to every child
// definition: the controller-scope props and which nodes are online.
type balanceGate struct {
	st        store.Store
	ctrlProps map[string]string
	online    map[string]bool
}

// newBalanceGate reads the controller props and the node list for one
// pass. Unreadable controller props fail open, as
// balanceResourcesDisabled does: the definition and group values still
// apply. An unreadable node list fails the pass, since without it no
// replica can be shown to sit on an online node.
func newBalanceGate(ctx context.Context, st store.Store) (*balanceGate, error) {
	g := &balanceGate{st: st, online: map[string]bool{}}

	props, err := st.ControllerProps().Get(ctx)
	if err != nil {
		logf.FromContext(ctx).Error(err, "controller props unreadable; balance checks use definition and group values only")
	} else {
		g.ctrlProps = props
	}

	nodes, err := st.Nodes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	for i := range nodes {
		if strings.EqualFold(nodes[i].ConnectionStatus, apiv1.NodeTypeOnline) {
			g.online[naming.Key(nodes[i].Name)] = true
		}
	}

	return g, nil
}

// skipReason reports why the pass must not add replicas to rd, or ""
// when it may. These are the checks LINSTOR's balance task makes
// (BalanceResources.shouldIgnoreRscDfn):
//
//   - a definition being deleted is skipped;
//   - BalanceResourcesEnabled=false on the definition, its group or the
//     controller skips it (balanceDisabled);
//   - a definition with no diskful replica that is UpToDate on an
//     online node is skipped. This is what keeps a definition whose
//     volume was deleted, and which lives on only for its snapshots, at
//     zero replicas until the last snapshot goes. Without it the pass
//     reads that definition as short of its place count and gives it a
//     full set of fresh, empty replicas. A replica on an offline node
//     does not count: its DiskState is the last one its satellite sent,
//     which LINSTOR does not hold for an offline peer either.
//
// The replicas are read from the cache first. A cache that shows no
// UpToDate replica ends the check there: at worst a replica that just
// went UpToDate waits for the next pass. Only a cache that shows one is
// confirmed past the cache, because the window that matters is the one
// right after the last replica went, which a stale cache still shows.
// That keeps the uncached read to the definitions that get placed on.
func (g *balanceGate) skipReason(ctx context.Context, rg *apiv1.ResourceGroup, rd *apiv1.ResourceDefinition) (string, error) {
	if slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return balanceSkipDeleting, nil
	}

	if balanceDisabled(rd.Props, rg.Props, g.ctrlProps) {
		return balanceSkipDisabled, nil
	}

	cached, err := g.st.Resources().ListByDefinition(ctx, rd.Name)
	if err != nil {
		return "", fmt.Errorf("list the replicas of %q: %w", rd.Name, err)
	}

	if !g.anyUpToDate(cached) {
		return balanceSkipNoUpToDate, nil
	}

	live, err := store.LiveReplicas(ctx, g.st, rd.Name)
	if err != nil {
		return "", fmt.Errorf("list the replicas of %q: %w", rd.Name, err)
	}

	if !g.anyUpToDate(live) {
		return balanceSkipNoUpToDate, nil
	}

	return "", nil
}

func (g *balanceGate) anyUpToDate(replicas []apiv1.Resource) bool {
	return slices.ContainsFunc(replicas, func(res apiv1.Resource) bool {
		return g.online[naming.Key(res.NodeName)] && upToDateDiskful(&res)
	})
}

// upToDateDiskful reports whether a replica holds a complete copy of
// the data: it has a disk, is not being deleted, and its satellite
// reports every volume UpToDate, compared without regard to case as
// LINSTOR does.
//
// A replica with no volume state is not counted. LINSTOR counts a
// replica whose satellite reported the resource with no volumes, which
// only a definition without volume definitions produces; blockstor
// cannot tell that from a replica not reported yet, and placing on a
// definition with no volumes moves no data either way.
func upToDateDiskful(res *apiv1.Resource) bool {
	if !store.HoldsData(res) || slices.Contains(res.Flags, apiv1.ResourceFlagDelete) ||
		slices.Contains(res.Flags, apiv1.ResourceFlagDeleting) {
		return false
	}

	if len(res.Volumes) == 0 {
		return false
	}

	for i := range res.Volumes {
		if !strings.EqualFold(res.Volumes[i].State.DiskState, drbdDiskStateUpToDate) {
			return false
		}
	}

	return true
}

// balanceDisabled resolves BalanceResourcesEnabled the way LINSTOR
// does: the definition's own value wins over its group's, which wins
// over the controller's, and "false" in any letter case disables.
// linstor-csi writes "False" on the definition for the length of a
// volume delete, so both the scope and the case matter.
func balanceDisabled(rdProps, rgProps, ctrlProps map[string]string) bool {
	for _, props := range []map[string]string{rdProps, rgProps, ctrlProps} {
		if v, ok := props[apiv1.PropBalanceResourcesEnabled]; ok {
			return strings.EqualFold(v, "false")
		}
	}

	return false
}
