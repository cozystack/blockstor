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
	"errors"
	"fmt"
	"slices"
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// Reasons an implicit placement pass leaves a definition alone. They
// only reach the debug log; the strings name the condition so an
// operator reading it knows which one held.
const (
	balanceSkipDeleting   = "definition is being deleted"
	balanceSkipGone       = "definition is gone"
	balanceSkipDisabled   = "BalanceResourcesEnabled is false"
	balanceSkipNoUpToDate = "no diskful replica on an online node is UpToDate"
	modifySkipAtCount     = "replicas other than tiebreakers already reach the place count"
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
//
// The definition and group passed in, and the controller props, are
// the ones the pass read when it started, and a pass over a large group
// runs for many seconds. linstor-csi switches balancing off on a
// definition right before it deletes its replicas, and LINSTOR's
// balance task never misses that, since it reads under the lock the
// switch is written under. So a definition that passes on those copies
// is checked again on current ones (currentSkip), and only that check
// decides.
func (g *balanceGate) skipReason(ctx context.Context, rg *apiv1.ResourceGroup, rd *apiv1.ResourceDefinition) (string, error) {
	if reason := definitionSkip(rd, rg.Props, g.ctrlProps); reason != "" {
		return reason, nil
	}

	cached, err := g.st.Resources().ListByDefinition(ctx, rd.Name)
	if err != nil {
		return "", fmt.Errorf("list the replicas of %q: %w", rd.Name, err)
	}

	if !g.anyUpToDate(cached) {
		return balanceSkipNoUpToDate, nil
	}

	reason, err := g.currentSkip(ctx, rg, rd.Name)
	if err != nil || reason != "" {
		return reason, err
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

// definitionSkip is the part of skipReason that reads props and flags
// alone: the definition being deleted, or balancing switched off for it.
func definitionSkip(rd *apiv1.ResourceDefinition, rgProps, ctrlProps map[string]string) string {
	if slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return balanceSkipDeleting
	}

	if balanceDisabled(rd.Props, rgProps, ctrlProps) {
		return balanceSkipDisabled
	}

	return ""
}

// currentSkip applies definitionSkip to current copies: the definition
// as the API server holds it now, and the group and the controller
// props as the cache holds them now.
//
// Only the definition is read past the cache. It is where linstor-csi
// writes the switch, right before it lists the replicas to delete, so
// it is the one read that has to be in order with that list. A switch
// an operator sets on the group or the controller during a pass is seen
// as soon as the cache has it, which is all a later read could give: no
// client lists and deletes replicas right after setting it there. A
// group that is gone, or controller props that cannot be read, leave
// the copies the pass started with in place.
func (g *balanceGate) currentSkip(ctx context.Context, rg *apiv1.ResourceGroup, rdName string) (string, error) {
	current, err := g.st.ResourceDefinitions().GetUncached(ctx, rdName)
	if errors.Is(err, store.ErrNotFound) {
		return balanceSkipGone, nil
	}

	if err != nil {
		return "", fmt.Errorf("read definition %q: %w", rdName, err)
	}

	rgProps := rg.Props

	group, err := g.st.ResourceGroups().Get(ctx, rg.Name)

	switch {
	case err == nil:
		rgProps = group.Props
	case !errors.Is(err, store.ErrNotFound):
		return "", fmt.Errorf("read group %q: %w", rg.Name, err)
	}

	ctrlProps := g.ctrlProps

	props, err := g.st.ControllerProps().Get(ctx)
	if err != nil {
		logf.FromContext(ctx).Error(err, "controller props unreadable; using the ones the pass started with")
	} else {
		ctrlProps = props
	}

	return definitionSkip(&current, rgProps, ctrlProps), nil
}

// place runs the placer on a definition skipReason let through. If it
// created any replica, it checks the definition once more (currentSkip)
// and, if balancing was switched off for it or it started going while
// the placer ran, deletes again the replicas this call created.
//
// That closes what the check in skipReason leaves open: linstor-csi
// switches balancing off and then lists the replicas it deletes, so a
// replica created after that list is never deleted, and it keeps the
// definition, a kernel resource and its ports for good. LINSTOR's
// balance task cannot interleave there, since it places under the lock
// the switch is written under; here the order of the two reads stands
// in for that lock. A replica created before this read was created
// before the switch, so linstor-csi lists it: the REST list of a
// definition with balancing off reads past the cache.
//
// Only the replicas the placer reports as its own are taken back. One
// another writer created meanwhile, by hand, through autoplace, or as a
// diskless attach, stays, as it would in LINSTOR.
func (g *balanceGate) place(
	ctx context.Context, p *placer.Placer, rg *apiv1.ResourceGroup, rdName string, filter *apiv1.AutoSelectFilter,
) error {
	created, _, _, placeErr := p.PlaceCreating(ctx, rdName, filter)
	if len(created) == 0 {
		return placeErr
	}

	reason, err := g.currentSkip(ctx, rg, rdName)
	if err != nil || reason == "" {
		return errors.Join(placeErr, err)
	}

	return errors.Join(placeErr, g.withdraw(ctx, rdName, created, reason))
}

// withdraw deletes the replicas of rdName on the given nodes.
func (g *balanceGate) withdraw(ctx context.Context, rdName string, nodes []string, reason string) error {
	var errs []error

	for _, node := range nodes {
		logf.FromContext(ctx).Info("deleting a replica placed while balancing stopped for its definition",
			"rd", rdName, "node", node, "reason", reason)

		err := g.st.Resources().Delete(ctx, rdName, node)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			errs = append(errs, fmt.Errorf("delete replica of %q on %q: %w", rdName, node, err))
		}
	}

	return errors.Join(errs...)
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

// modifyPlaceCountSkip reports why the pass for an `rg modify` that
// named a place count must not place on rd, or "" when it may. It
// makes the one check LINSTOR's modify makes before it autoplaces a
// child (CtrlRscGrpApiCallHandler.modifyInTransaction): the
// definition's replicas, every one but a tiebreaker, diskless ones and
// ones being deleted included, number fewer than the place count. None
// of the balance task's checks apply, so a definition kept only by its
// snapshots is topped up from zero and BalanceResourcesEnabled is not
// read.
//
// A definition being deleted is skipped as well, which LINSTOR does
// not do. In blockstor that flag is the object's deletion timestamp,
// which cannot be withdrawn; a replica placed under it is torn down
// with the definition or outlives it.
func modifyPlaceCountSkip(ctx context.Context, st store.Store, rg *apiv1.ResourceGroup, rd *apiv1.ResourceDefinition) (string, error) {
	if slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return balanceSkipDeleting, nil
	}

	replicas, err := st.Resources().ListByDefinition(ctx, rd.Name)
	if err != nil {
		return "", fmt.Errorf("list the replicas of %q: %w", rd.Name, err)
	}

	counted := 0

	for i := range replicas {
		if !slices.Contains(replicas[i].Flags, apiv1.ResourceFlagTieBreaker) {
			counted++
		}
	}

	if counted >= max(int(rg.SelectFilter.PlaceCount), 1) {
		return modifySkipAtCount, nil
	}

	return "", nil
}
