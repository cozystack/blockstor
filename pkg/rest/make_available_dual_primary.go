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
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// writeDualPrimaryError answers a dual-Primary refusal with its own
// envelope and any other error as the store error it is.
func writeDualPrimaryError(w http.ResponseWriter, err error) {
	var refusal *dualPrimaryRefusalError
	if errors.As(err, &refusal) {
		refusal.write(w)

		return
	}

	writeStoreError(w, err)
}

// makeAvailableAndPrepareMigration makes rd available on node and, when
// the caller set auto_manage_dual_primary, prepares the live migration to
// node. It answers the request.
func (s *Server) makeAvailableAndPrepareMigration(w http.ResponseWriter, r *http.Request, rd *apiv1.ResourceDefinition, node string, req *apiv1.ResourceMakeAvailable) {
	rdName := r.PathValue("rd")

	// The migration source is read before the replica changes: it is
	// the node the resource is in use on now, and a refusal must leave
	// the target untouched.
	source := ""

	if req.AutoManageDualPrimary {
		var err error

		source, err = s.liveMigrationSource(r.Context(), rd, node, req.LayerList)
		if err != nil {
			writeDualPrimaryError(w, err)

			return
		}
	}

	if !s.applyMakeAvailable(w, r, rdName, node, req) {
		return
	}

	msgs := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "Resource '" + rdName + "' on node '" + node + "' made available",
	}}

	if req.AutoManageDualPrimary {
		opened, err := s.openDualPrimary(r.Context(), rdName, source, node)
		if err != nil {
			writeDualPrimaryError(w, err)

			return
		}

		msgs = append(msgs, opened...)
	}

	writeJSON(w, http.StatusOK, msgs)
}

// liveMigrationSource checks that a make-available with
// auto_manage_dual_primary can be served and answers the node the
// migration to target starts from ("" when there is none). requested is
// the layer list of the call, which LINSTOR judges in place of the
// definition's when the caller gives one.
func (s *Server) liveMigrationSource(ctx context.Context, rd *apiv1.ResourceDefinition, target string, requested []string) (string, error) {
	layers := requested
	if len(layers) == 0 {
		stack, err := store.EffectiveLayerStack(ctx, s.Store, rd)
		if err != nil {
			return "", errors.Wrapf(err, "layer stack of %s", rd.Name)
		}

		layers = stack
	}

	err := requireDRBDForDualPrimary(rd.Name, layers)
	if err != nil {
		return "", err
	}

	replicas, err := s.Store.Resources().ListByDefinition(ctx, rd.Name)
	if err != nil {
		return "", errors.Wrapf(err, "list replicas of %s", rd.Name)
	}

	// An in-use reported by a node that is gone is stale: see inUseOnline.
	for i := range replicas {
		if inUse(&replicas[i]) && nodeOffline(ctx, s.Store, replicas[i].NodeName) {
			replicas[i].State.InUse = nil
		}
	}

	return migrationSource(rd.Name, rd.Props, replicas, target)
}

// openDualPrimary writes the dual-Primary props for a migration from
// source to target onto the definition, once target is available. With no
// source there is no migration and only the note saying so is answered.
//
// The source was picked before target was made available, outside the
// definition write. LINSTOR picks it and writes the props under one lock;
// here the write re-checks what the pick relied on against the definition
// it changes, so a call that lost a race does not overwrite the winner:
//
//   - marks naming another migration refuse the call, as they would have
//     had they been there at the pick (two targets attached at once);
//   - a source that let go of the device meanwhile means there is no
//     migration to prepare any more (an unmake-available on the source
//     ran in between), and nothing is written.
//
// Unlike LINSTOR, the call does not wait for the satellites to apply the
// change: no definition prop write in blockstor does. The satellites of
// both ends re-render their resource file from the definition watch.
func (s *Server) openDualPrimary(ctx context.Context, rdName, source, target string) ([]apiv1.APICallRc, error) {
	if source == "" {
		return notMigrating(rdName), nil
	}

	plan, err := s.planDualPrimary(ctx, rdName, source, target)
	if err != nil {
		return nil, err
	}

	err = plan.refuseShadowedOverride()
	if err != nil {
		return nil, err
	}

	var (
		msgs      []apiv1.APICallRc
		migrating bool
	)

	err = s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(live *apiv1.ResourceDefinition) error {
			if live.Props == nil {
				live.Props = map[string]string{}
			}

			markSource, markTarget := live.Props[liveMigrateSourceProp], live.Props[liveMigrateTargetProp]
			if apiv1.LiveMigrationMarked(live.Props) && !plan.is(markSource, markTarget) {
				return conflictingMigration(rdName, markSource, markTarget)
			}

			migrating = s.inUseOnline(ctx, rdName, source)
			if !migrating {
				return nil
			}

			msgs = plan.apply(live.Props)

			return nil
		})
	if err != nil {
		return nil, errors.Wrapf(err, "set dual-primary props on %s", rdName)
	}

	if !migrating {
		return notMigrating(rdName), nil
	}

	return msgs, nil
}

// planDualPrimary reads what the protocol of each end depends on below and
// above the definition, and whether both ends are diskless.
func (s *Server) planDualPrimary(ctx context.Context, rdName, source, target string) (dualPrimaryPlan, error) {
	plan := dualPrimaryPlan{rdName: rdName, source: source, target: target, bothDiskless: true}

	for i, node := range []string{source, target} {
		res, err := getResourceWithCacheRetry(ctx, s.Store, rdName, node)
		if err != nil {
			return plan, errors.Wrapf(err, "replica of %s on %s", rdName, node)
		}

		plan.bothDiskless = plan.bothDiskless && drbdDiskless(&res)

		eff, _, err := effectivePropsAndRDForResource(ctx, s.Client, s.Store, &res)
		if err != nil {
			return plan, err
		}

		plan.ends[i] = protocolSourcesOf(eff)
	}

	return plan, nil
}

// notMigrating is the answer of a make-available with
// auto_manage_dual_primary that has no live migration to prepare.
func notMigrating(rdName string) []apiv1.APICallRc {
	return []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("Resource '%s' is not in use on another node, no dual-primary preparation needed",
			rdName),
	}}
}

// inUseOnline reports whether the replica of rdName on node holds the
// device open, as far as anyone can tell: a replica on an OFFLINE node is
// not. The satellite of a node that went away cannot clear the in-use it
// last reported, so that report outlives the VM it described; LINSTOR
// forgets a satellite's state when the connection drops, and a node that
// is gone is in use nowhere.
func (s *Server) inUseOnline(ctx context.Context, rdName, node string) bool {
	res, err := s.Store.Resources().Get(ctx, rdName, node)
	if err != nil {
		return false
	}

	return inUse(&res) && !nodeOffline(ctx, s.Store, node)
}

// nodeOffline reports whether the node is known to be unreachable: its
// heartbeat went stale and the controller marked it OFFLINE.
func nodeOffline(ctx context.Context, st store.Store, node string) bool {
	n, err := st.Nodes().Get(ctx, node)
	if err != nil {
		return false
	}

	return strings.EqualFold(n.ConnectionStatus, apiv1.NodeTypeOffline)
}

// handleResourceUnmakeAvailable answers
// `POST /v1/resource-definitions/{rd}/resources/{node}/unmake-available`,
// the call linstor-csi from v1.13 makes on every Detach. It reverts a
// make-available on the node:
//
//   - refused with FAIL_IN_USE while the replica on node is in use, and
//     nothing is changed then, the dual-Primary props included: the
//     caller retries once the node let go. An OFFLINE node is in use
//     nowhere, whatever it reported last (see inUseOnline);
//   - the dual-Primary props come off the definition (see
//     revertDualPrimary);
//   - the replica on node is deleted when it is a diskless attachment;
//     a diskful replica or a tiebreaker stays, with a note saying so.
//
// A definition that does not exist is a successful no-op, as is a node
// without a replica, so a client can always issue the call after a
// migration. An unknown node is refused: it is most likely a typo.
func (s *Server) handleResourceUnmakeAvailable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	_, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnNotFound,
			Message: fmt.Sprintf("Resource definition '%s' not found. Nothing to do.", rdName),
		}})

		return
	}

	if err != nil {
		writeStoreError(w, err)

		return
	}

	_, err = s.Store.Nodes().Get(ctx, node)
	if err != nil {
		writeStoreErrorTyped(w, err, storeKindNode)

		return
	}

	present, err := s.replicaOrNil(ctx, rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if present != nil && inUse(present) && !nodeOffline(ctx, s.Store, node) {
		writeStillInUse(w, rdName, node)

		return
	}

	msgs, err := s.revertDualPrimaryOn(ctx, rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	deleteMsgs, err := s.releaseAttachment(ctx, rdName, node, present)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, append(msgs, deleteMsgs...))
}

// writeStillInUse refuses an unmake-available on a node that still holds
// the device open, with LINSTOR's FAIL_IN_USE so a driver can tell it
// from a failure and retry.
func writeStillInUse(w http.ResponseWriter, rdName, node string) {
	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: fmt.Sprintf("Resource '%s' is still in use.", rdName),
		Cause:   "Resource is mounted/in use.",
		Correc:  fmt.Sprintf("Un-mount resource '%s' on the node '%s'.", rdName, node),
		ObjRefs: map[string]string{objRefRscDfn: rdName, objRefNode: node},
	}})
}

// replicaOrNil answers the replica of rdName on node, or nil when there
// is none.
func (s *Server) replicaOrNil(ctx context.Context, rdName, node string) (*apiv1.Resource, error) {
	replica, err := s.Store.Resources().Get(ctx, rdName, node)

	switch {
	case err == nil:
		return &replica, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, nil //nolint:nilnil // no replica is an answer, not an error
	}

	return nil, errors.Wrapf(err, "replica of %s on %s", rdName, node)
}

// revertDualPrimaryOn takes the dual-Primary props off the definition.
func (s *Server) revertDualPrimaryOn(ctx context.Context, rdName, node string) ([]apiv1.APICallRc, error) {
	var msgs []apiv1.APICallRc

	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(live *apiv1.ResourceDefinition) error {
			_, msgs = revertDualPrimary(rdName, node, live.Props)

			return nil
		})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, errors.Wrapf(err, "revert dual-primary props on %s", rdName)
	}

	return msgs, nil
}

// releaseAttachment removes the replica make-available may have put on
// node, when doing so cannot lose data: only a diskless replica goes. A
// tiebreaker stays, it is the quorum of the replicas that hold the data,
// and so does a diskful replica, which may be the last up-to-date one. A
// diskless replica that is the tie breaking voter of an even number of
// diskful ones turns into the tiebreaker in place, as LINSTOR's delete
// with keepTiebreaker does, instead of leaving them without one until
// the reconciler places a new one.
func (s *Server) releaseAttachment(ctx context.Context, rdName, node string, replica *apiv1.Resource) ([]apiv1.APICallRc, error) {
	switch {
	case replica == nil:
		return []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: fmt.Sprintf("Resource '%s' is not deployed on node '%s'. Nothing to delete.", rdName, node),
		}}, nil
	case slices.Contains(replica.Flags, apiv1.ResourceFlagTieBreaker):
		return []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: fmt.Sprintf("Resource '%s' on node '%s' is a tiebreaker and is left in place", rdName, node),
		}}, nil
	case !drbdDiskless(replica):
		return []apiv1.APICallRc{{
			RetCode: maskWarn,
			Message: fmt.Sprintf("Resource '%s' is diskful on node '%s', not deleting it", rdName, node),
			Cause:   "Deleting a diskful resource could remove the last up-to-date replica or break quorum.",
			Correc: "Delete the resource explicitly or use toggle-disk/migrate-disk if the local replica " +
				"is no longer wanted.",
		}}, nil
	}

	keep, err := s.attachmentKeepsQuorum(ctx, rdName, node)
	if err != nil {
		return nil, err
	}

	if keep {
		return s.keepAsTiebreaker(ctx, rdName, node)
	}

	err = s.Store.Resources().Delete(ctx, rdName, node)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, errors.Wrapf(err, "delete %s on %s", rdName, node)
	}

	s.waitForResourceDeletionVisible(ctx, rdName, node)
	s.bumpPeerChangedOnSiblings(ctx, rdName, node)

	return []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRscDeleted,
		Message: "resource deleted: " + rdName + " on " + node,
	}}, nil
}

// attachmentKeepsQuorum reports whether the diskless replica on node is
// the voter the auto-tiebreaker would put back the moment it went: the
// definition runs DRBD with the auto-tiebreaker on and not suppressed, and
// without node it would have an even number, at least two, of active
// diskful replicas and no diskless one. The rule is the reconciler's
// (shouldTieBreakerExist), so the two agree on which shape needs one.
func (s *Server) attachmentKeepsQuorum(ctx context.Context, rdName, node string) (bool, error) {
	rd, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return false, errors.Wrapf(err, "get %s", rdName)
	}

	if strings.EqualFold(rd.Props[autoTiebreakerProp], "false") || tiebreakerSuppressed(&rd) {
		return false, nil
	}

	stack, err := store.EffectiveLayerStack(ctx, s.Store, &rd)
	if err != nil {
		return false, errors.Wrapf(err, "layer stack of %s", rdName)
	}

	if !apiv1.ContainsReplicationLayer(stack) {
		return false, nil
	}

	replicas, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return false, errors.Wrapf(err, "list replicas of %s", rdName)
	}

	diskful := 0

	for i := range replicas {
		replica := &replicas[i]

		switch {
		case naming.Same(replica.NodeName, node), slices.Contains(replica.Flags, apiv1.ResourceFlagInactive):
		case drbdDiskless(replica):
			return false, nil
		default:
			diskful++
		}
	}

	return diskful >= 2 && diskful%2 == 0, nil
}

// autoTiebreakerProp turns the auto-tiebreaker off for a definition when
// set to false.
const autoTiebreakerProp = "DrbdOptions/AutoAddQuorumTiebreaker"

// tiebreakerSuppressed reports whether a recent tiebreaker delete asked
// the reconciler not to put one back yet.
func tiebreakerSuppressed(rd *apiv1.ResourceDefinition) bool {
	deadline, err := time.Parse(time.RFC3339, rd.Annotations[apiv1.AutoTiebreakerSuppressedUntilAnnotation])

	return err == nil && time.Now().Before(deadline)
}

// keepAsTiebreaker turns the diskless replica on node into the
// definition's tiebreaker.
func (s *Server) keepAsTiebreaker(ctx context.Context, rdName, node string) ([]apiv1.APICallRc, error) {
	err := s.Store.Resources().PatchResourceSpec(ctx, rdName, node, func(live *apiv1.Resource) error {
		if !slices.Contains(live.Flags, apiv1.ResourceFlagTieBreaker) {
			live.Flags = append(live.Flags, apiv1.ResourceFlagTieBreaker)
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "make %s on %s a tiebreaker", rdName, node)
	}

	return []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("Resource '%s' on node '%s' is kept as the tiebreaker", rdName, node),
	}}, nil
}
