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

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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

	return migrationSource(rd.Name, rd.Props, replicas, target)
}

// openDualPrimary writes the dual-Primary props for a migration from
// source to target onto the definition, once target is available. With no
// source there is no migration and only the note saying so is answered.
//
// Unlike LINSTOR, the call does not wait for the satellites to apply the
// change: no definition prop write in blockstor does. The satellites of
// both ends re-render their resource file from the definition watch.
func (s *Server) openDualPrimary(ctx context.Context, rdName, source, target string) ([]apiv1.APICallRc, error) {
	if source == "" {
		return []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: fmt.Sprintf("Resource '%s' is not in use on another node, no dual-primary preparation needed",
				rdName),
		}}, nil
	}

	plan := dualPrimaryPlan{rdName: rdName, source: source, target: target, bothDiskless: true}

	for i, node := range []string{source, target} {
		res, err := getResourceWithCacheRetry(ctx, s.Store, rdName, node)
		if err != nil {
			return nil, errors.Wrapf(err, "replica of %s on %s", rdName, node)
		}

		plan.bothDiskless = plan.bothDiskless && drbdDiskless(&res)

		eff, _, err := effectivePropsAndRDForResource(ctx, s.Client, s.Store, &res)
		if err != nil {
			return nil, err
		}

		plan.ends[i] = protocolSourcesOf(eff)
	}

	var msgs []apiv1.APICallRc

	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(live *apiv1.ResourceDefinition) error {
			if live.Props == nil {
				live.Props = map[string]string{}
			}

			msgs = plan.apply(live.Props)

			return nil
		})
	if err != nil {
		return nil, errors.Wrapf(err, "set dual-primary props on %s", rdName)
	}

	return msgs, nil
}

// handleResourceUnmakeAvailable answers
// `POST /v1/resource-definitions/{rd}/resources/{node}/unmake-available`,
// the call linstor-csi from v1.13 makes on every Detach. It reverts a
// make-available on the node:
//
//   - refused with FAIL_IN_USE while the replica on node is in use, and
//     nothing is changed then, the dual-Primary props included: the
//     caller retries once the node let go;
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
			RetCode: warnRDNotFound,
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

	if present != nil && inUse(present) {
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
// and so does a diskful replica, which may be the last up-to-date one.
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

	err := s.Store.Resources().Delete(ctx, rdName, node)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, errors.Wrapf(err, "delete %s on %s", rdName, node)
	}

	s.waitForResourceDeletionVisible(ctx, rdName, node)
	s.dropConnectionPropsOf(ctx, rdName, node)
	s.bumpPeerChangedOnSiblings(ctx, rdName, node)

	return []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRscDeleted,
		Message: "resource deleted: " + rdName + " on " + node,
	}}, nil
}

// dropConnectionPropsOf removes the resource-connection props of a
// replica that is gone. Best-effort: a connection prop left behind only
// matters when a replica comes back on the node, and the next
// unmake-available there clears allow-two-primaries anyway.
func (s *Server) dropConnectionPropsOf(ctx context.Context, rdName, node string) {
	_ = s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(live *apiv1.ResourceDefinition) error {
			dropConnectionProps(node, live.Props)

			return nil
		})
}
