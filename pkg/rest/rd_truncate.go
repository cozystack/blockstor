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

// deleteEmptyRDQuery is the query parameter of truncate and snapshot
// delete (REST 1.29) that also deletes the definition once it has neither
// replicas nor snapshots left.
const deleteEmptyRDQuery = "delete_empty_resource_definition"

// registerRDTruncate wires LINSTOR's resource-definition truncate.
func (s *Server) registerRDTruncate(mux *http.ServeMux) {
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/resources", s.requireStore(s.handleRDTruncate))
}

// handleRDTruncate answers `DELETE /v1/resource-definitions/{rd}/resources`
// (REST 1.29), the call linstor-csi from v1.13 deletes a volume with: every
// replica of the definition goes in one call, the definition and its
// snapshots stay. With `delete_empty_resource_definition` the definition
// goes as well when no replica and no snapshot is left, see
// deleteDefinitionIfEmpty.
//
// As in LINSTOR:
//
//   - an unknown definition is answered 200 with WARN_NOT_FOUND;
//   - a replica in use refuses the whole call with FAIL_IN_USE and nothing
//     is deleted. A replica on an OFFLINE node is in use nowhere, see
//     inUseOnline;
//   - the diskless replicas go before the ones with a disk, because DRBD
//     refuses to lose the last peer with a disk under a diskless one.
//
// The answer comes once the deletes are visible, as for a definition
// delete; the satellites tear the replicas down from there.
func (s *Server) handleRDTruncate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rdName := r.PathValue("rd")
	deleteEmpty := queryFlag(r, deleteEmptyRDQuery)

	rd, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{rdNotFoundWarning(rdName)})

		return
	}

	if err != nil {
		writeStoreError(w, err)

		return
	}

	replicas, err := s.Store.Resources().ListByDefinition(ctx, rd.Name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	for i := range replicas {
		if inUse(&replicas[i]) && !nodeOffline(ctx, s.Store, replicas[i].NodeName) {
			writeJSON(w, http.StatusConflict, []apiv1.APICallRc{rdInUseRefusal(rd.Name, replicas[i].NodeName)})

			return
		}
	}

	answer, err := s.truncateReplicas(ctx, rd.Name, replicas)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if deleteEmpty {
		deleted, delErr := s.deleteDefinitionIfEmpty(ctx, rd.Name)
		if delErr != nil {
			writeStoreError(w, delErr)

			return
		}

		answer = append(answer, deleted...)
	}

	writeJSON(w, http.StatusOK, answer)
}

// rdNotFoundWarning is LINSTOR's answer for a definition that is not there.
func rdNotFoundWarning(rdName string) apiv1.APICallRc {
	return apiv1.APICallRc{
		RetCode: warnNotFound,
		Message: fmt.Sprintf("Resource definition '%s' not found.", rdName),
		ObjRefs: map[string]string{objRefRscDfn: rdName},
	}
}

// rdInUseRefusal is LINSTOR's FAIL_IN_USE for a definition whose replica on
// node is in use.
func rdInUseRefusal(rdName, node string) apiv1.APICallRc {
	return apiv1.APICallRc{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: fmt.Sprintf("Resource '%s' on node '%s' is still in use.", rdName, node),
		Cause:   "Resource is mounted/in use.",
		Correc:  fmt.Sprintf("Un-mount resource '%s' on the node '%s'.", rdName, node),
		ObjRefs: map[string]string{objRefRscDfn: rdName, objRefNode: node},
	}
}

// truncateReplicas deletes every replica of rdName, the diskless ones first,
// and waits until the deletes are visible. A replica that appears while it
// runs, such as a tiebreaker the controller adds, goes as well, through the
// same retry the definition delete uses.
func (s *Server) truncateReplicas(ctx context.Context, rdName string, replicas []apiv1.Resource) ([]apiv1.APICallRc, error) {
	ordered := slices.Clone(replicas)
	slices.SortStableFunc(ordered, func(a, b apiv1.Resource) int {
		switch da, db := drbdDiskless(&a), drbdDiskless(&b); {
		case da == db:
			return 0
		case da:
			return -1
		default:
			return 1
		}
	})

	answer := make([]apiv1.APICallRc, 0, len(ordered))

	for i := range ordered {
		err := s.Store.Resources().Delete(ctx, rdName, ordered[i].NodeName)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, errors.Wrapf(err, "delete replica %s on %s", rdName, ordered[i].NodeName)
		}

		answer = append(answer, apiv1.APICallRc{
			RetCode: apiCallRcInfo | apiCallRcRscDeleted,
			Message: fmt.Sprintf("Resource '%s' on node '%s' deleted.", rdName, ordered[i].NodeName),
			ObjRefs: map[string]string{objRefRscDfn: rdName, objRefNode: ordered[i].NodeName},
		})
	}

	err := s.cascadeDeleteResources(ctx, rdName)
	if err != nil {
		return nil, err
	}

	for i := range ordered {
		s.waitForResourceDeletionVisible(ctx, rdName, ordered[i].NodeName)
	}

	return answer, nil
}

// deleteDefinitionIfEmpty deletes rdName when it has neither replicas nor
// snapshots, LINSTOR's deleteResourceDefinitionIfEmpty behind
// `delete_empty_resource_definition`. A definition that still has either,
// or is already gone, stays as it is and the answer is empty; see
// store.ClaimEmptyDefinition for how a racing create keeps it.
//
// One kept only by the internal snapshots of a clone taken from it is
// marked store.DeleteWhenEmptyProp instead, and the definition controller
// deletes it once the clone's snapshot is gone: LINSTOR has no snapshot
// definition there and deletes it at once, and the client does not ask
// again.
func (s *Server) deleteDefinitionIfEmpty(ctx context.Context, rdName string) ([]apiv1.APICallRc, error) {
	contents, claimed, err := store.ClaimEmptyDefinition(ctx, s.Store, rdName)
	if err != nil {
		return nil, ignoreNotFound(err)
	}

	if !claimed {
		if contents.OnlyCloneSnapshots {
			return nil, ignoreNotFound(store.SetDeleteWhenEmpty(ctx, s.Store, rdName, true))
		}

		return nil, nil
	}

	answer, err := s.deleteDefinitionData(ctx, rdName)

	return answer, ignoreNotFound(err)
}

// ignoreNotFound drops an error that only says the object is gone.
func ignoreNotFound(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}

	return err
}
