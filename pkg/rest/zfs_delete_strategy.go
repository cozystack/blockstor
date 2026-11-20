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
	"net/http"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"
)

// apiCallRcFailExistsSnapshot mirrors upstream LINSTOR's
// `ApiConsts.FAIL_EXISTS_SNAPSHOT` (`516 | MASK_ERROR`), which its ZFS delete
// strategy check raises. The MASK_ERROR bit is OR'd in at the call site.
const apiCallRcFailExistsSnapshot int64 = 516

// refuseZFSDeleteStrategy answers a replica or volume definition delete the
// way LINSTOR does under `StorDriver/Zfs/DeleteStrategy` DELETE: refused while
// the resource definition has snapshots, `force` or not. It returns true
// when it wrote the refusal.
func (s *Server) refuseZFSDeleteStrategy(w http.ResponseWriter, r *http.Request, rdName string) bool {
	refused, err := store.ZFSDeleteRefused(r.Context(), s.Store, rdName)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if !refused {
		return false
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailExistsSnapshot,
		Message: "Cannot use " + string(validate.ZFSDeleteStrategyDelete) +
			" strategy since the resource definition '" + rdName + "' still has snapshots",
		Correc: "Delete the snapshots first, or set " + validate.ZFSDeleteStrategyProp +
			" to " + string(validate.ZFSDeleteStrategyRename) + " or " + string(validate.ZFSDeleteStrategyDynamic) + ".",
		ObjRefs: map[string]string{objRefRscDfn: rdName},
	}})

	return true
}
