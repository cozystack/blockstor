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
	"fmt"
	"net/http"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// knownNodeTypes is the closed enum upstream LINSTOR accepts for
// `spec.type` on `POST /v1/nodes`. The CRD CEL rule on
// pkg/store/k8s mirrors the same list; any change here must stay in
// sync with the schema. Order is informational — the enumeration
// returned via the refusal message matches the order operators see
// in upstream documentation.
var knownNodeTypes = []string{
	apiv1.NodeTypeController,
	apiv1.NodeTypeSatellite,
	apiv1.NodeTypeCombined,
	apiv1.NodeTypeAuxiliary,
	apiv1.NodeTypeRemoteSpdk,
	apiv1.NodeTypeOpenflexTarget,
	apiv1.NodeTypeEbsTarget,
	apiv1.NodeTypeEbsInitiator,
}

// validateNodeType returns a non-nil error when `t` is non-empty and
// outside the upstream enum. Empty string is accepted — the downstream
// store-write path defaults a missing Type to SATELLITE so callers that
// post a body without the `type` key (the canonical `linstor n c <name>
// <ip>` shape) stay unaffected.
func validateNodeType(t string) error {
	if t == "" {
		return nil
	}

	upper := strings.ToUpper(t)
	for _, k := range knownNodeTypes {
		if k == upper {
			return nil
		}
	}

	return fmt.Errorf(
		"node_type %q is invalid: supported values are %s",
		t, strings.Join(knownNodeTypes, ", "))
}

// writeNodeTypeError writes the Bug 370 refusal envelope: HTTP 400 +
// FAIL_INVLD_NODE_TYPE sub-code so python-linstor (and golinstor)
// classify the response as a typed enum-violation instead of an
// opaque MASK_ERROR. Mirrors the upstream LINSTOR
// `ApiConsts.FAIL_INVLD_NODE_TYPE` (430) on the same input.
func writeNodeTypeError(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInvldNodeType,
		Message: msg,
	}})
}

// apiCallRcFailInvldNodeType mirrors upstream LINSTOR's
// `ApiConsts.FAIL_INVLD_NODE_TYPE` (430). Emitted by
// `POST /v1/nodes` when the body carries a `type` outside the
// upstream enum. Choosing 430 (rather than a fresh sub-code in
// blockstor's 996+ band) lets audit-log greppers that already
// classify upstream's FAIL_INVLD_NODE_TYPE traffic catch
// blockstor's equivalent without a separate rule.
const apiCallRcFailInvldNodeType int64 = 430
