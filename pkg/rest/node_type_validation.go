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
	"slices"
	"strings"

	"github.com/pkg/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// ErrInvldNodeType is the sentinel for Bug 370 — POST /v1/nodes with
// an unknown `type` value. Callers (handleNodeCreate) wrap it with the
// offending value + accepted enumeration via errors.Wrap so the wire
// envelope still carries the contextual hint while the sentinel
// supports `errors.Is` matching for tests / future callers.
var ErrInvldNodeType = errors.New("invalid node type")

// knownNodeTypesList is the closed enum upstream LINSTOR accepts for
// `spec.type` on `POST /v1/nodes`. The CRD CEL rule on
// pkg/store/k8s mirrors the same list; any change here must stay in
// sync with the schema. Order is informational — the enumeration
// returned via the refusal message matches the order operators see
// in upstream documentation.
//
// Wrapped in a function rather than a package-level slice so the
// linter's gochecknoglobals rule stays happy while the data still
// reads as a single source of truth.
func knownNodeTypesList() []string {
	return []string{
		apiv1.NodeTypeController,
		apiv1.NodeTypeSatellite,
		apiv1.NodeTypeCombined,
		apiv1.NodeTypeAuxiliary,
		apiv1.NodeTypeRemoteSpdk,
		apiv1.NodeTypeOpenflexTarget,
		apiv1.NodeTypeEbsTarget,
		apiv1.NodeTypeEbsInitiator,
	}
}

// validateNodeType returns a non-nil error when nodeType is non-empty
// and outside the upstream enum. Empty string is accepted — the
// downstream store-write path defaults a missing Type to SATELLITE so
// callers that post a body without the `type` key (the canonical
// `linstor n c <name> <ip>` shape) stay unaffected.
func validateNodeType(nodeType string) error {
	if nodeType == "" {
		return nil
	}

	known := knownNodeTypesList()
	if slices.Contains(known, strings.ToUpper(nodeType)) {
		return nil
	}

	return errors.Wrap(ErrInvldNodeType, fmt.Sprintf(
		"%q: supported values are %s",
		nodeType, strings.Join(known, ", ")))
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
