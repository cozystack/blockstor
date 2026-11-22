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
	"errors"
	"net/http"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/propwhitelist"
)

// apiCallRcFailInvldProp mirrors upstream LINSTOR's `ApiConsts.FAIL_INVLD_PROP`
// (213), the code it refuses a property its whitelist does not allow with.
const apiCallRcFailInvldProp int64 = 213

// refuseUnwhitelistedProps answers a write that would store a DrbdOptions
// property LINSTOR's whitelist does not allow on obj: a key it does not list
// for that object, or a value its rule refuses. LINSTOR stores nothing in
// that case and names the first offending key; so does this. An empty value
// is the set-property delete (applyPropsModify) and stores nothing, so it is
// not judged; nor is anything outside DrbdOptions/. Returns true when the
// request was answered.
//
// An accepted request has its values rewritten in props, in place, to the
// form LINSTOR stores them in (propwhitelist.Normalize), so the caller
// stores Yes as yes as LINSTOR does.
//
// Without this a typo such as DrbdOptions/AutoSnapshot/Keep was stored, the
// satellites rendered it into the .res, drbdadm refused the file, and every
// replica of the definition failed each apply until the key was deleted.
func refuseUnwhitelistedProps(w http.ResponseWriter, obj propwhitelist.Object, props map[string]string) bool {
	set := make(map[string]string, len(props))

	for key, value := range props {
		if value != "" {
			set[key] = value
		}
	}

	err := propwhitelist.CheckAll(obj, set)
	if err == nil {
		propwhitelist.NormalizeAll(obj, props)

		return false
	}

	var refusal *propwhitelist.Error
	if !errors.As(err, &refusal) {
		writeError(w, http.StatusBadRequest, err.Error())

		return true
	}

	writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{invalidPropRc(obj, refusal)})

	return true
}

// invalidPropRc words the refusal the way LINSTOR does: the controller's
// own wording for controller properties (CtrlConfApiCallHandler.setProp),
// CtrlPropsHelper.fillProperties' for every other object.
func invalidPropRc(obj propwhitelist.Object, refusal *propwhitelist.Error) apiv1.APICallRc {
	entry := apiv1.APICallRc{RetCode: apiCallRcError | apiCallRcFailInvldProp}

	switch {
	case obj == propwhitelist.Controller && refusal.Known:
		entry.Message = "The value '" + refusal.Value + "' is not valid."
		entry.Details = refusal.Detail
	case obj == propwhitelist.Controller:
		entry.Message = "The key '" + refusal.Key + "' is not whitelisted"
	case refusal.Known:
		entry.Message = "Invalid property(" + refusal.Key + ") value: " + refusal.Value
		entry.Cause = "The value '" + refusal.Value + "' is not valid for the key '" + refusal.Key + "'"
		entry.Details = refusal.Detail
	default:
		entry.Message = "Invalid property key: " + refusal.Key
		entry.Cause = "The key '" + refusal.Key + "' is not whitelisted."
	}

	return entry
}
