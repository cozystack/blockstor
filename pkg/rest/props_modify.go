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

	"github.com/cozystack/blockstor/pkg/store"
)

// applyPropsModify is the shared property-bag merge core. Every
// LINSTOR object that accepts a `GenericPropsModify` envelope
// (override_props + delete_props) applies the same semantic, and
// this helper centralises it so the empty-value=delete rule (below)
// lives in exactly one place.
//
// Semantics (mirrors upstream LINSTOR's `Props.map` + the CLI
// behaviour documented in UG9 §"Auto-quorum policies" ~4277-4279):
//
//   - override entries land onto the bag first;
//   - an override entry whose VALUE is the empty string DELETES the
//     key instead of storing an empty value. Upstream's NOTE is
//     explicit: "Setting `DrbdOptions/Resource/on-no-quorum` to an
//     empty value … deletes the property from the object entirely."
//     The python-linstor CLI also routes `set-property KEY ""` to a
//     delete; folding empty-override → delete here makes the server
//     converge to the same final state regardless of which wire shape
//     the client chose (delete_props vs override_props with "").
//   - explicit delete_props keys strip last, so a key named in both
//     override and delete still ends up removed.
//
// Returns the (possibly freshly-allocated) map so callers that start
// from a nil bag get a usable map back. Callers that already hold a
// non-nil map can ignore the return value — the same map is mutated
// in place.
func applyPropsModify(props, override map[string]string, del []string) map[string]string {
	if props == nil && (len(override) > 0 || len(del) > 0) {
		props = map[string]string{}
	}

	for key, value := range override {
		if value == "" {
			// Empty value = delete the key (upstream NOTE).
			delete(props, key)

			continue
		}

		props[key] = value
	}

	for _, key := range del {
		delete(props, key)
	}

	return props
}

// deletePropNamespaces strips every property under the named namespaces, the
// `delete_namespaces` half of upstream's props-modify envelope, and is the one
// implementation every door uses. A namespace covers the keys below it and not
// a key that spells it exactly: `DrbdOptions` takes `DrbdOptions/Net/protocol`
// and leaves both `DrbdOptions` and `DrbdOptionsOther` alone. That is
// upstream's reading: its PropsContainer keeps a key and the namespace of the
// same name apart, and removeNamespace clears only the namespace.
func deletePropNamespaces(props map[string]string, namespaces []string) {
	for _, ns := range namespaces {
		for key := range props {
			if store.NamespaceCovers(ns, key) {
				delete(props, key)
			}
		}
	}
}

// The wire names of the props-modify envelope's three fields.
const (
	fieldOverrideProps    = "override_props"
	fieldDeleteProps      = "delete_props"
	fieldDeleteNamespaces = "delete_namespaces"
)

// refusePropsModifyOnCreate answers a create door that received the
// props-modify fields. Those fields edit props that already exist, and a
// create has none yet; neither golinstor nor python-linstor sends them on
// rd create or rg spawn. Honouring them would invent semantics, and dropping
// them answers success for an edit that never happened, so they are refused
// with the field named. Returns true when the request was answered.
func refusePropsModifyOnCreate(w http.ResponseWriter, door string, override map[string]string, del, namespaces []string) bool {
	var field string

	switch {
	case len(override) > 0:
		field = fieldOverrideProps
	case len(del) > 0:
		field = fieldDeleteProps
	case len(namespaces) > 0:
		field = fieldDeleteNamespaces
	default:
		return false
	}

	writeError(w, http.StatusBadRequest,
		door+" does not take "+field+": set the properties on the definition once it exists, "+
			"with `linstor rd set-property` or a resource-definition modify")

	return true
}
