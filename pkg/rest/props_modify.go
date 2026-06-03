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
