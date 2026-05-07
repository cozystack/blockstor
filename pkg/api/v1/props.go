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

package v1

// GenericPropsModify is the upstream payload for any "modify properties"
// request. Set/delete pairs are mutually independent — they all run in one
// transaction.
type GenericPropsModify struct {
	OverrideProps   map[string]string `json:"override_props,omitempty"`
	DeleteProps     []string          `json:"delete_props,omitempty"`
	DeleteNamespace []string          `json:"delete_namespaces,omitempty"`
}

// KV is the upstream `KeyValueStore` view of a single instance — name plus
// its current property map.
type KV struct {
	Name  string            `json:"name"`
	Props map[string]string `json:"props,omitempty"`
}
