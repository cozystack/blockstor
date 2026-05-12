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

// NodeConnection carries DRBD options that apply to a single peer
// pair — `linstor node-connection set-property nodeA nodeB
// DrbdOptions/Net/ping-timeout 100` lands here. The pair is
// unordered (sorted at the store layer) so set + get can use either
// canonical order without caring about caller-side normalisation.
type NodeConnection struct {
	NodeA      string            `json:"node_a"`
	NodeB      string            `json:"node_b"`
	Properties map[string]string `json:"properties,omitempty"`
}
