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

package controllers

import "github.com/cozystack/blockstor/pkg/store/k8s"

// isNode reports whether name, as a replica or a pool spells its node, is
// node. The store addresses a Node under its folded name, so a Node
// registered as Node-1 and a satellite started as node-1 are the same
// node, and a replica written under either spelling is this satellite's.
func isNode(name, node string) bool {
	return k8s.Name(name) == k8s.Name(node)
}
