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

package v1

import (
	"strings"

	"github.com/cozystack/blockstor/pkg/naming"
)

// ResourceConnectionPropsPrefix is the definition prop namespace that holds
// the props of a resource connection, the link between the replicas of one
// definition on two nodes. LINSTOR keeps them on a connection object of its
// own; blockstor has none, so each prop is one definition prop named
//
//	Cozystack/ResourceConnectionProps/<node-a>/<node-b>/<prop key>
//
// with the two nodes in canonical order (see ResourceConnectionPropKey).
// A node name never holds a slash, so the first two segments after the
// prefix are always the pair and the rest is the prop key verbatim.
//
// The props describe the connection between two replicas of THIS
// definition, so they do not travel with a clone, a snapshot or a restore:
// LINSTOR does not copy a definition's connections either.
const ResourceConnectionPropsPrefix = "Cozystack/ResourceConnectionProps/"

// ResourceConnectionPropKey names the definition prop that holds key for the
// connection between nodeA and nodeB. The pair is written as the store names
// nodes and in sorted order, so either order and any spelling of a node
// address the same pair.
func ResourceConnectionPropKey(nodeA, nodeB, key string) string {
	first, second := naming.Key(nodeA), naming.Key(nodeB)
	if second < first {
		first, second = second, first
	}

	return ResourceConnectionPropsPrefix + first + "/" + second + "/" + key
}

// ResourceConnectionPair names the two nodes of a resource connection, in
// the canonical order ResourceConnectionPropKey writes them.
type ResourceConnectionPair struct {
	NodeA string
	NodeB string
}

// Has reports whether node is one end of the pair.
func (p ResourceConnectionPair) Has(node string) bool {
	return naming.Same(p.NodeA, node) || naming.Same(p.NodeB, node)
}

// resourceConnectionPropParts is node-a, node-b and the prop key.
const resourceConnectionPropParts = 3

// ParseResourceConnectionPropKey splits a definition prop written by
// ResourceConnectionPropKey into its pair and prop key. ok is false for any
// other prop.
func ParseResourceConnectionPropKey(prop string) (ResourceConnectionPair, string, bool) {
	rest, found := strings.CutPrefix(prop, ResourceConnectionPropsPrefix)
	if !found {
		return ResourceConnectionPair{}, "", false
	}

	parts := strings.SplitN(rest, "/", resourceConnectionPropParts)
	if len(parts) != resourceConnectionPropParts || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return ResourceConnectionPair{}, "", false
	}

	return ResourceConnectionPair{NodeA: parts[0], NodeB: parts[1]}, parts[2], true
}

// ResourceConnectionProps collects the props of every connection a
// definition's props hold, keyed by the canonical pair. The inner maps carry
// the prop keys as a client wrote them (DrbdOptions/Net/protocol, …).
func ResourceConnectionProps(defProps map[string]string) map[ResourceConnectionPair]map[string]string {
	var out map[ResourceConnectionPair]map[string]string

	for prop, value := range defProps {
		pair, key, ok := ParseResourceConnectionPropKey(prop)
		if !ok {
			continue
		}

		if out == nil {
			out = map[ResourceConnectionPair]map[string]string{}
		}

		if out[pair] == nil {
			out[pair] = map[string]string{}
		}

		out[pair][key] = value
	}

	return out
}
