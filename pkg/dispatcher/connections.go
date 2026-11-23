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

package dispatcher

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// ResourceConnectionPathsPropPrefix mirrors the REST handler's storage
// prefix (pkg/rest/resource_connections.go::resourceConnectionPathsPropKey).
// Duplicated here so pkg/dispatcher doesn't import pkg/rest — the
// REST package depends on pkg/dispatcher transitively for the satellite
// reconciler chain, so an inverse import would form a cycle.
//
// Keep the string in sync; the value is part of the on-RD wire shape
// and changing it on one side without the other silently breaks
// scenario 3.7 end-to-end.
const ResourceConnectionPathsPropPrefix = "Cozystack/ResourceConnectionPaths/"

// connectionsFromRD walks rd.Spec.Props for entries under the
// resource-connection-paths and resource-connection-props prefixes and
// returns the decoded list of DesiredConnections, sorted by (NodeA, NodeB) so the satellite .res
// render is deterministic regardless of map iteration order.
//
// Returns nil when rd has no connection props — the satellite falls
// back to the default single-host-pair render in that case.
//
// The JSON decode uses untyped maps rather than a typed struct so
// pkg/dispatcher doesn't have to re-declare the wire shape from
// pkg/rest (tagliatelle would reject snake_case tags outside
// pkg/rest, and adding a per-tag nolint would just hide the
// duplication smell). Schema is whatever pkg/rest writes; if the
// blob is malformed the entry is silently skipped (the satellite
// reconciler can't fix it, and surfacing here would block the
// non-multi-path render path).
func connectionsFromRD(rd *blockstoriov1alpha1.ResourceDefinition) []intent.DesiredConnection {
	if rd == nil || len(rd.Spec.Props) == 0 {
		return nil
	}

	var out []intent.DesiredConnection

	for propKey, propVal := range rd.Spec.Props {
		nodeA, nodeB, ok := splitConnectionKey(propKey)
		if !ok {
			continue
		}

		paths := decodePathList(propVal)
		if paths == nil {
			continue
		}

		// Stable Name ordering inside a connection so the .res render
		// is byte-identical across reconciles. The REST POST preserves
		// declaration order on disk, but Go's map iteration scrambles
		// it on read — sort here to anchor the output.
		sort.Slice(paths, func(i, j int) bool {
			return paths[i].Name < paths[j].Name
		})

		out = append(out, intent.DesiredConnection{
			NodeA: nodeA,
			NodeB: nodeB,
			Paths: paths,
		})
	}

	out = withConnectionOptions(out, rd.Spec.Props)

	if len(out) == 0 {
		return nil
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeA != out[j].NodeA {
			return out[i].NodeA < out[j].NodeA
		}

		return out[i].NodeB < out[j].NodeB
	})

	return out
}

// withConnectionOptions joins the DRBD props of each resource connection
// (apiv1.ResourceConnectionPropsPrefix) onto the connection entry for the
// same pair, or adds an entry without paths when the pair has none. Only
// DrbdOptions/* props reach the satellite: the rest of a connection's
// props are for its readers, not for DRBD.
func withConnectionOptions(conns []intent.DesiredConnection, props map[string]string) []intent.DesiredConnection {
	for pair, connProps := range apiv1.ResourceConnectionProps(props) {
		opts := map[string]string{}

		for key, value := range connProps {
			if strings.HasPrefix(key, drbd.PropPrefix) {
				opts[key] = value
			}
		}

		if len(opts) == 0 {
			continue
		}

		idx := slices.IndexFunc(conns, func(c intent.DesiredConnection) bool {
			return pair.Has(c.NodeA) && pair.Has(c.NodeB) && !naming.Same(c.NodeA, c.NodeB)
		})
		if idx < 0 {
			conns = append(conns, intent.DesiredConnection{NodeA: pair.NodeA, NodeB: pair.NodeB})
			idx = len(conns) - 1
		}

		conns[idx].DrbdOptions = opts
	}

	return conns
}

// decodePathList unmarshals the JSON-encoded path list (as written
// by pkg/rest/resource_connections.go) into a DesiredConnectionPath
// slice. Returns nil on decode error or empty input — the caller
// uses that as "skip this prop entry".
func decodePathList(raw string) []intent.DesiredConnectionPath {
	if raw == "" {
		return nil
	}

	var wire []map[string]string

	err := json.Unmarshal([]byte(raw), &wire)
	if err != nil || len(wire) == 0 {
		return nil
	}

	out := make([]intent.DesiredConnectionPath, 0, len(wire))

	for _, entry := range wire {
		out = append(out, intent.DesiredConnectionPath{
			Name:     entry["name"],
			AddressA: entry["node_a_address"],
			AddressB: entry["node_b_address"],
		})
	}

	return out
}

// splitConnectionKey returns (a, b, true) when key is in the prop
// shape `Cozystack/ResourceConnectionPaths/<a>/<b>`. Anything else
// returns (_, _, false) and the caller skips the entry.
func splitConnectionKey(propKey string) (string, string, bool) {
	rest, ok := strings.CutPrefix(propKey, ResourceConnectionPathsPropPrefix)
	if !ok {
		return "", "", false
	}

	slash := strings.IndexByte(rest, '/')
	if slash < 0 || slash == len(rest)-1 {
		return "", "", false
	}

	return rest[:slash], rest[slash+1:], true
}
