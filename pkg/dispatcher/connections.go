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
	"sort"
	"strings"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
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
// resource-connection-paths prefix and returns the decoded list of
// DesiredConnections, sorted by (NodeA, NodeB) so the satellite .res
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

	type key struct{ a, b string }

	groups := map[key][]intent.DesiredConnectionPath{}

	for propKey, propVal := range rd.Spec.Props {
		nodeA, nodeB, ok := splitConnectionKey(propKey)
		if !ok {
			continue
		}

		paths := decodePathList(propVal)
		if paths == nil {
			continue
		}

		groups[key{a: nodeA, b: nodeB}] = paths
	}

	if len(groups) == 0 {
		return nil
	}

	out := make([]intent.DesiredConnection, 0, len(groups))

	for pair, paths := range groups {
		// Stable Name ordering inside a connection so the .res render
		// is byte-identical across reconciles. The REST POST preserves
		// declaration order on disk, but Go's map iteration scrambles
		// it on read — sort here to anchor the output.
		sort.Slice(paths, func(i, j int) bool {
			return paths[i].Name < paths[j].Name
		})

		out = append(out, intent.DesiredConnection{
			NodeA: pair.a,
			NodeB: pair.b,
			Paths: paths,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeA != out[j].NodeA {
			return out[i].NodeA < out[j].NodeA
		}

		return out[i].NodeB < out[j].NodeB
	})

	return out
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
