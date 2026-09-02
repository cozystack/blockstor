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

package view

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// NodeList builds the `node list` table.
//
// The Status cell renders the tokens the install and validation
// scripts grep for — ONLINE / OFFLINE / EVICTED — with an evicted node
// reporting its flag rather than a connection state, because that is
// the thing an operator has to act on.
func NodeList(nodes []apiv1.Node) *metav1.Table {
	tbl := &metav1.Table{
		ColumnDefinitions: columns("Node", "NodeType", "Addresses", "State"),
	}

	for i := range nodes {
		node := &nodes[i]

		tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{
			node.Name,
			node.Type,
			nodeAddresses(node),
			nodeState(node),
		}})
	}

	return tbl
}

// nodeAddresses renders the advertised endpoints.
func nodeAddresses(node *apiv1.Node) string {
	for i := range node.NetInterfaces {
		if node.NetInterfaces[i].Address != "" {
			return node.NetInterfaces[i].Address
		}
	}

	return ""
}

// nodeState prefers a terminal flag over the connection status: an
// EVICTED node may still report a stale ONLINE, and showing that would
// hide the reason its resources moved.
func nodeState(node *apiv1.Node) string {
	for _, flag := range node.Flags {
		switch flag {
		case "EVICTED", "EVACUATING", flagDelete:
			return flag
		}
	}

	if node.ConnectionStatus == "" {
		return "UNKNOWN"
	}

	return node.ConnectionStatus
}
