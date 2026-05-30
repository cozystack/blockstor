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

// EventMayPromoteChange is the JSON payload of a single
// `may-promote-change` SSE event on `/v1/events/drbd/promotion`. The
// wire shape matches golinstor's `client.EventMayPromoteChange` so the
// canonical Go client unmarshals it without translation.
//
// MayPromote tracks the DRBD `may_promote` flag on a per-(resource,
// node) tuple — the same signal `linstor r l` surfaces as the "Promote"
// column. ha-controller subscribes to this stream to drive failover
// decisions: when MayPromote transitions to false on the currently-
// active node, the controller looks for a peer with MayPromote == true
// and shifts the workload there.
type EventMayPromoteChange struct {
	ResourceName string `json:"resource_name,omitempty"`
	NodeName     string `json:"node_name,omitempty"`
	MayPromote   bool   `json:"may_promote,omitempty"`
}

// EventNodeChange is the JSON payload of a single `node-change` SSE
// event on `/v1/events/nodes`. Upstream LINSTOR ships this as a free-
// form event whose shape varies across minor versions; the field set
// here mirrors what `linstor node list -w` surfaces today and what
// piraeus-affinity-controller actually consumes.
//
// ConnectionStatus uses the same enum upstream populates on the Node
// read-side ("ONLINE", "OFFLINE", "EVICTED", "STANDBY", "VERSION_
// MISMATCH", "OTHER_CONTROLLER", "AUTHENTICATION_ERROR", "FULL_SYNC_
// FAILED", "HOSTNAME_MISMATCH", "NO_STLT_CONN", "UNKNOWN"). Consumers
// compare values literally; we pass through whatever the controller
// publishes.
type EventNodeChange struct {
	NodeName         string `json:"node_name,omitempty"`
	ConnectionStatus string `json:"connection_status,omitempty"`
}
