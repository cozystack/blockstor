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

// Package stream wires satellite-to-satellite snapshot transfer for
// cross-node clone. The receiving satellite GETs the raw byte stream
// from a peer that hosts the snapshot locally and pipes it into the
// local provider's RecvSnapshot — upstream LINSTOR's equivalent of
// `zfs send | zfs recv` over the network.
//
// Wire shape:
//
//	GET /v1/satellite/snapshots/{rd}/{snap}/{vol}
//	→ 200 + Content-Type: application/octet-stream
//	   body = raw bytes from provider.SendSnapshot
//	→ 404 when this satellite doesn't host the snapshot
//	→ 501 when the resolved provider has no SnapshotShipper capability
//
// Security: cluster-internal traffic only. The daemonset binds to
// hostPort outside the DRBD range; production deployments should
// front this with a NetworkPolicy or mTLS. No auth on the wire yet.
package stream

// Port is the satellite-to-satellite stream port. Chosen outside the
// DRBD-9 default range (7000-7999) and outside any LINSTOR
// satellite port (3366/3367). The DaemonSet binds it on hostNetwork
// so the address is each node's host IP.
const Port = 9100

// PathPrefix is the URL prefix for the snapshot-stream endpoint. The
// satellite mux registers GET {PathPrefix}/{rd}/{snap}/{vol}.
const PathPrefix = "/v1/satellite/snapshots"
