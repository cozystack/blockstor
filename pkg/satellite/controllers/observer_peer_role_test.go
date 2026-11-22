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

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

func connectionFrame(fields map[string]string) drbd.Event {
	all := map[string]string{"name": "pvc-1", "peer-node-id": "1", "conn-name": "n2"}
	for k, v := range fields {
		all[k] = v
	}

	return drbd.Event{Action: eventActionChange, Kind: eventKindConnection, Fields: all}
}

// The observer follows each peer's role: a day0 GI moves only once some
// replica is Primary, so the resync reads it only then (giMayMove).
func TestObserverFollowsThePeerRole(t *testing.T) {
	o := &ObserverRunnable{}

	ingest := func(fields map[string]string) {
		t.Helper()

		obs, ok := translateEvent(connectionFrame(fields))
		if !ok {
			t.Fatalf("frame %v dropped", fields)
		}

		o.mergeConnections(&obs)
	}

	peerRole := func() (string, bool) {
		c, ok := o.connCache["pvc-1"]["n2"]

		return c.PeerRole, ok
	}

	// A role alone does not make a peer the observer has not seen.
	ingest(map[string]string{"role": drbdRolePrimary})

	if _, ok := peerRole(); ok {
		t.Fatal("a role-only frame created a peer")
	}

	ingest(map[string]string{"connection": "Connected", "role": "Secondary"})

	if role, _ := peerRole(); role != "Secondary" {
		t.Fatalf("peer role %q, want Secondary", role)
	}

	if o.anyPrimary("pvc-1") {
		t.Fatal("a Secondary peer counted as Primary")
	}

	// A change frame may carry the role alone.
	ingest(map[string]string{"role": drbdRolePrimary})

	if !o.anyPrimary("pvc-1") {
		t.Fatal("a promoted peer did not count as Primary")
	}

	if c := o.connCache["pvc-1"]["n2"]; !c.Connected || c.Message != "Connected" {
		t.Fatalf("a role-only frame changed the connection state: %+v", c)
	}

	// A peer out of reach has no role the observer knows.
	ingest(map[string]string{"connection": "Connecting"})

	if o.anyPrimary("pvc-1") {
		t.Fatal("a disconnected peer still counted as Primary")
	}
}
