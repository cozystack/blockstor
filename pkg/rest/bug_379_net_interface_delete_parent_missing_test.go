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

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 379 (P3, bughunt round 11 — 2026-06-02): the per-NIC DELETE on
// `/v1/nodes/{node}/net-interfaces/{name}` returned a raw 404
// `patch NetInterfaces of Node "X": node "X": object not found` when
// the parent node was already gone — symmetric to the Bug 378 gap on
// per-key property delete. The Bug-hunt v0.1.3 Finding 9 fix already
// made "node present, NIC missing" idempotent
// (warnNetInterfaceNotFound + 200) but never extended that
// idempotency to the parent-missing branch, so a teardown script
// that runs `linstor n d <node>` followed by
// `linstor node interface delete <node> default` raced into a fatal
// 404 on the second call once the node finally cleared.
//
// Both DELETE no-ops now fold parent-NotFound into the same warn-band
// 200 envelope that handleNodePropDelete + handleNodeDelete use, so
// audit-log greppers can distinguish a real drop from an idempotent
// no-op via the warnNodeNotFound mask.

// TestBug379_NetInterfaceDeleteMissingNodeIsIdempotent: NIC delete on
// a node that does not exist returns 200 + warn-band envelope, not
// 404 with a raw `patch NetInterfaces of Node ... object not found`.
func TestBug379_NetInterfaceDeleteMissingNodeIsIdempotent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t,
		base+"/v1/nodes/ghost-node/net-interfaces/default")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200 (parent-missing should be idempotent); body=%s",
			resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(body, &rcs); err != nil {
		t.Fatalf("unmarshal envelope: %v; body=%s", err, body)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope length: got %d, want 1; body=%s", len(rcs), body)
	}

	// Mask must be the warn-band, not the success band — operators
	// grep audit logs on this to tell "no-op" apart from "I actually
	// dropped a NIC".
	if rcs[0].RetCode != maskWarn {
		t.Errorf("ret_code mask: got 0x%x, want warn-band 0x%x; full=%+v",
			rcs[0].RetCode, maskWarn, rcs[0])
	}

	// Message must name BOTH the missing parent node and the NIC the
	// caller asked us to drop — otherwise the operator can't tell
	// which retry-loop iteration logged the no-op.
	if !strings.Contains(rcs[0].Message, "ghost-node") {
		t.Errorf("message does not name parent node: %q", rcs[0].Message)
	}
	if !strings.Contains(rcs[0].Message, "default") {
		t.Errorf("message does not name NIC: %q", rcs[0].Message)
	}

	// ObjRefs must pin the Node — same shape every other Bug 378 /
	// Bug 66 / Bug 142 envelope uses, so audit-log filters built
	// around objRefNode catch the cascade.
	if rcs[0].ObjRefs[objRefNode] != "ghost-node" {
		t.Errorf("obj_refs[Node]: got %q, want %q",
			rcs[0].ObjRefs[objRefNode], "ghost-node")
	}
}

// TestBug379_NetInterfaceDeleteMissingNICOnExistingNode preserves the
// pre-existing Bug-hunt v0.1.3 Finding 9 envelope shape: when the
// node is present but the NIC name was never registered, the warn
// band is `warnNetInterfaceNotFound`, not the Bug 379
// `warnNodeNotFound`. Pinned here so a regression on either branch
// of the dual-warn-mask split is caught by CI.
func TestBug379_NetInterfaceDeleteMissingNICKeepsExistingShape(t *testing.T) {
	st := store.NewInMemory()

	err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "extant-node",
		Type: "SATELLITE",
		NetInterfaces: []apiv1.NetInterface{{
			Name:                    "default",
			Address:                 "10.0.0.1",
			SatellitePort:           3366,
			SatelliteEncryptionType: "PLAIN",
		}},
	})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		base+"/v1/nodes/extant-node/net-interfaces/never-registered")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200; body=%s",
			resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(body, &rcs); err != nil {
		t.Fatalf("unmarshal envelope: %v; body=%s", err, body)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope length: got %d, want 1; body=%s", len(rcs), body)
	}

	// Existing branch returns warnNetInterfaceNotFound, not maskWarn,
	// so the audit log can tell parent-missing apart from
	// nic-missing-on-extant-parent.
	if rcs[0].RetCode != warnNetInterfaceNotFound {
		t.Errorf("ret_code: got 0x%x, want warnNetInterfaceNotFound 0x%x",
			rcs[0].RetCode, warnNetInterfaceNotFound)
	}
}
