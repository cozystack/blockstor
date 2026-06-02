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
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 378 (P3, bughunt round 10 — 2026-06-02): the per-key DELETE on
// node + RG properties returned 404 when the parent (node / RG) was
// already gone, in contrast to the sibling
// `DELETE /v1/controller/properties/{key...}` which has been
// idempotent on every input since Bug 142. The drift broke
// cozystack's node-evacuation playbook: `linstor n d <node>`
// followed by `linstor n dp <node> Foo` raced into a 404 on the
// second call once the node finally cleared.
//
// Both per-key drop-property endpoints now fold parent-NotFound into
// the same warn-band 200 envelope that the "key already absent on
// extant parent" branch uses. Mirrors the controller-prop sibling +
// the parent-delete handlers (`handleNodeDelete` / `handleRGDelete`)
// which both surface warnNodeNotFound / warnRGNotFound on the same
// input shape.

// TestBug378_NodePropDeleteMissingNodeIsIdempotent: drop-property on
// a node that does not exist returns 200, not 404.
func TestBug378_NodePropDeleteMissingNodeIsIdempotent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t,
		base+"/v1/nodes/ghost-node/properties/Aux/anything")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200 (parent-missing should be idempotent)",
			resp.StatusCode)
	}
}

// TestBug378_RGPropDeleteMissingRGIsIdempotent: drop-property on an
// RG that does not exist returns 200, not 404.
func TestBug378_RGPropDeleteMissingRGIsIdempotent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t,
		base+"/v1/resource-groups/ghost-rg/properties/DrbdOptions/auto-quorum")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200 (parent-missing should be idempotent)",
			resp.StatusCode)
	}
}
