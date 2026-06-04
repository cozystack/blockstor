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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceConnectionDrbdPeerOptionsRouteAbsent pins corner-case C4
// as a documented delta (docs/cli-parity-known-deltas.md #56): the
// connection-scoped `resource-connection drbd-peer-options` surface
// (PUT /v1/resource-definitions/{rd}/resource-connections/{a}/{b}/
// drbd-peer-options) is NOT wired on main — only the `/paths`
// sub-surface exists. A PUT to the peer-options path therefore does
// not reach a handler. This test is the canary: if a future change
// wires connection-scoped peer-options, this expectation flips and the
// author must move row #56 out of the accept-list and add a real
// round-trip + render test.
func TestResourceConnectionDrbdPeerOptionsRouteAbsent(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"override_props":{"DrbdOptions/Net/max-buffers":"8192"}}`)
	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/drbd-peer-options",
		body)
	defer func() { _ = resp.Body.Close() }()

	// No handler is registered for the bare peer-options path, so the
	// mux falls through. We accept any non-2xx (404 / 405) — the point
	// is that the option is NOT silently persisted as if it took effect.
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Fatalf("C4 delta drift: resource-connection drbd-peer-options PUT returned %d "+
			"(connection-scoped peer-options now appear wired — update known-deltas #56 "+
			"and add a real round-trip/render test)", resp.StatusCode)
	}
}

// TestVolumeDefinitionAcceptsNetOptionPermissively pins corner-case C6
// as a documented delta (#58): blockstor does NOT enforce DRBD option
// classes per object level. A `net{}`-class option (`DrbdOptions/Net/
// protocol`) set on a volume-definition — which upstream LINSTOR
// rejects with a typed FAIL_INVLD_PROP because a VD only accepts
// disk/peer-device classes — is accepted permissively and stored.
//
// When the per-object-class validator lands, this expectation flips to
// a 400 + typed envelope and row #58 leaves the accept-list.
func TestVolumeDefinitionAcceptsNetOptionPermissively(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-c6"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-c6",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// `vd drbd-options --protocol C` routes through the VD PUT with an
	// override_props envelope. protocol is a net{} option, NOT valid on
	// a volume-definition.
	body := []byte(`{"override_props":{"DrbdOptions/Net/protocol":"C"}}`)
	resp := httpPut(t, base+"/v1/resource-definitions/pvc-c6/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("C6 delta drift: VD net-option PUT returned %d, expected the permissive 200 "+
			"(if option-class validation now rejects it, remove known-deltas #58 and assert the "+
			"typed FAIL_INVLD_PROP envelope instead)", resp.StatusCode)
	}

	// The permissive accept must actually persist the prop (the delta is
	// "accepted + stored", not "accepted + dropped").
	vd, err := st.VolumeDefinitions().Get(ctx, "pvc-c6", 0)
	if err != nil {
		t.Fatalf("get VD: %v", err)
	}

	if vd.Props["DrbdOptions/Net/protocol"] != "C" {
		t.Fatalf("C6: expected the net option stored permissively on the VD, got props=%v", vd.Props)
	}
}
