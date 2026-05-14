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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Scenario 1.W02 — Aux property set / unset via `--aux` shorthand.
//
// CLI surface: `linstor node set-property --aux <node> rack-id rack-7`.
// The `--aux` flag is a CLI-side shorthand that auto-prefixes the
// LINSTOR property namespace `Aux/` to the operator-supplied key, so
// `--aux foo bar` and the long form `Aux/foo bar` MUST land on the
// same wire shape and the same store path. Empty value triggers a
// delete (golinstor turns "no value" into `delete_props`).
//
// These tests pin the REST contract at /v1/nodes/{node} (PUT) which
// `NodeService.Modify` calls. They are CLI-agnostic — they assert the
// wire shape the CLI must produce, regardless of which client (python
// CLI, golinstor, raw curl) the operator drives.

// auxKeyForRest mirrors the CLI-side `--aux` prefix rule. It is the
// same rule placer.auxKey enforces inside the autoplacer; lifting it
// into the test keeps the contract visible without dragging the
// placer package into pkg/rest.
func auxKeyForRest(k string) string {
	const prefix = "Aux/"
	if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
		return k
	}

	return prefix + k
}

// TestAuxSetPropertyWireShape: `--aux rack-id rack-7` and the long
// form `Aux/rack-id rack-7` resolve to the same REST body and the same
// stored Props entry. This is the core wave2 1.W02 invariant — the CLI
// shorthand is a pre-handler convention, the controller never sees a
// "this is an aux flag" hint of its own.
func TestAuxSetPropertyWireShape(t *testing.T) {
	t.Parallel()

	// Shorthand: CLI rewrites `--aux foo bar` to `Aux/foo=bar`.
	shorthandBody, err := json.Marshal(apiv1.NodeModify{
		GenericPropsModify: apiv1.GenericPropsModify{
			OverrideProps: map[string]string{
				auxKeyForRest("rack-id"): "rack-7",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shorthand: %v", err)
	}

	// Long form: operator typed `Aux/rack-id rack-7` explicitly.
	longBody, err := json.Marshal(apiv1.NodeModify{
		GenericPropsModify: apiv1.GenericPropsModify{
			OverrideProps: map[string]string{
				"Aux/rack-id": "rack-7",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal long: %v", err)
	}

	// Byte-identical request bodies → same handler path → same store
	// write. This locks the CLI rewrite at the wire boundary.
	if string(shorthandBody) != string(longBody) {
		t.Fatalf("aux shorthand drift:\nshorthand: %s\nlong:      %s",
			shorthandBody, longBody)
	}

	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/nodes/n1", shorthandBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["Aux/rack-id"] != "rack-7" {
		t.Errorf("Props after PUT: got %v, want Aux/rack-id=rack-7", got.Props)
	}
}

// TestAuxUnsetPropertyDeletesKey: `set-property --aux <node> rack-id`
// with no value tells the CLI to delete. golinstor emits a
// `delete_props: ["Aux/rack-id"]` body; the REST handler MUST remove
// the key from Node.Props. A subsequent list-properties must NOT show
// the key.
func TestAuxUnsetPropertyDeletesKey(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"Aux/rack-id": "rack-7",
			"Aux/zone":    "us-east-1a",
			"NotAuxKeep":  "stay",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// "--aux rack-id" with no value → delete_props on the wire.
	body, err := json.Marshal(apiv1.NodeModify{
		GenericPropsModify: apiv1.GenericPropsModify{
			DeleteProps: []string{auxKeyForRest("rack-id")},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/nodes/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["Aux/rack-id"]; present {
		t.Errorf("Aux/rack-id still present after delete: %v", got.Props)
	}

	// Sibling Aux keys and unrelated keys must survive — the delete is
	// scoped to the named key only.
	if got.Props["Aux/zone"] != "us-east-1a" {
		t.Errorf("Aux/zone clobbered: got %v", got.Props)
	}

	if got.Props["NotAuxKeep"] != "stay" {
		t.Errorf("NotAuxKeep clobbered: got %v", got.Props)
	}
}

// TestAuxSetAndDeleteInOneCall: the LINSTOR `GenericPropsModify`
// envelope allows OverrideProps + DeleteProps in the same PUT — the
// CLI uses this when a script calls set then unset back-to-back and
// the bridge collapses them. Set must apply before delete, matching
// `mergeControllerProps` precedence — but since the keys are
// independent here we just check both effects landed.
func TestAuxSetAndDeleteInOneCall(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"Aux/old-rack": "rack-3",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.NodeModify{
		GenericPropsModify: apiv1.GenericPropsModify{
			OverrideProps: map[string]string{
				auxKeyForRest("new-rack"): "rack-9",
			},
			DeleteProps: []string{auxKeyForRest("old-rack")},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/nodes/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["Aux/new-rack"] != "rack-9" {
		t.Errorf("Aux/new-rack: got %v, want rack-9", got.Props)
	}

	if _, present := got.Props["Aux/old-rack"]; present {
		t.Errorf("Aux/old-rack still present after delete: %v", got.Props)
	}
}

// TestAuxKeyForRestPrefixes locks the CLI prefix rule the wire-shape
// tests depend on. Bare key gets `Aux/` prepended; an already-prefixed
// key passes through unchanged so `linstor n sp --aux Aux/foo bar`
// does not become `Aux/Aux/foo`.
func TestAuxKeyForRestPrefixes(t *testing.T) {
	t.Parallel()

	cases := []struct{ in, want string }{
		{"rack-id", "Aux/rack-id"},
		{"zone", "Aux/zone"},
		{"Aux/rack-id", "Aux/rack-id"},
		{"Aux/", "Aux/"},
	}

	for _, c := range cases {
		if got := auxKeyForRest(c.in); got != c.want {
			t.Errorf("auxKeyForRest(%q): got %q, want %q", c.in, got, c.want)
		}
	}
}
