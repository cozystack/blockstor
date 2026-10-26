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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func namespacesOnly() apiv1.GenericPropsModify {
	return apiv1.GenericPropsModify{DeleteNamespace: []string{"Aux"}}
}

func wantNamespaceGone(t *testing.T, props map[string]string, door string) {
	t.Helper()

	if _, ok := props["Aux/a"]; ok {
		t.Errorf("%s: a key under the deleted namespace survived: %v", door, props)
	}

	if props["Auxiliary"] != "keep" || props["Other/b"] != "keep" {
		t.Errorf("%s: a key outside the namespace went: %v", door, props)
	}
}

func seededProps() map[string]string {
	return map[string]string{"Aux/a": "1", "Auxiliary": "keep", "Other/b": "keep"}
}

// A body carrying only delete_namespaces is a props edit; answered 200 with
// nothing changed, it is the accept-and-drop the field exists to end.
func TestResourceModifyHonoursDeleteNamespaces(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "rd-ns"}); err != nil {
		t.Fatalf("seed the definition: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "rd-ns", NodeName: "node-a", Props: seededProps(),
	}); err != nil {
		t.Fatalf("seed the replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceModify{GenericPropsModify: namespacesOnly()})

	resp := httpPut(t, base+"/v1/resource-definitions/rd-ns/resources/node-a", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resource modify = %d, want 200", resp.StatusCode)
	}

	res, err := st.Resources().Get(t.Context(), "rd-ns", "node-a")
	if err != nil {
		t.Fatalf("read the replica: %v", err)
	}

	wantNamespaceGone(t, res.Props, "resource modify")
}

func TestNodeModifyHonoursANamespacesOnlyBody(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "node-ns", Type: "SATELLITE", Props: seededProps(),
	}); err != nil {
		t.Fatalf("seed the node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NodeModify{GenericPropsModify: namespacesOnly()})

	resp := httpPut(t, base+"/v1/nodes/node-ns", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("node modify = %d, want 200", resp.StatusCode)
	}

	node, err := st.Nodes().Get(t.Context(), "node-ns")
	if err != nil {
		t.Fatalf("read the node: %v", err)
	}

	wantNamespaceGone(t, node.Props, "node modify")
}

// The controller door patches a ControllerConfig through the API server; the
// merge it re-runs on every retry is checked directly.
func TestControllerPropsModifyHonoursDeleteNamespaces(t *testing.T) {
	t.Parallel()

	props := seededProps()
	modify := namespacesOnly()
	applyControllerPropsModify(props, &modify)

	wantNamespaceGone(t, props, "controller set-property")
}
