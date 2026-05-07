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

// TestResourcesViewEmpty: empty list, never nil — golinstor iterates blindly.
func TestResourcesViewEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	got, err := c.Resources.GetResourceView(t.Context())
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestResourcesViewAcrossNodes: replicas on different nodes are all returned.
func TestResourcesViewAcrossNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-1", NodeName: "n2"},
		{Name: "pvc-2", NodeName: "n2"},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)

	got, err := c.Resources.GetResourceView(t.Context())
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if len(got) != 3 {
		t.Errorf("len: got %d, want 3", len(got))
	}
}

// TestResourcesViewWithoutStore: 503 when store is nil.
func TestResourcesViewWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
}
