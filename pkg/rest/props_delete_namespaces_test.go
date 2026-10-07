// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The modify body declares delete_namespaces and the merge dropped it, so a
// modify carrying it answered 200 and changed nothing.
func TestRDModifyHonoursDeleteNamespaces(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd-ns",
		Props: map[string]string{
			"DrbdOptions":              "top",
			"DrbdOptions/Net/protocol": "C",
			"DrbdOptionsOther":         "keep-me",
		},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{"delete_namespaces": []string{"DrbdOptions"}})

	resp := httpPut(t, base+"/v1/resource-definitions/rd-ns", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rd-ns")
	if err != nil {
		t.Fatalf("read the RD back: %v", err)
	}

	for _, key := range []string{"DrbdOptions", "DrbdOptions/Net/protocol"} {
		if _, present := got.Props[key]; present {
			t.Errorf("prop %q survived delete_namespaces", key)
		}
	}

	// The neighbouring key that merely shares a prefix is not in the
	// namespace and must stay.
	if got.Props["DrbdOptionsOther"] != "keep-me" {
		t.Errorf("DrbdOptionsOther = %q, want keep-me", got.Props["DrbdOptionsOther"])
	}
}

// The resource-group modify declares delete_namespaces and merged only the
// other two halves of the envelope.
func TestRGModifyHonoursDeleteNamespaces(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-ns",
		Props: map[string]string{
			"DrbdOptions":              "top",
			"DrbdOptions/Net/protocol": "C",
			"DrbdOptionsOther":         "keep-me",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{"delete_namespaces": []string{"DrbdOptions"}})

	resp := httpPut(t, base+"/v1/resource-groups/rg-ns", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-ns")
	if err != nil {
		t.Fatalf("read the RG back: %v", err)
	}

	for _, key := range []string{"DrbdOptions", "DrbdOptions/Net/protocol"} {
		if _, present := got.Props[key]; present {
			t.Errorf("prop %q survived delete_namespaces", key)
		}
	}

	if got.Props["DrbdOptionsOther"] != "keep-me" {
		t.Errorf("DrbdOptionsOther = %q, want keep-me", got.Props["DrbdOptionsOther"])
	}
}
