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

// Bug 86: `PUT /v1/resource-definitions/{rd}/resources/{node}` is the
// per-Resource property-modify route python-linstor's
// `linstor r set-property <node> <rd> <key> <val>` calls. Without it
// the CLI hits 404. These tests pin the merge / delete / round-trip
// contract on Resource.Spec.Props — the dispatcher's effective-props
// chain (Controller→RG→RD→Resource) folds the per-Resource rung in at
// the highest precedence, so a value written via this route MUST land
// on the stored Resource.

// TestResourceSetPropertyMerges pins the OverrideProps half: a single
// key on an existing Resource is merged into Spec.Props without
// touching other keys.
func TestResourceSetPropertyMerges(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Props: map[string]string{
			"keep-me": "stay",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"DrbdOptions/Net/ping-timeout": "500",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["DrbdOptions/Net/ping-timeout"] != "500" {
		t.Errorf("DrbdOptions/Net/ping-timeout: got %v, want 500", got.Props)
	}

	if got.Props["keep-me"] != "stay" {
		t.Errorf("keep-me clobbered: got %v", got.Props)
	}
}

// TestResourceSetPropertyDeletes pins the DeleteProps half: a key
// listed in DeleteProps is removed; sibling keys survive.
func TestResourceSetPropertyDeletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Props: map[string]string{
			"DrbdOptions/Net/ping-timeout": "500",
			"keep-me":                      "stay",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		DeleteProps: []string{"DrbdOptions/Net/ping-timeout"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["DrbdOptions/Net/ping-timeout"]; present {
		t.Errorf("DrbdOptions/Net/ping-timeout still present after delete: %v", got.Props)
	}

	if got.Props["keep-me"] != "stay" {
		t.Errorf("keep-me clobbered: got %v", got.Props)
	}
}

// TestResourceSetPropertyMissingResource: PUT against an unknown
// (rd, node) replica returns 404 — the python CLI surfaces this as
// "Resource X on node Y not found", an operator-actionable error.
func TestResourceSetPropertyMissingResource(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/ghost/resources/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("PUT status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceSetPropertyNilPropsInitialised: a Resource seeded with
// a nil Props map MUST get a fresh non-nil map allocated by the
// handler when OverrideProps is non-empty. Without this guard the
// `maps.Copy` no-ops onto a nil destination and the prop never lands.
func TestResourceSetPropertyNilPropsInitialised(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"DrbdOptions/Net/ping-timeout": "500",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["DrbdOptions/Net/ping-timeout"] != "500" {
		t.Errorf("DrbdOptions/Net/ping-timeout never landed (nil-map guard): %v", got.Props)
	}
}

// TestResourceSetPropertySetAndDeleteInOneCall mirrors the
// GenericPropsModify contract used elsewhere (controller / node /
// RD modify): a single PUT may carry both halves. The CLI emits this
// when a script collapses a set+unset pair on the same Resource.
func TestResourceSetPropertySetAndDeleteInOneCall(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Props: map[string]string{
			"old-key": "drop",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"new-key": "set",
		},
		DeleteProps: []string{"old-key"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["new-key"] != "set" {
		t.Errorf("new-key: got %v, want set", got.Props)
	}

	if _, present := got.Props["old-key"]; present {
		t.Errorf("old-key still present: got %v", got.Props)
	}
}

// TestResourceSetPropertyMalformedBody: a body that isn't valid JSON
// returns 400 — golinstor surfaces this as a fatal error rather than
// silently no-opping the modify.
func TestResourceSetPropertyMalformedBody(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1",
		[]byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT status: got %d, want 400", resp.StatusCode)
	}
}

// TestResourceSetPropertyReturnsApiCallRc pins the success envelope:
// 200 + `[]ApiCallRc` with the INFO mask bit set and a message that
// names the resource + node. linstor CLI surfaces this in the
// "Successfully ..." line; an empty body would print nothing.
func TestResourceSetPropertyReturnsApiCallRc(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("len(rcs): got %d, want 1", len(rcs))
	}

	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("RetCode = %x, want maskInfo bit set", rcs[0].RetCode)
	}
}
