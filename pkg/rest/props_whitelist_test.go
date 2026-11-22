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

// typoKey is the key the alpha.14 report stored on a definition: a typo of
// AutoSnapshot/Keep under DrbdOptions/, which no DRBD section knows.
const typoKey = "DrbdOptions/AutoSnapshot/Keep"

// decodeInvalidPropRefusal asserts resp is LINSTOR's FAIL_INVLD_PROP
// refusal and returns its entries.
func decodeInvalidPropRefusal(t *testing.T, resp *http.Response) []apiv1.APICallRc {
	t.Helper()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc

	err := json.NewDecoder(resp.Body).Decode(&rcs)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope: got %d entries, want 1: %+v", len(rcs), rcs)
	}

	if rcs[0].RetCode != apiCallRcError|apiCallRcFailInvldProp {
		t.Errorf("ret_code: got %#x, want MASK_ERROR|FAIL_INVLD_PROP", rcs[0].RetCode)
	}

	return rcs
}

// seedWhitelistObjects creates one of every object a props door edits:
// nodes n1 and n2, pool sp1 on n1, group rg1 with volume group 0,
// definition rd1 with volume definition 0, and its replica on n1 with
// volume 0.
func seedWhitelistObjects(t *testing.T, st store.Store) {
	t.Helper()

	ctx := t.Context()

	for _, node := range []string{"n1", "n2"} {
		err := st.Nodes().Create(ctx, &apiv1.Node{Name: node, Type: "SATELLITE"})
		if err != nil {
			t.Fatalf("seed node: %v", err)
		}
	}

	err := st.StoragePools().Create(ctx, &apiv1.StoragePool{StoragePoolName: "sp1", NodeName: "n1", ProviderKind: "LVM_THIN"})
	if err != nil {
		t.Fatalf("seed storage pool: %v", err)
	}

	err = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg1", VolumeGroups: []apiv1.VolumeGroup{{VolumeNumber: 0}},
	})
	if err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	err = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd1"})
	if err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	err = st.VolumeDefinitions().Create(ctx, "rd1", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024})
	if err != nil {
		t.Fatalf("seed vd: %v", err)
	}

	err = st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd1", NodeName: "n1", Volumes: []apiv1.Volume{{VolumeNumber: 0}},
	})
	if err != nil {
		t.Fatalf("seed resource: %v", err)
	}
}

// TestPropsDoorsRefuseAnUnwhitelistedDrbdOption sends the alpha.14 typo to
// every door that stores properties. Each must answer LINSTOR's
// FAIL_INVLD_PROP refusal instead of storing a key the satellites would
// render into a .res drbdadm refuses.
func TestPropsDoorsRefuseAnUnwhitelistedDrbdOption(t *testing.T) {
	override := `{"override_props":{"` + typoKey + `":"2"}}`
	props := `{"` + typoKey + `":"2"}`

	doors := []struct {
		name, method, path, body string
	}{
		{"controller set-property", http.MethodPost, "/v1/controller/properties", override},
		{
			"node create", http.MethodPost, "/v1/nodes",
			`{"name":"n2","type":"SATELLITE","net_interfaces":[{"name":"default","address":"10.0.0.2"}],"props":` +
				props + `}`,
		},
		{"node modify", http.MethodPut, "/v1/nodes/n1", override},
		{"resource group create", http.MethodPost, "/v1/resource-groups", `{"name":"rg2","props":` + props + `}`},
		{
			"resource group create, volume group", http.MethodPost, "/v1/resource-groups",
			`{"name":"rg3","volume_groups":[{"volume_number":0,"props":` + props + `}]}`,
		},
		{"resource group modify", http.MethodPut, "/v1/resource-groups/rg1", override},
		{"volume group modify", http.MethodPut, "/v1/resource-groups/rg1/volume-groups/0", override},
		{
			"resource definition create", http.MethodPost, "/v1/resource-definitions",
			`{"resource_definition":{"name":"rd2","props":` + props + `}}`,
		},
		{"resource definition modify", http.MethodPut, "/v1/resource-definitions/rd1", override},
		{
			"volume definition create", http.MethodPost, "/v1/resource-definitions/rd1/volume-definitions",
			`{"volume_definition":{"volume_number":1,"size_kib":1048576,"props":` + props + `}}`,
		},
		{"volume definition modify", http.MethodPut, "/v1/resource-definitions/rd1/volume-definitions/0", override},
		{"resource modify", http.MethodPut, "/v1/resource-definitions/rd1/resources/n1", override},
		{"volume modify", http.MethodPut, "/v1/resource-definitions/rd1/resources/n1/volumes/0", override},
		{
			"resource create", http.MethodPost, "/v1/resource-definitions/rd1/resources",
			`[{"resource":{"node_name":"n2","props":` + props + `}}]`,
		},
		{
			"volume group create", http.MethodPost, "/v1/resource-groups/rg1/volume-groups",
			`{"volume_number":1,"props":` + props + `}`,
		},
		{
			"resource definition clone", http.MethodPost, "/v1/resource-definitions/rd1/clone",
			`{"name":"rd3","override_props":` + props + `}`,
		},
		{"node connection modify", http.MethodPut, "/v1/node-connections/n1/n2", override},
		{"storage pool modify", http.MethodPut, "/v1/nodes/n1/storage-pools/sp1", override},
		{"storage pool definition modify", http.MethodPut, "/v1/storage-pool-definitions/sp1", override},
	}

	for _, door := range doors {
		t.Run(door.name, func(t *testing.T) {
			st := store.NewInMemory()
			seedWhitelistObjects(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			var resp *http.Response
			if door.method == http.MethodPost {
				resp = httpPost(t, base+door.path, []byte(door.body))
			} else {
				resp = httpPut(t, base+door.path, []byte(door.body))
			}

			defer func() { _ = resp.Body.Close() }()

			rcs := decodeInvalidPropRefusal(t, resp)

			want := "Invalid property key: " + typoKey
			if door.name == "controller set-property" {
				want = "The key '" + typoKey + "' is not whitelisted"
			}

			if rcs[0].Message != want {
				t.Errorf("message: got %q, want %q", rcs[0].Message, want)
			}
		})
	}
}

// TestRDModifyStoresNothingWhenOneKeyIsRefused: LINSTOR refuses the whole
// request, so a valid key sent beside the refused one is not stored either.
func TestRDModifyStoresNothingWhenOneKeyIsRefused(t *testing.T) {
	st := store.NewInMemory()
	seedWhitelistObjects(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := `{"override_props":{"DrbdOptions/Net/protocol":"C","` + typoKey + `":"2"}}`
	resp := httpPut(t, base+"/v1/resource-definitions/rd1", []byte(body))

	defer func() { _ = resp.Body.Close() }()

	rcs := decodeInvalidPropRefusal(t, resp)
	if rcs[0].Cause != "The key '"+typoKey+"' is not whitelisted." {
		t.Errorf("cause: got %q", rcs[0].Cause)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "rd1")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if len(rd.Props) != 0 {
		t.Errorf("a refused request stored props: %v", rd.Props)
	}
}

// TestRDModifyRefusesAValueTheRuleRefuses is corner-case B8: LINSTOR
// checks a whitelisted DRBD option's value against its rule, and refuses
// quorum-minimum-redundancy=bogus with the rule in the details.
func TestRDModifyRefusesAValueTheRuleRefuses(t *testing.T) {
	st := store.NewInMemory()
	seedWhitelistObjects(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := `{"override_props":{"DrbdOptions/Resource/quorum-minimum-redundancy":"bogus"}}`
	resp := httpPut(t, base+"/v1/resource-definitions/rd1", []byte(body))

	defer func() { _ = resp.Body.Close() }()

	rcs := decodeInvalidPropRefusal(t, resp)

	if rcs[0].Message != "Invalid property(DrbdOptions/Resource/quorum-minimum-redundancy) value: bogus" {
		t.Errorf("message: got %q", rcs[0].Message)
	}

	if rcs[0].Details != "This value has to match (1 - 32) or (?:all|majority|off)." {
		t.Errorf("details: got %q", rcs[0].Details)
	}
}

// TestControllerRefusesAValueInItsOwnWording: the controller answers a
// value its rule refuses with CtrlConfApiCallHandler's message, not the
// one the other objects use.
func TestControllerRefusesAValueInItsOwnWording(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body := `{"override_props":{"DrbdOptions/Net/max-buffers":"8"}}`
	resp := httpPost(t, base+"/v1/controller/properties", []byte(body))

	defer func() { _ = resp.Body.Close() }()

	rcs := decodeInvalidPropRefusal(t, resp)

	if rcs[0].Message != "The value '8' is not valid." || rcs[0].Details != "This value has to match (32 - 131072)." {
		t.Errorf("got message %q details %q", rcs[0].Message, rcs[0].Details)
	}
}

// TestRDModifyStillDeletesAKeyStoredBeforeTheCheck: a definition written
// before the check may carry a key the check now refuses. Deleting it is
// how it is cleaned, by delete_props or by the empty-value set-property,
// so neither is judged.
func TestRDModifyStillDeletesAKeyStoredBeforeTheCheck(t *testing.T) {
	for _, body := range []string{
		`{"delete_props":["` + typoKey + `"]}`,
		`{"override_props":{"` + typoKey + `":""}}`,
	} {
		st := store.NewInMemory()

		err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
			Name: "rd1", Props: map[string]string{typoKey: "2"},
		})
		if err != nil {
			t.Fatalf("seed rd: %v", err)
		}

		base, stop := startServerWithStore(t, st)

		resp := httpPut(t, base+"/v1/resource-definitions/rd1", []byte(body))
		_ = resp.Body.Close()

		stop()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d, want 200", body, resp.StatusCode)
		}

		rd, err := st.ResourceDefinitions().Get(t.Context(), "rd1")
		if err != nil {
			t.Fatalf("get rd: %v", err)
		}

		if _, kept := rd.Props[typoKey]; kept {
			t.Errorf("%s: the key is still stored: %v", body, rd.Props)
		}
	}
}

// TestRDModifyAcceptsWhitelistedOptionsAndOtherNamespaces: what LINSTOR
// accepts still lands, and keys outside DrbdOptions/ are not judged.
func TestRDModifyAcceptsWhitelistedOptionsAndOtherNamespaces(t *testing.T) {
	st := store.NewInMemory()
	seedWhitelistObjects(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	want := map[string]string{
		"DrbdOptions/Resource/on-no-quorum":      "suspend-io",
		"DrbdOptions/Net/allow-two-primaries":    "yes",
		"DrbdOptions/auto-add-quorum-tiebreaker": "false",
		"Aux/anything/goes":                      "x",
		"AutoSnapshot/Keep":                      "2",
	}

	body, err := json.Marshal(apiv1.GenericPropsModify{OverrideProps: want})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/rd1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "rd1")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	for key, value := range want {
		if rd.Props[key] != value {
			t.Errorf("%s: got %q, want %q", key, rd.Props[key], value)
		}
	}
}

// TestPropsDoorsAcceptBlockstorsOwnDrbdOptions: blockstor reads a few
// DrbdOptions keys LINSTOR's whitelist does not list, and documents them as
// the way to set what they set. The tiebreaker switch on a definition or a
// group and the port and minor ranges on a node must still be stored, and
// only on the objects they are read from.
func TestPropsDoorsAcceptBlockstorsOwnDrbdOptions(t *testing.T) {
	st := store.NewInMemory()
	seedWhitelistObjects(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	tiebreaker := `{"override_props":{"DrbdOptions/AutoAddQuorumTiebreaker":"false"}}`
	ranges := `{"override_props":{"DrbdOptions/TcpPortRange":"21000-21999","DrbdOptions/MinorNrRange":"21000-29999"}}`

	for _, door := range []struct{ path, body string }{
		{"/v1/resource-definitions/rd1", tiebreaker},
		{"/v1/resource-groups/rg1", tiebreaker},
		{"/v1/nodes/n1", ranges},
	} {
		resp := httpPut(t, base+door.path, []byte(door.body))
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("PUT %s %s: status %d, want 200", door.path, door.body, resp.StatusCode)
		}
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "rd1")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	rg, err := st.ResourceGroups().Get(t.Context(), "rg1")
	if err != nil {
		t.Fatalf("get rg: %v", err)
	}

	node, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	for what, got := range map[string]string{
		"definition tiebreaker": rd.Props["DrbdOptions/AutoAddQuorumTiebreaker"],
		"group tiebreaker":      rg.Props["DrbdOptions/AutoAddQuorumTiebreaker"],
		"node port range":       node.Props["DrbdOptions/TcpPortRange"],
		"node minor range":      node.Props["DrbdOptions/MinorNrRange"],
	} {
		if got == "" {
			t.Errorf("%s was not stored", what)
		}
	}
}

// TestPropsDoorsRefuseBlockstorsOwnKeysElsewhere: an own key is judged like
// a LINSTOR one, on the wrong object and with a value its reader cannot
// parse.
func TestPropsDoorsRefuseBlockstorsOwnKeysElsewhere(t *testing.T) {
	st := store.NewInMemory()
	seedWhitelistObjects(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, door := range []struct{ path, body string }{
		{"/v1/resource-definitions/rd1", `{"override_props":{"DrbdOptions/TcpPortRange":"21000-21999"}}`},
		{"/v1/nodes/n1", `{"override_props":{"DrbdOptions/AutoAddQuorumTiebreaker":"false"}}`},
		{"/v1/nodes/n1", `{"override_props":{"DrbdOptions/TcpPortRange":"bogus"}}`},
		{"/v1/resource-definitions/rd1", `{"override_props":{"DrbdOptions/AutoAddQuorumTiebreaker":"maybe"}}`},
	} {
		resp := httpPut(t, base+door.path, []byte(door.body))
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("PUT %s %s: status %d, want 400", door.path, door.body, resp.StatusCode)
		}
	}
}

// TestPropsDoorsStoreBooleansAsLinstorDoes: LINSTOR accepts Yes and TRUE
// but stores yes and true (WhitelistProps.normalize). Stored as given, Yes
// is left out of the .res by the satellite, which takes what drbdadm takes,
// and No on a true/false key reads as unset to strconv.ParseBool.
func TestPropsDoorsStoreBooleansAsLinstorDoes(t *testing.T) {
	st := store.NewInMemory()
	seedWhitelistObjects(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := `{"override_props":{"DrbdOptions/Net/load-balance-paths":"Yes",` +
		`"DrbdOptions/auto-add-quorum-tiebreaker":"No","DrbdOptions/AutoAddQuorumTiebreaker":"YES"}}`

	resp := httpPut(t, base+"/v1/resource-definitions/rd1", []byte(body))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}

	nodeBody := `{"override_props":{"DrbdOptions/AutoEvictAllowEviction":"No"}}`

	resp = httpPut(t, base+"/v1/nodes/n1", []byte(nodeBody))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("node: status %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "rd1")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	node, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	for key, want := range map[string]string{
		"DrbdOptions/Net/load-balance-paths":     "yes",
		"DrbdOptions/auto-add-quorum-tiebreaker": "false",
		"DrbdOptions/AutoAddQuorumTiebreaker":    "true",
	} {
		if rd.Props[key] != want {
			t.Errorf("%s: stored %q, want %q", key, rd.Props[key], want)
		}
	}

	if got := node.Props["DrbdOptions/AutoEvictAllowEviction"]; got != "false" {
		t.Errorf("node AutoEvictAllowEviction: stored %q, want false", got)
	}
}
