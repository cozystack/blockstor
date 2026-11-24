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
	"slices"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func TestMatchPropsFollowsLinstor(t *testing.T) {
	props := map[string]string{
		"Aux/topology/zone":                    "zone-a",
		"DrbdOptions/Net/allow-two-primaries":  "yes",
		"DrbdOptions/Disk/disk-flushes":        "no",
		"StorDriver/StorPoolName":              "pool.with.dots",
		"Aux/topology/kubernetes.io/hostname":  "w1",
		"Aux/csi-provisioning-completed-by":    "linstor-csi",
		"DrbdOptions/Resource/on-no-quorum":    "io-error",
		"DrbdOptions/Resource/quorum":          "majority",
		"DrbdOptions/Net/protocol":             "C",
		"DrbdOptions/Net/verify-alg":           "crc32c",
		"DrbdOptions/PeerDevice/c-plan-ahead":  "0",
		"DrbdOptions/PeerDevice/c-max-rate":    "4G",
		"DrbdOptions/Handlers/fence-peer":      "",
		"FileSystem/Type":                      "ext4",
		"FileSystem/MkfsParams":                "-E nodiscard,lazy_itable_init=0",
		"Internal/Drbd/LiveMigrate/SourceNode": "w1",
	}

	for _, tc := range []struct {
		filters []string
		want    bool
	}{
		{nil, true},
		{[]string{"Aux/topology/zone=zone-a"}, true},
		{[]string{"Aux/topology/zone=zone-b"}, false},
		{[]string{"Aux/topology/zone"}, true},
		{[]string{"Aux/topology/region"}, false},
		{[]string{"aux/topology/zone=zone-a"}, false},
		{[]string{"Aux/topology/zone=ZONE-A"}, false},
		{[]string{"Aux/.*"}, true},
		{[]string{"DrbdOptions/.*=yes"}, true},
		{[]string{"DrbdOptions/Disk/.*=yes"}, false},
		{[]string{"Aux/topology/zone=zone-.*"}, true},
		{[]string{"Aux/topology/zone=zone-"}, false},
		{[]string{"StorDriver/StorPoolName=pool.with.dots"}, true},
		{[]string{"StorDriver/StorPoolName=poolXwithXdots"}, false},
		{[]string{"FileSystem/MkfsParams=-E nodiscard,lazy_itable_init=0"}, true},
		{[]string{"Aux/topology/zone=zone-a", "FileSystem/Type=ext4"}, true},
		{[]string{"Aux/topology/zone=zone-a", "FileSystem/Type=xfs"}, false},
		{[]string{"Aux/[unclosed"}, false},
	} {
		if got := matchProps(tc.filters, props); got != tc.want {
			t.Errorf("matchProps(%q) = %v, want %v", tc.filters, got, tc.want)
		}
	}
}

func TestNameFilterIsCaseInsensitiveRegex(t *testing.T) {
	for _, tc := range []struct {
		filter, name string
		want         bool
	}{
		{"pvc-1", "pvc-1", true},
		{"PVC-1", "pvc-1", true},
		{"pvc-1", "pvc-10", false},
		{"pvc-.*", "pvc-10", true},
		{"PVC-.*", "pvc-10", true},
		{"pvc-[0-9]", "pvc-10", false},
		{"pvc-[0-9]+", "pvc-10", true},
		{"w1|w2", "w2", true},
		{"w1|w2", "w3", false},
		{"a.b", "a.b", true},
		{"a.b", "axb", false},
	} {
		if got := nameMatches(tc.filter, tc.name); got != tc.want {
			t.Errorf("nameMatches(%q, %q) = %v, want %v", tc.filter, tc.name, got, tc.want)
		}
	}
}

// TestNodesListHonoursNameAndPropsFilters drives the two node reads
// linstor-csi makes for topology through golinstor: the nodes a volume is
// deployed on, by name, and the nodes of a topology segment, by prop.
// Answering either with every node reports a volume accessible on nodes
// that do not hold it and stretches a segment over the whole cluster.
func TestNodesListHonoursNameAndPropsFilters(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []struct{ name, zone string }{{"w1", "zone-a"}, {"w2", "zone-a"}, {"w3", "zone-b"}} {
		err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  n.name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/topology/zone": n.zone},
		})
		if err != nil {
			t.Fatalf("seed %s: %v", n.name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)

	names := func(opts *lapi.ListOpts) []string {
		t.Helper()

		nodes, err := c.Nodes.GetAll(ctx, opts)
		if err != nil {
			t.Fatalf("GetAll(%+v): %v", opts, err)
		}

		out := make([]string, 0, len(nodes))
		for i := range nodes {
			out = append(out, nodes[i].Name)
		}

		slices.Sort(out)

		return out
	}

	for _, tc := range []struct {
		opts *lapi.ListOpts
		want []string
	}{
		{&lapi.ListOpts{}, []string{"w1", "w2", "w3"}},
		{&lapi.ListOpts{Node: []string{"w1", "w3"}}, []string{"w1", "w3"}},
		{&lapi.ListOpts{Node: []string{"W2"}}, []string{"w2"}},
		{&lapi.ListOpts{Prop: []string{"Aux/topology/zone=zone-a"}}, []string{"w1", "w2"}},
		{&lapi.ListOpts{Node: []string{"w1"}, Prop: []string{"Aux/topology/zone=zone-b"}}, []string{}},
		{&lapi.ListOpts{Node: []string{"w[23]"}}, []string{"w2", "w3"}},
	} {
		if got := names(tc.opts); !slices.Equal(got, tc.want) {
			t.Errorf("GetAll(%+v) = %v, want %v", tc.opts, got, tc.want)
		}
	}
}

// TestListsHonourPropsFilter covers the other list endpoints that take a
// `props` filter: resource definitions, resource groups, the storage pool
// view and a node's storage pools (the last one since REST 1.28).
func TestListsHonourPropsFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seedOnlineNodeWithPool(t, st, "w1")

	err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "fast",
		NodeName:        "w1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"Aux/tier": "fast"},
	})
	if err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	for _, rd := range []apiv1.ResourceDefinition{
		{Name: "rd-a", Props: map[string]string{"Aux/app": "db"}},
		{Name: "rd-b", Props: map[string]string{"Aux/app": "web"}},
	} {
		if err := st.ResourceDefinitions().Create(ctx, &rd); err != nil {
			t.Fatalf("seed %s: %v", rd.Name, err)
		}
	}

	for _, rg := range []apiv1.ResourceGroup{
		{Name: "rg-a", Props: map[string]string{"Aux/app": "db"}},
		{Name: "rg-b"},
	} {
		if err := st.ResourceGroups().Create(ctx, &rg); err != nil {
			t.Fatalf("seed %s: %v", rg.Name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	listNames := func(path, field string) []string {
		t.Helper()

		resp := httpGet(t, base+path)
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: %d", path, resp.StatusCode)
		}

		var rows []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}

		out := make([]string, 0, len(rows))
		for _, row := range rows {
			name, _ := row[field].(string)
			out = append(out, name)
		}

		slices.Sort(out)

		return out
	}

	for _, tc := range []struct {
		path, field string
		want        []string
	}{
		{"/v1/resource-definitions?props=Aux/app=db", "name", []string{"rd-a"}},
		{"/v1/resource-definitions?props=Aux/app", "name", []string{"rd-a", "rd-b"}},
		{"/v1/resource-groups?props=Aux/app=db", "name", []string{"rg-a"}},
		{"/v1/view/storage-pools?props=Aux/tier=fast", "storage_pool_name", []string{"fast"}},
		{"/v1/nodes/w1/storage-pools?props=Aux/tier=.*", "storage_pool_name", []string{"fast"}},
	} {
		if got := listNames(tc.path, tc.field); !slices.Equal(got, tc.want) {
			t.Errorf("GET %s = %v, want %v", tc.path, got, tc.want)
		}
	}
}
