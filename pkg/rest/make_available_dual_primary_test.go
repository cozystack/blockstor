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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// liveMigrationFixture is a definition with a diskful replica on node-a,
// the node the VM runs on, and a diskful one on node-c; node-b is the
// migration target and has none. The group runs protocol A, so a
// migration has to override it.
func liveMigrationFixture(t *testing.T, layers []string) (store.Store, string) {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, node := range []string{"node-a", "node-b", "node-c"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-async",
		Props: map[string]string{drbdProtocolProp: "A"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "vm-disk",
		ResourceGroupName: "rg-async",
		LayerStack:        layers,
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{"node-a", "node-c"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "vm-disk", NodeName: node}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}
	}

	setInUse(t, st, "node-a", true)

	base, stop := startServerWithStore(t, st)
	t.Cleanup(stop)

	return st, base
}

func setInUse(t *testing.T, st store.Store, node string, using bool) {
	t.Helper()

	if err := st.Resources().SetState(t.Context(), "vm-disk", node, apiv1.ResourceState{InUse: &using}, nil); err != nil {
		t.Fatalf("set in_use on %s: %v", node, err)
	}
}

func postRcs(t *testing.T, url, body string) (int, []apiv1.APICallRc) {
	t.Helper()

	resp := httpPost(t, url, []byte(body))
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(raw, &rcs); err != nil {
		t.Fatalf("decode %s answer %q: %v", url, raw, err)
	}

	return resp.StatusCode, rcs
}

func rdProps(t *testing.T, st store.Store) map[string]string {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), "vm-disk")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	return rd.Props
}

// The sequence linstor-csi v1.13 drives for a KubeVirt live migration of
// an RWX block volume: attach on the target with auto_manage_dual_primary
// while the source still runs, detach the source, later detach the
// target. The connection between source and target allows two Primaries
// and runs protocol C only between the attach and the source detach.
func TestLiveMigrationMakeAvailableUnmakeAvailable(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)
	url := base + "/v1/resource-definitions/vm-disk/resources/"
	conn := func(key string) string { return apiv1.ResourceConnectionPropKey("node-a", "node-b", key) }

	// Attach on the target: a diskless replica on node-b, and the a-b
	// connection opened to two Primaries over protocol C.
	status, rcs := postRcs(t, url+"node-b/make-available", `{"diskful":false,"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available on the target: %d %+v", status, rcs)
	}

	target, err := st.Resources().Get(t.Context(), "vm-disk", "node-b")
	if err != nil || !drbdDiskless(&target) {
		t.Fatalf("target replica: %+v, %v; want a diskless one", target, err)
	}

	props := rdProps(t, st)
	for key, want := range map[string]string{
		conn(allowTwoPrimariesProp):  "yes",
		conn(drbdProtocolProp):       "C",
		liveMigrateSourceProp:        "node-a",
		liveMigrateTargetProp:        "node-b",
		liveMigrateProtocolSetOnProp: protocolSetOnConnection,
	} {
		if props[key] != want {
			t.Errorf("after attach %s = %q, want %q (props %v)", key, props[key], want, props)
		}
	}

	if _, ok := props[allowTwoPrimariesProp]; ok {
		t.Errorf("allow-two-primaries landed on the definition, every connection would allow it: %v", props)
	}

	// The target opens the device: both ends in use. A retried attach
	// repeats the marked migration instead of refusing it.
	setInUse(t, st, "node-b", true)

	status, rcs = postRcs(t, url+"node-b/make-available", `{"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("repeated make-available: %d %+v", status, rcs)
	}

	// Detaching the source while it still holds the device is refused
	// and changes nothing.
	status, rcs = postRcs(t, url+"node-a/unmake-available", ``)
	if status == http.StatusOK || len(rcs) == 0 || rcs[0].RetCode != apiCallRcError|apiCallRcFailInUse {
		t.Fatalf("unmake-available on an in-use source: %d %+v, want FAIL_IN_USE", status, rcs)
	}

	if rdProps(t, st)[conn(allowTwoPrimariesProp)] != "yes" {
		t.Fatal("a refused unmake-available reverted allow-two-primaries")
	}

	// The source lets go: the migration is reverted, the diskful
	// replica on node-a stays.
	setInUse(t, st, "node-a", false)

	status, rcs = postRcs(t, url+"node-a/unmake-available", ``)
	if status != http.StatusOK {
		t.Fatalf("unmake-available on the source: %d %+v", status, rcs)
	}

	props = rdProps(t, st)
	for key := range props {
		if key == conn(allowTwoPrimariesProp) || key == conn(drbdProtocolProp) || propInNamespace(key, liveMigrateNamespace) {
			t.Errorf("after the source detach %s is still set: %v", key, props)
		}
	}

	if _, err := st.Resources().Get(t.Context(), "vm-disk", "node-a"); err != nil {
		t.Fatalf("the diskful source replica was removed: %v", err)
	}

	// Later the VM stops on node-b. Its diskless attachment is the
	// third voter of the two diskful replicas, so it stays as their
	// tiebreaker rather than leaving them without one.
	setInUse(t, st, "node-b", false)

	status, rcs = postRcs(t, url+"node-b/unmake-available", ``)
	if status != http.StatusOK {
		t.Fatalf("unmake-available on the target: %d %+v", status, rcs)
	}

	kept, err := st.Resources().Get(t.Context(), "vm-disk", "node-b")
	if err != nil || !slices.Contains(kept.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Fatalf("the attachment on node-b: %+v, %v; want it kept as the tiebreaker", kept, err)
	}
}

// A diskless attachment that no quorum needs goes on unmake-available:
// here the auto-tiebreaker is off for the definition.
func TestUnmakeAvailableDeletesAnAttachmentNoQuorumNeeds(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)
	setInUse(t, st, "node-a", false)
	createDisklessReplica(t, st, "node-b")

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "vm-disk", func(rd *apiv1.ResourceDefinition) error {
		rd.Props = map[string]string{autoTiebreakerProp: "false"}

		return nil
	})
	if err != nil {
		t.Fatalf("turn the auto-tiebreaker off: %v", err)
	}

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/unmake-available", ``)
	if status != http.StatusOK {
		t.Fatalf("unmake-available: %d %+v", status, rcs)
	}

	if _, err = st.Resources().Get(t.Context(), "vm-disk", "node-b"); err == nil {
		t.Fatal("the diskless attachment on node-b is still there")
	}
}

// A plain attach with the flag set (the driver sets it on every RWX block
// attach) prepares nothing when the volume is in use nowhere.
func TestMakeAvailableDualPrimaryWithoutMigration(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)
	setInUse(t, st, "node-a", false)

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/make-available",
		`{"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available: %d %+v", status, rcs)
	}

	for key := range rdProps(t, st) {
		if key == allowTwoPrimariesProp || propInNamespace(key, liveMigrateNamespace) ||
			propInNamespace(key, "Cozystack/ResourceConnectionProps") {
			t.Errorf("a plain attach set %s", key)
		}
	}
}

func TestMakeAvailableDualPrimaryNeedsDRBD(t *testing.T) {
	st, base := liveMigrationFixture(t, []string{"STORAGE"})

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/make-available",
		`{"auto_manage_dual_primary":true}`)
	if status == http.StatusOK || len(rcs) == 0 || rcs[0].RetCode != apiCallRcError|apiCallRcFailInvldLayerStack {
		t.Fatalf("make-available without DRBD: %d %+v, want FAIL_INVLD_LAYER_STACK", status, rcs)
	}

	if _, err := st.Resources().Get(t.Context(), "vm-disk", "node-b"); err == nil {
		t.Fatal("a refused make-available created the target replica")
	}
}

func TestUnmakeAvailableNoOps(t *testing.T) {
	_, base := liveMigrationFixture(t, nil)

	status, rcs := postRcs(t, base+"/v1/resource-definitions/no-such-rd/resources/node-a/unmake-available", ``)
	if status != http.StatusOK || len(rcs) == 0 || rcs[0].RetCode != warnNotFound {
		t.Fatalf("unknown definition: %d %+v, want a 200 not-found warning", status, rcs)
	}

	status, rcs = postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/unmake-available", ``)
	if status != http.StatusOK {
		t.Fatalf("node without a replica: %d %+v, want 200", status, rcs)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/vm-disk/resources/no-such-node/unmake-available", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown node: %d, want 404", resp.StatusCode)
	}
}

// Deleting a replica deletes its resource connections, as in LINSTOR: a
// replica created on the node again does not inherit allow-two-primaries.
func TestResourceDeleteDropsItsConnectionProps(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/make-available",
		`{"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available: %d %+v", status, rcs)
	}

	resp := httpDelete(t, base+"/v1/resource-definitions/vm-disk/resources/node-b")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete: %d", resp.StatusCode)
	}

	for key := range rdProps(t, st) {
		if pair, _, ok := apiv1.ParseResourceConnectionPropKey(key); ok && pair.Has("node-b") {
			t.Errorf("connection prop %s outlived the replica on node-b", key)
		}
	}
}

func TestResourceConnectionListShowsMigrationProps(t *testing.T) {
	_, base := liveMigrationFixture(t, nil)

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/make-available",
		`{"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available: %d %+v", status, rcs)
	}

	resp := httpGet(t, base+"/v1/resource-definitions/vm-disk/resource-connections")
	defer resp.Body.Close()

	var got []resourceConnectionListEntry
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 || got[0].NodeA != "node-a" || got[0].NodeB != "node-b" ||
		got[0].Props[allowTwoPrimariesProp] != "yes" || got[0].Props[drbdProtocolProp] != "C" {
		t.Fatalf("resource connections: %+v", got)
	}
}

// liveMigrationServer is a Server on st for calling the dual-Primary steps
// directly, to put them in the order a race would.
func liveMigrationServer(t *testing.T, st store.Store) *Server {
	t.Helper()

	return &Server{Store: st, Client: newFakeRESTClient(t), Namespace: testRESTNamespace}
}

func createDisklessReplica(t *testing.T, st store.Store, node string) {
	t.Helper()

	err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "vm-disk", NodeName: node, Flags: []string{apiv1.ResourceFlagDiskless},
	})
	if err != nil {
		t.Fatalf("create diskless replica on %s: %v", node, err)
	}
}

// Two attaches with the flag on two targets at once (an RWX block volume
// with pods on two nodes) both pick the one node in use as their source
// before either writes. The one that writes second must find the first
// one's marks and be refused, not move the marks to its own pair: an
// unmake-available on the source would then put back the protocol of only
// one of the two connections and leave protocol C on the other for good.
func TestOpenDualPrimaryRefusesWhenAnotherMigrationWonTheRace(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)
	srv := liveMigrationServer(t, st)

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/make-available",
		`{"diskful":false,"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available on node-b: %d %+v", status, rcs)
	}

	// The call for node-c picked node-a as its source before node-b's
	// marks landed, and writes now.
	_, err := srv.openDualPrimary(t.Context(), "vm-disk", "node-a", "node-c")
	if code := refusalCode(t, err); code != apiCallRcError|apiCallRcFailExistsLiveMigrate {
		t.Fatalf("losing make-available: rc %d, want FAIL_EXISTS_LIVE_MIGRATE", code)
	}

	props := rdProps(t, st)
	if props[liveMigrateTargetProp] != "node-b" {
		t.Errorf("marks moved to %q, want them left on node-b", props[liveMigrateTargetProp])
	}

	for key := range props {
		if pair, _, ok := apiv1.ParseResourceConnectionPropKey(key); ok && pair.Has("node-c") {
			t.Errorf("the refused call wrote %s", key)
		}
	}

	// The winner's repeat still goes through.
	if _, err = srv.openDualPrimary(t.Context(), "vm-disk", "node-a", "node-b"); err != nil {
		t.Fatalf("repeat of the prepared migration: %v", err)
	}
}

// A retried attach on the target picked the source while the VM still ran
// there; an unmake-available on the source reverted everything before the
// attach wrote. The attach must not write the migration back: nobody would
// revert it again, and every later migration would be refused.
func TestOpenDualPrimaryAfterTheSourceLetGo(t *testing.T) {
	st, _ := liveMigrationFixture(t, nil)
	srv := liveMigrationServer(t, st)

	createDisklessReplica(t, st, "node-b")
	setInUse(t, st, "node-a", false)

	rcs, err := srv.openDualPrimary(t.Context(), "vm-disk", "node-a", "node-b")
	if err != nil {
		t.Fatalf("openDualPrimary: %v", err)
	}

	if len(rcs) != 1 || rcs[0].RetCode != maskInfo {
		t.Errorf("answer %+v, want the no-migration note", rcs)
	}

	for key := range rdProps(t, st) {
		if propInNamespace(key, liveMigrateNamespace) || propInNamespace(key, "Cozystack/ResourceConnectionProps") {
			t.Errorf("a migration whose source let go wrote %s", key)
		}
	}
}

// The node the VM ran on dies and the VM comes back on another node. The
// dead node's last report still says in use, but it is OFFLINE: the attach
// on the new node is a plain one, and the detach of the dead node is not
// refused with FAIL_IN_USE, which linstor-csi takes as done and never
// retries.
func TestDualPrimaryIgnoresTheInUseOfAnOfflineNode(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)
	url := base + "/v1/resource-definitions/vm-disk/resources/"

	if err := st.Nodes().SetConnectionStatus(t.Context(), "node-a", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("mark node-a offline: %v", err)
	}

	status, rcs := postRcs(t, url+"node-b/make-available", `{"diskful":false,"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available on node-b: %d %+v", status, rcs)
	}

	for key := range rdProps(t, st) {
		if propInNamespace(key, liveMigrateNamespace) || propInNamespace(key, "Cozystack/ResourceConnectionProps") {
			t.Errorf("an attach next to an offline node prepared a migration: %s", key)
		}
	}

	status, rcs = postRcs(t, url+"node-a/unmake-available", ``)
	if status != http.StatusOK {
		t.Fatalf("unmake-available on the offline node: %d %+v, want 200", status, rcs)
	}
}

func markOnline(t *testing.T, st store.Store, nodes ...string) {
	t.Helper()

	for _, node := range nodes {
		if err := st.Nodes().SetConnectionStatus(t.Context(), node, apiv1.NodeTypeOnline); err != nil {
			t.Fatalf("mark %s online: %v", node, err)
		}
	}
}

// reportApplied stamps the definition's live migration token on the
// replica of node, as its satellite does after it applied the definition.
func reportApplied(t *testing.T, st store.Store, node string) {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(context.Background(), "vm-disk")
	if err != nil {
		t.Errorf("get RD: %v", err)

		return
	}

	token := rd.Annotations[blockstoriov1alpha1.ResourceDefinitionAnnotationLiveMigrateToken]

	err = st.Resources().PatchResourceSpec(context.Background(), "vm-disk", node, func(res *apiv1.Resource) error {
		if res.Annotations == nil {
			res.Annotations = map[string]string{}
		}

		res.Annotations[blockstoriov1alpha1.ResourceAnnotationLiveMigrateApplied] = token

		return nil
	})
	if err != nil {
		t.Errorf("report applied on %s: %v", node, err)
	}
}

// linstor-csi starts the migration when the attach answers, and the target
// can only become Primary once allow-two-primaries is in effect on both
// ends. make-available therefore answers only after the satellites of
// both ends reported they applied the definition carrying its props, as
// LINSTOR's answers after its satellites applied the change.
func TestMakeAvailableWaitsForBothEndsToApply(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)
	markOnline(t, st, "node-a", "node-b", "node-c")

	var reported atomic.Bool

	go func() {
		// The satellites take their time: report only once the
		// migration props are written, and a while after.
		for range 200 {
			rd, err := st.ResourceDefinitions().Get(context.Background(), "vm-disk")
			if err == nil && rd.Annotations[blockstoriov1alpha1.ResourceDefinitionAnnotationLiveMigrateToken] != "" {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		time.Sleep(300 * time.Millisecond)
		reportApplied(t, st, "node-a")
		reportApplied(t, st, "node-b")
		reported.Store(true)
	}()

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/make-available",
		`{"diskful":false,"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available: %d %+v", status, rcs)
	}

	if !reported.Load() {
		t.Fatal("make-available answered before both ends applied the dual-primary props")
	}

	for _, rc := range rcs {
		if rc.RetCode == maskWarn {
			t.Errorf("unexpected warning %+v", rc)
		}
	}
}

// A satellite that never reports (one that predates the report, or one
// whose adjust keeps failing) does not fail the attach: the bound runs out
// and the answer carries a warning naming the node.
func TestMakeAvailableWarnsWhenAnEndDoesNotApply(t *testing.T) {
	prev := dualPrimaryApplyTimeout
	dualPrimaryApplyTimeout = 300 * time.Millisecond

	t.Cleanup(func() { dualPrimaryApplyTimeout = prev })

	st, base := liveMigrationFixture(t, nil)
	markOnline(t, st, "node-a", "node-b")

	status, rcs := postRcs(t, base+"/v1/resource-definitions/vm-disk/resources/node-b/make-available",
		`{"diskful":false,"auto_manage_dual_primary":true}`)
	if status != http.StatusOK {
		t.Fatalf("make-available: %d %+v", status, rcs)
	}

	last := rcs[len(rcs)-1]
	if last.RetCode != maskWarn || !strings.Contains(last.Message, "node-a, node-b") {
		t.Fatalf("last answer %+v, want a warning naming node-a and node-b", last)
	}
}

// An allow-two-primaries a live migration left on a connection shows in
// `linstor resource-connection list`, and `linstor resource-connection
// set-property` takes it off again, as on LINSTOR.
func TestResourceConnectionModify(t *testing.T) {
	st, base := liveMigrationFixture(t, nil)
	url := base + "/v1/resource-definitions/vm-disk/resource-connections/node-c/node-a"
	conn := func(key string) string { return apiv1.ResourceConnectionPropKey("node-a", "node-c", key) }

	resp := httpPut(t, url, []byte(`{"override_props":{"DrbdOptions/Net/allow-two-primaries":"yes","DrbdOptions/Net/protocol":"C"}}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set: %d", resp.StatusCode)
	}

	props := rdProps(t, st)
	if props[conn(allowTwoPrimariesProp)] != "yes" || props[conn(drbdProtocolProp)] != "C" {
		t.Fatalf("after set: %v", props)
	}

	resp = httpPut(t, url, []byte(`{"delete_props":["DrbdOptions/Net/allow-two-primaries"],"delete_namespaces":["DrbdOptions/Net"]}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete: %d", resp.StatusCode)
	}

	for key := range rdProps(t, st) {
		if propInNamespace(key, "Cozystack/ResourceConnectionProps") {
			t.Errorf("connection prop %s survived the delete", key)
		}
	}

	// A connection is one between two replicas: node-b has none.
	resp = httpPut(t, base+"/v1/resource-definitions/vm-disk/resource-connections/node-a/node-b",
		[]byte(`{"override_props":{"DrbdOptions/Net/protocol":"C"}}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("connection to a node without a replica: %d, want 404", resp.StatusCode)
	}
}
