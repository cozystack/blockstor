// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// csiPollBudget bounds cloneLikeCSI's poll loop, which in the driver has none.
const csiPollBudget = 5

// cloneLikeCSI drives the clone the way linstor-csi v1.10.1 does
// (pkg/client/linstor.go): GET the status, POST the clone only when that GET
// is a 404, then poll until COMPLETE. The driver has no FAILED branch and no
// second POST, so any answer other than COMPLETE after the POST is a wait that
// never ends; here it is a bounded loop that reports what it saw.
func cloneLikeCSI(t *testing.T, base, src, dst string) (bool, string) {
	t.Helper()

	statusURL := base + "/v1/resource-definitions/" + src + "/clone/" + dst

	code, status := cloneStatusOnce(t, statusURL)
	if code == http.StatusNotFound {
		resp := postClone(t, base, src, map[string]any{
			"name": dst, "use_zfs_clone": true,
			"layer_list": []string{"DRBD", "STORAGE"}, "resource_group": "DfltRscGrp",
		})
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			return false, "POST answered " + http.StatusText(resp.StatusCode)
		}

		code, status = cloneStatusOnce(t, statusURL)
	}

	for range csiPollBudget {
		if code != http.StatusOK {
			return false, "poll answered " + http.StatusText(code)
		}

		if status == "COMPLETE" {
			return true, status
		}

		code, status = cloneStatusOnce(t, statusURL)
	}

	return false, "poll never left " + status
}

func cloneStatusOnce(t *testing.T, url string) (int, string) {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, ""
	}

	var got struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode clone-status: %v", err)
	}

	return resp.StatusCode, got.Status
}

func seedDefaultGroup(t *testing.T, st store.Store) {
	t.Helper()

	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "DfltRscGrp"}); err != nil &&
		!strings.Contains(err.Error(), "exist") {
		t.Fatalf("seed DfltRscGrp: %v", err)
	}
}

func seedOnlineNodeWithPool(t *testing.T, st store.Store, node string) {
	t.Helper()

	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node, ConnectionStatus: "ONLINE"}); err != nil {
		t.Fatalf("seed %s: %v", node, err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName:  "zfs-thin",
		NodeName:         node,
		ProviderKind:     "ZFS_THIN",
		SupportsSnapshot: true,
	}); err != nil {
		t.Fatalf("seed pool on %s: %v", node, err)
	}
}

// Evacuating a node moves a clone's replica off a node the snapshot recorded.
// The replay then read the clone as unfinished and re-stamped a replica on the
// node the operator had just emptied, restored from the point-in-time while the
// migrated replica had moved on with live writes.
func TestRDCloneReplayLeavesAMigratedReplicaWhereItIs(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-mig")
	seedOnlineNodeWithPool(t, st, "node-c")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-mig", "dst-mig", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-mig", NodeName: "node-c", Props: map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("place the replica on node-c: %v", err)
	}

	if err := st.Resources().Delete(ctx, "dst-mig", "node-a"); err != nil {
		t.Fatalf("evacuate node-a: %v", err)
	}

	if code := cloneOnce(t, base, "src-mig", "dst-mig", nil); code != http.StatusCreated {
		t.Fatalf("replay after migration = %d, want 201", code)
	}

	after, err := st.Resources().ListByDefinition(ctx, "dst-mig")
	if err != nil {
		t.Fatalf("list the clone's replicas: %v", err)
	}

	if names := nodeNamesOf(after); !slices.Equal(names, []string{"node-c"}) {
		t.Errorf("replicas after the replay = %v, want [node-c]", names)
	}

	if code, status := cloneStatusOnce(t, base+"/v1/resource-definitions/src-mig/clone/dst-mig"); status != "COMPLETE" {
		t.Errorf("poll after migration = %d %q, want COMPLETE", code, status)
	}
}

// The status poll answered FAILED over a leftover the POST knows how to resume,
// and linstor-csi POSTs only after a 404 and polls with no FAILED branch, so
// the leftover was never resumed and the driver waited on it forever.
func TestRDCloneStatusSendsAnUnfinishedCloneBackToThePost(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-csi6")
	seedDefaultGroup(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-csi6", "dst-csi6", map[string]any{
		"layer_list": []string{"DRBD", "STORAGE"}, "resource_group": "DfltRscGrp",
	}); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	// The first attempt died after hydrating and before any replica held the
	// data: every volume, no replica.
	if err := st.Resources().Delete(ctx, "dst-csi6", "node-a"); err != nil {
		t.Fatalf("drop the replica: %v", err)
	}

	if done, saw := cloneLikeCSI(t, base, "src-csi6", "dst-csi6"); !done {
		t.Fatalf("the driver's clone flow over an unfinished leftover did not complete: %s", saw)
	}

	after, err := st.Resources().ListByDefinition(ctx, "dst-csi6")
	if err != nil {
		t.Fatalf("list the clone's replicas: %v", err)
	}

	if len(after) == 0 {
		t.Error("COMPLETE was reached without the resume placing a replica")
	}
}

// A foreign target is refused by the POST with a cause and a correction. The
// poll has to send the driver there too, rather than answer a status it waits
// on forever.
func TestRDCloneStatusSendsAForeignTargetToTheRefusal(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-for6")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-for6", "dst-for6", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	vd, err := st.VolumeDefinitions().Get(ctx, "dst-for6", 0)
	if err != nil {
		t.Fatalf("read the clone's volume: %v", err)
	}

	vd.SizeKib /= 2
	if err := st.VolumeDefinitions().Update(ctx, "dst-for6", &vd); err != nil {
		t.Fatalf("shrink the volume: %v", err)
	}

	code, status := cloneStatusOnce(t, base+"/v1/resource-definitions/src-for6/clone/dst-for6")
	if code != http.StatusNotFound {
		t.Errorf("poll over a foreign target = %d %q, want 404 so the driver reaches the POST's refusal", code, status)
	}
}

// "Could not read" is not an answer about the clone. FAILED on a read failure
// was a wait without end; a 500 is an error the driver returns and retries.
func TestRDCloneStatusIsAnErrorWhenTheCloneCannotBeRead(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-err6")

	base, stop := startServerWithStore(t, backend)

	if code := cloneOnce(t, base, "src-err6", "dst-err6", nil); code != http.StatusCreated {
		stop()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stop()

	base2, stop2 := startServerWithStore(t, failingSnapshotGetStore{backend})
	defer stop2()

	code, status := cloneStatusOnce(t, base2+"/v1/resource-definitions/src-err6/clone/dst-err6")
	if code != http.StatusInternalServerError {
		t.Errorf("poll over an unreadable snapshot = %d %q, want 500", code, status)
	}
}

// The classifier calls a volume larger than the snapshot recorded this clone's
// own, expanded since; the hydrate the resume routes into refused any size that
// was not equal, so the same leftover was admitted by one half of the state
// machine and answered a bare 500 by the other, on every retry.
func TestRDCloneResumeOfAnExpandedCloneWithNoReplicaCompletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-grow6")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-grow6", "dst-grow6", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	vd, err := st.VolumeDefinitions().Get(ctx, "dst-grow6", 0)
	if err != nil {
		t.Fatalf("read the clone's volume: %v", err)
	}

	grown := vd.SizeKib * 2

	vd.SizeKib = grown
	if err := st.VolumeDefinitions().Update(ctx, "dst-grow6", &vd); err != nil {
		t.Fatalf("expand the clone: %v", err)
	}

	if err := st.Resources().Delete(ctx, "dst-grow6", "node-a"); err != nil {
		t.Fatalf("drop the replica: %v", err)
	}

	if code := cloneOnce(t, base, "src-grow6", "dst-grow6", nil); code != http.StatusCreated {
		t.Fatalf("retry over an expanded leftover with no replica = %d, want 201", code)
	}

	after, err := st.VolumeDefinitions().Get(ctx, "dst-grow6", 0)
	if err != nil {
		t.Fatalf("read the clone's volume after the retry: %v", err)
	}

	if after.SizeKib != grown {
		t.Errorf("volume size after the retry = %d, want the expanded %d", after.SizeKib, grown)
	}
}

// materializeRestoredRD stamps the source's group on the clone when the request
// names none, so a real leftover carries one. A request that names no group has
// to be compared as naming nothing, not as naming "", or a source moved to
// another group refuses every later retry of the clone it already made.
func TestRDCloneRetryNamingNoGroupPassesALeftoverThatCarriesOne(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		unfinish bool
	}{
		{name: "finished", unfinish: false},
		{name: "unfinished", unfinish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()
			src, dst := "src-rg6-"+tc.name, "dst-rg6-"+tc.name
			seedDeployedCloneSource(t, st, src)

			for _, group := range []string{"grp-a", "grp-b"} {
				if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: group}); err != nil {
					t.Fatalf("seed %s: %v", group, err)
				}
			}

			moveSourceToGroup(t, st, src, "grp-a")

			base, stop := startServerWithStore(t, st)
			defer stop()

			if code := cloneOnce(t, base, src, dst, nil); code != http.StatusCreated {
				t.Fatalf("first clone = %d, want 201", code)
			}

			leftover, err := st.ResourceDefinitions().Get(ctx, dst)
			if err != nil {
				t.Fatalf("read the clone: %v", err)
			}

			if leftover.ResourceGroupName != "grp-a" {
				t.Fatalf("fixture: the clone carries group %q, want grp-a", leftover.ResourceGroupName)
			}

			if tc.unfinish {
				if err := st.Resources().Delete(ctx, dst, "node-a"); err != nil {
					t.Fatalf("drop the replica: %v", err)
				}
			}

			moveSourceToGroup(t, st, src, "grp-b")

			if code := cloneOnce(t, base, src, dst, nil); code != http.StatusCreated {
				t.Errorf("retry naming no group over a leftover in grp-a = %d, want 201", code)
			}
		})
	}
}

func moveSourceToGroup(t *testing.T, st store.Store, rdName, group string) {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("read %s: %v", rdName, err)
	}

	rd.ResourceGroupName = group
	if err := st.ResourceDefinitions().Update(t.Context(), &rd); err != nil {
		t.Fatalf("move %s to %s: %v", rdName, group, err)
	}
}

// The shape refusal runs before anything established whether the leftover is
// finished, so it must not tell the operator that it is.
func TestRDCloneShapeRefusalDoesNotCallAnUnfinishedLeftoverFinished(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-word6")
	seedCloneLeftover(t, st, "src-word6", "dst-word6")

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp-other"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-word6", map[string]any{
		"name": "dst-word6", "use_zfs_clone": true, "resource_group": "grp-other",
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}

	envelope := decodeCloneStarted(t, resp)
	if envelope.Messages == nil || len(*envelope.Messages) == 0 {
		t.Fatal("empty envelope")
	}

	msg := (*envelope.Messages)[0]
	if text := msg.Message + " " + msg.Cause; strings.Contains(text, "finished") {
		t.Errorf("refusal over a leftover with no volumes says it is finished: %q", text)
	}
}

// The replay is asked before every check that reads the live source, and each
// of those is a way to refuse a clone that already completed. Only the snapshot
// step was pinned; these two hold the others.
func TestRDCloneReplayIsAskedBeforeTheSourceChecks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		change func(t *testing.T, st store.Store, src string)
	}{
		{
			name: "source-being-deleted",
			change: func(t *testing.T, st store.Store, src string) {
				t.Helper()
				updateSource(t, st, src, func(rd *apiv1.ResourceDefinition) {
					rd.Flags = append(rd.Flags, rdFlagDelete)
				})
			},
		},
		{
			name: "source-stack-changed",
			change: func(t *testing.T, st store.Store, src string) {
				t.Helper()
				updateSource(t, st, src, func(rd *apiv1.ResourceDefinition) {
					rd.LayerStack = []string{"STORAGE"}
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			src, dst := "src-ord6-"+tc.name, "dst-ord6-"+tc.name
			seedDeployedCloneSource(t, st, src)
			seedDefaultGroup(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			body := map[string]any{"layer_list": []string{"DRBD", "STORAGE"}, "resource_group": "DfltRscGrp"}

			if code := cloneOnce(t, base, src, dst, body); code != http.StatusCreated {
				t.Fatalf("first clone = %d, want 201", code)
			}

			tc.change(t, st, src)

			if code := cloneOnce(t, base, src, dst, body); code != http.StatusCreated {
				t.Errorf("replay of the finished clone after %s = %d, want 201", tc.name, code)
			}
		})
	}
}

func updateSource(t *testing.T, st store.Store, rdName string, mutate func(*apiv1.ResourceDefinition)) {
	t.Helper()

	ctx := context.WithoutCancel(t.Context())

	rd, err := st.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		t.Fatalf("read %s: %v", rdName, err)
	}

	mutate(&rd)

	if err := st.ResourceDefinitions().Update(ctx, &rd); err != nil {
		t.Fatalf("update %s: %v", rdName, err)
	}
}
