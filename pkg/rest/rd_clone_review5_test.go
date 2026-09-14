// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedTwoNodeCloneSource is seedDeployedCloneSource with a second diskful
// replica, so a clone places two and "half-placed" is a state that can exist.
func seedTwoNodeCloneSource(t *testing.T, st store.Store, rdName string) {
	t.Helper()

	ctx := t.Context()
	seedDeployedCloneSource(t, st, rdName)

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "node-b", ConnectionStatus: "ONLINE"}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName:  "zfs-thin",
		NodeName:         "node-b",
		ProviderKind:     "ZFS_THIN",
		SupportsSnapshot: true,
	}); err != nil {
		t.Fatalf("seed pool on node-b: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "node-b",
		Props:    map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed replica on node-b: %v", err)
	}
}

func cloneOnce(t *testing.T, base, src, dst string, extra map[string]any) int {
	t.Helper()

	body := map[string]any{"name": dst, "use_zfs_clone": true}
	for k, v := range extra {
		body[k] = v
	}

	resp := postClone(t, base, src, body)
	_ = resp.Body.Close()

	return resp.StatusCode
}

// Hoisting the finished question above everything that reads the live source
// also hoisted it above everything that reads the REQUEST. A replay naming a
// shape the finished clone does not have was told the clone completed and
// handed the other shape: a caller who names LUKS gets plaintext.
func TestRDCloneReplayRefusesANamedShapeTheFinishedCloneDoesNotHave(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-shape5")

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp-other"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-shape5", "dst-shape5", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	if code := cloneOnce(t, base, "src-shape5", "dst-shape5",
		map[string]any{"resource_group": "grp-other"}); code == http.StatusCreated {
		t.Error("replay naming a different resource_group was answered 201 with it dropped")
	}

	if code := cloneOnce(t, base, "src-shape5", "dst-shape5",
		map[string]any{"layer_list": []string{"storage"}}); code == http.StatusCreated {
		t.Error("replay naming a different layer_list was answered 201 with it dropped")
	}

	// The CSI replay names neither field and has to stay an idempotent success.
	if code := cloneOnce(t, base, "src-shape5", "dst-shape5", nil); code != http.StatusCreated {
		t.Errorf("plain replay = %d, want 201", code)
	}
}

// stampRestoredResourcesOnNodes creates one replica per snapshot node and
// returns on the first hard error, so 1 of N is an ordinary intermediate state.
// One replica used to certify it finished, and nothing tops it up afterwards.
func TestRDCloneReplayDoesNotCertifyAHalfPlacedClone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedTwoNodeCloneSource(t, st, "src-half")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-half", "dst-half", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	placed, err := st.Resources().ListByDefinition(ctx, "dst-half")
	if err != nil {
		t.Fatalf("list the clone's replicas: %v", err)
	}

	if len(placed) != 2 {
		t.Fatalf("a complete clone of a two-node source placed %d replica(s), want 2", len(placed))
	}

	// Leave the clone half-placed, the way a first attempt that died between
	// the two stamps does.
	if err := st.Resources().Delete(ctx, "dst-half", "node-b"); err != nil {
		t.Fatalf("drop one replica: %v", err)
	}

	if code := cloneOnce(t, base, "src-half", "dst-half", nil); code != http.StatusCreated {
		t.Fatalf("retry = %d, want 201", code)
	}

	after, err := st.Resources().ListByDefinition(ctx, "dst-half")
	if err != nil {
		t.Fatalf("list the clone's replicas after the retry: %v", err)
	}

	if len(after) != 2 {
		t.Errorf("retry left %d replica(s); a half-placed clone was reported finished", len(after))
	}
}

var errSnapshotReadFailed = errors.New("read the clone snapshot failed")

type failingSnapshotGets struct{ store.SnapshotStore }

func (failingSnapshotGets) Get(context.Context, string, string) (apiv1.Snapshot, error) {
	return apiv1.Snapshot{}, errSnapshotReadFailed
}

type failingSnapshotGetStore struct{ store.Store }

func (f failingSnapshotGetStore) Snapshots() store.SnapshotStore {
	return failingSnapshotGets{f.Store.Snapshots()}
}

// "The snapshot does not exist" is a fact about the world; "the snapshot could
// not be read" is a fact about this request. Only the first may decide a clone
// at face value, and the replay used to take either.
func TestRDCloneReplayDoesNotTakeAnUnreadableSnapshotAtFaceValue(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-unread")

	base, stop := startServerWithStore(t, backend)

	if code := cloneOnce(t, base, "src-unread", "dst-unread", nil); code != http.StatusCreated {
		stop()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stop()

	base2, stop2 := startServerWithStore(t, failingSnapshotGetStore{backend})
	defer stop2()

	if code := cloneOnce(t, base2, "src-unread", "dst-unread", nil); code == http.StatusCreated {
		t.Error("replay answered 201 on a snapshot read that failed rather than one that was absent")
	}
}

// The hoist took the live-source comparison off the replay and left it on the
// resume, which is where a retry of an unfinished clone lives. A first attempt
// that dies before placing, a source moved to another group for unrelated
// reasons, and the retry arrives with the same body it always sends.
func TestRDCloneResumeOfAnUnfinishedLeftoverSurvivesASourceGroupMove(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-unf")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-unf"),
		ResourceName:      "src-unf",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-unf", "dst-unf")

	src, err := st.ResourceDefinitions().Get(ctx, "src-unf")
	if err != nil {
		t.Fatalf("read the source: %v", err)
	}

	src.ResourceGroupName = "grp-new"
	if err := st.ResourceDefinitions().Update(ctx, &src); err != nil {
		t.Fatalf("move the source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-unf", "dst-unf", nil); code != http.StatusCreated {
		t.Fatalf("retry of an unfinished clone after the source moved = %d, want 201", code)
	}
}

// The POST was made independent of the live source; the GET linstor-csi polls
// right after it was not. A volume added to the source after the clone
// finished turned the poll that follows a 201 into FAILED, one request apart.
func TestRDCloneStatusPollAgreesWithTheReplayAfterTheSourceGainsAVolume(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-poll")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-poll", "dst-poll", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	if err := st.VolumeDefinitions().Create(ctx, "src-poll",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("give the source a second volume: %v", err)
	}

	if code := cloneOnce(t, base, "src-poll", "dst-poll", nil); code != http.StatusCreated {
		t.Fatalf("replay = %d, want 201", code)
	}

	resp := httpGet(t, base+"/v1/resource-definitions/src-poll/clone/dst-poll")
	defer func() { _ = resp.Body.Close() }()

	var got struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode clone-status: %v", err)
	}

	if got.Status != "COMPLETE" {
		t.Errorf("poll after a 201 replay = %q, want COMPLETE", got.Status)
	}
}

// Following the printed correction has to work. Deleting only the snapshot
// when the leftover already holds volumes hydrated from it leads through a bare
// 500 with no correction, over a fresh snapshot of the live source.
func TestRDCloneStaleSnapshotCorrectionNamesTheTargetWhenItHoldsVolumes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-stale5")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-stale5"),
		ResourceName:      "src-stale5",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-stale5", "dst-stale5")

	if err := st.VolumeDefinitions().Create(ctx, "dst-stale5",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	growSourceVolume(t, st, "src-stale5", 128*1024)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-stale5", map[string]any{"name": "dst-stale5", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}

	var envelope cloneStartedResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode the envelope: %v", err)
	}

	if envelope.Messages == nil || len(*envelope.Messages) == 0 {
		t.Fatal("empty envelope")
	}

	if correc := (*envelope.Messages)[0].Correc; !strings.Contains(correc, "delete 'dst-stale5'") {
		t.Errorf("correction %q does not name the target, so following it leads to a 500", correc)
	}
}

// A finished clone owes the snapshot nothing either. An ordinary expansion of
// the clone read as "holds a volume this clone would not have written", and the
// correction said to delete a clone with data on it.
func TestRDCloneReplayOfAnExpandedCloneIsStillFinished(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-exp")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-exp", "dst-exp", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	vd, err := st.VolumeDefinitions().Get(ctx, "dst-exp", 0)
	if err != nil {
		t.Fatalf("read the clone's volume: %v", err)
	}

	vd.SizeKib *= 2
	if err := st.VolumeDefinitions().Update(ctx, "dst-exp", &vd); err != nil {
		t.Fatalf("expand the clone: %v", err)
	}

	if code := cloneOnce(t, base, "src-exp", "dst-exp", nil); code != http.StatusCreated {
		t.Errorf("replay after expanding the clone = %d, want 201", code)
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
