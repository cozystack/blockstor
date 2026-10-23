// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
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
	seedDeployedCloneSource(t, st, "src-shape")

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp-other"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-shape", "dst-shape", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	if code := cloneOnce(t, base, "src-shape", "dst-shape",
		map[string]any{"resource_group": "grp-other"}); code == http.StatusCreated {
		t.Error("replay naming a different resource_group was answered 201 with it dropped")
	}

	if code := cloneOnce(t, base, "src-shape", "dst-shape",
		map[string]any{"layer_list": []string{"storage"}}); code == http.StatusCreated {
		t.Error("replay naming a different layer_list was answered 201 with it dropped")
	}

	// The CSI replay names neither field and has to stay an idempotent success.
	if code := cloneOnce(t, base, "src-shape", "dst-shape", nil); code != http.StatusCreated {
		t.Errorf("plain replay = %d, want 201", code)
	}
}

// Scaling a clone down is an ordinary operation, and a replay after it must not
// undo it. Judging the clone by whether every snapshot node still carried a
// replica read the scaled-down clone as unfinished, and the resume re-stamped
// the removed replica from the point-in-time while the survivor had moved on.
// Once the source had also grown, the resume refused the replay permanently,
// with a correction that deletes a clone holding data.
func TestRDCloneReplayLeavesAScaledDownCloneAlone(t *testing.T) {
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

	if err := st.Resources().Delete(ctx, "dst-half", "node-b"); err != nil {
		t.Fatalf("scale the clone down: %v", err)
	}

	growSourceVolume(t, st, "src-half", 128*1024)

	if code := cloneOnce(t, base, "src-half", "dst-half", nil); code != http.StatusCreated {
		t.Fatalf("replay after scaling the clone down and growing the source = %d, want 201", code)
	}

	after, err := st.Resources().ListByDefinition(ctx, "dst-half")
	if err != nil {
		t.Fatalf("list the clone's replicas after the replay: %v", err)
	}

	if len(after) != 1 || after[0].NodeName != "node-a" {
		t.Errorf("replay left replicas %v, want only node-a: it re-stamped the one removed", nodeNamesOf(after))
	}
}

func nodeNamesOf(replicas []apiv1.Resource) []string {
	names := make([]string, 0, len(replicas))
	for i := range replicas {
		names = append(names, replicas[i].NodeName)
	}

	return names
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
	seedDeployedCloneSource(t, st, "src-stale")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-stale"),
		ResourceName:      "src-stale",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-stale", "dst-stale")

	if err := st.VolumeDefinitions().Create(ctx, "dst-stale",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	growSourceVolume(t, st, "src-stale", 128*1024)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-stale", map[string]any{"name": "dst-stale", "use_zfs_clone": true})
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

	if correc := (*envelope.Messages)[0].Correc; !strings.Contains(correc, "delete 'dst-stale'") {
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

// A finished clone whose definition carries the DELETE flag is being torn down
// whatever its replicas say, and the POST refuses it as that. The poll judged
// it by its replicas alone and read COMPLETE, which linstor-csi binds on
// without a POST: deleted directly rather than through `rd d`, which cascades
// the replicas first, the definition still lists an unflagged one.
func TestRDCloneStatusTreatsADeleteFlaggedDefinitionAsTornDown(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-flagged")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-flagged",
		Flags: []string{rdFlagDelete},
		Props: map[string]string{store.RestoreFromSnapshotProp: "src-flagged:" + cloneSnapshotName("dst-flagged")},
	}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-flagged",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the target's volume: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "dst-flagged", NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the target's replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, status := cloneStatusOnce(t, base+"/v1/resource-definitions/src-flagged/clone/dst-flagged")
	if code != http.StatusNotFound || status == "COMPLETE" {
		t.Errorf("poll of a DELETE-flagged finished clone = %d %q, want 404", code, status)
	}

	if post := cloneOnce(t, base, "src-flagged", "dst-flagged", nil); post == http.StatusCreated {
		t.Errorf("POST of the same clone = %d, want a refusal", post)
	}
}

// A replay of a finished clone owes the source nothing, and was still asked
// behind a read of it: a source deleted once its clone was done answered the
// replay 404, and one whose volumes were removed sent it down the empty-shell
// path to an already-exists.
func TestRDCloneReplayOfAFinishedCloneDoesNotReadTheSource(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, st store.Store, src string)
	}{
		{name: "source-deleted", mutate: func(t *testing.T, st store.Store, src string) {
			t.Helper()

			if err := st.ResourceDefinitions().Delete(t.Context(), src); err != nil {
				t.Fatalf("delete the source: %v", err)
			}
		}},
		{name: "source-volumes-removed", mutate: func(t *testing.T, st store.Store, src string) {
			t.Helper()

			if err := st.VolumeDefinitions().Delete(t.Context(), src, 0); err != nil {
				t.Fatalf("remove the source's volume: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedDeployedCloneSource(t, st, "src-gone")

			base, stop := startServerWithStore(t, st)
			defer stop()

			if code := cloneOnce(t, base, "src-gone", "dst-gone", nil); code != http.StatusCreated {
				t.Fatalf("first clone = %d, want 201", code)
			}

			tc.mutate(t, st, "src-gone")

			if code := cloneOnce(t, base, "src-gone", "dst-gone", nil); code != http.StatusCreated {
				t.Errorf("replay of the finished clone = %d, want 201", code)
			}
		})
	}
}

// The poll answered a finished clone COMPLETE without the gate the POST's
// replay asks before its 201: a clone the RG-deleted rollback was deleting, or
// one whose rollback gave up, read done to the driver, which binds on the poll
// and never reaches the POST that refuses it.
func TestRDCloneStatusAsksTheGateTheReplayAsks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(rd *apiv1.ResourceDefinition)
	}{
		{name: "rollback-in-progress", mutate: func(rd *apiv1.ResourceDefinition) {
			rd.Props[store.RollbackAbandonedProp] = store.RollbackInProgress
		}},
		{name: "rollback-gave-up", mutate: func(rd *apiv1.ResourceDefinition) {
			rd.Props[store.RollbackAbandonedProp] = "delete-replicas"
		}},
		{name: "group-gone", mutate: func(rd *apiv1.ResourceDefinition) {
			rd.ResourceGroupName = "grp-gone"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()
			seedDeployedCloneSource(t, st, "src-gate")

			base, stop := startServerWithStore(t, st)
			defer stop()

			if code := cloneOnce(t, base, "src-gate", "dst-gate", nil); code != http.StatusCreated {
				t.Fatalf("first clone = %d, want 201", code)
			}

			if err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "dst-gate",
				func(rd *apiv1.ResourceDefinition) error {
					if rd.Props == nil {
						rd.Props = map[string]string{}
					}

					tc.mutate(rd)

					return nil
				}); err != nil {
				t.Fatalf("put the clone in the state under test: %v", err)
			}

			if code, status := cloneStatusOnce(t, base+"/v1/resource-definitions/src-gate/clone/dst-gate"); status == "COMPLETE" {
				t.Errorf("poll = %d %q, want the clone sent back to the POST", code, status)
			}

			if code := cloneOnce(t, base, "src-gate", "dst-gate", nil); code == http.StatusCreated {
				t.Errorf("POST = %d, want a refusal", code)
			}
		})
	}
}

// cacheMissesTheDeleteFlag serves one definition from a cache that has not
// seen its DELETE flag; the API server holds it.
type cacheMissesTheDeleteFlag struct {
	store.Store

	target string
}

type cacheMissesTheDeleteFlagRDs struct {
	store.ResourceDefinitionStore

	target string
}

func (c cacheMissesTheDeleteFlag) ResourceDefinitions() store.ResourceDefinitionStore {
	return cacheMissesTheDeleteFlagRDs{ResourceDefinitionStore: c.Store.ResourceDefinitions(), target: c.target}
}

func (c cacheMissesTheDeleteFlagRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	rd, err := c.ResourceDefinitionStore.Get(ctx, name)
	if err == nil && name == c.target {
		rd.Flags = slices.DeleteFunc(slices.Clone(rd.Flags), func(f string) bool { return f == rdFlagDelete })
	}

	return rd, err //nolint:wrapcheck // pass-through test double
}

// A definition deleted directly, read from a cache that has not seen its
// DELETE flag yet, was answered COMPLETE by the poll and 201 by the replay.
// The gate both share reads the flag past the cache.
func TestRDCloneAnswersADeleteTheCacheHasNotSeen(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-lagdel")

	plain, stopPlain := startServerWithStore(t, backend)

	if code := cloneOnce(t, plain, "src-lagdel", "dst-lagdel", nil); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stopPlain()

	if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "dst-lagdel",
		func(rd *apiv1.ResourceDefinition) error {
			rd.Flags = append(rd.Flags, rdFlagDelete)

			return nil
		}); err != nil {
		t.Fatalf("flag the clone for deletion: %v", err)
	}

	base, stop := startServerWithStore(t, cacheMissesTheDeleteFlag{Store: backend, target: "dst-lagdel"})
	defer stop()

	if _, status := cloneStatusOnce(t, base+"/v1/resource-definitions/src-lagdel/clone/dst-lagdel"); status == "COMPLETE" {
		t.Error("poll answered COMPLETE over a definition being deleted")
	}

	if code := cloneOnce(t, base, "src-lagdel", "dst-lagdel", nil); code == http.StatusCreated {
		t.Error("replay answered 201 over a definition being deleted")
	}
}

// staleSourceVolumes serves one definition's volumes from a cache that has
// not seen a resize; the API server holds the new size.
type staleSourceVolumes struct {
	store.Store

	source string
	stale  []apiv1.VolumeDefinition
}

type staleSourceVolumesVDs struct {
	store.VolumeDefinitionStore

	outer staleSourceVolumes
}

func (s staleSourceVolumes) VolumeDefinitions() store.VolumeDefinitionStore {
	return staleSourceVolumesVDs{VolumeDefinitionStore: s.Store.VolumeDefinitions(), outer: s}
}

func (v staleSourceVolumesVDs) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	if rdName == v.outer.source {
		return v.outer.stale, nil
	}

	return v.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (v staleSourceVolumesVDs) ListUncached(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	return v.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // the API server's answer
}

// Whether a reused clone snapshot still describes the source is asked of the
// API server: a cache that had not seen the source's resize let a snapshot of
// the old size through as current.
func TestRDCloneSnapshotIsCurrentReadsTheSourcePastTheCache(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-cur")

	stale, err := st.VolumeDefinitions().List(ctx, "src-cur")
	if err != nil {
		t.Fatalf("read the source's volumes: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-cur"),
		ResourceName:      "src-cur",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: stale[0].SizeKib}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedMarkedCloneLeftover(t, st, "src-cur", "dst-cur")
	growSourceVolume(t, st, "src-cur", 2*stale[0].SizeKib)

	base, stop := startServerWithStore(t, staleSourceVolumes{Store: st, source: "src-cur", stale: stale})
	defer stop()

	if code := cloneOnce(t, base, "src-cur", "dst-cur", nil); code != http.StatusConflict {
		t.Errorf("resume over a snapshot the source outgrew, read through a stale cache = %d, want 409", code)
	}
}

// caseFoldingDefinitions finds a definition under any spelling of its name,
// as the Kubernetes store does, while replicas stay selected by the name
// exactly as stored.
type caseFoldingDefinitions struct{ store.Store }

type caseFoldingDefinitionsRDs struct{ store.ResourceDefinitionStore }

func (c caseFoldingDefinitions) ResourceDefinitions() store.ResourceDefinitionStore {
	return caseFoldingDefinitionsRDs{c.Store.ResourceDefinitions()}
}

func (r caseFoldingDefinitionsRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	rd, err := r.ResourceDefinitionStore.Get(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return r.ResourceDefinitionStore.Get(ctx, strings.ToLower(name)) //nolint:wrapcheck // pass-through test double
	}

	return rd, err //nolint:wrapcheck // pass-through test double
}

func (r caseFoldingDefinitionsRDs) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	rd, err := r.ResourceDefinitionStore.GetUncached(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return r.ResourceDefinitionStore.GetUncached(ctx, strings.ToLower(name)) //nolint:wrapcheck // pass-through test double
	}

	return rd, err //nolint:wrapcheck // pass-through test double
}

// A replay spelling the target in another case finds the definition, whose
// replicas are selected by the name as stored. Judged under the request's
// spelling it counted none, read as unfinished, and re-stamped a replica on
// the node the operator had emptied. It is judged under the stored name.
func TestRDCloneReplayInAnotherCaseIsJudgedUnderTheStoredName(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedTwoNodeCloneSource(t, backend, "src-case")

	base, stop := startServerWithStore(t, caseFoldingDefinitions{backend})
	defer stop()

	if code := cloneOnce(t, base, "src-case", "dst-case", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	if err := backend.Resources().Delete(ctx, "dst-case", "node-b"); err != nil {
		t.Fatalf("empty node-b: %v", err)
	}

	if code := cloneOnce(t, base, "src-case", "DST-CASE", nil); code != http.StatusCreated {
		t.Errorf("replay in another case = %d, want 201", code)
	}

	all, err := backend.Resources().List(ctx)
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	var placed []string

	for i := range all {
		if strings.EqualFold(all[i].Name, "dst-case") {
			placed = append(placed, all[i].Name+"@"+all[i].NodeName)
		}
	}

	if len(placed) != 1 {
		t.Errorf("the replay left replicas %v, want only dst-case@node-a", placed)
	}
}

// The restore door's twin: a placed restore replayed in another case is judged
// under the stored name and leaves the emptied node alone.
func TestSnapshotRestoreReplayInAnotherCaseIsJudgedUnderTheStoredName(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedTwoNodeSource(t, backend, "src-rcase")
	seedRestoreSnapshot(t, backend, "src-rcase", "snap-rcase", []string{"node-a", "node-b"})

	base, stop := startServerWithStore(t, caseFoldingDefinitions{backend})
	defer stop()

	body := func(name string) map[string]any {
		return map[string]any{"to_resource": name, "nodes": []string{"node-a", "node-b"}}
	}

	if code, rc := restoreAnswer(t, base, "src-rcase", "snap-rcase", body("dst-rcase")); code != http.StatusCreated {
		t.Fatalf("first restore = %d %q, want 201", code, rc.Message)
	}

	if err := backend.Resources().Delete(ctx, "dst-rcase", "node-b"); err != nil {
		t.Fatalf("empty node-b: %v", err)
	}

	if code, rc := restoreAnswer(t, base, "src-rcase", "snap-rcase", body("DST-RCASE")); code != http.StatusCreated {
		t.Errorf("replay in another case = %d %q, want 201", code, rc.Message)
	}

	all, err := backend.Resources().List(ctx)
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	var placed []string

	for i := range all {
		if strings.EqualFold(all[i].Name, "dst-rcase") {
			placed = append(placed, all[i].Name+"@"+all[i].NodeName)
		}
	}

	if len(placed) != 1 {
		t.Errorf("the replay left replicas %v, want only dst-rcase@node-a", placed)
	}
}

// adoptRacesTheRebind folds case the way the Kubernetes store does, Create
// included, except that the first cached read of a spelling other than the
// stored one misses: the read that names the target as it is stored lost a
// race with the request that created it, so the create collides and adopts.
type adoptRacesTheRebind struct {
	store.Store

	misses *atomic.Int32
}

type adoptRacesTheRebindRDs struct {
	store.ResourceDefinitionStore

	misses *atomic.Int32
}

func (a adoptRacesTheRebind) ResourceDefinitions() store.ResourceDefinitionStore {
	return adoptRacesTheRebindRDs{ResourceDefinitionStore: a.Store.ResourceDefinitions(), misses: a.misses}
}

func (r adoptRacesTheRebindRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if name != strings.ToLower(name) && r.misses.Add(-1) >= 0 {
		return apiv1.ResourceDefinition{}, store.ErrNotFound
	}

	return r.ResourceDefinitionStore.Get(ctx, strings.ToLower(name)) //nolint:wrapcheck // pass-through test double
}

func (r adoptRacesTheRebindRDs) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	return r.ResourceDefinitionStore.GetUncached(ctx, strings.ToLower(name)) //nolint:wrapcheck // pass-through test double
}

func (r adoptRacesTheRebindRDs) Create(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if _, err := r.ResourceDefinitionStore.GetUncached(ctx, strings.ToLower(rd.Name)); err == nil {
		return store.ErrAlreadyExists
	}

	return r.ResourceDefinitionStore.Create(ctx, rd) //nolint:wrapcheck // pass-through test double
}

// A definition adopted through a create that collided is hydrated and placed
// under the name it is stored with: a replica stamped under the request's
// spelling is invisible to the definition's own cascade.
func TestAnAdoptedDefinitionIsPlacedUnderItsStoredName(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedTwoNodeSource(t, backend, "src-adopt")
	seedRestoreSnapshot(t, backend, "src-adopt", "snap-adopt", []string{"node-a", "node-b"})

	plain, stopPlain := startServerWithStore(t, backend)

	code, rc := restoreAnswer(t, plain, "src-adopt", "snap-adopt",
		map[string]any{"to_resource": "dst-adopt", "nodes": []string{"node-a"}})
	stopPlain()

	if code != http.StatusCreated {
		t.Fatalf("first restore = %d %q, want 201", code, rc.Message)
	}

	misses := &atomic.Int32{}
	misses.Store(1)

	base, stop := startServerWithStore(t, adoptRacesTheRebind{Store: backend, misses: misses})
	defer stop()

	if code, rc := restoreAnswer(t, base, "src-adopt", "snap-adopt",
		map[string]any{"to_resource": "DST-ADOPT", "nodes": []string{"node-a", "node-b"}}); code != http.StatusCreated {
		t.Fatalf("restore in another case over the leftover = %d %q, want 201", code, rc.Message)
	}

	all, err := backend.Resources().List(ctx)
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	for i := range all {
		if all[i].Name == "DST-ADOPT" {
			t.Errorf("replica on %s stamped under the request's spelling", all[i].NodeName)
		}
	}
}

// The clone door's twin of TestAnAdoptedDefinitionIsPlacedUnderItsStoredName:
// a clone that adopts its leftover through a collided create goes on under
// the name the definition is stored with, so its prop edits land on it rather
// than failing over a name nothing is stored under.
func TestAnAdoptedCloneIsFinishedUnderItsStoredName(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedTwoNodeCloneSource(t, backend, "src-cadopt")

	plain, stopPlain := startServerWithStore(t, backend)

	if code := cloneOnce(t, plain, "src-cadopt", "dst-cadopt", nil); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stopPlain()

	if err := backend.Resources().Delete(ctx, "dst-cadopt", "node-b"); err != nil {
		t.Fatalf("leave the clone unfinished: %v", err)
	}

	if err := backend.Resources().Delete(ctx, "dst-cadopt", "node-a"); err != nil {
		t.Fatalf("leave the clone unfinished: %v", err)
	}

	misses := &atomic.Int32{}
	misses.Store(1)

	base, stop := startServerWithStore(t, adoptRacesTheRebind{Store: backend, misses: misses})
	defer stop()

	if code := cloneOnce(t, base, "src-cadopt", "DST-CADOPT",
		map[string]any{"override_props": map[string]string{"Aux/edit": "x"}}); code != http.StatusCreated {
		t.Fatalf("clone in another case over the leftover = %d, want 201", code)
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "dst-cadopt")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	if rd.Props["Aux/edit"] != "x" {
		t.Errorf("the clone's prop edit did not land on the stored definition: %v", rd.Props)
	}
}
