// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	linstor "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func seedRestoreLeftover(t *testing.T, st store.Store, src, snap, target string, props map[string]string) {
	t.Helper()

	all := map[string]string{restoreFromSnapshotKey: restoreMarker(src, snap)}
	for k, v := range props {
		all[k] = v
	}

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: target, Props: all,
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}
}

func restoreAnswer(t *testing.T, base, src, snap string, body map[string]any) (int, apiv1.APICallRc) {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal restore body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+src+"/snapshot-restore-resource/"+snap, raw)
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the answer (status %d): %v", resp.StatusCode, err)
	}

	// golinstor's ApiCallError.Is matches a band on any rc of the answer, so
	// the band a test checks is every rc's, folded into the first.
	rc := rcs[0]
	for _, more := range rcs[1:] {
		rc.RetCode |= more.RetCode
	}

	return resp.StatusCode, rc
}

// The restore resumed a volume-less leftover whose only replica was going
// before anything looked at its replicas: a bare restore answered 201 with a
// volume hydrated into the tear-down, a node-named one hydrated first and was
// stopped only by the stamp.
func TestSnapshotRestoreResumeRefusesAVolumeLessLeftoverBeingTornDown(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "bare", body: map[string]any{"to_resource": "dst-vl-bare"}},
		{name: "node-named", body: map[string]any{"to_resource": "dst-vl-named", "node_names": []string{"node-a"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()
			target, _ := tc.body["to_resource"].(string)
			seedDeployedCloneSource(t, st, "src-vl")
			seedRestoreSnapshot(t, st, "src-vl", "snap-vl", []string{"node-a"})
			seedRestoreLeftover(t, st, "src-vl", "snap-vl", target, nil)
			seedTerminatingReplica(t, st, target, "node-a")

			base, stop := startServerWithStore(t, st)
			defer stop()

			code, rc := restoreAnswer(t, base, "src-vl", "snap-vl", tc.body)
			if code != http.StatusConflict || !strings.Contains(rc.Message, "torn down") {
				t.Errorf("resume over a leftover being torn down = %d %q, want the tear-down refusal", code, rc.Message)
			}

			if vds, _ := st.VolumeDefinitions().List(ctx, target); len(vds) > 0 {
				t.Errorf("the resume hydrated %d volume(s) into a leftover being torn down", len(vds))
			}
		})
	}
}

// The shape arms returned before the replicas were read, so a multi-volume
// leftover missing a snapshot volume was resumed into its tear-down too.
func TestSnapshotRestoreResumeRefusesAPartialLeftoverBeingTornDown(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-pl")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "snap-pl", ResourceName: "src-pl", Nodes: []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64 * 1024}, {VolumeNumber: 1, SizeKib: 64 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	seedRestoreLeftover(t, st, "src-pl", "snap-pl", "dst-pl", nil)

	if err := st.VolumeDefinitions().Create(ctx, "dst-pl",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the first volume: %v", err)
	}

	seedTerminatingReplica(t, st, "dst-pl", "node-a")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code, rc := restoreAnswer(t, base, "src-pl", "snap-pl",
		map[string]any{"to_resource": "dst-pl"}); code != http.StatusConflict {
		t.Errorf("resume over a partial leftover being torn down = %d %q, want 409", code, rc.Message)
	}

	if _, err := st.VolumeDefinitions().Get(ctx, "dst-pl", 1); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the missing volume was hydrated into the tear-down: %v", err)
	}
}

// One leftover that is both being torn down and carries an abandoned-rollback
// mark got opposite advice: wait on one door, delete by hand on the other. The
// tear-down refusal is the more precise one, and both doors give it.
func TestTearDownOutranksTheAbandonedRollbackMarkOnBothDoors(t *testing.T) {
	t.Parallel()

	mark := map[string]string{rollbackAbandonedKey: "replicas"}

	t.Run("restore", func(t *testing.T) {
		t.Parallel()

		st := store.NewInMemory()
		seedDeployedCloneSource(t, st, "src-om")
		seedRestoreSnapshot(t, st, "src-om", "snap-om", []string{"node-a"})
		seedRestoreLeftover(t, st, "src-om", "snap-om", "dst-om", mark)
		seedTerminatingReplica(t, st, "dst-om", "node-a")

		base, stop := startServerWithStore(t, st)
		defer stop()

		if _, rc := restoreAnswer(t, base, "src-om", "snap-om",
			map[string]any{"to_resource": "dst-om"}); !strings.Contains(rc.Message, "torn down") {
			t.Errorf("restore answer %q is not the tear-down refusal", rc.Message)
		}
	})

	t.Run("clone", func(t *testing.T) {
		t.Parallel()

		st := store.NewInMemory()
		seedDeployedCloneSource(t, st, "src-oc")
		seedRestoreLeftover(t, st, "src-oc", cloneSnapshotName("dst-oc"), "dst-oc", mark)
		seedTerminatingReplica(t, st, "dst-oc", "node-a")

		base, stop := startServerWithStore(t, st)
		defer stop()

		resp := postClone(t, base, "src-oc", map[string]any{"name": "dst-oc"})
		defer func() { _ = resp.Body.Close() }()

		if rc := decodeCloneMessage(t, resp); !strings.Contains(rc.Message, "torn down") {
			t.Errorf("clone answer %q is not the tear-down refusal", rc.Message)
		}
	})
}

// replicaReadsMiss answers every cached replica Get with NotFound, the way a
// cache that has not seen a replica's create does, while the replica is there
// and the API server answers for it.
type replicaReadsMiss struct{ store.Store }

type replicaReadsMissResources struct{ store.ResourceStore }

func (r replicaReadsMiss) Resources() store.ResourceStore {
	return replicaReadsMissResources{r.Store.Resources()}
}

func (replicaReadsMissResources) Get(_ context.Context, rdName, node string) (apiv1.Resource, error) {
	return apiv1.Resource{}, errors.Wrapf(store.ErrNotFound, "resource %s/%s", rdName, node)
}

func (r replicaReadsMissResources) GetUncached(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	return r.ResourceStore.Get(ctx, rdName, node) //nolint:wrapcheck // pass-through test double
}

// A replica the Create reports as existing and a lagging cache cannot find was
// taken as a failed materialisation: a fresh restore was rolled back, a resume
// answered 404 for a volume that is fine. The deciding read goes to the API
// server, which has it.
func TestSnapshotRestoreTakesAnUnreadableExistingReplicaAsPlaced(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-rm")
	seedRestoreSnapshot(t, backend, "src-rm", "snap-rm", []string{"node-a"})

	if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "dst-rm", NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the live replica: %v", err)
	}

	base, stop := startServerWithStore(t, replicaReadsMiss{backend})
	defer stop()

	if code := restoreOnce(t, base, "src-rm", "snap-rm", map[string]any{
		"to_resource": "dst-rm", "node_names": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Errorf("restore over a live replica the cache cannot read yet = %d, want 201", code)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-rm"); err != nil {
		t.Errorf("the restore was rolled back over a replica that is fine: %v", err)
	}
}

// The refusal over a replica still being deleted carries no FAIL_EXISTS_RSC
// band: the replica is being reaped, not made by a concurrent restore, and
// the 409 stays a retryable failure.
func TestSnapshotRestoreRefusalOverADeletingReplicaCarriesNoExistsBand(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-band")
	seedRestoreSnapshot(t, st, "src-band", "snap-band", []string{"node-a", "node-b"})
	seedRestoreLeftover(t, st, "src-band", "snap-band", "dst-band", nil)

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{Name: "dst-band", NodeName: "node-b"}); err != nil {
		t.Fatalf("seed the live replica: %v", err)
	}

	seedTerminatingReplica(t, st, "dst-band", "node-a")

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-band", "snap-band", map[string]any{
		"to_resource": "dst-band", "node_names": []string{"node-a"},
	})
	if code != http.StatusConflict {
		t.Fatalf("restore stamping over a replica still being deleted = %d %q, want 409", code, rc.Message)
	}

	// Asked the way linstor-csi asks it, through golinstor's own match, and
	// by this server's own band, which golinstor reads once the numbering
	// matches upstream's.
	if (&lapi.ApiCallRc{RetCode: rc.RetCode}).Is(linstor.FailExistsRsc) ||
		rc.RetCode&apiCallRcFailExistsRsc == apiCallRcFailExistsRsc {
		t.Errorf("refusal ret_code %#x carries the FAIL_EXISTS_RSC band linstor-csi takes as success", rc.RetCode)
	}
}

// A diskless replica already on the requested node holds no copy of the data.
// Counted as the restore's own placement, the restore answered 201 with no
// data on any node; it is refused, without the FAIL_EXISTS band linstor-csi
// takes as success.
func TestSnapshotRestoreDoesNotCountADisklessReplicaAsPlaced(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-dl")
	seedRestoreSnapshot(t, st, "src-dl", "snap-dl", []string{"node-a"})
	seedRestoreLeftover(t, st, "src-dl", "snap-dl", "dst-dl", nil)

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "dst-dl", NodeName: "node-a", Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed the diskless replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-dl", "snap-dl", map[string]any{
		"to_resource": "dst-dl", "node_names": []string{"node-a"},
	})
	if code != http.StatusConflict {
		t.Fatalf("restore onto a node holding a diskless replica = %d %q, want 409", code, rc.Message)
	}

	if (&lapi.ApiCallRc{RetCode: rc.RetCode}).Is(linstor.FailExistsRsc) ||
		rc.RetCode&apiCallRcFailExistsRsc == apiCallRcFailExistsRsc {
		t.Errorf("refusal ret_code %#x carries the FAIL_EXISTS_RSC band linstor-csi takes as success", rc.RetCode)
	}
}

// The controller places a tie-breaker witness once two replicas with a disk
// exist, so a restore placing onto three nodes can meet it on the third. It is
// promoted to the replica the restore asked for, as autoplace promotes one.
func TestSnapshotRestorePromotesATieBreakerOnARequestedNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-tb")
	seedRestoreSnapshot(t, st, "src-tb", "snap-tb", []string{"node-a"})
	seedRestoreLeftover(t, st, "src-tb", "snap-tb", "dst-tb", nil)

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "dst-tb", NodeName: "node-a",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed the witness: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-tb", "snap-tb", map[string]any{
		"to_resource": "dst-tb", "node_names": []string{"node-a"},
	})
	if code != http.StatusCreated {
		t.Fatalf("restore onto a node holding the witness = %d %q, want 201", code, rc.Message)
	}

	res, err := st.Resources().Get(t.Context(), "dst-tb", "node-a")
	if err != nil {
		t.Fatalf("read the replica: %v", err)
	}

	if !store.HoldsData(&res) {
		t.Errorf("the witness was counted as placed without a disk: flags %v", res.Flags)
	}
}

// A witness whose promotion finds no pool to take stays diskless, and is then
// refused like any replica without a disk rather than counted as placed.
func TestSnapshotRestoreRefusesAWitnessItCouldNotGiveADisk(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-nopool")

	src, err := st.Resources().Get(ctx, "src-nopool", "node-a")
	if err != nil {
		t.Fatalf("read the source replica: %v", err)
	}

	src.Props = nil
	if err := st.Resources().Update(ctx, &src); err != nil {
		t.Fatalf("leave the source without a recorded pool: %v", err)
	}

	seedRestoreSnapshot(t, st, "src-nopool", "snap-nopool", []string{"node-a"})
	seedRestoreLeftover(t, st, "src-nopool", "snap-nopool", "dst-nopool", nil)

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-nopool", NodeName: "node-a",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed the witness: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-nopool", "snap-nopool", map[string]any{
		"to_resource": "dst-nopool", "node_names": []string{"node-a"},
	})
	if code != http.StatusConflict {
		t.Errorf("restore onto a witness no pool could be found for = %d %q, want 409", code, rc.Message)
	}

	// The refusal leaves the witness as it was: stripped of TIE_BREAKER it
	// would read as an operator's diskless replica on every retry.
	witness, err := st.Resources().Get(ctx, "dst-nopool", "node-a")
	if err != nil {
		t.Fatalf("read the witness: %v", err)
	}

	if !slices.Contains(witness.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("the refused witness lost its TIE_BREAKER flag: %v", witness.Flags)
	}
}

// A witness whose promotion fails is not counted as placed: the restore is
// refused, without the FAIL_EXISTS band, and the witness keeps its flags.
func TestSnapshotRestoreRefusesAWitnessItFailedToPromote(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-tbp")
	seedRestoreSnapshot(t, st, "src-tbp", "snap-tbp", []string{"node-a"})
	seedRestoreLeftover(t, st, "src-tbp", "snap-tbp", "dst-tbp", nil)

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-tbp", NodeName: "node-a",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed the witness: %v", err)
	}

	base, stop := startServerWithStore(t, faultyStore{
		Store: st, f: storeFaults{target: "dst-tbp", replicaPatch: errors.New("apiserver refused the patch")},
	})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-tbp", "snap-tbp", map[string]any{
		"to_resource": "dst-tbp", "node_names": []string{"node-a"},
	})
	if code != http.StatusInternalServerError {
		t.Fatalf("restore over a witness whose promotion failed = %d %q, want 500", code, rc.Message)
	}

	if (&lapi.ApiCallRc{RetCode: rc.RetCode}).Is(linstor.FailExistsRsc) ||
		rc.RetCode&apiCallRcFailExistsRsc == apiCallRcFailExistsRsc {
		t.Errorf("refusal ret_code %#x carries the FAIL_EXISTS_RSC band linstor-csi takes as success", rc.RetCode)
	}

	witness, err := st.Resources().Get(ctx, "dst-tbp", "node-a")
	if err != nil {
		t.Fatalf("read the witness: %v", err)
	}

	if !slices.Contains(witness.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("the witness lost its TIE_BREAKER flag: %v", witness.Flags)
	}
}

// The authoritative read finding nothing means the cached read that found
// the leftover was stale: there is no mark to refuse over.
func TestAbandonedRollbackGateProceedsWhenTheDefinitionIsGone(t *testing.T) {
	t.Parallel()

	s := &Server{Store: store.NewInMemory()}

	if status, refusal := s.abandonedRollbackRefusal(t.Context(), "restore", "dst-gone"); refusal != nil {
		t.Errorf("gate over a definition that is gone = %d %q, want no refusal", status, refusal.Message)
	}
}

func seedTerminatingReplica(t *testing.T, st store.Store, rdName, node string) {
	t.Helper()

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: rdName, NodeName: node, Flags: []string{apiv1.ResourceFlagDelete},
	}); err != nil {
		t.Fatalf("seed the terminating replica: %v", err)
	}
}

// A volume-less leftover was judged unfinished before its replicas were looked
// at, and the resume then counted a replica mid-tear-down as placed: 201 over
// a definition with no live replica.
func TestRDCloneResumeRefusesAVolumeLessLeftoverWhoseReplicaIsGoing(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-na")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-na",
		Props: map[string]string{restoreFromSnapshotKey: restoreMarker("src-na", cloneSnapshotName("dst-na"))},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	seedTerminatingReplica(t, st, "dst-na", "node-a")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-na", "dst-na", nil); code == http.StatusCreated {
		t.Errorf("resume over a leftover whose only replica is being torn down = 201")
	}

	// Refused before the resume writes anything: hydrating volumes into a
	// definition its tear-down is reaping races that tear-down.
	if vds, _ := st.VolumeDefinitions().List(ctx, "dst-na"); len(vds) > 0 {
		t.Errorf("the resume ran over a leftover being torn down: %d volume(s) hydrated", len(vds))
	}
}

// A fresh clone under a name whose old replica is still held by its finalizer
// met AlreadyExists on the stamp, took it for its own placement, and reported
// the clone done over a definition with no live replica.
func TestRDCloneDoesNotCountATerminatingReplicaAsPlaced(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-nb")
	seedTerminatingReplica(t, st, "dst-nb", "node-a")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-nb", "dst-nb", nil); code == http.StatusCreated {
		t.Errorf("clone over a replica still being torn down = 201")
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-nb"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the clone this request created was not rolled back: %v", err)
	}
}

// The restore writes the abandoned-rollback mark through the same failed
// materialisation the clone does, and resumed the leftover its twin refuses.
func TestSnapshotRestoreResumeRefusesALeftoverWhoseRollbackGaveUp(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-rgu")
	seedRestoreSnapshot(t, st, "src-rgu", "snap-rgu", []string{"node-a"})

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-rgu",
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-rgu", "snap-rgu"),
			rollbackAbandonedKey:   "replicas",
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-rgu", "snap-rgu",
		map[string]any{"to_resource": "dst-rgu"}); code != http.StatusConflict {
		t.Errorf("restore resume over a leftover whose rollback gave up = %d, want 409", code)
	}

	if vds, _ := st.VolumeDefinitions().List(ctx, "dst-rgu"); len(vds) > 0 {
		t.Errorf("the resume ran over the marked leftover: %d volume(s) hydrated", len(vds))
	}
}

var errReplicaWriteRefused = errors.New("replica write refused")

// replicaCreateFails refuses every replica write, the way an apiserver that
// times out on the stamp does.
type replicaCreateFails struct{ store.ResourceStore }

func (replicaCreateFails) Create(context.Context, *apiv1.Resource) error {
	return errReplicaWriteRefused
}

type replicaCreateFailsStore struct{ store.Store }

func (r replicaCreateFailsStore) Resources() store.ResourceStore {
	return replicaCreateFails{r.Store.Resources()}
}

// The restore resumes a leftover an earlier attempt left. A failure while it
// does is not this request's partial work to undo: the definition was there
// before it, and rolling it back takes another attempt's work with it.
func TestSnapshotRestoreResumeFailureLeavesTheAdoptedLeftover(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-adopt")
	seedRestoreSnapshot(t, backend, "src-adopt", "snap-adopt", []string{"node-a"})

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-adopt",
		Props: map[string]string{restoreFromSnapshotKey: restoreMarker("src-adopt", "snap-adopt")},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, replicaCreateFailsStore{backend})
	defer stop()

	code := restoreOnce(t, base, "src-adopt", "snap-adopt", map[string]any{
		"to_resource": "dst-adopt", "node_names": []string{"node-a"},
	})
	if code == http.StatusCreated {
		t.Fatalf("resume with every replica write refused = 201")
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-adopt"); err != nil {
		t.Errorf("the adopted leftover was rolled back by a request that did not create it: %v", err)
	}
}

// A leftover whose rollback gave up may hold less than the clone intended,
// and finishing it is exactly what the mark exists to stop. The resume is
// refused as the replay of a finished one is.
func TestRDCloneResumeRefusesALeftoverWhoseRollbackGaveUp(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-gaveup")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-gaveup",
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-gaveup", cloneSnapshotName("dst-gaveup")),
			rollbackAbandonedKey:   "replicas",
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-gaveup", "dst-gaveup", nil); code != http.StatusConflict {
		t.Errorf("resume over a leftover whose rollback gave up = %d, want 409", code)
	}

	if vds, _ := st.VolumeDefinitions().List(ctx, "dst-gaveup"); len(vds) > 0 {
		t.Errorf("the resume ran over the marked leftover: %d volume(s) hydrated", len(vds))
	}
}

// The replay of a finished restore answers for a leftover the same way the
// resume does: one parented to a group that is gone is refused, not reported
// restored.
func TestSnapshotRestoreReplayOfAFinishedLeftoverChecksItsGroup(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-fin")
	seedRestoreSnapshot(t, st, "src-fin", "snap-fin", []string{"node-a"})

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "dst-fin",
		ResourceGroupName: "grp-gone",
		Props:             map[string]string{restoreFromSnapshotKey: restoreMarker("src-fin", "snap-fin")},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-fin",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the leftover's volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code := restoreOnce(t, base, "src-fin", "snap-fin", map[string]any{"to_resource": "dst-fin"})
	if code == http.StatusCreated {
		t.Errorf("replay of a finished restore parented to a group that is gone = 201")
	}
}

// A rollback that has not reported a step may still be running, and ends on its
// own. The replay gate read the in-progress mark as an unknown step and told
// the operator the compensation could not complete and to delete by hand.
func TestRDCloneResumeWordsARollbackStillInProgress(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-rolling")

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "dst-rolling",
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-rolling", cloneSnapshotName("dst-rolling")),
			rollbackAbandonedKey:   rollbackInProgress,
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-rolling", map[string]any{"name": "dst-rolling"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("resume over a rollback in progress = %d, want 409", resp.StatusCode)
	}

	rc := decodeCloneMessage(t, resp)
	if !strings.Contains(rc.Correc, "once that rollback has finished") {
		t.Errorf("correction %q does not say the rollback may still finish", rc.Correc)
	}
}

// The clone door gives the same typed answer the restore door does when the
// replica it stamps is still being deleted, instead of a bare 500.
func TestRDCloneRefusalOverADeletingReplicaCarriesNoExistsBand(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-cband")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-cband",
		Props: map[string]string{restoreFromSnapshotKey: restoreMarker("src-cband", cloneSnapshotName("dst-cband"))},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "dst-cband", NodeName: "node-b"}); err != nil {
		t.Fatalf("seed the live replica: %v", err)
	}

	seedTerminatingReplica(t, st, "dst-cband", "node-a")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-cband", map[string]any{"name": "dst-cband"})
	defer func() { _ = resp.Body.Close() }()

	rc := decodeCloneMessage(t, resp)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("clone stamping over a replica still being deleted = %d %q, want 409", resp.StatusCode, rc.Message)
	}

	// Asked the way linstor-csi asks it, through golinstor's own match, and
	// by this server's own band, which golinstor reads once the numbering
	// matches upstream's.
	if (&lapi.ApiCallRc{RetCode: rc.RetCode}).Is(linstor.FailExistsRsc) ||
		rc.RetCode&apiCallRcFailExistsRsc == apiCallRcFailExistsRsc {
		t.Errorf("refusal ret_code %#x carries the FAIL_EXISTS_RSC band linstor-csi takes as success", rc.RetCode)
	}
}

// A witness the restore promoted is the restore's replica, and a rollback of
// that restore deletes it like one it created.
func TestAPromotedWitnessIsCountedAsPlaced(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-tbc")
	seedRestoreSnapshot(t, st, "src-tbc", "snap-tbc", []string{"node-a"})
	seedRestoreLeftover(t, st, "src-tbc", "snap-tbc", "dst-tbc", nil)

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "dst-tbc", NodeName: "node-a",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed the witness: %v", err)
	}

	created, err := (&Server{Store: st}).stampRestoredReplica(t.Context(), &apiv1.Resource{
		Name: "dst-tbc", NodeName: "node-a", Props: map[string]string{"StorPoolName": "zfs-thin"},
	})
	if err != nil {
		t.Fatalf("stamp over the witness: %v", err)
	}

	if !created {
		t.Error("the promoted witness was not counted as placed, so a rollback would leave it behind")
	}
}

// A replica with a disk already on the node is a previous attempt's, not this
// call's: counted as placed, a rollback of this call would delete it.
func TestAnExistingReplicaIsNotCountedAsPlaced(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-ex")
	seedRestoreSnapshot(t, st, "src-ex", "snap-ex", []string{"node-a"})
	seedRestoreLeftover(t, st, "src-ex", "snap-ex", "dst-ex", nil)

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "dst-ex", NodeName: "node-a", Props: map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed the earlier attempt's replica: %v", err)
	}

	created, err := (&Server{Store: st}).stampRestoredReplica(t.Context(), &apiv1.Resource{
		Name: "dst-ex", NodeName: "node-a", Props: map[string]string{"StorPoolName": "zfs-thin"},
	})
	if err != nil {
		t.Fatalf("stamp over the existing replica: %v", err)
	}

	if created {
		t.Error("an existing replica was counted as this call's, so a rollback would delete it")
	}
}
