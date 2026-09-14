// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func decodeCloneMessage(t *testing.T, resp *http.Response) apiv1.APICallRc {
	t.Helper()

	var envelope cloneStartedResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode the envelope: %v", err)
	}

	if envelope.Messages == nil || len(*envelope.Messages) == 0 {
		t.Fatal("empty envelope")
	}

	return (*envelope.Messages)[0]
}

// The resources listing trails the deletes the rollback just issued, and the
// cascade sleeps nowhere across its passes, so one read outruns the cache by
// construction. Deciding on that read refused a rollback whose replicas were
// going, and left the definition parented to a group that is gone.
func TestRDCloneRollbackWaitsForTheCacheToSeeItsDeletes(t *testing.T) {
	st := newLaggingStore(laggingDuration)
	ctx := t.Context()
	seedGroupedCloneSource(t, st, "src-lag", "grp-lag-gone", false)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-lag", map[string]any{"name": "dst-lag", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — the replicas were deleted, the cache only trailed it",
			resp.StatusCode)
	}

	if _, err := st.inner.ResourceDefinitions().Get(ctx, "dst-lag"); err == nil {
		t.Error("the definition survived a rollback whose replicas all went")
	}
}

// unlistedPlacements is the other direction of the same trail: a cache that has
// not yet seen the replicas this request placed a moment ago. It lists nothing
// for the target while every write still reaches the backend.
type unlistedPlacements struct {
	store.ResourceStore

	hidden string
}

func (u unlistedPlacements) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	if rdName == u.hidden {
		return nil, nil
	}

	replicas, err := u.ResourceStore.ListByDefinition(ctx, rdName)

	return replicas, errors.Wrap(err, "list through the unlisted-placement double")
}

type unlistedPlacementsStore struct {
	store.Store

	hidden string
}

func (u unlistedPlacementsStore) Resources() store.ResourceStore {
	return unlistedPlacements{ResourceStore: u.Store.Resources(), hidden: u.hidden}
}

// A cache that has not yet listed the placements makes the cascade delete
// nothing and the stranded check find nothing, so the definition used to go
// over live replicas that were never stamped — the orphan the rollback exists
// to prevent. Deleting what this request placed by name does not depend on
// any listing having caught up.
func TestRDCloneRollbackReapsReplicasTheCacheHasNotListedYet(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, backend, "src-unlisted", "grp-unlisted-gone", false)

	base, stop := startServerWithStore(t, unlistedPlacementsStore{Store: backend, hidden: "dst-unlisted"})
	defer stop()

	resp := postClone(t, base, "src-unlisted", map[string]any{"name": "dst-unlisted", "use_zfs_clone": true})
	_ = resp.Body.Close()

	replicas, err := backend.Resources().ListByDefinition(ctx, "dst-unlisted")
	if err != nil {
		t.Fatalf("list the backend's replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("%d replica(s) left on the backend after the rollback; the cache had not "+
			"listed them, so nothing reaped them", len(replicas))
	}
}

// stampedButListedDeletes is the ordinary cascade outcome in a cluster: the
// DELETE is accepted, the object stays behind its finalizer, and the listing
// shows it carrying the deletion stamp.
type stampedButListedDeletes struct {
	store.ResourceStore
}

func (s stampedButListedDeletes) Delete(ctx context.Context, rdName, node string) error {
	replica, err := s.Get(ctx, rdName, node)
	if err != nil {
		return errors.Wrap(err, "read the replica to stamp")
	}

	if !slices.Contains(replica.Flags, apiv1.ResourceFlagDelete) {
		replica.Flags = append(replica.Flags, apiv1.ResourceFlagDelete)
	}

	return errors.Wrap(s.Update(ctx, &replica), "stamp the replica")
}

type stampedButListedStore struct{ store.Store }

func (s stampedButListedStore) Resources() store.ResourceStore {
	return stampedButListedDeletes{s.Store.Resources()}
}

// A replica already stamped for deletion is not stranded: the stamp is what
// makes the finalizer run, whether or not the parent outlives it. No fixture
// presented one, so the term the stranded check turns on was unpinned.
func TestRDCloneRollbackProceedsOverReplicasAlreadyStampedForDeletion(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, backend, "src-stamped", "grp-stamped-gone", false)

	base, stop := startServerWithStore(t, stampedButListedStore{backend})
	defer stop()

	resp := postClone(t, base, "src-stamped", map[string]any{"name": "dst-stamped", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — every replica is stamped for deletion", resp.StatusCode)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-stamped"); err == nil {
		t.Error("the definition survived although every replica was accepted for deletion")
	}
}

// snapshotsOnTarget reports a snapshot on the clone target, the shape of one
// taken on it inside the rollback window.
type snapshotsOnTarget struct {
	store.SnapshotStore

	target string
}

func (s snapshotsOnTarget) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if rdName == s.target {
		return []apiv1.Snapshot{{Name: "snap-in-window", ResourceName: rdName}}, nil
	}

	snaps, err := s.SnapshotStore.ListByDefinition(ctx, rdName)

	return snaps, errors.Wrap(err, "list through the snapshots-on-target double")
}

type snapshotsOnTargetStore struct {
	store.Store

	target string
}

func (s snapshotsOnTargetStore) Snapshots() store.SnapshotStore {
	return snapshotsOnTarget{SnapshotStore: s.Store.Snapshots(), target: s.target}
}

// The rollback borrowed rd d's sweep without rd d's refusal. The sweep deletes
// every snapshot row under the definition, which is safe in the handler only
// because it refuses outright when snapshots exist.
func TestRDCloneRollbackRefusesOverASnapshotOnTheTarget(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, backend, "src-snapwin", "grp-snapwin-gone", false)

	base, stop := startServerWithStore(t, snapshotsOnTargetStore{Store: backend, target: "dst-snapwin"})
	defer stop()

	resp := postClone(t, base, "src-snapwin", map[string]any{"name": "dst-snapwin", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("status = 404 — the rollback went ahead over a snapshot on the target")
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-snapwin"); err != nil {
		t.Errorf("the definition was dropped over a snapshot: %v", err)
	}

	// And nothing under it was touched. The refusal's correction is to drop the
	// snapshots and retry, which only makes sense while the replicas are still
	// there: refusing after the reap leaves the target half torn down.
	replicas, err := backend.Resources().ListByDefinition(ctx, "dst-snapwin")
	if err != nil {
		t.Fatalf("list the target's replicas: %v", err)
	}

	if len(replicas) == 0 {
		t.Error("the replicas were reaped before the snapshot refusal fired")
	}

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDelete) {
			t.Errorf("replica on %s was stamped for deletion before the refusal", replicas[i].NodeName)
		}
	}

	if rc := decodeCloneMessage(t, resp); !strings.Contains(strings.ToLower(rc.Cause), "snapshot") {
		t.Errorf("cause = %q, want it to point at the snapshot", rc.Cause)
	}
}

// A failed rollback may reap every replica and still keep the definition, and
// both corrections tell the operator to re-create the group. The moment they
// do, a gate asking only about the group answered 201 for a clone on no node.
func TestRDCloneReplayRefusesALeftoverWhoseReplicasWereReaped(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, st, "src-reaped", "grp-reaped", true)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "dst-reaped",
		ResourceGroupName: "grp-reaped",
		Props: map[string]string{
			"BlockstorRestoreFromSnapshot": "src-reaped:" + cloneSnapshotName("dst-reaped"),
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-reaped",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the leftover's volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-reaped", map[string]any{"name": "dst-reaped", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		t.Error("replay answered 201 for a clone whose replicas were all reaped")
	}
}

// Refusing on an unreadable group costs nothing, since the CSI retry heals
// itself; a 201 binds a PV to a definition that may be parented to nothing.
func TestRDCloneReplayRefusesWhenTheParentGroupCannotBeRead(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-rgread", "grp-rgread", true)

	base, stop := startServerWithStore(t, backend)

	first := postClone(t, base, "src-rgread", map[string]any{"name": "dst-rgread", "use_zfs_clone": true})
	_ = first.Body.Close()

	stop()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", first.StatusCode)
	}

	base2, stop2 := startServerWithStore(t, failingRGReadStore{backend})
	defer stop2()

	replay := postClone(t, base2, "src-rgread", map[string]any{"name": "dst-rgread", "use_zfs_clone": true})
	_ = replay.Body.Close()

	if replay.StatusCode == http.StatusCreated {
		t.Error("replay answered 201 with the parent group unreadable")
	}
}

var errRDDeleteFailed = errors.New("delete the resource definition failed")

type failingRDDeletes struct{ store.ResourceDefinitionStore }

func (failingRDDeletes) Delete(context.Context, string) error { return errRDDeleteFailed }

type failingRDDeleteStore struct{ store.Store }

func (f failingRDDeleteStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return failingRDDeletes{f.Store.ResourceDefinitions()}
}

// The shell rollback threw its Delete error away and told the caller the clone
// was rolled back, over a shell still parented to a group that is gone.
func TestRDCloneOfAVolumelessSourceReportsAFailedShellDelete(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "src-shell-del",
		ResourceGroupName: "grp-shell-del-gone",
	}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, failingRDDeleteStore{backend})
	defer stop()

	resp := postClone(t, base, "src-shell-del", map[string]any{"name": "dst-shell-del"})
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		t.Error("status = 404 \"rolled back\" although the shell's delete failed")
	}
}

// One Cause used to be written for every rollback failure, pointing the
// operator at replicas when it was the definition's own delete that failed.
func TestRDCloneRollbackNamesTheStepThatFailed(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-rddel", "grp-rddel-gone", false)

	base, stop := startServerWithStore(t, failingRDDeleteStore{backend})
	defer stop()

	resp := postClone(t, base, "src-rddel", map[string]any{"name": "dst-rddel", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}

	rc := decodeCloneMessage(t, resp)
	if strings.Contains(rc.Cause, "replicas could not all be reaped") {
		t.Errorf("cause = %q, which blames replicas for a failed definition delete", rc.Cause)
	}
}

// The convergence wait after the rollback's own delete: without it a retry
// landing in the cache-lag window reads the pre-delete definition. Nothing
// pinned it, so removing it left the suite green.
func TestRDCloneRollbackWaitsForItsDefinitionDeleteToBeVisible(t *testing.T) {
	st := newLaggingStore(laggingDuration)
	seedGroupedCloneSource(t, st, "src-rdlag", "grp-rdlag-gone", false)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-rdlag", map[string]any{"name": "dst-rdlag", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}

	get := httpGet(t, base+"/v1/resource-definitions/dst-rdlag")
	_ = get.Body.Close()

	if get.StatusCode != http.StatusNotFound {
		t.Errorf("GET right after the rollback = %d, want 404 — the reply went out before "+
			"its own delete was visible", get.StatusCode)
	}
}
