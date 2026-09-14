// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedCloneLeftover writes a definition carrying the clone marker of
// src→dst straight into the store, with a volume when withVolume is set and
// one replica on node-a when replicaFlags is non-nil.
func seedCloneLeftover(t *testing.T, st store.Store, src, dst string, withVolume bool, replicaFlags []string) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  dst,
		Props: map[string]string{"BlockstorRestoreFromSnapshot": src + ":" + cloneSnapshotName(dst)},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if withVolume {
		if err := st.VolumeDefinitions().Create(ctx, dst,
			&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
			t.Fatalf("seed the leftover's volume: %v", err)
		}
	}

	if replicaFlags != nil {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     dst,
			NodeName: "node-a",
			Flags:    replicaFlags,
		}); err != nil {
			t.Fatalf("seed the leftover's replica: %v", err)
		}
	}
}

// Both rollback steps that keep a definition used to run after the replicas
// were accepted for deletion, so their leftover's replicas all carry the stamp,
// for as long as the satellite finalizer holds them. Counting those as live
// answered a CSI retry 201 over a clone being torn down.
func TestRDCloneReplayRefusesALeftoverWhoseReplicasAreAllBeingDeleted(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, backend, "src-term", "grp-term", false)

	base, stop := startServerWithStore(t, failingRDDeleteStore{stampedButListedStore{backend}})
	defer stop()

	first := postClone(t, base, "src-term", map[string]any{"name": "dst-term", "use_zfs_clone": true})
	_ = first.Body.Close()

	if first.StatusCode != http.StatusInternalServerError {
		t.Fatalf("first attempt = %d, want 500 — the rollback's definition delete failed", first.StatusCode)
	}

	replicas, err := backend.Resources().ListByDefinition(ctx, "dst-term")
	if err != nil {
		t.Fatalf("list the leftover's replicas: %v", err)
	}

	if len(replicas) == 0 || slices.ContainsFunc(replicas, func(r apiv1.Resource) bool {
		return !slices.Contains(r.Flags, apiv1.ResourceFlagDelete)
	}) {
		t.Fatalf("the fixture must leave replicas that all carry DELETE, got %+v", replicas)
	}

	// The operator follows the correction and re-creates the group.
	if err := backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp-term"}); err != nil {
		t.Fatalf("re-create the group: %v", err)
	}

	replay := postClone(t, base, "src-term", map[string]any{"name": "dst-term", "use_zfs_clone": true})
	defer func() { _ = replay.Body.Close() }()

	if replay.StatusCode == http.StatusCreated {
		t.Fatal("replay = 201 over a clone whose every replica is being deleted")
	}

	rc := decodeCloneMessage(t, replay)
	if !strings.Contains(rc.Cause, "accepted for deletion") {
		t.Errorf("cause = %q, want it to say the replicas are being deleted", rc.Cause)
	}

	if strings.Contains(rc.Correc, "by hand") {
		t.Errorf("correc = %q, which tells the operator to delete what is already going", rc.Correc)
	}
}

// The stamp is the only thing that separates the two leftovers below, so the
// refusal above is about the stamp and not about the gate as such.
func TestRDCloneReplayOverASeededLeftoverTurnsOnTheDeletionStamp(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		flags   []string
		replay  bool
		dstName string
	}{
		{name: "stamped", flags: []string{apiv1.ResourceFlagDelete}, replay: false, dstName: "dst-seed-stamped"},
		{name: "unstamped", flags: []string{}, replay: true, dstName: "dst-seed-live"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedDeployedCloneSource(t, st, "src-seed")
			seedCloneLeftover(t, st, "src-seed", tc.dstName, true, tc.flags)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := postClone(t, base, "src-seed", map[string]any{"name": tc.dstName, "use_zfs_clone": true})
			_ = resp.Body.Close()

			if got := resp.StatusCode == http.StatusCreated; got != tc.replay {
				t.Errorf("status = %d, replay answered = %v, want %v", resp.StatusCode, got, tc.replay)
			}
		})
	}
}

// Only the replicas term of the wholeness check had a fixture. A leftover with
// a replica and no volume isolates the volumes term.
func TestRDCloneReplayRefusesALeftoverWithReplicasButNoVolumes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-novol")
	seedCloneLeftover(t, st, "src-novol", "dst-novol", false, []string{})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-novol", map[string]any{"name": "dst-novol", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusCreated {
		t.Fatal("replay = 201 over a leftover with no volumes")
	}

	if rc := decodeCloneMessage(t, resp); !strings.Contains(rc.Cause, "no volumes") {
		t.Errorf("cause = %q, want it to name the missing volumes", rc.Cause)
	}
}

// A leftover that stopped before creating any volume is not rollback debris,
// and the refusal must not tell a first attempt still running to delete itself.
func TestRDCloneReplayWordsAMarkerOnlyLeftoverForBothWaysItArises(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-bare")
	seedCloneLeftover(t, st, "src-bare", "dst-bare", false, nil)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-bare", map[string]any{"name": "dst-bare", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusCreated {
		t.Fatal("replay = 201 over a definition with no volumes and no replicas")
	}

	rc := decodeCloneMessage(t, resp)
	if strings.Contains(rc.Cause, "rollback") {
		t.Errorf("cause = %q, which blames a rollback that never ran", rc.Cause)
	}

	if !strings.Contains(rc.Correc, "still running") {
		t.Errorf("correc = %q, want it conditioned on no attempt still running", rc.Correc)
	}
}

// trailingReplicaListing hides the target's replicas from the first few
// listings, the skew between the definition informer that matched the marker
// and the resource informer answering the listing.
type trailingReplicaListing struct {
	store.ResourceStore

	target string
	hides  *atomic.Int32
}

func (l trailingReplicaListing) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	if rdName == l.target && l.hides.Add(-1) >= 0 {
		return nil, nil
	}

	replicas, err := l.ResourceStore.ListByDefinition(ctx, rdName)

	return replicas, errors.Wrap(err, "list through the trailing double")
}

type trailingReplicaListingStore struct {
	store.Store

	target string
	hides  *atomic.Int32
}

func (s trailingReplicaListingStore) Resources() store.ResourceStore {
	return trailingReplicaListing{ResourceStore: s.Store.Resources(), target: s.target, hides: s.hides}
}

// The wholeness reads carried no cache-retry budget while the group read four
// lines below did, so a complete clone whose replica listing trailed was told
// to delete itself.
func TestRDCloneReplayWaitsForAReplicaListingThatTrails(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-trail", "grp-trail", true)

	base, stop := startServerWithStore(t, backend)

	first := postClone(t, base, "src-trail", map[string]any{"name": "dst-trail", "use_zfs_clone": true})
	_ = first.Body.Close()

	stop()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", first.StatusCode)
	}

	hides := &atomic.Int32{}
	hides.Store(cacheRetryAttempts - 1)

	base2, stop2 := startServerWithStore(t, trailingReplicaListingStore{
		Store: backend, target: "dst-trail", hides: hides,
	})
	defer stop2()

	replay := postClone(t, base2, "src-trail", map[string]any{"name": "dst-trail", "use_zfs_clone": true})
	defer func() { _ = replay.Body.Close() }()

	if replay.StatusCode != http.StatusCreated {
		t.Errorf("replay = %d, want 201 — the clone is complete, its listing only trailed; message: %+v",
			replay.StatusCode, decodeCloneMessage(t, replay))
	}
}

// lateWitness lands an unstamped replica on the target on the first listing
// after the rollback's deletes have come back empty: an auto-tiebreaker the
// controller stamped moments after placement, which the cascade's back-to-back
// passes miss and the wait is the first to see.
type lateWitness struct {
	store.ResourceStore

	target  string
	deleted *atomic.Bool
	emptied *atomic.Bool
	landed  *atomic.Bool
}

func (l lateWitness) Delete(ctx context.Context, rdName, node string) error {
	if rdName == l.target {
		l.deleted.Store(true)
	}

	return errors.Wrap(l.ResourceStore.Delete(ctx, rdName, node), "delete through the late-witness double")
}

func (l lateWitness) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	replicas, err := l.ResourceStore.ListByDefinition(ctx, rdName)
	if err != nil || rdName != l.target || !l.deleted.Load() {
		return replicas, errors.Wrap(err, "list through the late-witness double")
	}

	if len(replicas) == 0 && l.emptied.CompareAndSwap(false, true) {
		return replicas, nil
	}

	if l.emptied.Load() && l.landed.CompareAndSwap(false, true) {
		if err := l.Create(ctx, &apiv1.Resource{Name: rdName, NodeName: "node-witness"}); err != nil {
			return nil, errors.Wrap(err, "land the witness")
		}

		replicas, err = l.ResourceStore.ListByDefinition(ctx, rdName)
	}

	return replicas, errors.Wrap(err, "list through the late-witness double")
}

type lateWitnessStore struct {
	store.Store

	target  string
	deleted *atomic.Bool
	emptied *atomic.Bool
	landed  *atomic.Bool
}

func (s lateWitnessStore) Resources() store.ResourceStore {
	return lateWitness{
		ResourceStore: s.Store.Resources(), target: s.target,
		deleted: s.deleted, emptied: s.emptied, landed: s.landed,
	}
}

// The wait only re-read, so a replica that became visible after the cascade
// was watched for the whole budget and never told to go, and the rollback gave
// up over a replica one delete would have removed.
func TestRDCloneRollbackReapsAReplicaThatLandsDuringTheWait(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, backend, "src-witness", "grp-witness-gone", false)

	st := lateWitnessStore{
		Store: backend, target: "dst-witness",
		deleted: &atomic.Bool{}, emptied: &atomic.Bool{}, landed: &atomic.Bool{},
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-witness", map[string]any{"name": "dst-witness", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if !st.landed.Load() {
		t.Fatal("the witness never landed, so this test proves nothing")
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — one delete removes the witness and finishes the rollback",
			resp.StatusCode)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-witness"); err == nil {
		t.Error("the definition survived a rollback that could have finished")
	}
}

var errVolumeCreateFailed = errors.New("probe: volume create failed")

// failingTargetVolumeCreates fails hydration of one definition, the failure
// after the marker-bearing definition already exists.
type failingTargetVolumeCreates struct {
	store.VolumeDefinitionStore

	target string
}

func (f failingTargetVolumeCreates) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if rdName == f.target {
		return errVolumeCreateFailed
	}

	return errors.Wrap(f.VolumeDefinitionStore.Create(ctx, rdName, vd), "create through the failing double")
}

type failingTargetVolumeCreateStore struct {
	store.Store

	target string
}

func (f failingTargetVolumeCreateStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return failingTargetVolumeCreates{VolumeDefinitionStore: f.Store.VolumeDefinitions(), target: f.target}
}

// The marker is stamped at RD-create and the error branch wrote a 500 without
// undoing anything, so every retry matched the marker, failed wholeness and was
// told to delete by hand a definition that was provably this clone's own debris.
func TestRDCloneRollsBackItsOwnPartialWorkWhenHydrationFails(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-hydrate")

	base, stop := startServerWithStore(t, failingTargetVolumeCreateStore{Store: backend, target: "dst-hydrate"})
	defer stop()

	resp := postClone(t, base, "src-hydrate", map[string]any{"name": "dst-hydrate", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-hydrate"); err == nil {
		t.Error("the half-made definition was left for every retry to trip over")
	}

	if rc := decodeCloneMessage(t, resp); !strings.Contains(rc.Message, "rolled back") {
		t.Errorf("message = %q, want it to say the partial clone was rolled back", rc.Message)
	}
}

// blockingTargetVolumeCreates holds hydration of one definition until the
// request's context ends, the shape of a CSI caller timing out mid-clone.
type blockingTargetVolumeCreates struct {
	store.VolumeDefinitionStore

	target string
}

func (b blockingTargetVolumeCreates) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if rdName == b.target {
		<-ctx.Done()

		return errors.Wrap(ctx.Err(), "hydrate through the blocking double")
	}

	return errors.Wrap(b.VolumeDefinitionStore.Create(ctx, rdName, vd), "create through the blocking double")
}

// contextHonouringRDDeletes refuses a delete on an ended context, as a real
// API client does; the in-memory store ignores the context altogether.
type contextHonouringRDDeletes struct {
	store.ResourceDefinitionStore
}

func (c contextHonouringRDDeletes) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "delete on an ended context")
	}

	return errors.Wrap(c.ResourceDefinitionStore.Delete(ctx, name), "delete through the context double")
}

type abandonedHydrationStore struct {
	store.Store

	target string
}

func (a abandonedHydrationStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return blockingTargetVolumeCreates{VolumeDefinitionStore: a.Store.VolumeDefinitions(), target: a.target}
}

func (a abandonedHydrationStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return contextHonouringRDDeletes{a.Store.ResourceDefinitions()}
}

// The likeliest way hydration fails is the caller going away, and a rollback on
// the request's own context fails on its first call for the same reason.
func TestRDCloneRollbackOfPartialWorkOutlivesTheRequest(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-abandon")

	base, stop := startServerWithStore(t, abandonedHydrationStore{Store: backend, target: "dst-abandon"})
	defer stop()

	raw, err := json.Marshal(map[string]any{"name": "dst-abandon", "use_zfs_clone": true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	reqCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		base+"/v1/resource-definitions/src-abandon/clone", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()

		t.Fatalf("the request finished with %d; the fixture needs it abandoned", resp.StatusCode)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := backend.ResourceDefinitions().Get(t.Context(), "dst-abandon"); errors.Is(err, store.ErrNotFound) {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Error("the half-made definition outlived an abandoned request")
}

var errTargetReadBlip = errors.New("probe: transient failure reading the target")

// blippingTargetRDReads fails the first read of one definition, which the
// pre-existence check treats as absent and proceeds past.
type blippingTargetRDReads struct {
	store.ResourceDefinitionStore

	target string
	blips  *atomic.Int32
}

func (b blippingTargetRDReads) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if name == b.target && b.blips.Add(-1) >= 0 {
		return apiv1.ResourceDefinition{}, errTargetReadBlip
	}

	rd, err := b.ResourceDefinitionStore.Get(ctx, name)

	return rd, errors.Wrap(err, "get through the blipping double")
}

type blippingTargetRDReadStore struct {
	store.Store

	target string
	blips  *atomic.Int32
}

func (b blippingTargetRDReadStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return blippingTargetRDReads{ResourceDefinitionStore: b.Store.ResourceDefinitions(), target: b.target, blips: b.blips}
}

// The rollback in the error branch undoes only what this request created. A
// definition that was there before it, another attempt's still running, must
// survive a request that failed on its create.
func TestRDCloneFailureDoesNotRollBackADefinitionItDidNotCreate(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-other")
	seedCloneLeftover(t, backend, "src-other", "dst-other", false, nil)

	blips := &atomic.Int32{}
	blips.Store(1)

	base, stop := startServerWithStore(t, blippingTargetRDReadStore{Store: backend, target: "dst-other", blips: blips})
	defer stop()

	resp := postClone(t, base, "src-other", map[string]any{"name": "dst-other", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		t.Fatalf("status = 201, but the create under an existing name cannot have succeeded")
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-other"); err != nil {
		t.Errorf("a definition this request did not create was deleted: %v", err)
	}
}

// countedRetainedDeletes accepts every delete, keeps the replica listed and
// unstamped, and counts the calls: a cache that never catches up.
type countedRetainedDeletes struct {
	store.ResourceStore

	calls *atomic.Int32
}

func (c countedRetainedDeletes) Delete(context.Context, string, string) error {
	c.calls.Add(1)

	return nil
}

type countedRetainedStore struct {
	store.Store

	calls *atomic.Int32
}

func (c countedRetainedStore) Resources() store.ResourceStore {
	return countedRetainedDeletes{ResourceStore: c.Store.Resources(), calls: c.calls}
}

// The wait deletes what it sees, but a replica already deleted also lists
// unstamped while the cache trails. Deleting on every poll would send the API
// server a hundred deletes per replica over one budget.
func TestRDCloneRollbackWaitTellsEachReplicaToGoOnce(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-once", "grp-once-gone", false)

	calls := &atomic.Int32{}

	base, stop := startServerWithStore(t, countedRetainedStore{Store: backend, calls: calls})
	defer stop()

	resp := postClone(t, base, "src-once", map[string]any{"name": "dst-once", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("status = 404 over replicas that were never stamped")
	}

	// One replica: the delete by name, one per cascade pass, one from the wait.
	if limit := int32(1 + store.CascadeDeleteMaxPasses + 1); calls.Load() > limit {
		t.Errorf("%d deletes for one replica, want at most %d", calls.Load(), limit)
	}
}
