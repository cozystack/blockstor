// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errHeldStampFailed = errors.New("held replica create failed")

// firstStampHeld holds the first replica Create until released, then fails
// it: the first attempt at an operation stalls on its stamp while a retry runs
// to completion, and only then fails.
type firstStampHeld struct {
	store.Store

	calls   *atomic.Int32
	entered chan struct{}
	release chan struct{}
}

type firstStampHeldResources struct {
	store.ResourceStore

	outer firstStampHeld
}

func (f firstStampHeld) Resources() store.ResourceStore {
	return firstStampHeldResources{ResourceStore: f.Store.Resources(), outer: f}
}

func (r firstStampHeldResources) Create(ctx context.Context, res *apiv1.Resource) error {
	if r.outer.calls.Add(1) == 1 {
		close(r.outer.entered)
		<-r.outer.release

		return errHeldStampFailed
	}

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

func newFirstStampHeld(backend store.Store) firstStampHeld {
	return firstStampHeld{
		Store: backend, calls: &atomic.Int32{},
		entered: make(chan struct{}), release: make(chan struct{}),
	}
}

// A retry adopts the definition an earlier attempt created while that attempt
// is still running, and answers 201. The earlier attempt then failed, and its
// rollback cascaded a delete over everything under the name: the caller held
// a 201 for a volume that no longer existed.
func TestTheCreatorsRollbackLeavesWhatARetryAdoptedAndAnsweredFor(t *testing.T) {
	t.Parallel()

	for _, door := range []struct {
		name   string
		target string
		post   func(t *testing.T, base string) int
	}{
		{name: "clone", target: "dst-adopt-c", post: func(t *testing.T, base string) int {
			t.Helper()

			return cloneOnce(t, base, "src-adopt-c", "dst-adopt-c", nil)
		}},
		{name: "restore", target: "dst-adopt-r", post: func(t *testing.T, base string) int {
			t.Helper()

			raw, err := json.Marshal(map[string]any{"to_resource": "dst-adopt-r", "node_names": []string{"node-a"}})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPost(t, base+"/v1/resource-definitions/src-adopt-r/snapshot-restore-resource/snap-adopt-r", raw)
			_ = resp.Body.Close()

			return resp.StatusCode
		}},
	} {
		t.Run(door.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()
			src := "src-adopt-" + door.name[:1]
			seedDeployedCloneSource(t, backend, src)
			seedRestoreSnapshot(t, backend, src, "snap-adopt-"+door.name[:1], []string{"node-a"})

			held := newFirstStampHeld(backend)

			base, stop := startServerWithStore(t, held)
			defer stop()

			first := make(chan int, 1)

			go func() { first <- door.post(t, base) }()

			<-held.entered

			if code := door.post(t, base); code != http.StatusCreated {
				t.Fatalf("the retry that adopted the leftover = %d, want 201", code)
			}

			close(held.release)

			if code := <-first; code == http.StatusCreated {
				t.Fatalf("the first attempt, whose stamp failed, = 201")
			}

			if _, err := backend.ResourceDefinitions().Get(ctx, door.target); err != nil {
				t.Fatalf("the definition the retry answered 201 for is gone: %v", err)
			}

			if replicas, _ := backend.Resources().ListByDefinition(ctx, door.target); len(replicas) == 0 {
				t.Errorf("the replica the retry answered 201 for is gone")
			}
		})
	}
}

// An adoption that finds the creator's rollback already started refuses
// rather than writing into a definition that is being taken apart.
func TestAnAdoptionRefusesALeftoverWhoseCreatorIsRollingBack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-rolling", Props: map[string]string{rollbackAbandonedKey: rollbackInProgress},
	}); err != nil {
		t.Fatalf("seed the definition: %v", err)
	}

	s := &Server{Store: st}

	if err := s.claimAdoptedLeftover(ctx, "dst-rolling", &apiv1.Snapshot{}); !errors.Is(err, errAdoptedLeftoverRollingBack) {
		t.Errorf("adoption over a rollback in progress = %v, want a refusal", err)
	}
}

// secondStampHeld holds the stamp of one node's replica of one definition until
// released, then fails it: a clone has placed its first replica, and so reads
// as finished, while the attempt that created it is still stamping the next.
type secondStampHeld struct {
	store.Store

	target, node     string
	entered, release chan struct{}
}

type secondStampHeldResources struct {
	store.ResourceStore

	outer secondStampHeld
}

func (h secondStampHeld) Resources() store.ResourceStore {
	return secondStampHeldResources{ResourceStore: h.Store.Resources(), outer: h}
}

func (r secondStampHeldResources) Create(ctx context.Context, res *apiv1.Resource) error {
	if res.Name == r.outer.target && res.NodeName == r.outer.node {
		select {
		case r.outer.entered <- struct{}{}:
			<-r.outer.release

			return errHeldStampFailed
		default:
		}
	}

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

// A replay and the status poll answer a clone finished the moment it has its
// volumes and a replica, and neither adopts it, since the answer writes
// nothing. That moment comes while the attempt that created it is still
// stamping its other replicas, so when the next stamp fails the rollback has
// to see the clone the way they did: deleting it took a volume the driver had
// just been told exists.
func TestTheCreatorsRollbackLeavesACloneAReplayAnsweredFinished(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedTwoNodeCloneSource(t, backend, "src-answered")

	held := secondStampHeld{
		Store: backend, target: "dst-answered", node: "node-b",
		entered: make(chan struct{}), release: make(chan struct{}),
	}

	base, stop := startServerWithStore(t, held)
	defer stop()

	first := make(chan int, 1)

	go func() { first <- cloneOnce(t, base, "src-answered", "dst-answered", nil) }()

	<-held.entered

	if code := cloneOnce(t, base, "src-answered", "dst-answered", nil); code != http.StatusCreated {
		t.Fatalf("replay while the first attempt stamps node-b = %d, want 201", code)
	}

	if _, got := cloneStatusOnce(t, base+"/v1/resource-definitions/src-answered/clone/dst-answered"); got != "COMPLETE" {
		t.Fatalf("status poll while the first attempt stamps node-b = %q, want COMPLETE", got)
	}

	close(held.release)

	if code := <-first; code != http.StatusInternalServerError {
		t.Errorf("first attempt whose node-b stamp failed = %d, want 500", code)
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "dst-answered")
	if err != nil {
		t.Fatalf("the clone a replay answered 201 for was rolled back: %v", err)
	}

	if mark := rd.Props[store.RollbackAbandonedProp]; mark != "" {
		t.Errorf("the clone left in place carries rollback mark %q, which refuses every retry", mark)
	}

	if code := cloneOnce(t, base, "src-answered", "dst-answered", nil); code != http.StatusCreated {
		t.Errorf("replay after the first attempt failed = %d, want 201", code)
	}
}

// A placed restore whose node-b replica fails has placed node-a, so it already
// reads as finished and the rollback leaves it. A retry judges it finished too
// and places nothing, so the refusal names the replica it never placed and the
// command that places it.
func TestTheRestoreLeftShortNamesTheReplicaItNeverPlaced(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedTwoNodeSource(t, backend, "src-short")
	seedRestoreSnapshot(t, backend, "src-short", "snap-short", []string{"node-a", "node-b"})

	base, stop := startServerWithStore(t, halfPlacingStore{Store: backend, target: "dst-short"})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-short", "snap-short", map[string]any{
		"to_resource": "dst-short", "nodes": []string{"node-a", "node-b"},
	})
	if code != http.StatusInternalServerError {
		t.Fatalf("restore whose node-b replica failed = %d %q, want 500", code, rc.Message)
	}

	if !strings.Contains(rc.Correc, "no replica on node-b") || !strings.Contains(rc.Correc, "linstor resource create") {
		t.Errorf("correction %q does not name the replica the restore never placed", rc.Correc)
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "dst-short")
	if err != nil {
		t.Fatalf("the restore a retry may have answered for was rolled back: %v", err)
	}

	if mark := rd.Props[store.RollbackAbandonedProp]; mark != "" {
		t.Errorf("the definition left in place carries rollback mark %q", mark)
	}
}

// The scope decides what a failed materialisation's rollback spares, and both
// sides of it are pinned: a definition that reads as finished on the door's
// own terms is left, one that does not is deleted.
func TestTheRollbackScopeSparesOnlyWhatReadsAsFinished(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		scope     rollbackScope
		replica   bool
		wantSpare bool
	}{
		{name: "bare-restore-hydrated", scope: rollbackUnlessHydrated, wantSpare: true},
		{name: "placed-without-a-replica", scope: rollbackUnlessPlaced},
		{name: "placed-with-a-replica", scope: rollbackUnlessPlaced, replica: true, wantSpare: true},
		{name: "rg-deleted", scope: rollbackEvenIfFinished, replica: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "dst-scope"}); err != nil {
				t.Fatalf("seed the definition: %v", err)
			}

			if err := backend.VolumeDefinitions().Create(ctx, "dst-scope",
				&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
				t.Fatalf("seed its volume: %v", err)
			}

			var placed []string

			if tc.replica {
				if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "dst-scope", NodeName: "node-a"}); err != nil {
					t.Fatalf("seed its replica: %v", err)
				}

				placed = []string{"node-a"}
			}

			err := (&Server{Store: backend}).rollBackCompensating(ctx, "dst-scope", placed, tc.scope)

			_, getErr := backend.ResourceDefinitions().Get(ctx, "dst-scope")

			if tc.wantSpare {
				if !errors.Is(err, errRollbackAnswered) || getErr != nil {
					t.Errorf("rollback = %v, definition read = %v: want it left as finished", err, getErr)
				}

				return
			}

			if err != nil || !errors.Is(getErr, store.ErrNotFound) {
				t.Errorf("rollback = %v, definition read = %v: want it deleted", err, getErr)
			}
		})
	}
}

// linstor-csi deletes the source's Aux/csi-provisioning-completed-by on every
// clone, and its CreateVolume takes a definition carrying it, with volumes of
// the right size, as one already provisioned: the retry never reaches the
// clone that would resume it. A leftover a failed clone keeps must not carry
// it, so the edits land with the definition, not after the clone finished.
func TestACloneLeftoverDoesNotCarryAPropTheCallerDeleted(t *testing.T) {
	t.Parallel()

	const completedBy = "Aux/csi-provisioning-completed-by"

	for _, tc := range []struct {
		name  string
		fails bool
		edits map[string]any
	}{
		{name: "left-by-a-failed-clone", fails: true},
		{name: "finished-clone"},
		{name: "namespace-deleted", fails: true, edits: map[string]any{"delete_namespaces": []string{"Aux"}}},
		{name: "override-kept", fails: true, edits: map[string]any{
			"delete_props":   []string{completedBy},
			"override_props": map[string]string{"Aux/owner": "csi"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()
			seedTwoNodeSource(t, backend, "src-csi")

			if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "src-csi",
				func(rd *apiv1.ResourceDefinition) error {
					if rd.Props == nil {
						rd.Props = map[string]string{}
					}

					rd.Props[completedBy] = "linstor-csi/v1.10.4"

					return nil
				}); err != nil {
				t.Fatalf("mark the source provisioned: %v", err)
			}

			var st store.Store = backend
			if tc.fails {
				st = halfPlacingStore{Store: backend, target: "dst-csi"}
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body := map[string]any{"name": "dst-csi", "use_zfs_clone": true, "delete_props": []string{completedBy}}
			if tc.edits != nil {
				body = map[string]any{"name": "dst-csi", "use_zfs_clone": true}
				maps.Copy(body, tc.edits)
			}

			resp := postClone(t, base, "src-csi", body)
			_ = resp.Body.Close()

			rd, err := backend.ResourceDefinitions().Get(ctx, "dst-csi")
			if err != nil {
				t.Fatalf("clone = %d, and no target to inspect: %v", resp.StatusCode, err)
			}

			if v, ok := rd.Props[completedBy]; ok {
				t.Errorf("clone = %d left a target carrying %s=%q, which linstor-csi reads as provisioned",
					resp.StatusCode, completedBy, v)
			}

			if tc.name == "override-kept" && rd.Props["Aux/owner"] != "csi" {
				t.Errorf("clone = %d left a target without the override the caller asked for: %v",
					resp.StatusCode, rd.Props)
			}
		})
	}
}

// A bare restore's replay judges a leftover with every volume and no replica
// finished and answers 201, while a placed restore of the same snapshot into
// the same name, still placing, judges it unfinished and rolls it back. The
// replay takes the adoption mark before it answers, so that rollback yields
// instead of deleting the definition the replay was answered for.
func TestABareReplayKeepsAPlacedRestoreFromRollingItBack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-bare")
	seedRestoreSnapshot(t, st, "src-bare", "snap-bare", []string{"node-a"})

	snap, err := st.Snapshots().Get(ctx, "src-bare", "snap-bare")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	seedRestoreLeftover(t, st, "src-bare", "snap-bare", "dst-bare", store.WithRestoreMarker(nil, &snap))

	if err := st.VolumeDefinitions().Create(ctx, "dst-bare",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-bare", "snap-bare", map[string]any{"to_resource": "dst-bare"}); code != http.StatusCreated {
		t.Fatalf("bare replay of the hydrated leftover = %d, want 201", code)
	}

	err = (&Server{Store: st}).rollBackCompensating(ctx, "dst-bare", []string{"node-a"}, rollbackUnlessPlaced)
	if !errors.Is(err, errRollbackYielded) {
		t.Errorf("placed restore's rollback after the bare replay = %v, want it to yield", err)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-bare"); err != nil {
		t.Errorf("the definition the replay answered 201 for is gone: %v", err)
	}
}

// rollbackStartsUnderTheClaim lands the creator's in-progress rollback mark on
// the target just before the replay's adoption patch: the creator began rolling
// back after the replay's gate and before its claim.
type rollbackStartsUnderTheClaim struct {
	store.ResourceDefinitionStore

	target string
}

func (r rollbackStartsUnderTheClaim) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if name == r.target {
		if err := r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name,
			func(rd *apiv1.ResourceDefinition) error {
				rd.Props[store.RollbackAbandonedProp] = store.RollbackInProgress

				return nil
			}); err != nil {
			return err //nolint:wrapcheck // test double
		}
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

type rollbackStartsUnderTheClaimStore struct {
	store.Store

	target string
}

func (s rollbackStartsUnderTheClaimStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return rollbackStartsUnderTheClaim{ResourceDefinitionStore: s.Store.ResourceDefinitions(), target: s.target}
}

// A replay whose claim finds the creator rolling back refuses with 409 in the
// FAIL_EXISTS_RSC_DFN band, the answer the gate gives for the same state,
// rather than one that reads as a volume already restored.
func TestAReplayRefusesWhenTheCreatorStartsRollingBackUnderItsClaim(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-under")
	seedRestoreSnapshot(t, st, "src-under", "snap-under", []string{"node-a"})

	snap, err := st.Snapshots().Get(ctx, "src-under", "snap-under")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	seedRestoreLeftover(t, st, "src-under", "snap-under", "dst-under", store.WithRestoreMarker(nil, &snap))

	if err := st.VolumeDefinitions().Create(ctx, "dst-under",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	base, stop := startServerWithStore(t, rollbackStartsUnderTheClaimStore{Store: st, target: "dst-under"})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-under", "snap-under", map[string]any{"to_resource": "dst-under"})
	if code != http.StatusConflict {
		t.Fatalf("replay whose claim met a rollback = %d %q, want 409", code, rc.Message)
	}

	if rc.RetCode&apiCallRcFailExistsRscDfn != apiCallRcFailExistsRscDfn {
		t.Errorf("refusal ret_code %#x lacks the FAIL_EXISTS_RSC_DFN band", rc.RetCode)
	}

	if rc.RetCode&apiCallRcFailExistsVlmDfn == apiCallRcFailExistsVlmDfn {
		t.Errorf("refusal ret_code %#x carries the FAIL_EXISTS_VLM_DFN band", rc.RetCode)
	}
}

// The clone's replay refuses the same way when its claim finds the creator
// rolling back: answered 201, the clone would be deleted under the caller.
func TestACloneReplayRefusesWhenTheCreatorStartsRollingBackUnderItsClaim(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoVolumeCloneLeftover(t, st, "src-cunder", "dst-cunder", 0, 1)

	base, stop := startServerWithStore(t, rollbackStartsUnderTheClaimStore{Store: st, target: "dst-cunder"})
	defer stop()

	if code := cloneOnce(t, base, "src-cunder", "dst-cunder", nil); code != http.StatusConflict {
		t.Errorf("clone replay whose claim met a rollback = %d, want 409", code)
	}
}

var errMarkRefused = errors.New("apiserver refused the patch")

// markRefused fails every spec patch of the target.
type markRefused struct {
	store.ResourceDefinitionStore

	target string
}

func (m markRefused) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if name == m.target {
		return errMarkRefused
	}

	return m.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

type markRefusedStore struct {
	store.Store

	target string
}

func (s markRefusedStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return markRefused{ResourceDefinitionStore: s.Store.ResourceDefinitions(), target: s.target}
}

// A replay that cannot take the adoption mark does not answer 201: without the
// mark, a creator still rolling back would delete what it answered for. It is
// a retryable 500, and the definition is left as it was.
func TestAReplayThatCannotTakeTheMarkAnswersRetryable(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-nomark")
	seedRestoreSnapshot(t, st, "src-nomark", "snap-nomark", []string{"node-a"})

	snap, err := st.Snapshots().Get(ctx, "src-nomark", "snap-nomark")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	seedRestoreLeftover(t, st, "src-nomark", "snap-nomark", "dst-nomark", store.WithRestoreMarker(nil, &snap))

	if err := st.VolumeDefinitions().Create(ctx, "dst-nomark",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	base, stop := startServerWithStore(t, markRefusedStore{Store: st, target: "dst-nomark"})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-nomark", "snap-nomark", map[string]any{"to_resource": "dst-nomark"})
	if code != http.StatusInternalServerError {
		t.Fatalf("replay whose mark could not be written = %d %q, want 500", code, rc.Message)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-nomark"); err != nil {
		t.Errorf("the definition is gone after a refused replay: %v", err)
	}
}
