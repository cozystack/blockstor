// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var (
	errPatchRefused    = errors.New("patch refused")
	errLiveReadRefused = errors.New("live read refused")
)

// handshakeBlind fails the writes or the live reads the adopt/rollback
// handshake depends on.
type handshakeBlind struct {
	store.Store

	failPatch, failLiveRead bool
}

type handshakeBlindRDs struct {
	store.ResourceDefinitionStore

	outer handshakeBlind
}

func (h handshakeBlind) ResourceDefinitions() store.ResourceDefinitionStore {
	return handshakeBlindRDs{ResourceDefinitionStore: h.Store.ResourceDefinitions(), outer: h}
}

func (r handshakeBlindRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.outer.failPatch {
		return errPatchRefused
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
}

func (r handshakeBlindRDs) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if r.outer.failLiveRead {
		return apiv1.ResourceDefinition{}, errLiveReadRefused
	}

	return r.ResourceDefinitionStore.GetUncached(ctx, name) //nolint:wrapcheck // pass-through test double
}

func seedRolledBackDefinition(t *testing.T, st store.Store, name string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: name}); err != nil {
		t.Fatalf("seed the definition: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{Name: name, NodeName: "node-a"}); err != nil {
		t.Fatalf("seed its replica: %v", err)
	}
}

// Without its in-progress mark an adopter cannot see the definition is being
// taken away, and without the read of the adoption mark the rollback cannot
// see a retry that answered for it. Either way it must not delete.
func TestTheRollbackDeletesNothingWhenItCannotHoldTheHandshake(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		blind    handshakeBlind
		wantStep string
	}{
		{name: "mark-not-written", blind: handshakeBlind{failPatch: true}, wantStep: "mark"},
		{name: "adoption-not-read", blind: handshakeBlind{failLiveRead: true}, wantStep: "read-adoption"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			seedRolledBackDefinition(t, backend, "dst-blind")

			blind := tc.blind
			blind.Store = backend
			srv := &Server{Store: blind}

			err := srv.rollBackCompensating(t.Context(), "dst-blind", []string{"node-a"}, rollbackEvenIfFinished)
			if err == nil {
				t.Fatal("the rollback reported success without the handshake")
			}

			if got := rollbackStepName(err); got != tc.wantStep {
				t.Errorf("step = %q, want %q", got, tc.wantStep)
			}

			if _, err := backend.ResourceDefinitions().Get(t.Context(), "dst-blind"); err != nil {
				t.Errorf("the definition was deleted: %v", err)
			}

			if replicas, _ := backend.Resources().ListByDefinition(t.Context(), "dst-blind"); len(replicas) != 1 ||
				replicaAcceptedForDeletion(&replicas[0]) {
				t.Errorf("the replica was reaped: %v", replicas)
			}
		})
	}
}

// The adopting half fails closed as well: a retry that cannot read the
// creator's state back does not take the definition.
func TestAnAdoptionRefusesWhenItCannotReadTheCreatorsState(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedRolledBackDefinition(t, backend, "dst-unread")

	srv := &Server{Store: handshakeBlind{Store: backend, failLiveRead: true}}

	if err := srv.claimAdoptedLeftover(t.Context(), "dst-unread", &apiv1.Snapshot{}); err == nil {
		t.Error("the adoption went ahead without reading the creator's state")
	}
}

// landsThenFails applies the first patch and reports it failed, the way a
// write that landed just as its deadline ran out reads to its caller.
type landsThenFails struct {
	store.Store

	failed *bool
}

type landsThenFailsRDs struct {
	store.ResourceDefinitionStore

	outer landsThenFails
}

func (l landsThenFails) ResourceDefinitions() store.ResourceDefinitionStore {
	return landsThenFailsRDs{ResourceDefinitionStore: l.Store.ResourceDefinitions(), outer: l}
}

func (r landsThenFailsRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	err := r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate)
	if err == nil && !*r.outer.failed {
		*r.outer.failed = true

		return errPatchRefused
	}

	return err //nolint:wrapcheck // pass-through test double
}

// patchFailsOnce fails one numbered patch and lets the rest through, on top of
// a store whose live reads fail: the rollback's mark lands, the adoption read
// fails, and the first attempt to take the mark back fails with it.
type patchFailsOnce struct {
	handshakeBlind

	calls  *int
	failAt int
}

type patchFailsOnceRDs struct {
	store.ResourceDefinitionStore

	outer patchFailsOnce
}

func (p patchFailsOnce) ResourceDefinitions() store.ResourceDefinitionStore {
	return patchFailsOnceRDs{ResourceDefinitionStore: p.handshakeBlind.ResourceDefinitions(), outer: p}
}

func (r patchFailsOnceRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	*r.outer.calls++
	if *r.outer.calls == r.outer.failAt {
		return errPatchRefused
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
}

// A rollback that deleted nothing leaves an unfinished leftover, which a retry
// resumes. A mark left on it made the replay gate refuse every retry while the
// correction said to retry, so linstor-csi looped on the same 409 for good.
func TestARollbackThatDeletedNothingLeavesNoMarkToRefuseTheRetry(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		build func(store.Store) store.Store
	}{
		{name: "adoption-not-read", build: func(b store.Store) store.Store {
			return handshakeBlind{Store: b, failLiveRead: true}
		}},
		{name: "mark-landed-but-reported-failed", build: func(b store.Store) store.Store {
			failed := false

			return landsThenFails{Store: b, failed: &failed}
		}},
		{name: "first-clear-failed", build: func(b store.Store) store.Store {
			calls := 0

			return patchFailsOnce{handshakeBlind: handshakeBlind{Store: b, failLiveRead: true}, calls: &calls, failAt: 2}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()
			seedRolledBackDefinition(t, backend, "dst-nomark")

			srv := &Server{Store: tc.build(backend)}

			if err := srv.rollBackCompensating(ctx, "dst-nomark", []string{"node-a"}, rollbackEvenIfFinished); err == nil {
				t.Fatal("the rollback reported success without the handshake")
			}

			left, err := backend.ResourceDefinitions().Get(ctx, "dst-nomark")
			if err != nil {
				t.Fatalf("the definition was deleted: %v", err)
			}

			if mark, ok := left.Props[rollbackAbandonedKey]; ok {
				t.Errorf("the definition carries the abandoned-rollback mark %q", mark)
			}

			gate := &Server{Store: backend}
			for retry := range 2 {
				if _, refusal := gate.abandonedRollbackRefusal(ctx, "clone", "dst-nomark"); refusal != nil {
					t.Errorf("retry %d refused: %s; %s", retry, refusal.Message, refusal.Correc)
				}
			}
		})
	}
}

// A mark the rollback could not take back stays, and the gate refuses over it.
// Its correction has to lead out rather than repeat the retry that just failed.
func TestAMarkThatCouldNotBeClearedNamesTheWayOut(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-stuck", Props: map[string]string{rollbackAbandonedKey: rollbackInProgress},
	}); err != nil {
		t.Fatalf("seed the definition: %v", err)
	}

	_, refusal := (&Server{Store: backend}).abandonedRollbackRefusal(ctx, "clone", "dst-stuck")
	if refusal == nil {
		t.Fatal("the gate let a retry through an in-progress mark")
	}

	if !strings.Contains(refusal.Correc, "by hand") {
		t.Errorf("correction %q does not name the way out", refusal.Correc)
	}
}

// The corrections for a rollback that deleted nothing name the way out as
// well, and over a deleted group they start with the group, since a plain
// retry meets the group refusal rather than a resume.
func TestTheCorrectionForARollbackThatDeletedNothing(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		errRollbackYielded,
		newRollbackError(rollbackStepMark, errPatchRefused),
		newRollbackError(rollbackStepReadAdoption, errLiveReadRefused),
	} {
		_, plain := rollbackFailureAdvice(err, "dst-x")
		if !strings.Contains(plain, "delete 'dst-x' by hand") {
			t.Errorf("%v: correction %q does not name the way out", err, plain)
		}

		_, overGroup := rollbackFailureAdviceOverDeletedGroup(err, "dst-x", "grp-x")
		if !strings.HasPrefix(overGroup, "re-create resource group 'grp-x'") {
			t.Errorf("%v: correction over a deleted group %q does not start with the group", err, overGroup)
		}
	}
}

// A rollback that cannot read whether the definition already reads as
// finished cannot tell whether a retry or the poll answered for it, so it
// deletes nothing, takes its mark back off, and says which read failed.
func TestTheRollbackDeletesNothingWhenItCannotJudgeFinished(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedRolledBackDefinition(t, backend, "dst-unjudged")

	srv := &Server{Store: faultyStore{backend, storeFaults{target: "dst-unjudged", volumeList: errStoreFault}}}

	err := srv.rollBackCompensating(t.Context(), "dst-unjudged", []string{"node-a"}, rollbackUnlessPlaced)
	if rollbackStepName(err) != "read-finished" {
		t.Errorf("rollback = %v, want it to stop at read-finished", err)
	}

	rd, getErr := backend.ResourceDefinitions().Get(t.Context(), "dst-unjudged")
	if getErr != nil {
		t.Fatalf("the rollback deleted a definition it could not judge: %v", getErr)
	}

	if mark := rd.Props[store.RollbackAbandonedProp]; mark != "" {
		t.Errorf("the definition left in place carries rollback mark %q", mark)
	}

	if cause, _ := rollbackFailureAdvice(err, "dst-unjudged"); !strings.Contains(cause, "volumes and replicas") {
		t.Errorf("cause %q does not name the read that failed", cause)
	}
}

// markLandsThenClearsFail lets the rollback's mark through and fails every
// definition patch after it, the clear among them.
type markLandsThenClearsFail struct {
	store.Store

	patches *atomic.Int32
}

type markLandsThenClearsFailRDs struct {
	store.ResourceDefinitionStore

	patches *atomic.Int32
}

func (m markLandsThenClearsFail) ResourceDefinitions() store.ResourceDefinitionStore {
	return markLandsThenClearsFailRDs{ResourceDefinitionStore: m.Store.ResourceDefinitions(), patches: m.patches}
}

func (r markLandsThenClearsFailRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.patches.Add(1) > 1 {
		return errPatchRefused
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
}

// A rollback that leaves a finished clone whole and cannot take its mark back
// off leaves one every retry refuses, over a volume the driver was told
// exists. The answer says so and names the command that clears the mark, and
// that command goes through: deleting the clone is not the only way out.
func TestARollbackThatLeftAFinishedCloneWithItsMarkNamesTheWayOut(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-stuck")

	base, stop := startServerWithStore(t, backend)
	defer stop()

	if code := cloneOnce(t, base, "src-stuck", "dst-stuck", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	srv := &Server{Store: markLandsThenClearsFail{Store: backend, patches: &atomic.Int32{}}}

	err := srv.rollBackCompensating(ctx, "dst-stuck", nil, rollbackUnlessPlaced)
	if !errors.Is(err, errRollbackAnswered) {
		t.Fatalf("rollback = %v, want it to leave the finished clone", err)
	}

	cause, correc := rollbackFailureAdvice(err, "dst-stuck")
	if !strings.Contains(cause, "every retry is refused") || !strings.Contains(correc, "set-property dst-stuck "+store.RollbackAbandonedProp) {
		t.Errorf("advice %q / %q does not say the mark stays and how to clear it", cause, correc)
	}

	if code := cloneOnce(t, base, "src-stuck", "dst-stuck", nil); code != http.StatusConflict {
		t.Fatalf("retry over the stuck mark = %d, want 409", code)
	}

	raw, err := json.Marshal(map[string]any{"delete_props": []string{store.RollbackAbandonedProp}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	put := httpPut(t, base+"/v1/resource-definitions/dst-stuck", raw)
	_ = put.Body.Close()

	if put.StatusCode != http.StatusOK {
		t.Fatalf("clearing the mark with rd modify = %d, want 200", put.StatusCode)
	}

	if code := cloneOnce(t, base, "src-stuck", "dst-stuck", nil); code != http.StatusCreated {
		t.Errorf("retry after clearing the mark = %d, want 201", code)
	}
}

// The in-progress refusal names the clear as the way out only after the
// rollback has had its time, and says what clearing it early does: a retry
// adopts the definition and the rollback, still running, deletes it under it.
func TestTheInProgressRefusalNamesTheClearAndWhenItIsSafe(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-inprog", Props: map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, refusal := (&Server{Store: backend}).abandonedRollbackRefusal(ctx, "clone", "dst-inprog")
	if refusal == nil {
		t.Fatal("no refusal over an in-progress mark")
	}

	for _, want := range []string{
		"set-property dst-inprog " + store.RollbackAbandonedProp, "a minute after the failure", "stops that rollback before it deletes anything",
	} {
		if !strings.Contains(refusal.Correc, want) {
			t.Errorf("correction %q does not say %q", refusal.Correc, want)
		}
	}
}

// The RG-deleted doors put the group first in their correction; a mark the
// rollback could not take off still refuses the retry after the group is
// back, so clearing it is named between the two.
func TestTheRGDeletedAdviceForAStuckMarkNamesTheClear(t *testing.T) {
	t.Parallel()

	_, correc := rollbackFailureAdviceOverDeletedGroup(
		&rollbackMarkStuckError{why: errRollbackYielded, err: errPatchRefused}, "dst-rg", "rg-gone")

	for _, want := range []string{"re-create resource group 'rg-gone'", "set-property dst-rg"} {
		if !strings.Contains(correc, want) {
			t.Errorf("correction %q does not say %q", correc, want)
		}
	}
}

// A finished but short restore whose mark could not be taken off names both
// ways out: the clear, and the replicas the retry will not place.
func TestAStuckMarkOnAShortRestoreNamesTheMissingReplicasToo(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-short2")

	base, stop := startServerWithStore(t, backend)
	defer stop()

	if code := cloneOnce(t, base, "src-short2", "dst-short2", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	srv := &Server{Store: markLandsThenClearsFail{Store: backend, patches: &atomic.Int32{}}}

	rc := srv.failedMaterialiseRefusal(ctx, "restore failed", "restore", "dst-short2", nil,
		rollbackUnlessPlaced, []string{"node-a", "node-zzz"}, &materialiseAfterCreateError{err: errPatchRefused})

	for _, want := range []string{"set-property dst-short2", "no replica on node-zzz"} {
		if !strings.Contains(rc.Correc, want) {
			t.Errorf("correction %q does not say %q", rc.Correc, want)
		}
	}
}

// Only an in-progress mark is the operator's to clear. One naming the step a
// rollback gave up at sits on a definition the rollback took partly apart,
// and clearing it hands the remains to a retry that resumes over them.
func TestAStepMarkIsNotTheOperatorsToClear(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-step", Props: map[string]string{store.RollbackAbandonedProp: "reap-replicas"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, backend)
	defer stop()

	raw, err := json.Marshal(map[string]any{"delete_props": []string{store.RollbackAbandonedProp}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	put := httpPut(t, base+"/v1/resource-definitions/dst-step", raw)
	_ = put.Body.Close()

	if put.StatusCode != http.StatusBadRequest {
		t.Errorf("clearing a step mark = %d, want 400", put.StatusCode)
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "dst-step")
	if err != nil || rd.Props[store.RollbackAbandonedProp] != "reap-replicas" {
		t.Errorf("the refused clear changed the mark: %v (%v)", rd.Props, err)
	}
}

// A rollback that stopped at the definition's snapshots touched nothing: the
// definition is whole, and `rd d` refuses it over those snapshots, so
// clearing the mark is the operator's way to keep it without destroying the
// snapshots the rollback refused to destroy.
func TestAMarkOverAWholeDefinitionIsTheOperatorsToClear(t *testing.T) {
	t.Parallel()

	for _, step := range []string{store.RollbackStepSnapshots, store.RollbackStepReadSnapshots} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: "dst-whole", Props: map[string]string{store.RollbackAbandonedProp: step},
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			base, stop := startServerWithStore(t, backend)
			defer stop()

			raw, err := json.Marshal(map[string]any{"delete_props": []string{store.RollbackAbandonedProp}})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			put := httpPut(t, base+"/v1/resource-definitions/dst-whole", raw)
			_ = put.Body.Close()

			if put.StatusCode != http.StatusOK {
				t.Errorf("clearing a %s mark = %d, want 200", step, put.StatusCode)
			}

			if _, cause := rollbackStepAdvice(mustStep(t, step), true, "dst-whole"); !strings.Contains(cause, "set-property dst-whole") {
				t.Errorf("advice for a %s mark %q does not name the clear", step, cause)
			}
		})
	}
}

func mustStep(t *testing.T, spelled string) rollbackStep {
	t.Helper()

	step, known := rollbackStepByName(spelled)
	if !known {
		t.Fatalf("no rollback step spelled %q", spelled)
	}

	return step
}

// The rule refuses one edit of a definition a rollback gave up on, the delete
// of its step mark, and lets every other through: linstor-csi's own
// delete_props, an operator's Aux edit, the empty-value spelling of the clear
// on a mark over a whole definition.
func TestOtherEditsOfAStepMarkedDefinitionGoThrough(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mark string
		body map[string]any
		gone bool
	}{
		{name: "aux-delete", mark: "reap-replicas", body: map[string]any{"delete_props": []string{"Aux/keep"}}},
		{name: "aux-set", mark: "reap-replicas", body: map[string]any{"override_props": map[string]string{"Aux/new": "x"}}},
		{
			name: "empty-override-clears-in-progress", mark: store.RollbackInProgress, gone: true,
			body: map[string]any{"override_props": map[string]string{store.RollbackAbandonedProp: ""}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name:  "dst-other",
				Props: map[string]string{store.RollbackAbandonedProp: tc.mark, "Aux/keep": "y"},
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			base, stop := startServerWithStore(t, backend)
			defer stop()

			raw, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			put := httpPut(t, base+"/v1/resource-definitions/dst-other", raw)
			_ = put.Body.Close()

			if put.StatusCode != http.StatusOK {
				t.Fatalf("modify = %d, want 200", put.StatusCode)
			}

			rd, err := backend.ResourceDefinitions().Get(ctx, "dst-other")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if _, kept := rd.Props[store.RollbackAbandonedProp]; kept == tc.gone {
				t.Errorf("mark kept=%v after %v, want kept=%v", kept, tc.body, !tc.gone)
			}
		})
	}
}

// The restore door's abandoned-rollback refusal carries the exists band only
// when it says what stands under the name: a 409 over the mark. A read that
// failed says nothing about it, and its 500 carries no band.
func TestTheRestoreAbandonedRollbackBandOnlyOnAConflict(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		blind    bool
		wantCode int
		wantBand bool
	}{
		{name: "mark", wantCode: http.StatusConflict, wantBand: true},
		{name: "read-failed", blind: true, wantCode: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: "dst-band2", Props: map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress},
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			var st store.Store = backend
			if tc.blind {
				st = handshakeBlind{Store: backend, failLiveRead: true}
			}

			w := httptest.NewRecorder()
			if !(&Server{Store: st}).restoreRollbackWasAbandoned(ctx, w, "dst-band2") {
				t.Fatal("no refusal written")
			}

			if w.Code != tc.wantCode {
				t.Fatalf("refusal = %d, want %d", w.Code, tc.wantCode)
			}

			var rcs []apiv1.APICallRc
			if err := json.Unmarshal(w.Body.Bytes(), &rcs); err != nil || len(rcs) != 1 {
				t.Fatalf("decode %q: %v", w.Body.String(), err)
			}

			if got := rcs[0].RetCode&apiCallRcFailExistsRscDfn == apiCallRcFailExistsRscDfn; got != tc.wantBand {
				t.Errorf("ret_code %#x carries the exists band = %v, want %v", rcs[0].RetCode, got, tc.wantBand)
			}
		})
	}
}
