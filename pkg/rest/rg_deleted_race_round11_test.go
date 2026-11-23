// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errSnapshotListingDown = errors.New("snapshot listing timed out")

type snapshotListingFails struct{ store.SnapshotStore }

func (snapshotListingFails) ListByDefinition(context.Context, string) ([]apiv1.Snapshot, error) {
	return nil, errSnapshotListingDown
}

type snapshotListingFailsStore struct{ store.Store }

func (s snapshotListingFailsStore) Snapshots() store.SnapshotStore {
	return snapshotListingFails{s.Store.Snapshots()}
}

// A timeout, a 403 and a decode failure on the snapshot listing all reached the
// advice for a snapshot that exists, which tells the operator to delete
// snapshots that may not be there, and the same words went into the mark the
// replay gate repeats.
func TestARollbackThatCouldNotReadTheSnapshotsSaysSo(t *testing.T) {
	t.Parallel()

	s := &Server{Store: snapshotListingFailsStore{store.NewInMemory()}}

	err := s.refuseRollbackOverSnapshots(t.Context(), "dst-read11")
	if err == nil {
		t.Fatal("fixture: a failed snapshot listing was supposed to stop the rollback")
	}

	if got := rollbackStepName(err); got != "read-snapshots" {
		t.Errorf("step = %q, want read-snapshots", got)
	}

	cause, correc := rollbackFailureAdvice(err, "dst-read11")
	if strings.Contains(cause, "a snapshot exists") {
		t.Errorf("cause %q states as fact a snapshot nobody could read", cause)
	}

	if !strings.Contains(cause, "could not be read") || !strings.Contains(correc, "dst-read11") {
		t.Errorf("advice %q / %q does not say the snapshots could not be read", cause, correc)
	}

	step, known := rollbackStepByName(rollbackStepName(err))
	if replayCause, _ := rollbackStepAdvice(step, known, "dst-read11"); replayCause != cause {
		t.Errorf("the replay gate reads the mark back as %q, not %q", replayCause, cause)
	}
}

// patchSleepsThroughItsContext stands for the conflict backoff, which sleeps
// without looking at the context.
type patchSleepsThroughItsContext struct{ store.ResourceDefinitionStore }

func (patchSleepsThroughItsContext) PatchResourceDefinitionSpec(
	context.Context, string, func(*apiv1.ResourceDefinition) error,
) error {
	time.Sleep(2 * time.Second)

	return nil
}

type patchSleepsStore struct{ store.Store }

func (p patchSleepsStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return patchSleepsThroughItsContext{p.Store.ResourceDefinitions()}
}

// A mark retrying on conflict could spend the convergence waits the cascade
// after it is budgeted for, and the chain set nothing aside for it.
func TestAMarkWriteStopsAtItsOwnBudget(t *testing.T) {
	t.Parallel()

	s := &Server{Store: patchSleepsStore{store.NewInMemory()}}

	start := time.Now()
	s.markRollbackAbandoned(t.Context(), "dst-mark11", rollbackInProgress)

	if took := time.Since(start); took > markWriteBudget+300*time.Millisecond {
		t.Errorf("the mark write held the compensation for %s, past its budget of %s", took, markWriteBudget)
	}
}
