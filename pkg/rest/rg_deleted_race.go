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
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// parentRGSurvived re-reads the resource group a freshly materialised
// definition was parented to, and reports whether it is still there.
//
// This is the post-write half of the Bug 174 guard. `POST
// /v1/resource-definitions` runs it twice — once before the write and once
// after, rolling the definition back when a concurrent `rg d` won the race —
// because a definition left pointing at a group that no longer exists lists
// fine and places badly: the placer's Controller→RG→RD prop-inheritance walk
// drops the RG tier without a word, taking auto-place, auto-diskful,
// place_count observability and rebalance scheduling with it.
//
// Clone and snapshot-restore create definitions the same way and inherit the
// group the same way, and had NEITHER half: refuseRDCreateOnRGDeletedRace has
// exactly one caller, and neither rd_clone.go nor snapshot_restore.go read
// ResourceGroups() at all.
//
// That distinction reaches the operator. A refusal derived from this check
// that always blames a concurrent delete sends someone whose group never
// existed — from adoption, or from data that predates Bug 134 — hunting a race
// that never happened, so the wording covers both.
//
// The read carries the standard cache-retry budget for the same reason
// refuseRDCreateOnRGDeletedRace does: on the CreateVolume hot path the group
// may have been created moments ago and the informer cache may still trail
// it, and mistaking that lag for a delete race would roll back a perfectly
// good clone (see pkg/rest/cache_retry.go). A real `rg d` still trips it once
// the budget is spent.
func (s *Server) parentRGSurvived(ctx context.Context, rgName string) (bool, error) {
	if rgName == "" {
		return true, nil
	}

	_, err := getRGWithCacheRetry(ctx, s.Store, rgName)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}

	return false, err
}

// detachedRollbackBudget bounds a rollback that runs after the request it
// belongs to has ended.
//
// It is the first term of one chain, and every term below it is derived so the
// process outlives a rollback it started:
//
//	detachedRollbackBudget  12s  two cacheConvergeBudget waits plus its writes
//	+ shutdownMargin         3s  the rest of a graceful shutdown
//	= gracefulShutdownWindow 15s how long Shutdown waits for in-flight handlers
//	+ terminationGraceMargin 5s
//	<= terminationGracePeriodSeconds in every manifest that serves REST
//
// The rollback runs inside the handler on a context Shutdown cannot cancel, so
// a window shorter than the budget means a SIGTERM during a rolling restart
// cuts a cascade in half and kills the connection that would have said so,
// which is the state WithoutCancel was added to prevent one failure mode over.
// Cutting the budget instead is not the trade: its two waits are what keep the
// definition from going over replicas that were never stamped.
//
// TestRollbackBudgetFitsTheShutdownWindow holds the chain, manifests included.
const (
	detachedRollbackBudget = 12 * time.Second
	shutdownMargin         = 3 * time.Second
	terminationGraceMargin = 5 * time.Second
)

// rollBackDetached runs rollBackMaterialisedRD on a context the request cannot
// end, bounded by detachedRollbackBudget.
//
// Every compensation on these paths is most likely to be needed when the
// caller has already gone: a CSI caller times out mid-clone, and the RG-deleted
// rollback itself waits out two cache-convergence budgets. A compensation that
// inherits the request's context fails on its first call once that happens and
// leaves exactly what it exists to remove, a definition every later retry is
// refused over until an operator deletes it.
func (s *Server) rollBackDetached(ctx context.Context, rdName string, placed []string) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedRollbackBudget)
	defer cancel()

	return s.rollBackMaterialisedRD(rollbackCtx, rdName, placed)
}

// failedMaterialiseRefusal rolls back what a failed materialisation left, when
// that is this request's own partial work, and words the refusal. noun names
// the operation for the operator ("clone", "restore").
//
// The marker is stamped at RD-create, so a failure after the create leaves a
// definition every retry matches, and nothing but an operator would ever remove
// it. Only a failure materializeRestoredRD reports as after its own create is
// rolled back; any other leaves whatever was there before the call, which may
// belong to another attempt that is still running, and is never touched.
func (s *Server) failedMaterialiseRefusal(
	ctx context.Context, message, noun, rdName string, placed []string, err error,
) *apiv1.APICallRc {
	var partial *materialiseAfterCreateError
	if !errors.As(err, &partial) {
		return &apiv1.APICallRc{RetCode: apiCallRcError, Message: message}
	}

	rollbackErr := s.rollBackDetached(ctx, rdName, placed)
	if rollbackErr != nil {
		cause, correc := rollbackFailureAdvice(rollbackErr, rdName)

		return &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: message + "; rolling the partial " + noun + " back failed too: " + rollbackErr.Error() +
				"; '" + rdName + "' is still there",
			Cause:  cause,
			Correc: correc,
		}
	}

	return &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: message + "; the partial " + noun + " '" + rdName + "' was rolled back",
		Correc:  "retry the " + noun,
	}
}

// adoptedOverDeletedGroupRefusal is the RG-deleted refusal over a definition
// this request did not create. It is left in place: the leftover belongs to an
// earlier attempt at the same operation, which may still be running, and
// reaping it would delete that attempt's work rather than this one's.
func adoptedOverDeletedGroupRefusal(noun, rdName, rgName, correc string) *apiv1.APICallRc {
	return &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: noun + " target '" + rdName + "' is parented to resource group '" + rgName +
			"', which no longer exists",
		Cause: "the definition was not created by this request, so it is left in place " +
			"rather than rolled back",
		Correc: correc,
	}
}

// correcRecreateGroupThenRestore is the restore door's twin of
// correcRecreateGroupThenClone.
const correcRecreateGroupThenRestore = "re-create the resource group, then restore again"

// errReplicasNotStamped is the rollback's own refusal: replicas that are still
// there and were never accepted for deletion, which is the one shape the
// parent must not be dropped over.
var errReplicasNotStamped = errors.New("replica(s) were not accepted for deletion")

// errSnapshotsOnTarget refuses to drop a definition that carries snapshots,
// the refusal handleRDDelete makes before the sweep it shares with this path.
var errSnapshotsOnTarget = errors.New("snapshot(s) exist on the definition")

// rollbackStep names where the compensation stopped, because the operator is
// pointed at a different object in each case: a replica that would not go, a
// read that could not confirm, a snapshot that is somebody's data, or a
// definition whose own delete failed after everything under it went.
type rollbackStep int

const (
	rollbackStepReapReplicas rollbackStep = iota
	rollbackStepRereadReplicas
	rollbackStepSnapshots
	rollbackStepDeleteDefinition
)

type rollbackStepError struct {
	step rollbackStep
	err  error
}

func (f *rollbackStepError) Error() string { return f.err.Error() }

func (f *rollbackStepError) Unwrap() error { return f.err }

func newRollbackError(step rollbackStep, err error) error {
	return &rollbackStepError{step: step, err: err}
}

// rollbackFailureAdvice is the Cause and Correc for a failed compensation,
// written for the step that failed rather than once for all of them.
func rollbackFailureAdvice(err error, rdName string) (string, string) {
	var failure *rollbackStepError
	if !errors.As(err, &failure) {
		return "the compensation could not complete", "delete '" + rdName + "' by hand"
	}

	switch failure.step {
	case rollbackStepRereadReplicas:
		return "the replicas were told to go, but reading them back to confirm failed, " +
				"so the definition was left in place rather than dropped over replicas " +
				"nobody could see",
			"check the replicas of '" + rdName + "', then delete it by hand"
	case rollbackStepSnapshots:
		return "a snapshot exists on the definition, and the rollback does not destroy " +
				"a snapshot the way `rd d` refuses to",
			"delete the snapshot(s) of '" + rdName + "' if they are not needed, then delete '" +
				rdName + "' by hand"
	case rollbackStepDeleteDefinition:
		return "every replica went, but deleting the definition itself failed",
			"delete '" + rdName + "' by hand"
	case rollbackStepReapReplicas:
	}

	return "the replicas could not all be reaped, so the definition was left in place " +
			"rather than orphaning them",
		"delete '" + rdName + "' by hand once the replicas can be removed"
}

// rollBackMaterialisedRD undoes a clone or restore whose parent group was
// deleted underneath it.
//
// RD-create can compensate with a single Delete because the definition it
// rolls back is bare. These paths cannot: by the time the group can vanish the
// target has volumes hydrated from the snapshot and replicas stamped on the
// nodes that hold it, so the compensation is the cascade `rd d` performs —
// replicas first, then the definition, which carries its inline volumes with
// it.
//
// The internal snapshot a clone took is deliberately left behind. It may be
// the only copy of something, and deleting one is the operator's decision, not
// this endpoint's — the same stance the clone's snapshot reuse takes.
//
// The order is not merely tidy: the definition goes ONLY if the replicas
// went. CascadeDeleteResources stops at the first replica it cannot delete and
// leaves the rest untried, and dropping the parent anyway produces precisely
// the orphan this rollback exists to avoid — a Resource whose RD vanished
// never gets a DeletionTimestamp, so the satellite's finalizer never runs,
// `drbdadm down` never happens, and the DRBD minor, port and peer entries stay
// live on every satellite until the next create with that name collides with
// them. On the CSI path the target name is deterministic, so the retry IS that
// collision. Both other doors that perform this teardown — handleRDDelete and
// the CLI's `rd d` — refuse to proceed on a failed cascade for the same
// reason, and a satellite writing status on the very replicas being reaped
// makes a conflict there an ordinary outcome rather than a rare one.
//
// So this returns an error, and a caller that gets one must not report a
// rollback. What is left behind is a definition parented to a group that is
// gone, which is the state the operator has to be told about, with its name.
func (s *Server) rollBackMaterialisedRD(ctx context.Context, rdName string, placed []string) error {
	// Nothing is touched until the snapshot refusal has run, for the reason
	// handleRDDelete gives for its own: once the replicas are reaped, a
	// refused definition delete leaves the target half torn down, with its
	// children going and its parent kept, which no retry reconciles. A refusal
	// whose correction is "drop the snapshots and retry" has to arrive while
	// there is still something to retry over.
	err := s.refuseRollbackOverSnapshots(ctx, rdName)
	if err != nil {
		return err
	}

	// The replicas this request placed are deleted by name first. A write goes
	// to the API server whatever the cache has seen, so this is the one step
	// that does not depend on a listing having caught up with the placement
	// that happened a moment ago. Without it, a cache that has not yet seen
	// the stamps lists nothing, the cascade deletes nothing, the check below
	// finds nothing stranded, and the definition goes over live replicas that
	// will never be stamped — the orphan this rollback exists to prevent.
	for _, node := range placed {
		err = s.Store.Resources().Delete(ctx, rdName, node)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return newRollbackError(rollbackStepReapReplicas,
				errors.Wrapf(err, "delete the replica of %q on %q", rdName, node))
		}
	}

	// And the cascade for anything else under the definition: an
	// auto-tiebreaker the controller stamped in the meantime is not in
	// `placed`.
	err = store.CascadeDeleteResources(ctx, s.Store, rdName)
	if err != nil {
		return newRollbackError(rollbackStepReapReplicas,
			errors.Wrapf(err, "cascade the replicas of %q", rdName))
	}

	err = s.waitForReplicasAcceptedForDeletion(ctx, rdName)
	if err != nil {
		return err
	}

	err = s.Store.ResourceDefinitions().Delete(ctx, rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return newRollbackError(rollbackStepDeleteDefinition,
			errors.Wrapf(err, "delete %q", rdName))
	}

	// The same two companions handleRDDelete runs after its own delete, for
	// the same two reasons.
	//
	// The convergence wait, because reads here are informer-cache backed and a
	// delete lags them: a retry landing inside that window reads the
	// pre-delete definition, matches the clone marker and is answered 201 for
	// a definition that is genuinely gone — the same false success as
	// answering over a leftover, by a different route.
	//
	// The sweep, because a snapshot create can land between the refusal above
	// and the delete, and the row it leaves has no parent to address it.
	s.waitForRDDeletionVisible(ctx, rdName)
	s.sweepOrphanSnapshotsAfterRDDelete(ctx, rdName)

	return nil
}

// refuseRollbackOverSnapshots is the refusal handleRDDelete makes before its
// cascade. The sweep that follows the definition delete removes every Snapshot
// row under the definition, which is safe there only because the handler
// refuses outright when snapshots exist, so the sweep can only ever see rows
// that raced in. A snapshot taken on the target inside the rollback window is
// somebody's data, not a race.
func (s *Server) refuseRollbackOverSnapshots(ctx context.Context, rdName string) error {
	snaps, err := s.Store.Snapshots().ListByDefinition(ctx, rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return newRollbackError(rollbackStepSnapshots,
			errors.Wrapf(err, "list the snapshots of %q", rdName))
	}

	if len(snaps) == 0 {
		return nil
	}

	names := make([]string, 0, len(snaps))
	for i := range snaps {
		names = append(names, snaps[i].Name)
	}

	return newRollbackError(rollbackStepSnapshots,
		fmt.Errorf("%q: %w: %s", rdName, errSnapshotsOnTarget, strings.Join(names, ", ")))
}

// waitForReplicasAcceptedForDeletion decides whether any replica is stranded
// on a read that has had time to see the deletes this request just issued.
//
// One read is wrong in both directions at the exact moment it would run. The
// resources listing is informer-cache backed and the cascade sleeps nowhere
// across its passes, so the read outruns the cache by construction: a replica
// that was deleted still lists, unstamped, and the rollback refuses over
// replicas that are going, leaving the definition parented to a group that is
// gone with nothing to retry it. So the decision waits, on the same budget the
// RD delete's convergence wait uses, and only a replica still unstamped when
// that budget runs out counts as stranded.
//
// The wait also deletes. A replica can become visible only now: an
// auto-tiebreaker the controller stamps moments after placement is not in
// `placed`, and the cascade's passes run back to back, so it typically
// surfaces during this wait, when nothing else would issue a delete for it.
// Every unstamped replica a read shows is told to go once. Once, because a
// replica the cascade already deleted also lists unstamped while the cache
// trails, and the one extra delete that costs is cheap where one per poll is
// not.
func (s *Server) waitForReplicasAcceptedForDeletion(ctx context.Context, rdName string) error {
	deadline := time.Now().Add(cacheConvergeBudget)
	told := map[string]struct{}{}

	for {
		stranded, err := replicasNotAcceptedForDeletion(ctx, s.Store, rdName)
		if err != nil {
			return newRollbackError(rollbackStepRereadReplicas,
				errors.Wrapf(err, "re-read the replicas of %q", rdName))
		}

		if len(stranded) == 0 {
			return nil
		}

		err = s.deleteReplicasNotYetTold(ctx, rdName, stranded, told)
		if err != nil {
			return err
		}

		if time.Now().After(deadline) {
			return newRollbackError(rollbackStepReapReplicas,
				fmt.Errorf("%q: %w: %s", rdName, errReplicasNotStamped, strings.Join(stranded, ", ")))
		}

		select {
		case <-ctx.Done():
			return newRollbackError(rollbackStepRereadReplicas,
				errors.Wrapf(ctx.Err(), "wait for the replicas of %q", rdName))
		case <-time.After(cacheConvergePollInterval):
		}
	}
}

// deleteReplicasNotYetTold issues one delete per replica node the wait has not
// already told to go, and records it.
func (s *Server) deleteReplicasNotYetTold(
	ctx context.Context, rdName string, nodes []string, told map[string]struct{},
) error {
	for _, node := range nodes {
		if _, done := told[node]; done {
			continue
		}

		told[node] = struct{}{}

		err := s.Store.Resources().Delete(ctx, rdName, node)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return newRollbackError(rollbackStepReapReplicas,
				errors.Wrapf(err, "delete the replica of %q on %q", rdName, node))
		}
	}

	return nil
}

// replicasNotAcceptedForDeletion names the replicas that are still there and
// carry no deletion stamp, which is the only shape the parent must not be
// dropped over.
//
// CascadeDeleteResources answers nil in two different situations: every
// replica went, and its pass budget ran out with replicas still listed. That
// is the right contract for `rd d`, whose caller asked for the definition to
// go and who gets a convergence wait behind it. It is the wrong one to build a
// compensation on, and the difference is not an edge case in a cluster: every
// Resource carries the satellite's finalizer, an apiserver DELETE on a
// finalizer-held object is accepted with no error, and the listing does not
// filter what is Terminating — so the ordinary path through the cascade is
// "accepted, still listed", five passes, nil.
//
// A replica already stamped for deletion is not stranded: the stamp is what
// makes the satellite's finalizer run, and it runs whether or not the parent
// outlives it. A replica with no stamp is the orphan this rollback exists to
// avoid, because nothing will ever give it one once the definition is gone.
func replicasNotAcceptedForDeletion(ctx context.Context, st store.Store, rdName string) ([]string, error) {
	replicas, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "list replicas of %q", rdName)
	}

	var stranded []string

	for i := range replicas {
		if replicaAcceptedForDeletion(&replicas[i]) {
			continue
		}

		stranded = append(stranded, replicas[i].NodeName)
	}

	return stranded, nil
}

// replicaAcceptedForDeletion is the one reading of the deletion stamp, shared
// by the rollback, which may drop a parent over a stamped replica, and the
// replay, which may not answer 201 over one.
func replicaAcceptedForDeletion(replica *apiv1.Resource) bool {
	return slices.Contains(replica.Flags, apiv1.ResourceFlagDelete)
}

// rollbackFailedMessage is what the operator is told when the compensation
// could not complete: naming the definition that is still there matters more
// than the refusal itself, because nothing else will name it.
func rollbackFailedMessage(rdName, rgName string, cause error) string {
	return "resource group '" + rgName + "' was deleted concurrently with the operation " +
		"(Bug 174) AND rolling '" + rdName + "' back failed: " + cause.Error() +
		"; '" + rdName + "' is still there, parented to a group that no longer exists"
}

// rgDeletedRaceCorrection is the one wording for the refusal, so an operator
// reads the same correction whichever endpoint lost the race.
func rgDeletedRaceCorrection(rgName string) string {
	return "resource group '" + rgName + "' does not exist — it was deleted while the " +
		"operation ran, or it was never there: retry after creating the resource group"
}
