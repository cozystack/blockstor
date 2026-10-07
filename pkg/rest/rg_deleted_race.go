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
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
// They are not the whole class. `rg spawn` (spawnCreate) also creates a
// definition parented to a group, on the path linstor-csi takes for every
// ordinary CreateVolume, and has neither half either. It is left out of this
// change on purpose: the doors here are the ones this fix set out to close,
// and spawn's compensation is a different shape (it rolls back through
// rollbackSpawn, not rollBackMaterialisedRD), so it needs its own change
// rather than a line added here.
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

// detachedRollbackBudget bounds the compensation a post-write door runs after
// the request it belongs to may have ended: the re-read of the parent group
// that decides whether to roll back, and the rollback itself.
//
// It is the first term of one chain, and every term below it is derived so the
// process outlives a compensation it started:
//
//	groupRecheckBudget        0.6s  parentRGSurvived's cache-retry
//	+ 2 * cacheConvergeBudget 10s   the rollback's two convergence waits
//	+ rollbackWriteBudget     2s    the rollback's own writes
//	+ 3 * markWriteBudget     3s    the abandoned-rollback mark: in progress, the
//	                                step it takes apart before the first delete,
//	                                and the step it stopped at
//	= detachedRollbackBudget  15.6s
//	+ shutdownMargin          2s    the rest of a graceful shutdown
//	= gracefulShutdownWindow  17.6s how long Shutdown waits for in-flight handlers
//	+ terminationGraceMargin  3s
//	<= terminationGracePeriodSeconds in every manifest that serves REST
//
// The compensation runs inside the handler on a context Shutdown cannot
// cancel, so a window shorter than the budget means a SIGTERM during a rolling
// restart cuts a cascade in half and kills the connection that would have said
// so, which is the state WithoutCancel was added to prevent one failure mode
// over. Cutting the budget instead is not the trade: its two waits are what
// keep the definition from going over replicas that were never stamped.
//
// TestRollbackBudgetFitsTheShutdownWindow holds the chain, manifests included.
const (
	groupRecheckBudget     = cacheRetryAttempts * cacheRetryDelay
	rollbackWriteBudget    = 2 * time.Second
	markWriteBudget        = time.Second
	detachedRollbackBudget = groupRecheckBudget + 2*cacheConvergeBudget + rollbackWriteBudget + 3*markWriteBudget
	shutdownMargin         = 2 * time.Second
	terminationGraceMargin = 3 * time.Second
)

// detachedCompensation is the context a compensation runs on: the request
// cannot end it, and detachedRollbackBudget bounds it.
//
// A post-write door takes one for the group re-read as well as for the
// rollback, because the read is what decides whether to roll back. On the
// request's context a caller that has gone, or a SIGTERM (the server hands
// every request the runnable's own context as its base), turns that read into
// a cancelled one, which parentRGSurvived can only report as "could not
// check", and the door then answers success over a group that is gone. A
// cancelled caller is not an inconclusive answer about the group.
func detachedCompensation(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), detachedRollbackBudget)
}

// rollBackCompensating runs rollBackMaterialisedRD on ctx, which has to be a
// detachedCompensation context already: every door detaches exactly once and
// hands the rollback what is left of that one budget. Detaching again here
// would restart it, because WithoutCancel drops the deadline along with the
// cancellation, and the chain the manifests' termination grace is derived
// from would then be the re-read plus a whole second budget.
//
// Every compensation on these paths is most likely to be needed when the
// caller has already gone: a CSI caller times out mid-clone, and the RG-deleted
// rollback itself waits out two cache-convergence budgets. A compensation that
// inherits the request's context fails on its first call once that happens and
// leaves exactly what it exists to remove, a definition every later retry is
// refused over until an operator deletes it.
//
// The abandoned-rollback mark is written before anything is touched, and a
// rollback that completes takes it away with the definition. Written after a
// failure instead, it could not land in the two cases that leave a genuinely
// half-torn leftover: the budget running out mid-cascade leaves no context to
// write it on, and a killed process runs nothing at all. A failure then only
// refines the mark to the step it stopped at, best-effort.
//
// The handshake with an adopting retry fails closed. Without the in-progress
// mark an adopter cannot see that the definition is being taken away, and
// without the read of the adoption mark the rollback cannot see that a retry
// answered for it; in either case deleting could take a definition somebody
// was just told is theirs, so nothing is deleted and the definition is left
// for the replay gate and the operator, named in the error.
//
// scope says whether a definition that already reads as finished is spared;
// see rollbackScope.
func (s *Server) rollBackCompensating(
	ctx context.Context, rdName string, placed []string, scope rollbackScope,
) error {
	err := s.markRollbackAbandoned(ctx, rdName, rollbackInProgress)
	if errors.Is(err, store.ErrNotFound) {
		// The definition this rollback was for is gone, so there is nothing of
		// it left to remove. Deleting by name past this point would take
		// whatever stands under the name by the time the cascade runs, and a
		// retry recreating the deterministic name inside that window comes in
		// as a creator, invisible to the handshake below.
		return nil
	}

	if err != nil {
		// A write that failed at its deadline may still have landed; nothing
		// is deleted either way, so the mark must not outlive this answer.
		_ = s.clearRollbackMark(ctx, rdName) // the step's own error is the answer

		return newRollbackError(rollbackStepMark, err)
	}

	// The creating half of the handshake claimAdoptedLeftover describes: a
	// retry that adopted the definition, and may already have answered for
	// it, owns it now.
	adopted, err := s.adoptedElsewhere(ctx, rdName)
	if err != nil {
		// Nothing was taken away, so the definition is no more than an
		// unfinished leftover, which a retry resumes. A mark naming this step
		// would instead refuse every retry while the advice says to retry;
		// the in-progress mark comes off for the same reason.
		_ = s.clearRollbackMark(ctx, rdName) // the step's own error is the answer

		return newRollbackError(rollbackStepReadAdoption, err)
	}

	if adopted {
		return s.leftWhole(ctx, rdName, errRollbackYielded)
	}

	if scope != rollbackEvenIfFinished {
		finished, err := store.ReadsAsFinished(ctx, s.Store, rdName, scope == rollbackUnlessPlaced)
		if err != nil {
			_ = s.clearRollbackMark(ctx, rdName) // the step's own error is the answer

			return newRollbackError(rollbackStepReadFinished, err)
		}

		if finished {
			return s.leftWhole(ctx, rdName, errRollbackAnswered)
		}
	}

	// Nothing is touched until the snapshot refusal has run, for the reason
	// handleRDDelete gives for its own: once the replicas are reaped, a
	// refused definition delete leaves the target half torn down, with its
	// children going and its parent kept, which no retry reconciles. A refusal
	// whose correction is "drop the snapshots and retry" has to arrive while
	// there is still something to retry over.
	err = s.refuseRollbackOverSnapshots(ctx, rdName)
	if err != nil {
		_ = s.markRollbackAbandoned(ctx, rdName, rollbackStepName(err))

		return err
	}

	gone, err := s.enterDestructiveRollback(ctx, rdName)
	if gone || err != nil {
		return err
	}

	err = s.rollBackMaterialisedRD(ctx, rdName, placed)
	if err != nil {
		_ = s.markRollbackAbandoned(ctx, rdName, rollbackStepName(err))
	}

	return err
}

// errRollbackMarkNotOurs is a definition that no longer carries the
// in-progress mark this rollback wrote: an operator cleared it, or the
// definition under the name is not the one the mark went on.
var errRollbackMarkNotOurs = errors.New("the rollback mark this rollback wrote is no longer on the definition")

// enterDestructiveRollback replaces the in-progress mark with the first step
// that takes the definition apart, before that step runs. The in-progress
// mark is then only ever over a definition left whole, which is what lets an
// operator clear it (store.RollbackMarkClearRefusal): written after a failure
// instead, the step name could not land in the cases that tear the definition
// halfway, the budget running out mid-cascade or the process being killed, and
// in-progress would stand over half-reaped replicas. A process killed between
// this write and the first delete leaves a step name over a whole definition;
// that fails closed, to a delete by hand.
//
// The write checks the definition still carries this rollback's own mark,
// which is the identity check deleting by name needs: a definition recreated
// under the name since carries none. gone means there is nothing left to roll
// back. An error means nothing was deleted, and the in-progress mark is taken
// off unless it was not ours to begin with.
func (s *Server) enterDestructiveRollback(ctx context.Context, rdName string) (bool, error) {
	markCtx, cancel := context.WithTimeout(ctx, markWriteBudget)
	defer cancel()

	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(markCtx, rdName,
		func(rd *apiv1.ResourceDefinition) error {
			if rd.Props[rollbackAbandonedKey] != rollbackInProgress {
				return errRollbackMarkNotOurs
			}

			rd.Props[rollbackAbandonedKey] = rollbackStepNames[rollbackStepReapReplicas]

			return nil
		})

	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, store.ErrNotFound):
		return true, nil
	case errors.Is(err, errRollbackMarkNotOurs):
		return false, newRollbackError(rollbackStepMark, fmt.Errorf("%q: %w", rdName, err))
	default:
		_ = s.clearRollbackMark(ctx, rdName) // the step's own error is the answer

		return false, newRollbackError(rollbackStepMark, errors.Wrapf(err, "mark %q as being taken apart", rdName))
	}
}

// finishedLeftoverRefusal takes the adoption mark on a finished leftover a
// replay is about to answer for. A replay writes nothing else, and a creator
// whose rollback judges "finished" by a replica a bare replay did not need
// would otherwise delete the definition after the 201. Nil means the replay may
// answer; a creator already rolling back is refused the way the gate refuses
// it, and a mark that cannot be written is a retryable 500.
func (s *Server) finishedLeftoverRefusal(ctx context.Context, door, rdName string) (int, *apiv1.APICallRc) {
	err := s.claimAdoptedLeftover(ctx, rdName, nil)
	if err == nil {
		return 0, nil
	}

	if errors.Is(err, errAdoptedLeftoverRollingBack) {
		if status, refusal := s.abandonedRollbackRefusal(ctx, door, rdName); refusal != nil {
			return status, refusal
		}

		return http.StatusConflict, &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: "'" + rdName + "' is being rolled back by the attempt that created it",
			Correc:  "retry once the rollback has finished",
		}
	}

	return http.StatusInternalServerError, &apiv1.APICallRc{
		RetCode: apiCallRcError,
		Message: "'" + rdName + "' is finished, but it could not be marked as answered for: " +
			scrubImplDetails(err.Error()),
		Correc: "retry the " + door,
	}
}

// errRollbackYielded reports a rollback that left the definition in place
// because a retry marked it adopted. That retry may have finished it, or may
// itself have seen this rollback's mark and refused: when both marks land
// before either side reads the other's, both sides stand down and nothing is
// deleted, and the next retry resumes the leftover.
var errRollbackYielded = errors.New("a retry marked the definition adopted, so it was left in place")

// errRollbackAnswered reports a rollback that left the definition in place
// because it already reads as finished: the clone status poll answers such a
// definition without adopting it, so it may already have told the caller the
// operation completed.
var errRollbackAnswered = errors.New("the definition already reads as finished, so it was left in place")

// rollbackScope is which definitions a rollback spares besides an adopted one.
//
// A failed materialisation spares one that already reads as finished
// (store.ReadsAsFinished): the clone status poll answers it as done the moment
// it is, without writing anything, which can be while the attempt that created it is still
// stamping its last replica, and deleting it afterwards takes a volume the
// driver was told exists. The RG-deleted rollback spares nothing: a finished
// definition parented to a group that is gone is what it exists to remove.
type rollbackScope int

const (
	// rollbackEvenIfFinished deletes a finished definition too.
	rollbackEvenIfFinished rollbackScope = iota
	// rollbackUnlessPlaced spares one holding every volume and a replica: a
	// clone, or a restore that was asked to place replicas.
	rollbackUnlessPlaced
	// rollbackUnlessHydrated spares one holding every volume: a bare restore,
	// which places nothing and is finished without a replica.
	rollbackUnlessHydrated
)

// adoptedElsewhere reads, past the cache, whether a retry has adopted the
// definition. A definition that is gone was adopted by nobody.
func (s *Server) adoptedElsewhere(ctx context.Context, rdName string) (bool, error) {
	return store.AdoptedElsewhere(ctx, s.Store, rdName) //nolint:wrapcheck // names the definition already
}

// clearRollbackMark takes back the in-progress mark of a rollback that deleted
// nothing, so a retry is not refused over a definition that was left whole.
//
// It is tried more than once: a mark that stays is read by the replay gate as
// a rollback that never reported how it ended, which it has to refuse, since a
// process killed mid-cascade leaves exactly that. Each attempt has the mark's
// own budget. If every attempt fails the mark stays and the error is
// returned, so a definition left whole says so (leftWhole); the gate's
// refusal over the mark names clearing it as the way out.
func (s *Server) clearRollbackMark(ctx context.Context, rdName string) error {
	const attempts = 3

	var err error

	for range attempts {
		err = s.patchRollbackMarkAway(ctx, rdName)
		if err == nil || errors.Is(err, store.ErrNotFound) || ctx.Err() != nil {
			break
		}
	}

	if err != nil && !errors.Is(err, store.ErrNotFound) {
		log.FromContext(ctx).Info("could not clear the mark of a rollback that deleted nothing",
			"resourceDefinition", rdName, "error", err.Error())

		return err
	}

	return nil
}

// leftWhole takes the mark off a definition the rollback left whole for why,
// a yield to an adopter or a definition that already reads as finished. One
// whose mark stays is refused by every retry over a definition somebody was
// told is theirs, so the answer says so and names the command that clears it.
func (s *Server) leftWhole(ctx context.Context, rdName string, why error) error {
	err := s.clearRollbackMark(ctx, rdName)
	if err != nil {
		return &rollbackMarkStuckError{why: why, err: err}
	}

	return why
}

// rollbackMarkStuckError is a rollback that left the definition whole, for
// why, and could not take its in-progress mark back off.
type rollbackMarkStuckError struct {
	why, err error
}

func (e *rollbackMarkStuckError) Error() string {
	return e.why.Error() + ", but its rollback mark could not be taken off: " + e.err.Error()
}

func (e *rollbackMarkStuckError) Unwrap() []error { return []error{e.why, e.err} }

func (s *Server) patchRollbackMarkAway(ctx context.Context, rdName string) error {
	markCtx, cancel := context.WithTimeout(ctx, markWriteBudget)
	defer cancel()

	return s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(markCtx, rdName, //nolint:wrapcheck // logged by the caller
		func(rd *apiv1.ResourceDefinition) error {
			delete(rd.Props, rollbackAbandonedKey)

			return nil
		})
}

// rollbackInProgress is the mark a rollback carries until it either completes,
// taking the definition and the mark with it, or names the step it stopped at.
// Read back, it is a rollback that never reported how it ended.
const rollbackInProgress = store.RollbackInProgress

// rollbackAbandonedKey marks a definition whose compensation gave up, with
// the step it stopped at.
//
// The replay gate needs it. A rollback runs over a definition that may
// already hold volumes and a live replica: the RG-deleted rollback deletes a
// finished one by design, and a failed materialisation's rollback deletes one
// that does not read as finished yet. Cut short mid-cascade (its budget runs
// out, the process is killed, a replica or a snapshot cannot be reaped), it
// leaves a definition the 500 says to delete by hand, and that 500 goes to a
// caller that retries under the same deterministic name long before anyone
// reads it. The retry can find volumes and a live replica and would answer 201
// over a definition the rollback was taking apart; the rollback is the one
// party that knows it gave up.
const rollbackAbandonedKey = store.RollbackAbandonedProp

// markRollbackAbandoned records on the definition that its rollback started,
// or where it stopped. Best-effort: a mark that does not land leaves the
// replay gate where it was before the mark existed.
//
// Each write gets markWriteBudget of its own and is not waited on past it: a
// second covers a get and a patch against a loaded API server, where a fraction
// of one would leave the first mark missing exactly when the cascade after it
// is cut short.
// The patch retries on conflict with a backoff that sleeps without a context,
// sized for heavy contention, so under a reconciler bumping the definition's
// resourceVersion a mark could otherwise spend the convergence waits the
// cascade after it is budgeted for. A write abandoned at its deadline cannot
// land later: every call it would still make runs on the expired context.
func (s *Server) markRollbackAbandoned(ctx context.Context, rdName, step string) error {
	markCtx, cancel := context.WithTimeout(ctx, markWriteBudget)
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(markCtx, rdName,
			func(rd *apiv1.ResourceDefinition) error {
				if rd.Props == nil {
					rd.Props = map[string]string{}
				}

				rd.Props[rollbackAbandonedKey] = step

				return nil
			})
	}()

	var err error

	select {
	case err = <-done:
	case <-markCtx.Done():
		err = errors.Wrapf(markCtx.Err(), "mark %q", rdName)
	}

	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		// The definition is gone, so there is nothing left to mark. Kept
		// at V(1) so a mark that did not land can still be told apart
		// from one that was never needed.
		log.FromContext(ctx).V(1).Info("no definition to mark an abandoned rollback on",
			"resourceDefinition", rdName, "step", step)
	default:
		log.FromContext(ctx).Info("could not mark an abandoned rollback on its definition",
			"resourceDefinition", rdName, "step", step, "reason", err.Error())
	}

	return err
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
//
// wanted names the nodes the request asked for replicas on, when the caller
// places them itself: a definition left because it already reads as finished
// is not re-placed by a retry, so the replicas it never got are named with the
// command that places them. A clone passes none, since linstor-csi reconciles
// a clone's placement right after it reports complete.
func (s *Server) failedMaterialiseRefusal(
	ctx context.Context, message, noun, rdName string, placed []string, scope rollbackScope,
	wanted []string, err error,
) *apiv1.APICallRc {
	message = scrubImplDetails(message)

	var partial *materialiseAfterCreateError
	if !errors.As(err, &partial) {
		return &apiv1.APICallRc{RetCode: apiCallRcError, Message: message}
	}

	rollbackCtx, cancel := detachedCompensation(ctx)
	defer cancel()

	rollbackErr := s.rollBackCompensating(rollbackCtx, rdName, placed, scope)
	if errors.Is(rollbackErr, errRollbackAnswered) {
		cause, correc := rollbackFailureAdvice(rollbackErr, rdName)

		// A stuck mark's way out comes first; the replicas the request never
		// got are still named after it, since the retry it leads to will
		// not place them either.
		var stuck *rollbackMarkStuckError
		if short := s.answeredLeftShortCorrection(rollbackCtx, rdName, wanted, ""); short != "" {
			if errors.As(rollbackErr, &stuck) {
				correc += "; " + short
			} else {
				correc = short
			}
		}

		return &apiv1.APICallRc{
			RetCode: apiCallRcError,
			Message: message + "; '" + rdName + "' already reads as finished, so it was left in place",
			Cause:   cause,
			Correc:  correc,
		}
	}

	if errors.Is(rollbackErr, errRollbackYielded) {
		cause, correc := rollbackFailureAdvice(rollbackErr, rdName)
		left := "; '" + rdName + "' was left in place, because a retry of this " + noun +
			" marked it adopted and may have answered for it"

		return &apiv1.APICallRc{RetCode: apiCallRcError, Message: message + left, Cause: cause, Correc: correc}
	}

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

// answeredLeftShortCorrection is the correction for a definition the rollback
// left because it already reads as finished, when the request asked for
// replicas on wanted: a retry judges it finished too and places nothing, so a
// replica that never landed is named with the command that places it. correc
// is the answer when every one of them landed.
func (s *Server) answeredLeftShortCorrection(ctx context.Context, rdName string, wanted []string, correc string) string {
	if len(wanted) == 0 {
		return correc
	}

	missing, err := store.MissingReplicas(ctx, s.Store, rdName, wanted)

	switch {
	case err != nil:
		return "check which of the requested replicas of '" + rdName + "' exist, and place the rest " +
			"with `linstor resource create <node> " + rdName + "`"
	case len(missing) > 0:
		return "'" + rdName + "' has no replica on " + strings.Join(missing, ", ") +
			", which a retry will not place: run `linstor resource create <node> " + rdName + "` for each"
	default:
		return correc
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
	rollbackStepReadSnapshots
	rollbackStepMark
	rollbackStepReadAdoption
	rollbackStepReadFinished
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

// rollbackStepNames spells each step for the abandoned-rollback mark.
var rollbackStepNames = map[rollbackStep]string{ //nolint:gochecknoglobals // a fixed table, read-only
	rollbackStepReapReplicas:     store.RollbackStepReapReplicas,
	rollbackStepRereadReplicas:   "reread-replicas",
	rollbackStepSnapshots:        store.RollbackStepSnapshots,
	rollbackStepDeleteDefinition: store.RollbackStepDeleteDefinition,
	rollbackStepReadSnapshots:    store.RollbackStepReadSnapshots,
	rollbackStepMark:             "mark",
	rollbackStepReadAdoption:     "read-adoption",
	rollbackStepReadFinished:     "read-finished",
}

// rollbackStepName spells the step a compensation stopped at, or "unknown".
func rollbackStepName(err error) string {
	var failure *rollbackStepError
	if errors.As(err, &failure) {
		return rollbackStepNames[failure.step]
	}

	return "unknown"
}

// rollbackStepByName reads a step back from its spelling.
func rollbackStepByName(name string) (rollbackStep, bool) {
	for step, spelled := range rollbackStepNames {
		if spelled == name {
			return step, true
		}
	}

	return rollbackStepReapReplicas, false
}

// retryOrDeleteByHand is the correction for a rollback that deleted nothing.
// The retry resumes the definition; the way out is named as well, since a mark
// the rollback could not take back makes the replay gate refuse that retry.
func retryOrDeleteByHand(rdName string) string {
	return "retry: the retry resumes '" + rdName + "' or reports it finished; if the retry is " +
		"refused over an abandoned rollback, delete '" + rdName + "' by hand"
}

// rollbackLeftNothingDeleted reports a rollback that, by design, deleted
// nothing: it yielded to an adopting retry or could not hold the handshake.
func rollbackLeftNothingDeleted(err error) bool {
	if errors.Is(err, errRollbackYielded) || errors.Is(err, errRollbackAnswered) {
		return true
	}

	var failure *rollbackStepError

	return errors.As(err, &failure) &&
		(failure.step == rollbackStepMark || failure.step == rollbackStepReadAdoption ||
			failure.step == rollbackStepReadFinished)
}

// rollbackFailureAdviceOverDeletedGroup is rollbackFailureAdvice for the
// doors whose rollback runs because the definition's group was deleted. A
// rollback that deleted nothing there leaves a definition parented to a group
// that is gone, and a plain retry meets the group refusal rather than a
// resume, so the correction starts with the group.
func rollbackFailureAdviceOverDeletedGroup(err error, rdName, rgName string) (string, string) {
	cause, correc := rollbackFailureAdvice(err, rdName)
	if !rollbackLeftNothingDeleted(err) {
		return cause, correc
	}

	// The mark it could not take off refuses the retry the group's return
	// would otherwise let through, so clearing it comes between the two.
	var stuck *rollbackMarkStuckError
	if errors.As(err, &stuck) {
		return cause, "re-create resource group '" + rgName + "', " + clearRollbackMarkCommand(rdName) +
			", then retry, which resumes '" + rdName + "'"
	}

	return cause, "re-create resource group '" + rgName + "' and retry, which resumes '" + rdName +
		"'; or delete '" + rdName + "' by hand"
}

// rollbackFailureAdvice is the Cause and Correc for a failed compensation,
// written for the step that failed rather than once for all of them.
func rollbackFailureAdvice(err error, rdName string) (string, string) {
	var stuck *rollbackMarkStuckError
	if errors.As(err, &stuck) {
		return "'" + rdName + "' was left in place, but the mark saying it is being rolled back " +
				"could not be taken off, so every retry is refused over it",
			clearRollbackMarkCommand(rdName) + ", then retry"
	}

	if errors.Is(err, errRollbackAnswered) {
		return "'" + rdName + "' already held everything a retry or a status poll reads as " +
				"finished when this attempt failed, so one of them may already have reported it " +
				"complete, and it was left in place",
			"retry, which answers for '" + rdName + "' as it stands"
	}

	if errors.Is(err, errRollbackMarkNotOurs) {
		return "the rollback's mark on '" + rdName + "' was cleared or replaced before it took " +
				"anything apart, so it may no longer be the definition the rollback set out to " +
				"remove, and it was left in place",
			retryOrDeleteByHand(rdName)
	}

	if errors.Is(err, errRollbackYielded) {
		return "a retry of the same operation marked '" + rdName + "' adopted while this attempt " +
				"failed, and may already have answered for it, so it was left in place",
			retryOrDeleteByHand(rdName)
	}

	var failure *rollbackStepError
	if !errors.As(err, &failure) {
		return rollbackStepAdvice(rollbackStepReapReplicas, false, rdName)
	}

	return rollbackStepAdvice(failure.step, true, rdName)
}

// rollbackStepAdvice is rollbackFailureAdvice for a step already known, which
// is what the replay gate has when it reads an abandoned-rollback mark.
func rollbackStepAdvice(step rollbackStep, known bool, rdName string) (string, string) {
	if !known {
		return "the compensation could not complete", "delete '" + rdName + "' by hand"
	}

	switch step {
	case rollbackStepRereadReplicas:
		return "the replicas were told to go, but reading them back to confirm failed, " +
				"so the definition was left in place rather than dropped over replicas " +
				"nobody could see",
			"check the replicas of '" + rdName + "', then delete it by hand"
	case rollbackStepSnapshots:
		return "a snapshot exists on the definition, and the rollback does not destroy " +
				"a snapshot the way `rd d` refuses to",
			"delete the snapshot(s) of '" + rdName + "' if they are not needed, then delete '" +
				rdName + "' by hand; or, since the rollback touched nothing, " +
				clearRollbackMarkCommand(rdName) + " to keep it as it stands"
	case rollbackStepReadSnapshots:
		return "the snapshots of the definition could not be read, and the rollback does " +
				"not delete a definition it cannot show has none",
			"check whether '" + rdName + "' has snapshots (`linstor s l`), delete any that are " +
				"not needed, then delete '" + rdName + "' by hand; or, since the rollback touched " +
				"nothing, " + clearRollbackMarkCommand(rdName) + " to keep it as it stands"
	case rollbackStepDeleteDefinition:
		return "every replica went, but deleting the definition itself failed",
			"delete '" + rdName + "' by hand"
	case rollbackStepMark:
		return "the rollback could not record that it started, so a retry of the same " +
				"operation could not have seen it, and the definition was left in place " +
				"rather than taken from under a retry that may have answered for it",
			retryOrDeleteByHand(rdName)
	case rollbackStepReadAdoption:
		return "the rollback could not read whether a retry had adopted the definition, " +
				"so it was left in place rather than taken from under one that may have " +
				"answered for it",
			retryOrDeleteByHand(rdName)
	case rollbackStepReadFinished:
		return "the rollback could not read the volumes and replicas of the definition to tell " +
				"whether a retry or a status poll had already been told it is complete, so it was " +
				"left in place rather than taken from under one that may have answered for it",
			retryOrDeleteByHand(rdName)
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
//
// The caller has already run the snapshot refusal and marked the definition
// as being taken apart (enterDestructiveRollback); everything here deletes.
func (s *Server) rollBackMaterialisedRD(ctx context.Context, rdName string, placed []string) error {
	var err error

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
	// A listing that failed is not a snapshot that exists: the operator is
	// told which of the two it was, since only one of them points at an
	// object to delete.
	snaps, err := s.Store.Snapshots().ListByDefinition(ctx, rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return newRollbackError(rollbackStepReadSnapshots,
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
//
// It offers both readings the success path offers. parentRGSurvived cannot
// tell a group deleted while the operation ran from one that was never there,
// which adoption and data predating Bug 134 both produce, and asserting the
// race sends the operator hunting one that may never have happened.
func rollbackFailedMessage(rdName, rgName string, cause error) string {
	return "resource group '" + rgName + "' does not exist (it was deleted while the " +
		"operation ran, or it was never there) AND rolling '" + rdName + "' back failed: " +
		cause.Error() + "; '" + rdName + "' is still there, parented to a group that does not exist"
}

// rgDeletedRaceCorrection is the one wording for the refusal, so an operator
// reads the same correction whichever endpoint lost the race.
func rgDeletedRaceCorrection(rgName string) string {
	return "resource group '" + rgName + "' does not exist — it was deleted while the " +
		"operation ran, or it was never there: retry after creating the resource group"
}
