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

package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// ErrRestoreTargetTaken reports a definition that carries a restore marker
// naming another snapshot.
var ErrRestoreTargetTaken = errors.New("resource definition is already a restore of another snapshot")

// ErrRestoreTargetLayers reports a prepared target whose layer stack is not
// the snapshot source's.
var ErrRestoreTargetLayers = errors.New("prepared restore target has a different layer stack than the snapshot's source")

// ErrRestoreTargetTearingDown reports a prepared target whose every replica is
// still being deleted.
var ErrRestoreTargetTearingDown = errors.New("prepared restore target still has replicas being deleted")

// uncachedVolumeLister lists a definition's volumes from the API server.
type uncachedVolumeLister interface {
	ListUncached(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error)
}

// uncachedReplicaLister lists a definition's replicas from the API server.
type uncachedReplicaLister interface {
	ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error)
}

// LiveVolumes reads a definition's volumes past the cache where the store can;
// a store without a cache answers the same question from its only copy.
func LiveVolumes(ctx context.Context, st Store, rdName string) ([]apiv1.VolumeDefinition, error) {
	vds := st.VolumeDefinitions()
	if direct, ok := vds.(uncachedVolumeLister); ok {
		return direct.ListUncached(ctx, rdName) //nolint:wrapcheck // the caller wraps with the definition
	}

	return vds.List(ctx, rdName) //nolint:wrapcheck // the caller wraps with the definition
}

// GetResourceUncached reads one replica from the API server where the store
// has a direct reader, and through the store otherwise. A decision on whether
// a replica a create collided with is live, or already being deleted, has to
// see the object as it is now: a cache that trails shows a replica accepted
// for deletion as a live one.
func GetResourceUncached(ctx context.Context, st Store, rdName, node string) (apiv1.Resource, error) {
	resources := st.Resources()
	if direct, ok := resources.(interface {
		GetUncached(ctx context.Context, rdName, node string) (apiv1.Resource, error)
	}); ok {
		return direct.GetUncached(ctx, rdName, node) //nolint:wrapcheck // the store wraps with the replica it names
	}

	return resources.Get(ctx, rdName, node) //nolint:wrapcheck // the store wraps with the replica it names
}

// LiveReplicas reads a definition's replicas past the cache where the store can.
func LiveReplicas(ctx context.Context, st Store, rdName string) ([]apiv1.Resource, error) {
	resources := st.Resources()
	if direct, ok := resources.(uncachedReplicaLister); ok {
		return direct.ListByDefinitionUncached(ctx, rdName) //nolint:wrapcheck // the caller wraps with the definition
	}

	return resources.ListByDefinition(ctx, rdName) //nolint:wrapcheck // the caller wraps with the definition
}

// PreparedRestoreTarget reports whether a definition with no restore marker is
// the target LINSTOR's own restore sequence prepares for snapshot: created
// empty, its volume definitions restored from that snapshot, and no replica
// holding data yet.
//
// That is the documented flow (`rd c`, `s vd restore`, `s rsc restore`) and the
// one linstor-csi runs for every volume restored from a snapshot: VolFromSnap
// creates the definition itself, restores the volume definitions only when the
// definition has none, and then asks for the resources. The definition it made
// carries no marker, and has to be taken as this restore's target rather than
// as somebody else's.
//
// Only that state is taken. A definition already marked is judged by its
// marker; one being deleted, one whose volumes are not exactly the snapshot's,
// and one with a live replica may hold somebody's data, and are not prepared.
//
// Two states are prepared in every other respect and still refused, with an
// error saying why, before anything is written to the definition:
//
//   - A layer stack that is not the source's (ErrRestoreTargetLayers). The
//     restore puts the source's bytes under the target's stack, so a layer the
//     source did not have writes its own metadata across them (LUKS formats),
//     and one it had is missing on read (the consumer sees ciphertext). The
//     create path inherits the source's stack and the clone refuses a changed
//     set for the same reason; through linstor-csi this is a VolumeSnapshot
//     restored into a PVC of another StorageClass.
//   - Replicas that are all still being deleted (ErrRestoreTargetTearingDown).
//     Placing over them races the tear-down, and a retry once it finished
//     takes the target cleanly.
//
// The definition, its volumes and its replicas are all read from the API
// server. linstor-csi's volume restore and the resource restore after it can
// land on different replicas of this server, and a cache on the second that
// has not seen the first's writes answers both questions wrong.
func PreparedRestoreTarget(ctx context.Context, st Store, cached *apiv1.ResourceDefinition, snap *apiv1.Snapshot) (bool, error) {
	live, err := st.ResourceDefinitions().GetUncached(ctx, cached.Name)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("read %q: %w", cached.Name, err)
	}

	rd := &live

	if rd.Props[RestoreFromSnapshotProp] != "" || slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return false, nil
	}

	vds, err := LiveVolumes(ctx, st, rd.Name)
	if err != nil {
		return false, fmt.Errorf("list the volumes of %q: %w", rd.Name, err)
	}

	if !sameVolumes(vds, snap.VolumeDefinitions) {
		return false, nil
	}

	replicas, err := LiveReplicas(ctx, st, rd.Name)
	if err != nil {
		return false, fmt.Errorf("list the replicas of %q: %w", rd.Name, err)
	}

	for i := range replicas {
		if !slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDelete) {
			return false, nil
		}
	}

	if len(replicas) > 0 {
		return false, fmt.Errorf("%w: %q", ErrRestoreTargetTearingDown, rd.Name)
	}

	source, err := st.ResourceDefinitions().Get(ctx, snap.ResourceName)
	if err != nil {
		return false, fmt.Errorf("read the source %q of snapshot %q: %w", snap.ResourceName, snap.Name, err)
	}

	have, want := EffectiveLayerStack(source.LayerStack), EffectiveLayerStack(rd.LayerStack)
	if !sameLayerSet(have, want) {
		return false, fmt.Errorf("%w: %q has %s, the source %q has %s", ErrRestoreTargetLayers,
			rd.Name, strings.Join(want, ","), source.Name, strings.Join(have, ","))
	}

	return true, nil
}

// EffectiveLayerStack resolves a stored stack the way the data plane does: the
// dispatcher hands the satellite the definition's own stack and nothing else,
// and the satellite brings up DRBD over STORAGE for an empty one. A parent
// group's stack is not part of it: it is copied onto a definition only when
// the definition is created through `rd c`. It returns a copy.
//
// The control plane reads the group's stack for a definition stored without
// one (the quorum witness, the autoplace gate), so a restored or cloned
// definition left empty under a group with another stack is brought up as one
// shape and judged as another. Stamping this onto it keeps the two in step.
func EffectiveLayerStack(stack []string) []string {
	if len(stack) == 0 {
		return apiv1.DefaultLayerStack()
	}

	return append([]string(nil), stack...)
}

// sameLayerSet compares two stacks as sets. Order follows from the kinds in a
// stack and LINSTOR folds name case, so neither is a difference.
func sameLayerSet(have, want []string) bool {
	for _, layer := range have {
		if !apiv1.LayerInStack(want, layer) {
			return false
		}
	}

	for _, layer := range want {
		if !apiv1.LayerInStack(have, layer) {
			return false
		}
	}

	return true
}

// sameVolumes reports whether a definition's volumes are exactly the ones the
// snapshot recorded: the same numbers at the same sizes, nothing more.
func sameVolumes(vds []apiv1.VolumeDefinition, recorded []apiv1.SnapshotVolumeDef) bool {
	if len(recorded) == 0 || len(vds) != len(recorded) {
		return false
	}

	sizes := make(map[int32]int64, len(recorded))
	for i := range recorded {
		sizes[recorded[i].VolumeNumber] = recorded[i].SizeKib
	}

	for i := range vds {
		size, ok := sizes[vds[i].VolumeNumber]
		if !ok || size != vds[i].SizeKib {
			return false
		}
	}

	return true
}

// stampRestoreMarker writes the restore marker for snap onto a prepared target,
// reporting whether this call wrote it, as opposed to finding the same one
// already there. It is a patch, so a second request racing this one over the
// same target writes the same value; a marker naming another snapshot that
// landed in between is ErrRestoreTargetTaken, and it is left as it is.
func stampRestoreMarker(ctx context.Context, st Store, rdName string, snap *apiv1.Snapshot) (bool, error) {
	marker := snap.ResourceName + ":" + snap.Name

	var wrote bool

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, func(rd *apiv1.ResourceDefinition) error {
		current := rd.Props[RestoreFromSnapshotProp]
		if current != "" && !strings.EqualFold(current, marker) {
			return fmt.Errorf("%w: %q carries %q", ErrRestoreTargetTaken, rdName, current)
		}

		// The mutate runs again on a conflict, so the answer is the last
		// run's, against the state the patch actually applied to.
		wrote = current == ""

		rd.Props = WithRestoreMarker(rd.Props, snap)
		rd.LayerStack = EffectiveLayerStack(rd.LayerStack)

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("stamp the restore marker on %q: %w", rdName, err)
	}

	return wrote, nil
}

// AdoptPreparedRestoreTarget marks a target PreparedRestoreTarget judged
// prepared as this restore's. It reports whether this call is the one that
// marked it: false means the same marker was already there, put by another
// request for the same restore, so the caller judges the target as that
// request's leftover, the way any retry is judged.
//
// Once the marker is on, it stays. linstor-csi re-issues CreateVolume on a
// timeout, and the retry finds the marker this call just wrote, resumes and
// places the replica; this call then meets that replica as AlreadyExists when
// it places its own, which the placement already takes as placed. Taking the
// marker back off on seeing a replica would leave a definition CSI was told is
// restored with no marker, so every later replica would come up blank and every
// later call would be refused.
//
// The window this leaves is a replica placed by hand between the judgement and
// the marker: it is treated as part of the restore. On a target the caller
// prepared for exactly this restore nothing else places one in that moment, and
// a replica placed any later lands on a definition already marked as a restore,
// the state any writer racing a restore on its own target meets.
func AdoptPreparedRestoreTarget(ctx context.Context, st Store, rdName string, snap *apiv1.Snapshot) (bool, error) {
	return stampRestoreMarker(ctx, st, rdName, snap)
}

// ErrRestoreTargetGroupGone reports a restore leftover whose parent resource
// group no longer exists.
var ErrRestoreTargetGroupGone = errors.New("restore leftover is parented to a resource group that no longer exists")

// ErrRestoreRollbackAbandoned reports a restore leftover an earlier attempt
// left when its rollback gave up.
var ErrRestoreRollbackAbandoned = errors.New("restore leftover is what an earlier attempt left when its rollback gave up")

// ErrRestoreTargetForeign reports a restore leftover holding a volume the
// restore would not have written.
var ErrRestoreTargetForeign = errors.New("restore leftover holds a volume this restore would not have written")

// JudgeRestoreLeftover is the CLI's door onto the judgement the REST restore
// makes over a definition that already carries this restore's marker, so the
// two judge a leftover the same way: neither re-places what the other leaves
// alone, nor finishes what the other refuses. What each door then places
// still differs where the doors themselves do: the CLI always places replicas,
// while a bare REST restore places none.
//
// It returns (progress, true, nil) for this restore's own leftover, and
// (_, false, nil) for anything else under the name, which is not this
// restore's to judge. The gates run in the order the REST door runs them:
// a tear-down (the DELETE flag, or every replica being deleted), then a
// volume the restore would not have written, then a parent group that is
// gone, then a mark left by a rollback that gave up.
// Past them the leftover is judged by store.AssessLeftover: finished means the
// snapshot's volumes and at least one replica with a disk (HoldsData), and is
// not re-placed.
// Re-placing it re-stamped a replica on a node the operator had emptied,
// restored from the point-in-time beside one that had moved on with live
// writes.
func JudgeRestoreLeftover(
	ctx context.Context, st Store, rdName string, snap *apiv1.Snapshot,
) (LeftoverProgress, bool, error) {
	return JudgeRestoreLeftoverOf(ctx, st, rdName, snap.ResourceName, snap.Name, snap)
}

// JudgeRestoreLeftoverOf is JudgeRestoreLeftover for a restore named by its
// source and snapshot, with snap nil once the snapshot is gone: the leftover
// is then judged against the volumes the definition recorded when it was
// restored (RestoreVolumesProp), since an absent snapshot cannot unmake a
// finished target.
func JudgeRestoreLeftoverOf(
	ctx context.Context, st Store, rdName, srcRD, snapName string, snap *apiv1.Snapshot,
) (LeftoverProgress, bool, error) {
	rd, err := st.ResourceDefinitions().GetUncached(ctx, rdName)
	if errors.Is(err, ErrNotFound) {
		return LeftoverUnfinished, false, nil
	}

	if err != nil {
		return LeftoverUnfinished, false, fmt.Errorf("read %q: %w", rdName, err)
	}

	marker := srcRD + ":" + snapName
	if !strings.EqualFold(rd.Props[RestoreFromSnapshotProp], marker) {
		return LeftoverUnfinished, false, nil
	}

	if slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return LeftoverTearingDown, true, fmt.Errorf("%w: %q is being deleted", ErrRestoreTargetTearingDown, rd.Name)
	}

	vds, err := LiveVolumes(ctx, st, rd.Name)
	if err != nil {
		return LeftoverUnfinished, true, fmt.Errorf("list the volumes of %q: %w", rd.Name, err)
	}

	progress, err := AssessLeftover(ctx, st, rd.Name, vds, snap, true)
	if err != nil {
		return LeftoverUnfinished, true, err
	}

	if progress == LeftoverTearingDown {
		return progress, true, fmt.Errorf("%w: %q", ErrRestoreTargetTearingDown, rd.Name)
	}

	if progress == LeftoverForeign {
		return progress, true, fmt.Errorf("%w: delete %q and %s", ErrRestoreTargetForeign, rd.Name, restoreAgainFrom(snap))
	}

	err = RestoreTargetGroupSurvived(ctx, st, &rd)
	if err != nil {
		return progress, true, err
	}

	err = rollbackMarkRefusal(rd.Name, rd.Props[RollbackAbandonedProp], snap)
	if err != nil {
		return progress, true, err
	}

	return progress, true, nil
}

// rollbackMarkRefusal is the CLI door's refusal over a rollback mark, worded
// for the step it names; nil when there is none.
func rollbackMarkRefusal(rdName, step string, snap *apiv1.Snapshot) error {
	again := restoreAgainFrom(snap)

	switch step {
	case "":
		return nil
	case RollbackInProgress:
		return fmt.Errorf("%w: %q is being rolled back by the attempt that created it, "+
			"or that attempt stopped mid-way; %s once it has finished, which on the REST door takes under a minute; "+
			"if the mark is still there a minute after the failure, run "+
			"`blockstor resource-definition set-property %s %s` with no value to keep it as it stands, "+
			"or delete it by hand. "+
			"Cleared while the rollback still runs, it stops that rollback before it deletes anything",
			ErrRestoreRollbackAbandoned, rdName, again, rdName, RollbackAbandonedProp)
	case RollbackStepSnapshots, RollbackStepReadSnapshots:
		// Stopped at the definition's snapshots, before touching anything:
		// `rd d` refuses it over those snapshots, so the way to keep it is
		// the clear, as the REST door says.
		return fmt.Errorf("%w (%s): %q is whole, and the rollback stopped at its snapshots; "+
			"delete the snapshots if they are not needed, then delete %q by hand and %s, or run "+
			"`blockstor resource-definition set-property %s %s` with no value to keep it as it stands",
			ErrRestoreRollbackAbandoned, step, rdName, rdName, again, rdName, RollbackAbandonedProp)
	default:
		return fmt.Errorf("%w (%s): delete %q by hand, then %s",
			ErrRestoreRollbackAbandoned, step, rdName, again)
	}
}

// restoreAgainFrom is the way back to a restore once a leftover has been
// deleted: the same restore while its snapshot is there, and another snapshot
// once it is gone, since the same one has nothing left to restore from.
func restoreAgainFrom(snap *apiv1.Snapshot) string {
	if snap == nil {
		return "restore it from another snapshot"
	}

	return "restore again"
}

// RestoreTargetGroupSurvived refuses a restore target whose parent group no
// longer exists. Both doors ask it before they write anything onto a
// definition that is already there, prepared or a leftover: a marker or a
// replica on a definition parented to nothing lists fine and places badly.
func RestoreTargetGroupSurvived(ctx context.Context, st Store, rd *apiv1.ResourceDefinition) error {
	if rd.ResourceGroupName == "" {
		return nil
	}

	_, err := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: %q, group %q", ErrRestoreTargetGroupGone, rd.Name, rd.ResourceGroupName)
	}

	if err != nil {
		return fmt.Errorf("read resource group %q: %w", rd.ResourceGroupName, err)
	}

	return nil
}

// ErrAdoptedLeftoverRollingBack refuses to adopt a leftover whose creator has
// started rolling it back.
var ErrAdoptedLeftoverRollingBack = errors.New("the attempt that created it is rolling it back")

// ClaimAdoptedLeftover is the adopting half of the handshake with the attempt
// that created a restore's definition, which may still be running: it adds
// this claim to RestoreAdoptedProp through the API server, then reads the
// definition back past the cache. A creator whose rollback starts after the
// mark sees it and yields; one that started before is seen here, and the
// adoption is refused. Both doors take it before they write anything onto a
// leftover.
//
// A replay of a finished leftover takes the same mark with a nil snap, which
// records nothing: it writes nothing else, and the mark is what tells a creator
// still rolling back, whose own judgement of "finished" may need a replica the
// replay did not, that somebody was answered for the definition.
//
// The mark is a set of claims, one token each, rather than one slot. Two
// claims can be in flight on one leftover, and a refused one takes only its
// own token back off: a slot restored to the value it held before would hand
// a refused claim's token back from under the other one, and the mark would
// then stand for nobody, which leaves the definition to the rollback's yield
// for good. A claim that is accepted settles the set to its own token, so
// claims that keep coming for a finished definition do not grow it.
//
// Only an accepted claim records the volumes snap holds (RestoreVolumesProp),
// in the same settling write: the leftover is finished from snap, which is not
// always the snapshot it was created from, since a clone's internal snapshot
// can be retaken between attempts. A refused claim writes nothing but its
// token, because the record is what a rollback judges "finished" by
// (ReadsAsFinished), and a record left by a claim that answered for nothing
// would turn a definition the rollback spares into one it deletes. The
// settling write is best-effort: missing it leaves this claim's token in the
// set and the earlier record in place, both of which still hold.
func ClaimAdoptedLeftover(ctx context.Context, st Store, rdName string, snap *apiv1.Snapshot) error {
	token := newAdoptionToken()

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(rd *apiv1.ResourceDefinition) error {
			if rd.Props == nil {
				rd.Props = map[string]string{}
			}

			rd.Props[RestoreAdoptedProp] = withAdoptionToken(rd.Props[RestoreAdoptedProp], token)

			return nil
		})
	if err != nil {
		// A patch that failed at its deadline may still have landed.
		releaseAdoption(ctx, st, rdName, token)

		return fmt.Errorf("mark %q as adopted: %w", rdName, err)
	}

	current, err := st.ResourceDefinitions().GetUncached(ctx, rdName)
	if err != nil {
		releaseAdoption(ctx, st, rdName, token)

		return fmt.Errorf("read %q back after adopting it: %w", rdName, err)
	}

	if current.Props[RollbackAbandonedProp] != "" {
		releaseAdoption(ctx, st, rdName, token)

		return fmt.Errorf("%q cannot be adopted: %w", rdName, ErrAdoptedLeftoverRollingBack)
	}

	settleAdoption(ctx, st, rdName, token, snap)

	return nil
}

// newAdoptionToken names one claim. It is random, so two claims in the same
// instant are still two tokens.
func newAdoptionToken() string {
	var b [8]byte

	_, _ = rand.Read(b[:]) // crypto/rand.Read does not fail

	return hex.EncodeToString(b[:])
}

// adoptionTokens splits a RestoreAdoptedProp value into its claims.
func adoptionTokens(value string) []string {
	if value == "" {
		return nil
	}

	return strings.Split(value, ",")
}

func withAdoptionToken(value, token string) string {
	return strings.Join(append(adoptionTokens(value), token), ",")
}

func withoutAdoptionToken(value, token string) string {
	return strings.Join(slices.DeleteFunc(adoptionTokens(value), func(t string) bool { return t == token }), ",")
}

// settleAdoption is an accepted claim's second write: it narrows the mark to
// this claim's token, which still says somebody was answered, and records the
// volumes snap holds. A token of a claim still in flight that it drops is not
// needed: if that claim is refused it takes nothing off, and if it is accepted
// it settles the mark to its own.
func settleAdoption(ctx context.Context, st Store, rdName, token string, snap *apiv1.Snapshot) {
	_ = st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(rd *apiv1.ResourceDefinition) error {
			if rd.Props == nil {
				rd.Props = map[string]string{}
			}

			rd.Props[RestoreAdoptedProp] = token

			if snap != nil {
				rd.Props[RestoreVolumesProp] = EncodeRestoreVolumes(snap.VolumeDefinitions)
			}

			return nil
		})
}

// ReleaseAdoptionBudget bounds the release of a refused claim's token. The
// release is needed most when the claim failed at its deadline under an
// apiserver too slow to answer, so it gets room to land rather than one more
// short deadline; a token it cannot take off is held by nobody and an operator
// cannot clear it. The release runs detached from the request, so the REST
// server's shutdown window waits for it as it does for a compensation.
const ReleaseAdoptionBudget = 10 * time.Second

// releaseAdoption takes a refused claim's token out of RestoreAdoptedProp and
// leaves every other token, which belongs to a claim this one cannot judge.
// Best-effort: the claim's own error is the answer.
//
// It runs detached from ctx: the claim it undoes may have failed because ctx
// ran out, and a release sent on the same context would never leave the
// process, leaving the token on for good.
func releaseAdoption(ctx context.Context, st Store, rdName, token string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ReleaseAdoptionBudget)
	defer cancel()

	_ = st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(rd *apiv1.ResourceDefinition) error {
			rest := withoutAdoptionToken(rd.Props[RestoreAdoptedProp], token)
			if rest == "" {
				delete(rd.Props, RestoreAdoptedProp)

				return nil
			}

			rd.Props[RestoreAdoptedProp] = rest

			return nil
		})
}

// ErrRollbackYielded reports a rollback that left the definition alone because
// another request marked it adopted, and may already have answered for it.
var ErrRollbackYielded = errors.New("another request marked the definition adopted, so it was left in place")

// ErrRollbackAnswered reports a rollback that left the definition alone because
// it already reads as finished. A replay takes the adoption mark before it
// answers, but the clone status poll answers a finished leftover without
// writing anything, so the mark cannot be what tells the creator somebody was
// told it is done.
var ErrRollbackAnswered = errors.New("it already reads as finished, so a retry or a status poll " +
	"may have answered for it, and it was left in place")

// ReadsAsFinished is the judgement a replay or a status poll would make of the
// definition now, with the volumes and replicas read past the cache. It is
// judged against the shape the definition recorded when it was restored
// (RestoreVolumesProp), which is the snapshot's, so it reads no snapshot.
// needReplica is the door's own: a clone and a placed restore need a replica,
// a bare restore does not.
//
// A rollback asks it after its in-progress mark is on. A replay settles this
// through the adoption handshake instead; the clone status poll, which writes
// nothing, may have answered for the definition before the mark landed, and is
// seen here because the creator's own writes have stopped and nothing it
// already placed is gone yet.
func ReadsAsFinished(ctx context.Context, st Store, rdName string, needReplica bool) (bool, error) {
	rd, err := st.ResourceDefinitions().GetUncached(ctx, rdName)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("read %q: %w", rdName, err)
	}

	// Being deleted is not finished, to the poll or the replay either.
	if slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return false, nil
	}

	vds, err := LiveVolumes(ctx, st, rdName)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("list the volumes of %q: %w", rdName, err)
	}

	progress, err := AssessLeftover(ctx, st, rdName, vds, nil, needReplica)
	if err != nil {
		return false, err
	}

	return progress == LeftoverFinished, nil
}

// AdoptedElsewhere reads, past the cache, whether a request has adopted the
// definition (ClaimAdoptedLeftover). A definition that is gone was adopted by
// nobody.
func AdoptedElsewhere(ctx context.Context, st Store, rdName string) (bool, error) {
	current, err := st.ResourceDefinitions().GetUncached(ctx, rdName)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("read %q back to check for a request that adopted it: %w", rdName, err)
	}

	return current.Props[RestoreAdoptedProp] != "", nil
}

// HoldRollback is the creating half of the handshake ClaimAdoptedLeftover
// describes, for a door that rolls back synchronously: it writes the
// in-progress mark through the API server, then reads the definition back
// past the cache. Nil means the rollback may delete. ErrRollbackYielded means a
// request adopted the definition, ErrRollbackAnswered that it already reads as
// finished (ReadsAsFinished, with needReplica the door's), and any other error
// means the handshake could not be held; all of them mean nothing may be
// deleted, and the mark is taken back off so a retry is not refused over a
// definition left whole.
//
// A definition already gone answers ErrRollbackGone: nothing of it is left to
// remove, and deleting by name would take whatever stands under the name by
// then.
func HoldRollback(ctx context.Context, st Store, rdName string, needReplica bool) error {
	err := setRollbackMark(ctx, st, rdName, RollbackInProgress)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%q: %w", rdName, ErrRollbackGone)
	}

	if err != nil {
		return standDown(ctx, st, rdName, fmt.Errorf("mark %q as rolling back: %w", rdName, err))
	}

	adopted, err := AdoptedElsewhere(ctx, st, rdName)
	if err != nil {
		return standDown(ctx, st, rdName, err)
	}

	if adopted {
		return standDown(ctx, st, rdName, fmt.Errorf("%q: %w", rdName, ErrRollbackYielded))
	}

	finished, err := ReadsAsFinished(ctx, st, rdName, needReplica)
	if err != nil {
		return standDown(ctx, st, rdName, err)
	}

	if finished {
		return standDown(ctx, st, rdName, fmt.Errorf("%q: %w", rdName, ErrRollbackAnswered))
	}

	return nil
}

// ErrRollbackGone reports a rollback whose definition was already gone, so
// there was nothing to remove.
var ErrRollbackGone = errors.New("the definition was already gone")

// errRollbackMarkNotOurs is a definition that no longer carries the
// in-progress mark the rollback wrote: an operator cleared it, or the
// definition under the name is not the one the mark went on.
var errRollbackMarkNotOurs = errors.New("the rollback mark is no longer on the definition")

// EnterDestructiveRollback replaces the in-progress mark HoldRollback wrote
// with step, the first step that takes the definition apart, before that step
// runs. In-progress is then only ever over a definition left whole, which is
// what lets an operator clear it (RollbackMarkClearRefusal). The write checks
// the definition still carries the in-progress mark, the identity check a
// delete by name needs: a definition recreated under the name since carries
// none.
//
// ErrRollbackGone means nothing is left to remove. Any other error means
// nothing may be deleted; the in-progress mark is taken back off unless it was
// not this rollback's to begin with.
func EnterDestructiveRollback(ctx context.Context, st Store, rdName, step string) error {
	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(rd *apiv1.ResourceDefinition) error {
			if rd.Props[RollbackAbandonedProp] != RollbackInProgress {
				return errRollbackMarkNotOurs
			}

			rd.Props[RollbackAbandonedProp] = step

			return nil
		})

	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotFound):
		return fmt.Errorf("%q: %w", rdName, ErrRollbackGone)
	case errors.Is(err, errRollbackMarkNotOurs):
		return fmt.Errorf("%q: %w", rdName, err)
	default:
		return standDown(ctx, st, rdName, fmt.Errorf("mark %q as being taken apart: %w", rdName, err))
	}
}

// standDown takes the in-progress mark back off a definition a rollback leaves
// whole, for why. A mark that cannot be taken off refuses every retry over the
// definition, so that is said beside why rather than dropped.
func standDown(ctx context.Context, st Store, rdName string, why error) error {
	err := ReleaseRollback(ctx, st, rdName)
	if err != nil {
		return errors.Join(why, fmt.Errorf("%w; a retry is refused until the mark is cleared", err))
	}

	return why
}

// ReleaseRollback takes the in-progress mark HoldRollback put back off, for a
// rollback that held the handshake and then could not delete: the definition is
// still there and nobody is rolling it back any more, so a retry has to be able
// to resume it rather than meet a rollback that will never finish.
func ReleaseRollback(ctx context.Context, st Store, rdName string) error {
	err := setRollbackMark(ctx, st, rdName, "")
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("clear the rollback mark on %q: %w", rdName, err)
	}

	return nil
}

// setRollbackMark writes the rollback mark, or removes it when step is empty.
func setRollbackMark(ctx context.Context, st Store, rdName, step string) error {
	return st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, //nolint:wrapcheck // callers wrap with the definition they name
		func(rd *apiv1.ResourceDefinition) error {
			if step == "" {
				delete(rd.Props, RollbackAbandonedProp)

				return nil
			}

			if rd.Props == nil {
				rd.Props = map[string]string{}
			}

			rd.Props[RollbackAbandonedProp] = step

			return nil
		})
}
