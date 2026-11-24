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

package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// AutoSnapshot prop keys mirror upstream LINSTOR's namespaced keys
// surfaced in third_party/linstor-openapi/rest_v1_openapi.yaml around
// lines 1475-1486. The runnable consumes them straight off the RD's
// Spec.Props map so an operator's `linstor rd set-property pvc-X
// AutoSnapshot/RunEvery 15` flows through the existing REST property
// pipeline with no shim.
const (
	// PropAutoSnapshotRunEvery is the cadence in minutes. A value
	// <=0 (or the prop being absent) disables auto-snapshotting for
	// this RD — matches upstream wording in the OpenAPI spec.
	PropAutoSnapshotRunEvery = "AutoSnapshot/RunEvery"

	// PropAutoSnapshotKeep bounds the retained auto-snapshot count.
	// Default 10 (DFLT_AUTO_SNAPSHOT_KEEP). Per upstream's documented
	// semantic, a value <= 0 (or the prop being absent) falls back to
	// that default of 10 — it does NOT disable the prune (G4).
	PropAutoSnapshotKeep = "AutoSnapshot/Keep"

	// PropAutoSnapshotNextID is the monotonically-incremented
	// counter the runnable uses to name the next auto-snapshot.
	// Stored on the RD so a controller restart picks up the same
	// counter sequence — the same property upstream uses.
	PropAutoSnapshotNextID = "AutoSnapshot/NextAutoId"

	// DefaultAutoSnapshotKeep matches upstream's `DFLT_AUTO_SNAPSHOT_KEEP`
	// (linstor-common/consts.json line 3809).
	DefaultAutoSnapshotKeep = 10

	// AutoSnapshotPrefix is the snapshot-name prefix. Upstream LINSTOR
	// uses `autoSnap` (InternalApiConsts.DEFAULT_AUTO_SNAPSHOT_PREFIX),
	// but the Snapshot CRD's `metadata.name` is composed as
	// `<rd>.<snapName>` and is validated against the RFC1123 subdomain
	// regex by the kube-apiserver — uppercase letters are rejected
	// (the camelCase `autoSnap` would become e.g. `pvc-x.autoSnap00001`
	// and fail with "must consist of lower case alphanumeric
	// characters, '-' or '.'"). Switch to lowercase-with-hyphens so
	// the composed CRD name stays RFC1123-clean. Width-5 zero padding
	// preserves the lexicographic-equals-numeric ordering the prune
	// step depends on. Bug 84.
	AutoSnapshotPrefix = "auto-snap-"

	// AnnotationAutoSnapshotLastAt stamps the wall-clock time of the
	// last auto-snapshot creation onto the parent RD's annotations.
	// The runnable reads this to decide whether enough time has
	// elapsed since the previous tick — avoids relying on the
	// freshly-created Snapshot CRD's metadata.creationTimestamp
	// (which lags by the kube-apiserver's stamp time and would
	// require a fresh GET each pass).
	AnnotationAutoSnapshotLastAt = "blockstor.org/auto-snapshot-last-at"

	// LabelAutoSnapshot is the marker the runnable stamps on every
	// Snapshot CRD it creates so the prune step can distinguish
	// auto-created snapshots from operator-created ones. The
	// scenario doc is explicit: manual snapshots are NOT counted
	// against the Keep budget.
	LabelAutoSnapshot = "blockstor.org/auto-snapshot"

	// labelResourceDefinition tags the Snapshot CRD with the parent RD
	// name so the prune step can match by RD without a full Spec walk.
	labelResourceDefinition = "blockstor.org/resource-definition"

	// labelTrueValue is the Kubernetes-canonical truthy string used for
	// boolean labels (Kubernetes labels are strings).
	labelTrueValue = "true"
)

// Clock is the time source the runnable consumes. Production wires
// `RealClock{}`; tests inject a stubbed clock so they can advance
// time deterministically without sleeping.
type Clock interface {
	Now() time.Time
}

// RealClock returns wall-clock time. Stateless — exposed as a value
// type so callers can construct it inline (`Clock: RealClock{}`).
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// AutoSnapshotPeriod is the default poll cadence for the runnable.
// Upstream LINSTOR's auto-snapshot service polls minute-aligned, so
// 60s is the smallest cadence the `AutoSnapshot/RunEvery` value
// (minutes) can meaningfully resolve. Tests override via .Period.
const AutoSnapshotPeriod = 1 * time.Minute

// AutoSnapshotRunnable is the cluster-wide cron that creates and
// prunes local-only auto-snapshots for every ResourceDefinition
// carrying `AutoSnapshot/RunEvery` + (optionally) `AutoSnapshot/Keep`.
//
// Scenario 8.W05 in tests/scenarios/wave2-08-snapshots.md. Per-RD
// semantic: every N minutes (RunEvery), create one Snapshot CRD;
// keep at most K auto-snapshots (Keep, default 10) — pruning the
// oldest beyond K. Operator-created snapshots are NOT counted
// against K.
//
// Implements manager.Runnable so the c-r manager starts/stops it
// alongside the reconcilers. Leader-elected (NeedLeaderElection
// returns true) so a multi-replica controller Deployment doesn't
// fan out duplicate auto-snapshots.
type AutoSnapshotRunnable struct {
	// Client is the controller-runtime client used for both reads
	// (RDs / Snapshots) and writes (Snapshot create + RD Update for
	// the NextAutoId / last-at bookkeeping).
	Client client.Client

	// Clock is the time source. Defaults to RealClock when nil.
	Clock Clock

	// Period overrides AutoSnapshotPeriod (test-only). A zero
	// Period falls back to the default.
	Period time.Duration

	// Store deletes an auto-snapshot through store.DeleteSnapshot, which
	// judges whether a restore from it is finished. Nil keeps every
	// auto-snapshot a definition was restored from.
	Store store.Store

	// APIReader, when set, reads the replicas an auto-snapshot is
	// taken from straight from the API server, as the store does on a
	// REST create: a LUKS password a fresh replica was just given may
	// not be in the cache yet. Nil reads through Client.
	APIReader client.Reader
}

// NeedLeaderElection returns true: the cron writes Snapshot CRDs
// cluster-wide. With multiple controller replicas running without
// leader election we'd race on the same RD and create N times the
// expected snapshot count per tick.
func (*AutoSnapshotRunnable) NeedLeaderElection() bool { return true }

// Start runs the auto-snapshot loop until ctx cancels. First tick
// fires one period after Start so the controller-runtime cache has
// a chance to warm; an immediate first tick against an empty cache
// would log "no RDs found" on every controller restart.
func (r *AutoSnapshotRunnable) Start(ctx context.Context) error {
	period := r.Period
	if period == 0 {
		period = AutoSnapshotPeriod
	}

	logger := log.FromContext(ctx).WithName("auto-snapshot")

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := r.Tick(ctx)
			if err != nil {
				logger.Error(err, "auto-snapshot tick")
			}
		}
	}
}

// Tick performs exactly one reconcile cycle: scan all RDs, for each
// RD whose `AutoSnapshot/RunEvery` is due, create a fresh Snapshot
// CRD and prune the oldest auto-snapshots beyond `AutoSnapshot/Keep`.
// Exported so unit tests can drive the cycle deterministically
// without running the ticker loop.
func (r *AutoSnapshotRunnable) Tick(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("auto-snapshot")

	clk := r.Clock
	if clk == nil {
		clk = RealClock{}
	}

	var rdList blockstoriov1alpha1.ResourceDefinitionList

	err := r.Client.List(ctx, &rdList)
	if err != nil {
		return errors.Wrap(err, "list ResourceDefinitions")
	}

	now := clk.Now()

	for i := range rdList.Items {
		rd := &rdList.Items[i]

		// Skip RDs being torn down — creating a snapshot on a
		// DeletionTimestamp-marked RD just produces a snapshot the
		// downstream RD-delete will block on.
		if !rd.DeletionTimestamp.IsZero() || slices.Contains(rd.Spec.Flags, rdFlagDelete) {
			continue
		}

		runEvery, ok := parsePositiveDurationMinutes(rd.Spec.Props[PropAutoSnapshotRunEvery])
		if !ok {
			continue
		}

		err := r.processRD(ctx, rd, runEvery, now)
		if err != nil {
			logger.Error(err, "auto-snapshot RD", "rd", rd.Name)
		}
	}

	return nil
}

// processRD handles one RD's auto-snapshot lifecycle in a single
// tick: decide whether the RunEvery interval has elapsed since the
// last auto-snapshot, create a new Snapshot CRD if so, then prune
// the oldest auto-snapshots beyond Keep.
func (r *AutoSnapshotRunnable) processRD(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	runEvery time.Duration,
	now time.Time,
) error {
	lastAt, hasLast := readLastAutoSnapshotAt(rd)
	if hasLast && now.Sub(lastAt) < runEvery {
		// Not due yet — but still prune in case Keep was lowered
		// since the last tick (operator typed `set-property
		// AutoSnapshot/Keep 3` on an RD with 10 existing
		// auto-snapshots).
		return r.pruneOldAutoSnapshots(ctx, rd)
	}

	id, err := nextAutoSnapshotID(rd)
	if err != nil {
		return errors.Wrap(err, "parse NextAutoId")
	}

	skip, err := r.takeUnderWay(ctx, rd)
	if err != nil {
		return err
	}

	var target autoSnapshotTarget

	if skip == "" {
		target, skip, err = r.autoSnapshotTarget(ctx, rd)
		if err != nil {
			return err
		}
	}

	if skip != "" {
		// Nothing is booked, so the next tick tries again. Logged at
		// V(1): the reason holds for as long as the node stays
		// offline, and the tick runs every minute.
		log.FromContext(ctx).WithName("auto-snapshot").V(1).Info("skipping auto-snapshot",
			"rd", rd.Name, "reason", skip)

		return r.pruneOldAutoSnapshots(ctx, rd)
	}

	snapName := formatAutoSnapshotName(id)

	err = r.createAutoSnapshot(ctx, rd, snapName, &target)
	if err != nil {
		return errors.Wrapf(err, "create auto-snapshot %q", snapName)
	}

	err = r.stampRDAfterCreate(ctx, rd, id+1, now)
	if err != nil {
		// The Snapshot is already created at this point — a failure
		// to stamp the RD only means the next tick will retry the
		// SAME id, which will collide with the just-created
		// snapshot. The collision short-circuits in createAutoSnapshot
		// (IsAlreadyExists), but the retried tick wastes a budget.
		// Surface so operators see the underlying conflict.
		return errors.Wrap(err, "stamp RD bookkeeping")
	}

	return r.pruneOldAutoSnapshots(ctx, rd)
}

// takeUnderWay returns a skip reason when a snapshot of rd, automatic
// or not, still holds I/O suspended or may: a second take would
// suspend the same replicas, and whichever resumed first would unfreeze
// the other in the middle of its take.
func (r *AutoSnapshotRunnable) takeUnderWay(
	ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition,
) (string, error) {
	var snapList blockstoriov1alpha1.SnapshotList

	err := r.Client.List(ctx, &snapList, client.MatchingLabels{labelResourceDefinition: rd.Name})
	if err != nil {
		return "", errors.Wrap(err, "list snapshots")
	}

	for i := range snapList.Items {
		if takeInFlight(&snapList.Items[i]) {
			return "snapshot " + snapList.Items[i].Spec.SnapshotName + " is still being taken", nil
		}
	}

	return "", nil
}

// takeInFlight reports whether a snapshot's take may still hold I/O
// suspended: it orders a suspend or a take, or it has no outcome yet
// and a node still reports its suspend.
func takeInFlight(snap *blockstoriov1alpha1.Snapshot) bool {
	return snap.Spec.SuspendIO || snap.Spec.TakeSnapshot ||
		(!takeEnded(snap) && anyNodeSuspendAcked(snap.Status.NodeStatus))
}

// prunable reports whether an auto-snapshot past the keep budget can be
// deleted now. LINSTOR prunes only once the take finished; a snapshot
// deleted mid-take is never resumed, because the orchestration stops at
// the deletion, and its replicas would stay frozen. A finished take
// whose resume a node has not confirmed waits too.
func prunable(snap *blockstoriov1alpha1.Snapshot) bool {
	return takeEnded(snap) && !snap.Spec.SuspendIO && !snap.Spec.TakeSnapshot &&
		!anyNodeSuspendAcked(snap.Status.NodeStatus)
}

// anyNodeSuspendAcked reports whether a node still reports its replica
// suspended for the take.
func anyNodeSuspendAcked(entries []blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
	return slices.ContainsFunc(entries, func(e blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		return e.SuspendIOAcked
	})
}

// autoSnapshotTarget is what an auto-snapshot is taken from: the
// nodes, and the wrapped LUKS passwords of the replicas on them.
type autoSnapshotTarget struct {
	nodes    []string
	luksKeys []blockstoriov1alpha1.SnapshotLUKSKey
}

// add takes a snapshot from res: its node, and the LUKS passwords of
// its volumes.
func (t *autoSnapshotTarget) add(res *blockstoriov1alpha1.Resource) {
	t.nodes = append(t.nodes, res.Spec.NodeName)

	for _, vol := range res.Spec.Volumes {
		if vol.LUKSKey != nil {
			t.luksKeys = append(t.luksKeys, blockstoriov1alpha1.SnapshotLUKSKey{
				NodeName:     res.Spec.NodeName,
				VolumeNumber: vol.VolumeNumber,
				LUKSKey:      *vol.LUKSKey.DeepCopy(),
			})
		}
	}
}

// autoSnapshotTarget returns what an auto-snapshot of rd is taken
// from: every node with an active diskful replica, the set a REST
// create defaults an empty node list to. The orchestration targets
// exactly these nodes and records SUCCESSFUL once each took its
// snapshot; with no node listed it never starts, and linstor-csi
// waits on the snapshot forever. The replicas' LUKS passwords are
// copied as the store copies them on a REST create: a satellite
// refuses to take a snapshot that does not record its replica's.
//
// skip is non-empty when no auto-snapshot should be taken this tick,
// as in LINSTOR's createAutoSnapshotInTransaction: a node of a replica
// is offline, where a take could only freeze I/O on the reachable
// replicas until the deadline aborts it, no replica can take one, or a
// replica's pool cannot take snapshots. A replica on a node being
// evacuated is no source, as LINSTOR leaves out a node flagged
// EVACUATE; blockstor's evacuate flags the node EVICTED.
func (r *AutoSnapshotRunnable) autoSnapshotTarget(
	ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition,
) (autoSnapshotTarget, string, error) {
	replicas, err := r.replicasOf(ctx, rd)
	if err != nil {
		return autoSnapshotTarget{}, "", err
	}

	var (
		target  autoSnapshotTarget
		offline []string
		pools   []poolRef
	)

	for i := range replicas {
		res := &replicas[i]

		node, nodeErr := r.replicaNode(ctx, res.Spec.NodeName)
		if nodeErr != nil {
			return autoSnapshotTarget{}, "", nodeErr
		}

		if node.offline {
			offline = append(offline, res.Spec.NodeName)
		}

		// A replica that holds no data, is down, or is being moved
		// off its node is no source, as in LINSTOR's createSnapshots.
		if node.evacuating || slices.ContainsFunc(res.Spec.Flags, isNoSnapshotSourceFlag) {
			continue
		}

		target.add(res)

		if pool := replicaPool(res); pool != "" {
			pools = append(pools, poolRef{node: res.Spec.NodeName, pool: pool})
		}
	}

	switch {
	case len(offline) > 0:
		sort.Strings(offline)

		return autoSnapshotTarget{}, "node(s) " + strings.Join(offline, ", ") + " offline", nil
	case len(target.nodes) == 0:
		return autoSnapshotTarget{}, "no active diskful replica", nil
	}

	thick, err := r.poolsWithoutSnapshots(ctx, pools)
	if err != nil {
		return autoSnapshotTarget{}, "", err
	}

	if len(thick) > 0 {
		return autoSnapshotTarget{}, "storage pool(s) " + strings.Join(thick, ", ") + " do not support snapshots", nil
	}

	sort.Strings(target.nodes)

	return target, "", nil
}

// replicasOf lists the replicas of rd, through APIReader when set.
func (r *AutoSnapshotRunnable) replicasOf(
	ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition,
) ([]blockstoriov1alpha1.Resource, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	replicas, err := k8s.ResourceCRDsOfDefinition(ctx, reader, rd.Name)
	if err != nil {
		return nil, errors.Wrap(err, "list Resources")
	}

	return replicas, nil
}

// replicaPool is the storage pool a replica is on: the spec field, or
// the StorPoolName prop a replica set up before the field records it
// in, which the REST create reads through the merged props.
func replicaPool(res *blockstoriov1alpha1.Resource) string {
	if res.Spec.StoragePool != "" {
		return res.Spec.StoragePool
	}

	return res.Spec.Props[propStorPoolName]
}

// propStorPoolName is the prop that named a replica's pool before
// Spec.StoragePool did.
const propStorPoolName = "StorPoolName"

// rdFlagDelete marks a definition whose delete is under way.
const rdFlagDelete = "DELETE"

// isNoSnapshotSourceFlag reports whether a replica flag rules the
// replica out as a snapshot source.
func isNoSnapshotSourceFlag(flag string) bool {
	switch flag {
	case apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker,
		apiv1.ResourceFlagInactive, resourceFlagEvacuate:
		return true
	}

	return false
}

// resourceFlagEvacuate marks a replica being moved off its node.
const resourceFlagEvacuate = "EVACUATE"

// poolRef names a storage pool on a node.
type poolRef struct {
	node, pool string
}

// poolsWithoutSnapshots returns the "node/pool" of every pool in refs
// whose provider reports it cannot take a snapshot, the refusal the
// REST create and LINSTOR make for a thick pool. A pool not found is
// not taken for one, as on the REST create.
func (r *AutoSnapshotRunnable) poolsWithoutSnapshots(ctx context.Context, refs []poolRef) ([]string, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	var list blockstoriov1alpha1.StoragePoolList

	err := r.Client.List(ctx, &list)
	if err != nil {
		return nil, errors.Wrap(err, "list StoragePools")
	}

	var out []string

	for _, ref := range refs {
		for i := range list.Items {
			pool := &list.Items[i]
			if naming.Same(pool.Spec.NodeName, ref.node) && naming.Same(pool.Spec.PoolName, ref.pool) &&
				!pool.Status.SupportsSnapshots {
				out = append(out, ref.node+"/"+ref.pool)
			}
		}
	}

	sort.Strings(out)

	return slices.Compact(out), nil
}

// replicaNodeState is what the node of a replica says about taking a
// snapshot there.
type replicaNodeState struct {
	// offline: the heartbeat watchdog marked the node OFFLINE.
	offline bool
	// evacuating: the node is being emptied (EVICTED, or LINSTOR's
	// EVACUATE).
	evacuating bool
}

// replicaNode reads the state of the node named. A node with no record
// is taken for neither, the same call the REST create makes.
func (r *AutoSnapshotRunnable) replicaNode(ctx context.Context, name string) (replicaNodeState, error) {
	var node blockstoriov1alpha1.Node

	err := r.Client.Get(ctx, client.ObjectKey{Name: naming.Key(name)}, &node)
	if apierrors.IsNotFound(err) {
		return replicaNodeState{}, nil
	}

	if err != nil {
		return replicaNodeState{}, errors.Wrapf(err, "get Node %q", name)
	}

	return replicaNodeState{
		offline: isNodeOffline(&node),
		evacuating: slices.ContainsFunc(node.Spec.Flags, func(f string) bool {
			return f == apiv1.NodeFlagEvicted || f == nodeFlagEvacuate
		}),
	}, nil
}

// nodeFlagEvacuate is LINSTOR's flag on a node being evacuated.
const nodeFlagEvacuate = "EVACUATE"

// createAutoSnapshot constructs the Snapshot CRD with the
// `LabelAutoSnapshot=true` marker so the prune step can find it.
// Idempotent: an `IsAlreadyExists` from kube-apiserver is treated as
// success (the previous tick's RD stamp failed but the snapshot
// itself made it).
func (r *AutoSnapshotRunnable) createAutoSnapshot(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	snapName string,
	target *autoSnapshotTarget,
) error {
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: rd.Name + "." + snapName,
			Labels: map[string]string{
				labelResourceDefinition:       rd.Name,
				"blockstor.org/snapshot-name": snapName,
				LabelAutoSnapshot:             labelTrueValue,
			},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rd.Name,
			SnapshotName:           snapName,
			Nodes:                  target.nodes,
			// Created suspended, as the REST create stamps a
			// single snapshot: the take starts at once and the
			// suspend deadline runs from creation.
			SuspendIO:         true,
			VolumeDefinitions: volumeRefsFromRD(rd),
			LUKSKeys:          target.luksKeys,
		},
	}

	err := r.Client.Create(ctx, snap)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			// A previous tick's RD-bookkeeping update failed; we
			// retried with the same id. The snapshot already
			// exists — that's the success state.
			return nil
		}

		return errors.Wrap(err, "create Snapshot")
	}

	return nil
}

// stampRDAfterCreate persists the new NextAutoId + last-at
// annotation so the next tick knows the cadence has been satisfied
// and which id to allocate next. Done in a single Update to keep the
// pair atomic — without that, a crash between the two writes would
// produce either a duplicated id (NextAutoId not bumped) or an
// uncreated snapshot (bumped without snapshot). The Snapshot CRD is
// the source of truth so id-only inconsistency is recoverable on
// retry.
// The rd argument comes from Tick's List and is stale by the time we
// write: the Snapshot create right before this wakes reconcilers that
// update the RD concurrently, so a bare Update on the listed object
// 409s (observed deterministically in the L-integration stack). The
// stamp re-reads the RD fresh and retries the conflict away — losing
// the stamp is worse than the extra read: the next tick re-derives
// the SAME id and only createAutoSnapshot's AlreadyExists guard keeps
// the loop idempotent.
func (r *AutoSnapshotRunnable) stampRDAfterCreate(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	nextID int64,
	now time.Time,
) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh blockstoriov1alpha1.ResourceDefinition

		getErr := r.Client.Get(ctx, client.ObjectKeyFromObject(rd), &fresh)
		if getErr != nil {
			// Bare error: RetryOnConflict matches on the apierrors type.
			return getErr
		}

		if fresh.Spec.Props == nil {
			fresh.Spec.Props = make(map[string]string)
		}

		fresh.Spec.Props[PropAutoSnapshotNextID] = strconv.FormatInt(nextID, 10)

		if fresh.Annotations == nil {
			fresh.Annotations = make(map[string]string)
		}

		fresh.Annotations[AnnotationAutoSnapshotLastAt] = now.UTC().Format(time.RFC3339Nano)

		// Bare error: RetryOnConflict matches on the apierrors type.
		return r.Client.Update(ctx, &fresh)
	})
	if err != nil {
		return errors.Wrap(err, "update RD")
	}

	return nil
}

// pruneOldAutoSnapshots lists every Snapshot CRD this runnable has
// stamped with `LabelAutoSnapshot=true` for the given RD, sorts by
// creation time (oldest first), and deletes anything beyond the
// Keep budget. Operator-created snapshots are NOT carried in the
// label set so they're never pruned — the scenario doc's
// "Manually-created snapshots NOT counted against the keep budget"
// invariant.
//
// G4 (corner-case): a Keep value of 0 or negative falls back to the
// DEFAULT Keep (10) — it does NOT disable cleanup. This matches the
// upstream administration guide: "If AutoSnapshot/Keep is omitted (or
// <= 0), LINSTOR will keep the last 10 snapshots by default"
// (linstor-administration.adoc ~2467).
func (r *AutoSnapshotRunnable) pruneOldAutoSnapshots(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
) error {
	keep := DefaultAutoSnapshotKeep

	if rawKeep, ok := rd.Spec.Props[PropAutoSnapshotKeep]; ok {
		parsed, err := strconv.ParseInt(rawKeep, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "parse %s=%q", PropAutoSnapshotKeep, rawKeep)
		}

		// G4 (corner-case): upstream LINSTOR's documented semantic is
		// "if AutoSnapshot/Keep is omitted (or <= 0), LINSTOR will keep
		// the last 10 snapshots by default" (linstor-administration.adoc
		// ~2467). A <= 0 value therefore falls back to the default Keep
		// (10) — it does NOT disable cleanup. Pre-fix blockstor treated
		// <= 0 as "keep everything", which let an unbounded number of
		// auto-snapshots accumulate and silently diverged from upstream.
		if parsed > 0 {
			keep = int(parsed)
		}
	}

	var snapList blockstoriov1alpha1.SnapshotList

	err := r.Client.List(ctx, &snapList, client.MatchingLabels{
		labelResourceDefinition: rd.Name,
		LabelAutoSnapshot:       labelTrueValue,
	})
	if err != nil {
		return errors.Wrap(err, "list auto-snapshots")
	}

	if len(snapList.Items) <= keep {
		return nil
	}

	// Sort oldest first. Snapshots being deleted (DeletionTimestamp
	// set) STILL count against the budget — a finalizer-blocked
	// snapshot is still occupying a slot from the operator's view,
	// and pretending the budget has been freed would cause the next
	// tick to over-allocate.
	sort.Slice(snapList.Items, func(i, j int) bool {
		return snapList.Items[i].CreationTimestamp.Before(&snapList.Items[j].CreationTimestamp)
	})

	excess := len(snapList.Items) - keep
	for i := range excess {
		snap := &snapList.Items[i]
		if !snap.DeletionTimestamp.IsZero() {
			// Already being deleted — skip the redundant Delete
			// call but DO count it against the excess so we don't
			// also delete the next one in line.
			continue
		}

		// A take not over yet is pruned on a later tick, and a newer
		// snapshot is not deleted in its place.
		if !prunable(snap) {
			continue
		}

		err := r.pruneAutoSnapshot(ctx, rd.Name, snap)
		if err != nil {
			return err
		}
	}

	return nil
}

// pruneAutoSnapshot deletes one auto-snapshot past the keep budget, unless a
// definition restored from it still needs it; it is then kept for this pass
// and pruned once that restore is finished. With a store the delete is
// store.DeleteSnapshot, the one every snapshot delete door runs, which takes
// a finished restore's marker off and holds off a restore racing the delete.
func (r *AutoSnapshotRunnable) pruneAutoSnapshot(
	ctx context.Context, rdName string, snap *blockstoriov1alpha1.Snapshot,
) error {
	if r.Store != nil {
		err := store.DeleteSnapshot(ctx, r.Store, rdName, snap.Spec.SnapshotName)
		if errors.Is(err, store.ErrRestoreSourceInUse) || errors.Is(err, store.ErrNotFound) {
			return nil
		}

		return errors.Wrapf(err, "delete auto-snapshot %q", snap.Name)
	}

	inUse, err := r.restoreReadsFrom(ctx, rdName, snap.Spec.SnapshotName)
	if err != nil || inUse {
		return err
	}

	err = r.Client.Delete(ctx, snap)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "delete auto-snapshot %q", snap.Name)
	}

	return nil
}

// restoreReadsFrom reports, for a runnable with no store, whether any
// definition was restored from rdName:snapName. Without a store a finished
// restore cannot be told from an unfinished one, so every such snapshot is
// kept.
func (r *AutoSnapshotRunnable) restoreReadsFrom(ctx context.Context, rdName, snapName string) (bool, error) {
	var rds blockstoriov1alpha1.ResourceDefinitionList

	err := r.Client.List(ctx, &rds)
	if err != nil {
		return false, errors.Wrap(err, "list definitions restored from an auto-snapshot")
	}

	for i := range rds.Items {
		if store.SameRestoreMarker(rds.Items[i].Spec.Props[store.RestoreFromSnapshotProp], rdName+":"+snapName) {
			return true, nil
		}
	}

	return false, nil
}

// RegisterWithManager wires the runnable into the controller-runtime
// manager. Symmetrical to other Runnables in this package
// (OrphanSweeperRunnable, ObserverRunnable).
func (r *AutoSnapshotRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(r)
	if err != nil {
		return errors.Wrap(err, "add AutoSnapshotRunnable")
	}

	return nil
}

// parsePositiveDurationMinutes parses the upstream RunEvery property
// (always a `long` in the OpenAPI spec — number of minutes).
// Returns ok=false when the prop is absent, blank, unparseable, or
// <=0 — all of which the OpenAPI doc explicitly maps to "disabled".
func parsePositiveDurationMinutes(raw string) (time.Duration, bool) {
	if raw == "" {
		return 0, false
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}

	if n <= 0 {
		return 0, false
	}

	return time.Duration(n) * time.Minute, true
}

// nextAutoSnapshotID reads the RD's `AutoSnapshot/NextAutoId` prop,
// defaulting to 0 (so the first snapshot allocates id=1). Surfaces
// parse errors so an operator-corrupted value doesn't silently
// reset the counter and let the next tick collide with an existing
// `auto-snap-00001`.
func nextAutoSnapshotID(rd *blockstoriov1alpha1.ResourceDefinition) (int64, error) {
	raw, ok := rd.Spec.Props[PropAutoSnapshotNextID]
	if !ok || raw == "" {
		return 1, nil
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse %s=%q", PropAutoSnapshotNextID, raw)
	}

	if n < 1 {
		return 1, nil
	}

	return n, nil
}

// formatAutoSnapshotName builds the per-tick snapshot name. Mirrors
// upstream LINSTOR's `String.format("%s%05d", snapPrefix, id)` shape
// (width-5 zero-padded so lexicographic sort equals numeric sort —
// the prune step relies on "oldest first" by name), but uses the
// RFC1123-clean lowercase-hyphen prefix `auto-snap-` so the composed
// CRD name `<rd>.<snapName>` survives kube-apiserver name validation.
// Yields `auto-snap-00001`, `auto-snap-00002`, ... Bug 84.
func formatAutoSnapshotName(id int64) string {
	return fmt.Sprintf("%s%05d", AutoSnapshotPrefix, id)
}

// readLastAutoSnapshotAt parses the RD's last-auto-snapshot
// annotation. Returns ok=false when the annotation is absent or
// unparseable — both treated as "fire now" because either the
// runnable has never run for this RD or the bookkeeping was
// hand-edited.
func readLastAutoSnapshotAt(rd *blockstoriov1alpha1.ResourceDefinition) (time.Time, bool) {
	raw, ok := rd.Annotations[AnnotationAutoSnapshotLastAt]
	if !ok || raw == "" {
		return time.Time{}, false
	}

	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}

	return t, true
}

// volumeRefsFromRD projects the RD's VolumeDefinitions slot array
// into the Snapshot CRD shape, each volume with its props as they are
// now, which a restore of the snapshot copies. Matches what
// `pkg/rest/snapshots.go#hydrateSnapshotFromRD` builds for the
// REST-driven path; kept inline so the runnable doesn't need to
// reach into the REST store layer.
func volumeRefsFromRD(rd *blockstoriov1alpha1.ResourceDefinition) []blockstoriov1alpha1.SnapshotVolumeRef {
	if len(rd.Spec.VolumeDefinitions) == 0 {
		return nil
	}

	out := make([]blockstoriov1alpha1.SnapshotVolumeRef, 0, len(rd.Spec.VolumeDefinitions))

	for _, vd := range rd.Spec.VolumeDefinitions {
		props := store.TravellingProps(vd.Props)
		if props == nil {
			props = map[string]string{}
		}

		out = append(out, blockstoriov1alpha1.SnapshotVolumeRef{
			VolumeNumber: vd.VolumeNumber,
			SizeKib:      vd.SizeKib,
			Props:        props,
		})
	}

	return out
}
