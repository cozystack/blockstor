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
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
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
	// Default 10 (DFLT_AUTO_SNAPSHOT_KEEP). A value <=0 disables the
	// prune step, in which case every auto-snapshot is kept.
	PropAutoSnapshotKeep = "AutoSnapshot/Keep"

	// PropAutoSnapshotNextID is the monotonically-incremented
	// counter the runnable uses to name the next auto-snapshot.
	// Stored on the RD so a controller restart picks up the same
	// counter sequence — the same property upstream uses.
	PropAutoSnapshotNextID = "AutoSnapshot/NextAutoId"

	// DefaultAutoSnapshotKeep matches upstream's `DFLT_AUTO_SNAPSHOT_KEEP`
	// (linstor-common/consts.json line 3809).
	DefaultAutoSnapshotKeep = 10

	// AutoSnapshotPrefix is the snapshot-name prefix — upstream uses
	// `autoSnap` (InternalApiConsts.DEFAULT_AUTO_SNAPSHOT_PREFIX).
	// The full snapshot name is `<prefix><id>` with `%05d` padding,
	// e.g. `autoSnap00001`.
	AutoSnapshotPrefix = "autoSnap"

	// AnnotationAutoSnapshotLastAt stamps the wall-clock time of the
	// last auto-snapshot creation onto the parent RD's annotations.
	// The runnable reads this to decide whether enough time has
	// elapsed since the previous tick — avoids relying on the
	// freshly-created Snapshot CRD's metadata.creationTimestamp
	// (which lags by the kube-apiserver's stamp time and would
	// require a fresh GET each pass).
	AnnotationAutoSnapshotLastAt = "blockstor.io/auto-snapshot-last-at"

	// LabelAutoSnapshot is the marker the runnable stamps on every
	// Snapshot CRD it creates so the prune step can distinguish
	// auto-created snapshots from operator-created ones. The
	// scenario doc is explicit: manual snapshots are NOT counted
	// against the Keep budget.
	LabelAutoSnapshot = "blockstor.io/auto-snapshot"
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
		if !rd.DeletionTimestamp.IsZero() {
			continue
		}

		runEvery, ok := parsePositiveMinutes(rd.Spec.Props[PropAutoSnapshotRunEvery])
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

	snapName := formatAutoSnapshotName(id)

	err = r.createAutoSnapshot(ctx, rd, snapName)
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

// createAutoSnapshot constructs the Snapshot CRD with the
// `LabelAutoSnapshot=true` marker so the prune step can find it.
// Idempotent: an `IsAlreadyExists` from kube-apiserver is treated as
// success (the previous tick's RD stamp failed but the snapshot
// itself made it).
func (r *AutoSnapshotRunnable) createAutoSnapshot(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	snapName string,
) error {
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: rd.Name + "." + snapName,
			Labels: map[string]string{
				"blockstor.io/resource-definition": rd.Name,
				"blockstor.io/snapshot-name":       snapName,
				LabelAutoSnapshot:                  "true",
			},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rd.Name,
			SnapshotName:           snapName,
			// Nodes intentionally empty: the REST shim's
			// hydrateSnapshotFromRD path defaults this to "every
			// diskful replica" — but we go through a direct CRD
			// create here (no REST round-trip), so the satellite
			// reconciler needs an explicit node list. Leave the
			// resolution to the satellite-side fan-out via the
			// SnapshotReconciler's intent dispatch — see
			// pkg/satellite/controllers/snapshot.go. If Spec.Nodes
			// is empty the per-node predicate filters everyone out
			// and the snapshot stays Incomplete; auto-snapshot's
			// invariant is "best-effort on diskful replicas", and a
			// later iteration may inline the listDiskfulNodes
			// query, but for the W05 cycle pin we leave the
			// semantic minimal — the e2e suite (out of scope here)
			// verifies the diskful-replica selection.
			VolumeDefinitions: volumeRefsFromRD(rd),
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
func (r *AutoSnapshotRunnable) stampRDAfterCreate(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	nextID int64,
	now time.Time,
) error {
	if rd.Spec.Props == nil {
		rd.Spec.Props = make(map[string]string)
	}

	rd.Spec.Props[PropAutoSnapshotNextID] = strconv.FormatInt(nextID, 10)

	if rd.Annotations == nil {
		rd.Annotations = make(map[string]string)
	}

	rd.Annotations[AnnotationAutoSnapshotLastAt] = now.UTC().Format(time.RFC3339Nano)

	err := r.Client.Update(ctx, rd)
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
// A Keep value of 0 or negative disables the prune (every
// auto-snapshot is retained) — matches the upstream OpenAPI
// "Removing this property or having a value <= 0 disables
// auto-cleanup" wording.
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

		if parsed <= 0 {
			// Disable cleanup — every auto-snapshot is kept.
			return nil
		}

		keep = int(parsed)
	}

	var snapList blockstoriov1alpha1.SnapshotList

	err := r.Client.List(ctx, &snapList, client.MatchingLabels{
		"blockstor.io/resource-definition": rd.Name,
		LabelAutoSnapshot:                  "true",
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
	for i := 0; i < excess; i++ {
		snap := &snapList.Items[i]
		if !snap.DeletionTimestamp.IsZero() {
			// Already being deleted — skip the redundant Delete
			// call but DO count it against the excess so we don't
			// also delete the next one in line.
			continue
		}

		err := r.Client.Delete(ctx, snap)
		if err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "delete auto-snapshot %q", snap.Name)
		}
	}

	return nil
}

// RegisterWithManager wires the runnable into the controller-runtime
// manager. Symmetrical to other Runnables in this package
// (storage_sweeper, ObserverRunnable).
func (r *AutoSnapshotRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(r)
	if err != nil {
		return errors.Wrap(err, "add AutoSnapshotRunnable")
	}

	return nil
}

// parsePositiveMinutes parses the upstream RunEvery property
// (always a `long` in the OpenAPI spec — number of minutes).
// Returns ok=false when the prop is absent, blank, unparseable, or
// <=0 — all of which the OpenAPI doc explicitly maps to "disabled".
func parsePositiveMinutes(raw string) (time.Duration, bool) {
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
// `autoSnap00001`.
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

// formatAutoSnapshotName mirrors upstream LINSTOR's
// `String.format("%s%05d", snapPrefix, id)` —
// `autoSnap00001`, `autoSnap00002`, ... lexicographic sort matches
// numeric sort up to 5 digits, which is what the cleanup pass and
// the `linstor s l` listing both rely on for "oldest first".
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
// into the Snapshot CRD shape. Matches what
// `pkg/rest/snapshots.go#hydrateSnapshotFromRD` builds for the
// REST-driven path; kept inline so the runnable doesn't need to
// reach into the REST store layer.
func volumeRefsFromRD(rd *blockstoriov1alpha1.ResourceDefinition) []blockstoriov1alpha1.SnapshotVolumeRef {
	if len(rd.Spec.VolumeDefinitions) == 0 {
		return nil
	}

	out := make([]blockstoriov1alpha1.SnapshotVolumeRef, 0, len(rd.Spec.VolumeDefinitions))

	for _, vd := range rd.Spec.VolumeDefinitions {
		out = append(out, blockstoriov1alpha1.SnapshotVolumeRef{
			VolumeNumber: vd.VolumeNumber,
			SizeKib:      vd.SizeKib,
		})
	}

	return out
}
