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

package controllers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// The suspend window of a snapshot is the time between this satellite
// suspending a resource's I/O and resuming it. While it is open the
// application on the Primary cannot write, so the window must close on
// every path and must not wait on anything that itself waits for I/O.
//
// LINSTOR keeps the window short by construction: the controller puts a
// two-minute timeout on the take and, on expiry or any error, clears
// the suspend flag and pushes the resource to every satellite
// (CtrlSnapshotCrtApiCallHandler takeSnapshotInTransaction and
// abortSnapshotInTransaction), and a satellite compares the kernel's
// suspended-user flag with the flag it was sent on every device-manager
// run and resumes when they differ (SuspendManager.manageSuspendIo,
// DrbdLayer.updateSuspendState). The controller side of that already
// exists here (internal/controller/snapshot_timeout.go). This file is
// the satellite side, with two additions LINSTOR does not need:
//
//   - The window is driven by a watchdog of its own, not by the
//     Snapshot workqueue. One worker serves every Snapshot event on the
//     node, deletes included, and a take that waited behind a slow
//     delete held I/O suspended for the whole wait.
//   - The fact that this satellite suspended a resource is written to
//     the state directory before the suspend, so a satellite that
//     restarts inside the window resumes what it suspended. LINSTOR's
//     controller re-sends the desired state on reconnect; here the
//     desired state lives in a CR a restarted satellite cannot tell
//     apart from one it never acted on.
const (
	// defaultSnapshotFlushTimeout bounds the flush before suspend.
	// Nothing is suspended yet, so a slow flush delays the snapshot but
	// cannot freeze I/O; past the bound the snapshot goes ahead
	// crash-consistent, as LINSTOR's always is.
	defaultSnapshotFlushTimeout = 20 * time.Second

	// defaultSnapshotWindowDeadline is how long a window may stay open
	// on this satellite before it resumes on its own and fails the
	// snapshot. Longer than the controller's two-minute deadline, so a
	// healthy controller always aborts first; this one covers a
	// controller that is down or cut off.
	defaultSnapshotWindowDeadline = 150 * time.Second

	// defaultSnapshotWatchdogTick is how often the watchdog looks at
	// every open window.
	defaultSnapshotWatchdogTick = time.Second

	// defaultSnapshotCommandTimeout bounds one drbdadm or drbdsetup call
	// on the window's path.
	defaultSnapshotCommandTimeout = 30 * time.Second

	// snapshotResumeRetry is the requeue after a resume that the kernel
	// did not confirm.
	snapshotResumeRetry = 2 * time.Second

	// snapshotTakeRetry is the pause before a take that failed
	// transiently runs again inside the same window.
	snapshotTakeRetry = time.Second

	// suspendMarkerSuffix names the file that records an open window.
	suspendMarkerSuffix = ".snapshot-suspended"

	suspendMarkerPerm = 0o600
)

// errWindowOpening is returned by a close that raced the suspend
// opening the same window; the caller retries once the suspend returned.
var errWindowOpening = errors.New("snapshot suspend window still opening")

// snapshotWindowTiming carries the window's time bounds. Zero fields
// take the defaults; tests shorten them.
type snapshotWindowTiming struct {
	flushTimeout   time.Duration
	windowDeadline time.Duration
	watchdogTick   time.Duration
	commandTimeout time.Duration
}

func (t snapshotWindowTiming) withDefaults() snapshotWindowTiming {
	if t.flushTimeout <= 0 {
		t.flushTimeout = defaultSnapshotFlushTimeout
	}

	if t.windowDeadline <= 0 {
		t.windowDeadline = defaultSnapshotWindowDeadline
	}

	if t.watchdogTick <= 0 {
		t.watchdogTick = defaultSnapshotWatchdogTick
	}

	if t.commandTimeout <= 0 {
		t.commandTimeout = defaultSnapshotCommandTimeout
	}

	return t
}

// suspendMarker is the on-disk record of a window: this satellite
// suspended Resource's I/O for the Snapshot CR named Snapshot.
type suspendMarker struct {
	Resource    string    `json:"resource"`
	Snapshot    string    `json:"snapshot"`
	Minors      []int32   `json:"minors,omitempty"`
	SuspendedAt time.Time `json:"suspendedAt"`
}

// takeRun is one attempt at the storage-layer snapshot inside a window.
type takeRun struct {
	done       chan struct{}
	transient  bool
	finishedAt time.Time
}

// snapshotWindow is an open window this process suspended.
type snapshotWindow struct {
	marker   suspendMarker
	deadline time.Time
	take     *takeRun
	// closing is set once the window is being resumed. A take that
	// returns after it is discarded rather than reported Ready, since
	// the resume may already have let writes through.
	closing  bool
	resuming bool
	// opening is set until the suspend has returned; the watchdog
	// leaves such a window alone.
	opening bool
}

// snapshotWindows is the set of windows open on this satellite.
type snapshotWindows struct {
	mu      sync.Mutex
	windows map[string]*snapshotWindow
	// resumingMarkers guards resumes of windows left by an earlier
	// process, which have a marker but no entry in windows.
	resumingMarkers map[string]bool
}

func newSnapshotWindows() *snapshotWindows {
	return &snapshotWindows{
		windows:         map[string]*snapshotWindow{},
		resumingMarkers: map[string]bool{},
	}
}

func (set *snapshotWindows) get(name string) *snapshotWindow {
	set.mu.Lock()
	defer set.mu.Unlock()

	return set.windows[name]
}

func (set *snapshotWindows) names() []string {
	set.mu.Lock()
	defer set.mu.Unlock()

	out := make([]string, 0, len(set.windows))
	for name := range set.windows {
		out = append(out, name)
	}

	return out
}

// resourceBusy reports whether another snapshot holds a window on the
// same resource. DRBD has one suspended-user flag per resource, so the
// resume of one window would end the other's.
func (set *snapshotWindows) resourceBusy(resource, snapshot string) bool {
	set.mu.Lock()
	defer set.mu.Unlock()

	for name, w := range set.windows {
		if name != snapshot && w.marker.Resource == resource {
			return true
		}
	}

	return false
}

// claimClose marks the window closing and claims its resume. It
// returns nil when there is no window or another caller holds the
// resume, and errWindowOpening while the suspend is still running.
func (set *snapshotWindows) claimClose(snapshot string) (*snapshotWindow, error) {
	set.mu.Lock()
	defer set.mu.Unlock()

	w := set.windows[snapshot]
	if w == nil || w.resuming {
		return nil, nil //nolint:nilnil // no window to close is not an error, and there is no window to return
	}

	if w.opening {
		return nil, errWindowOpening
	}

	w.closing = true
	w.resuming = true

	return w, nil
}

// windowsFor returns the reconciler's window set, creating it on first
// use so a reconciler built without a manager in tests works too.
func (r *SnapshotReconciler) windowsFor() *snapshotWindows {
	r.windowsOnce.Do(func() {
		r.windows = newSnapshotWindows()
		r.timing = r.timing.withDefaults()
	})

	return r.windows
}

func (r *SnapshotReconciler) windowTiming() snapshotWindowTiming {
	r.windowsFor()

	return r.timing
}

// markerPath returns where the marker for snapshot lives, or "" when
// the satellite has no state directory.
func (r *SnapshotReconciler) markerPath(snapshot string) string {
	dir := r.Config.Apply.StateDir()
	if dir == "" {
		return ""
	}

	return filepath.Join(dir, snapshot+suspendMarkerSuffix)
}

// writeMarker records the window before the suspend, through a rename
// so a crash leaves either no marker or a whole one.
func (r *SnapshotReconciler) writeMarker(marker *suspendMarker) error {
	path := r.markerPath(marker.Snapshot)
	if path == "" {
		return nil
	}

	body, err := json.Marshal(marker)
	if err != nil {
		return errors.Wrap(err, "encode suspend marker")
	}

	tmp := path + ".tmp"

	err = os.WriteFile(tmp, body, suspendMarkerPerm)
	if err != nil {
		return errors.Wrapf(err, "write suspend marker %s", tmp)
	}

	err = os.Rename(tmp, path)
	if err != nil {
		return errors.Wrapf(err, "install suspend marker %s", path)
	}

	return nil
}

func (r *SnapshotReconciler) removeMarker(snapshot string) {
	path := r.markerPath(snapshot)
	if path == "" {
		return
	}

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		log.Log.Info("remove suspend marker", "path", path, "error", err.Error())
	}
}

func (r *SnapshotReconciler) readMarker(snapshot string) (suspendMarker, bool) {
	path := r.markerPath(snapshot)
	if path == "" {
		return suspendMarker{}, false
	}

	body, err := os.ReadFile(path)
	if err != nil {
		return suspendMarker{}, false
	}

	var marker suspendMarker

	err = json.Unmarshal(body, &marker)
	if err != nil {
		return suspendMarker{}, false
	}

	return marker, true
}

// listMarkers returns every marker in the state directory.
func (r *SnapshotReconciler) listMarkers() []suspendMarker {
	dir := r.Config.Apply.StateDir()
	if dir == "" {
		return nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var out []suspendMarker

	for _, entry := range entries {
		name, ok := strings.CutSuffix(entry.Name(), suspendMarkerSuffix)
		if entry.IsDir() || !ok {
			continue
		}

		marker, ok := r.readMarker(name)
		if !ok {
			// An unreadable marker still names the snapshot; the
			// resume then works from the kernel's own minor list.
			marker = suspendMarker{Snapshot: name}
		}

		out = append(out, marker)
	}

	return out
}

// openWindow suspends the resource's I/O for snap and records the
// window. The flush happens first and is bounded; the marker is written
// before the suspend so no suspend exists that a restart cannot find.
func (r *SnapshotReconciler) openWindow(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) error {
	timing := r.windowTiming()
	resource := naming.Key(snap.Spec.ResourceDefinitionName)
	volumes := snapshotVolumeNumbers(snap)

	state, stateErr := r.Config.Apply.ResourceSuspendState(ctx, resource)
	if stateErr != nil {
		log.FromContext(ctx).Info("suspend state unknown before suspend; skipping the flush",
			"resource", resource, "error", stateErr.Error())
	} else {
		r.runBounded(ctx, timing.flushTimeout, func(flushCtx context.Context) {
			r.Config.Apply.FlushBeforeSuspend(flushCtx, resource, state, volumes)
		})
	}

	marker := suspendMarker{
		Resource:    resource,
		Snapshot:    snap.Name,
		SuspendedAt: time.Now(),
	}

	for _, vol := range volumes {
		if minor, ok := state.Minors[vol]; ok {
			marker.Minors = append(marker.Minors, minor)
		}
	}

	// The window is registered before the marker exists, as opening, so
	// the watchdog neither takes the marker for one an earlier process
	// left nor closes the window before its suspend has run.
	set := r.windowsFor()
	window := &snapshotWindow{
		marker:   marker,
		deadline: marker.SuspendedAt.Add(timing.windowDeadline),
		opening:  true,
	}

	set.mu.Lock()
	set.windows[snap.Name] = window
	set.mu.Unlock()

	err := r.suspendRecorded(ctx, &marker)

	set.mu.Lock()
	if err != nil {
		// A marker left behind by an unconfirmed resume is now the
		// sweep's to retry.
		delete(set.windows, snap.Name)
	} else {
		window.opening = false
	}
	set.mu.Unlock()

	return err
}

// suspendRecorded writes the marker and suspends. A suspend that
// returned an error may still have frozen the device, so it is resumed
// before the failure is reported.
func (r *SnapshotReconciler) suspendRecorded(ctx context.Context, marker *suspendMarker) error {
	err := r.writeMarker(marker)
	if err != nil {
		return err
	}

	cmdCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.windowTiming().commandTimeout)
	defer cancel()

	err = r.Config.Apply.SuspendResource(cmdCtx, marker.Resource)
	if err == nil {
		return nil
	}

	resumeErr := r.resumeAndVerify(ctx, marker)
	if resumeErr == nil {
		r.removeMarker(marker.Snapshot)
	}

	return errors.Wrap(err, "suspend")
}

// runBounded runs fn and waits for it at most bound. fn keeps running
// past the bound if it does not honour its context; nothing waits on
// it after that.
func (*SnapshotReconciler) runBounded(ctx context.Context, bound time.Duration, body func(context.Context)) {
	boundCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)

	done := make(chan struct{})

	go func() {
		defer close(done)
		defer cancel()

		body(boundCtx)
	}()

	select {
	case <-done:
	case <-boundCtx.Done():
	}
}

// resumeAndVerify resumes the marker's resource and returns nil only
// once the kernel reports it not suspended. It reads the state first so
// it can fall back to resuming by minor when the .res is gone, and
// again afterwards so the answer is the kernel's, not the exit code's.
func (r *SnapshotReconciler) resumeAndVerify(ctx context.Context, marker *suspendMarker) error {
	if !r.Config.Apply.DRBDEnabled() {
		return nil
	}

	timing := r.windowTiming()

	cmdCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timing.commandTimeout)
	defer cancel()

	resource := marker.Resource

	before, beforeErr := r.Config.Apply.ResourceSuspendState(cmdCtx, resource)
	if beforeErr == nil && (!before.Loaded || !before.SuspendedUser) {
		return nil
	}

	resumeErr := r.Config.Apply.ResumeResource(cmdCtx, resource)
	if resumeErr != nil {
		minors := slices.Clone(marker.Minors)
		for _, minor := range before.Minors {
			if !slices.Contains(minors, minor) {
				minors = append(minors, minor)
			}
		}

		if len(minors) > 0 {
			resumeErr = r.Config.Apply.ResumeMinors(cmdCtx, minors)
		}
	}

	after, afterErr := r.Config.Apply.ResourceSuspendState(cmdCtx, resource)
	if afterErr != nil {
		return errors.Wrap(errors.CombineErrors(afterErr, resumeErr), "verify resume")
	}

	if after.Loaded && after.SuspendedUser {
		return errors.Wrap(errors.CombineErrors(
			errors.Newf("kernel still reports %s suspended-user after resume-io", resource), resumeErr), "verify resume")
	}

	return nil
}

// closeWindow resumes I/O for snapshot and forgets the window once the
// kernel confirms. failSnapshot stamps this node Failed first, for a
// window that ends without a snapshot the controller asked to keep.
// Returns the resume error so the caller can retry; a concurrent close
// of the same window reports nil and leaves the work to that caller.
func (r *SnapshotReconciler) closeWindow(ctx context.Context, snapshot string, failSnapshot bool, reason string) error {
	set := r.windowsFor()

	w, err := set.claimClose(snapshot)
	if w == nil || err != nil {
		return err
	}

	marker := w.marker

	defer func() {
		set.mu.Lock()
		w.resuming = false
		set.mu.Unlock()
	}()

	logger := log.FromContext(ctx).WithValues("snapshot", snapshot, "resource", marker.Resource)

	snap, found := r.cachedSnapshot(ctx, snapshot)
	if failSnapshot && found {
		logger.Info("closing snapshot suspend window without a snapshot", "reason", reason)

		err := r.stampSnapshotPerNodeFailed(ctx, snap)
		if err != nil {
			logger.Info("stamp Failed on window close", "error", err.Error())
		}
	}

	err = r.resumeAndVerify(ctx, &marker)
	if err != nil {
		logger.Info("resume-io not confirmed; retrying", "error", err.Error())
		r.warnResumeFailed(snap, found, err)

		return err
	}

	set.mu.Lock()
	delete(set.windows, snapshot)
	set.mu.Unlock()

	r.removeMarker(snapshot)

	if found {
		clearErr := r.stampSnapshotPerNodeSuspendCleared(ctx, snap)
		if clearErr != nil {
			logger.Info("clear SuspendIOAcked after resume", "error", clearErr.Error())
		}
	}

	return nil
}

// resumeLeftover resumes a window this process did not open: a marker
// left by an earlier process, or a CR that says this node acked a
// suspend no window here remembers. The snapshot is failed, since the
// window it was taken in is gone, and the ack is cleared once the
// kernel confirms the resume.
func (r *SnapshotReconciler) resumeLeftover(ctx context.Context, marker *suspendMarker) error {
	set := r.windowsFor()

	set.mu.Lock()
	if set.windows[marker.Snapshot] != nil || set.resumingMarkers[marker.Snapshot] {
		set.mu.Unlock()

		return nil
	}

	set.resumingMarkers[marker.Snapshot] = true
	set.mu.Unlock()

	defer func() {
		set.mu.Lock()
		delete(set.resumingMarkers, marker.Snapshot)
		set.mu.Unlock()
	}()

	snap, found := r.cachedSnapshot(ctx, marker.Snapshot)
	if found && marker.Resource == "" {
		marker.Resource = naming.Key(snap.Spec.ResourceDefinitionName)
	}

	if found && snap.Spec.SuspendIO {
		err := r.stampSnapshotPerNodeFailed(ctx, snap)
		if err != nil {
			return err
		}
	}

	if marker.Resource == "" {
		// Neither the marker nor a CR names the resource; nothing can
		// be resumed by name, and keeping the marker would retry forever.
		r.removeMarker(marker.Snapshot)

		return nil
	}

	err := r.resumeAndVerify(ctx, marker)
	if err != nil {
		r.warnResumeFailed(snap, found, err)

		return err
	}

	r.removeMarker(marker.Snapshot)

	if found {
		return r.stampSnapshotPerNodeSuspendCleared(ctx, snap)
	}

	return nil
}

// warnResumeFailed puts a failed resume on the Snapshot's events so it
// reaches `kubectl describe`, not only the satellite log.
func (r *SnapshotReconciler) warnResumeFailed(snap *blockstoriov1alpha1.Snapshot, found bool, err error) {
	if !found || r.Config.Recorder == nil {
		return
	}

	r.Config.Recorder.Eventf(snap, nil, corev1.EventTypeWarning, "ResumeIOFailed", "ResumeIO",
		"I/O of %s is still suspended on node %s: %s",
		snap.Spec.ResourceDefinitionName, r.Config.NodeName, err.Error())
}

// cachedSnapshot reads a Snapshot by CR name.
func (r *SnapshotReconciler) cachedSnapshot(ctx context.Context, name string) (*blockstoriov1alpha1.Snapshot, bool) {
	var snap blockstoriov1alpha1.Snapshot

	err := r.Get(ctx, types.NamespacedName{Name: name}, &snap)
	if err != nil {
		return nil, false
	}

	return &snap, true
}

// startTake starts the storage-layer snapshot for an open window unless
// one is running or has already finished. A take that failed
// transiently is started again after snapshotTakeRetry. Nothing waits
// for the take: it reports its own verdict, and the watchdog closes the
// window if the take outlives it. ctx supplies values only; the take
// is bounded by the window's deadline, not by the caller.
func (r *SnapshotReconciler) startTake(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) {
	set := r.windowsFor()

	set.mu.Lock()
	defer set.mu.Unlock()

	w := set.windows[snap.Name]
	if w == nil || w.closing {
		return
	}

	if w.take != nil {
		select {
		case <-w.take.done:
			if !w.take.transient || time.Since(w.take.finishedAt) < snapshotTakeRetry {
				return
			}
		default:
			return
		}
	}

	run := &takeRun{done: make(chan struct{})}
	w.take = run
	deadline := w.deadline
	target := snap.DeepCopy()

	go r.runTake(context.WithoutCancel(ctx), target, deadline, run)
}

// runTake takes the snapshot under the window's deadline and reports
// the verdict, unless the window started closing meanwhile: a take that
// returns after the resume may hold writes from after the suspend.
func (r *SnapshotReconciler) runTake(
	parent context.Context, snap *blockstoriov1alpha1.Snapshot, deadline time.Time, run *takeRun,
) {
	defer close(run.done)

	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()

	logger := log.FromContext(parent).WithValues("snapshot", snap.Name)
	ctx = log.IntoContext(ctx, logger)

	verdict, createdAt, err := r.takeVerdict(ctx, snap)
	retry := verdict == takeTransient || err != nil

	switch {
	case r.windowClosing(snap.Name, run):
		logger.Info("snapshot take returned after its suspend window closed; result discarded",
			"verdict", int(verdict), "error", errString(err))
	case err != nil:
		logger.Info("snapshot take failed; retrying inside the window", "error", err.Error())
	case !retry:
		stampErr := r.reportTake(ctx, snap, verdict, createdAt)
		if stampErr != nil {
			logger.Info("stamp snapshot take result; retaking", "error", stampErr.Error())

			retry = true
		}
	}

	set := r.windowsFor()

	set.mu.Lock()
	run.transient = retry
	run.finishedAt = time.Now()
	set.mu.Unlock()
}

// takeInFlight reports whether a take for snapshot is still running.
func (r *SnapshotReconciler) takeInFlight(snapshot string) bool {
	set := r.windowsFor()

	set.mu.Lock()
	defer set.mu.Unlock()

	w := set.windows[snapshot]
	if w == nil || w.take == nil {
		return false
	}

	select {
	case <-w.take.done:
		return false
	default:
		return true
	}
}

// windowClosing reports whether the window that started run is closing
// or gone.
func (r *SnapshotReconciler) windowClosing(snapshot string, run *takeRun) bool {
	set := r.windowsFor()

	set.mu.Lock()
	defer set.mu.Unlock()

	w := set.windows[snapshot]

	return w == nil || w.closing || w.take != run
}

// reportTake stamps a final take verdict on the Snapshot.
func (r *SnapshotReconciler) reportTake(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, verdict takeOutcome, createdAt int64,
) error {
	if verdict == takeOK {
		return r.stampSnapshotPerNodeReady(ctx, snap, createdAt)
	}

	return r.stampSnapshotPerNodeFailed(ctx, snap)
}

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

// SnapshotSuspendWatchdog drives every open suspend window on its own
// goroutine: it starts the take once the controller asks for it,
// resumes once the controller ends the window, the Snapshot goes away
// or the local deadline passes, and on start resumes whatever an
// earlier process left suspended. None of it goes through the Snapshot
// workqueue, so a queue busy with deletes cannot hold I/O suspended.
type SnapshotSuspendWatchdog struct {
	Reconciler *SnapshotReconciler
}

// NeedLeaderElection reports false: every satellite owns its windows.
func (*SnapshotSuspendWatchdog) NeedLeaderElection() bool { return false }

// Start runs the watchdog until ctx ends, then resumes every window
// still open so a stopping satellite does not leave I/O suspended.
func (w *SnapshotSuspendWatchdog) Start(ctx context.Context) error {
	r := w.Reconciler
	timing := r.windowTiming()

	r.sweepLeftoverMarkers(ctx)

	ticker := time.NewTicker(timing.watchdogTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.closeAllWindows(context.WithoutCancel(ctx))

			return nil
		case <-ticker.C:
			r.watchWindows(ctx)
			r.sweepLeftoverMarkers(ctx)
		}
	}
}

// watchWindows makes one pass over the open windows.
func (r *SnapshotReconciler) watchWindows(ctx context.Context) {
	set := r.windowsFor()

	for _, name := range set.names() {
		r.watchWindow(ctx, name)
	}
}

func (r *SnapshotReconciler) watchWindow(ctx context.Context, name string) {
	set := r.windowsFor()

	set.mu.Lock()

	w := set.windows[name]
	if w == nil {
		set.mu.Unlock()

		return
	}

	if w.opening {
		set.mu.Unlock()

		return
	}

	closing := w.closing
	expired := time.Now().After(w.deadline)
	set.mu.Unlock()

	var snap blockstoriov1alpha1.Snapshot

	err := r.Get(ctx, types.NamespacedName{Name: name}, &snap)

	switch {
	case closing:
		_ = r.closeWindow(ctx, name, false, "")
	case apierrors.IsNotFound(err):
		_ = r.closeWindow(ctx, name, false, "")
	case err != nil:
		// The cache did not answer; the deadline still applies.
		if expired {
			_ = r.closeWindow(ctx, name, true, "suspend window deadline passed")
		}
	case !snap.DeletionTimestamp.IsZero() || !snap.Spec.SuspendIO:
		_ = r.closeWindow(ctx, name, false, "")
	case expired:
		_ = r.closeWindow(ctx, name, true, "suspend window deadline passed")
	case perNodeStatusFailed(snap.Status.NodeStatus, r.Config.NodeName):
		_ = r.closeWindow(ctx, name, false, "")
	case snap.Spec.TakeSnapshot && !perNodeStatusReady(snap.Status.NodeStatus, r.Config.NodeName):
		r.startTake(ctx, &snap)
	}
}

// sweepLeftoverMarkers resumes every marker without an open window.
func (r *SnapshotReconciler) sweepLeftoverMarkers(ctx context.Context) {
	set := r.windowsFor()

	for _, marker := range r.listMarkers() {
		if set.get(marker.Snapshot) != nil {
			continue
		}

		err := r.resumeLeftover(ctx, &marker)
		if err != nil {
			log.FromContext(ctx).Info("resume of a suspend left by an earlier satellite not confirmed; retrying",
				"snapshot", marker.Snapshot, "resource", marker.Resource, "error", err.Error())
		}
	}
}

// closeAllWindows resumes every open window on shutdown, each bounded
// by the command timeout. A window whose resume fails keeps its marker
// for the next start. ctx must outlive the stop that triggered it.
func (r *SnapshotReconciler) closeAllWindows(ctx context.Context) {
	for _, name := range r.windowsFor().names() {
		err := r.closeWindow(ctx, name, true, "satellite stopping")
		if err != nil {
			log.FromContext(ctx).Info("resume on shutdown not confirmed; the next start resumes it",
				"snapshot", name, "error", err.Error())
		}
	}
}

// perNodeStatusFailed reports whether nodeName's entry is Failed.
func perNodeStatusFailed(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string) bool {
	for i := range entries {
		if naming.Same(entries[i].NodeName, nodeName) && entries[i].Failed {
			return true
		}
	}

	return false
}

// snapshotVolumeNumbers lists the volumes a snapshot captures.
func snapshotVolumeNumbers(snap *blockstoriov1alpha1.Snapshot) []int32 {
	out := make([]int32, 0, len(snap.Spec.VolumeDefinitions))
	for i := range snap.Spec.VolumeDefinitions {
		out = append(out, snap.Spec.VolumeDefinitions[i].VolumeNumber)
	}

	return out
}
