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
	"github.com/cozystack/blockstor/pkg/drbd"
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
//   - A suspend that neither an open window nor a marker accounts for
//     is resumed by a periodic sweep over the kernel's flags, which is
//     the comparison LINSTOR makes on every run; without it, a suspend
//     left by an older satellite that wrote no markers stays forever.
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

	// defaultUnownedSuspendSweep is how often the watchdog compares the
	// kernel's suspended-user flag of every local resource with the
	// windows and markers that account for it. LINSTOR makes the same
	// comparison on every device-manager run.
	defaultUnownedSuspendSweep = 10 * time.Second

	// resumeWarnInterval bounds the ResumeIOFailed Events of one
	// snapshot; the watchdog retries every tick, the Event need not.
	resumeWarnInterval = time.Minute

	// snapshotResumeRetry is the requeue after a resume that the kernel
	// did not confirm.
	snapshotResumeRetry = 2 * time.Second

	// snapshotBusyRetry is the requeue of a suspend that waits for
	// another snapshot of the same resource.
	snapshotBusyRetry = time.Second

	// snapshotTakeRetry is the pause before a take that failed
	// transiently runs again inside the same window.
	snapshotTakeRetry = time.Second

	// suspendMarkerSuffix names the file that records an open window.
	suspendMarkerSuffix = ".snapshot-suspended"

	suspendMarkerPerm = 0o600
)

var (
	// errWindowOpening is returned by a close that raced the suspend
	// opening the same window; the caller retries once the suspend
	// returned.
	errWindowOpening = errors.New("snapshot suspend window still opening")

	// errResourceBusy is returned by an open while something else holds
	// the resource's one suspended-user flag: another snapshot's window,
	// a marker an earlier process left, or a resume of a suspend nothing
	// accounted for. The resume of either would end the other.
	errResourceBusy = errors.New("resource held by another suspend window")

	// errSatelliteStopping is returned by an open once the watchdog has
	// run its shutdown resume; a suspend after it would outlive the
	// process.
	errSatelliteStopping = errors.New("satellite stopping")
)

// snapshotWindowTiming carries the window's time bounds. Zero fields
// take the defaults; tests shorten them.
type snapshotWindowTiming struct {
	flushTimeout   time.Duration
	windowDeadline time.Duration
	watchdogTick   time.Duration
	commandTimeout time.Duration
	unownedSweep   time.Duration
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

	if t.unownedSweep <= 0 {
		t.unownedSweep = defaultUnownedSuspendSweep
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
	// Resumed is set once the kernel confirmed the resume while the
	// Snapshot's status still owes the outcome; the marker then only
	// keeps the snapshot from being suspended again until it lands.
	Resumed bool `json:"resumed,omitempty"`
	// FailOwed is set when this node's Failed stamp is among what is
	// owed.
	FailOwed bool `json:"failOwed,omitempty"`
	// HoldUntil keeps the marker after a failed suspend. A suspend-io
	// killed at its timeout may still land in the kernel after the
	// resume that followed it read the device as running.
	HoldUntil time.Time `json:"holdUntil,omitzero"`
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

	// stampMu orders a take's verdict against the Failed stamp of a
	// close, so a take that returned in time and a deadline that
	// passed while it was being stamped do not both land.
	stampMu sync.Mutex
	// reported is set, under stampMu, once a take stamped Ready.
	reported bool
}

// snapshotWindows is the set of windows open on this satellite.
type snapshotWindows struct {
	mu      sync.Mutex
	windows map[string]*snapshotWindow
	// takes holds every take still running, by snapshot. It outlives
	// the window: a take the deadline gave up on can still create the
	// storage snapshot, and a delete must not run before it returns.
	takes map[string]*takeRun
	// resumingMarkers guards resumes of windows left by an earlier
	// process, which have a marker but no entry in windows.
	resumingMarkers map[string]bool
	// resumingResources guards resumes of suspends that no window or
	// marker accounts for, by resource.
	resumingResources map[string]bool
	// ended names the snapshots whose window this process opened and
	// closed, or failed to open. A Snapshot is suspended at most once
	// per process: a window that closed without its outcome reaching
	// the Snapshot would otherwise open again on the next pass.
	ended map[string]bool
	// lastWarn is when each snapshot last got a ResumeIOFailed Event.
	lastWarn map[string]time.Time
	// stopping is set once the watchdog runs its shutdown resume.
	stopping bool
	// lastUnownedSweep is when the sweep for unaccounted suspends last
	// started; unownedSweeping is set while it runs.
	lastUnownedSweep time.Time
	unownedSweeping  bool

	// work counts the resumes the watchdog started on goroutines of
	// their own, so its shutdown waits for them.
	work sync.WaitGroup
}

func newSnapshotWindows() *snapshotWindows {
	return &snapshotWindows{
		windows:           map[string]*snapshotWindow{},
		takes:             map[string]*takeRun{},
		resumingMarkers:   map[string]bool{},
		resumingResources: map[string]bool{},
		ended:             map[string]bool{},
		lastWarn:          map[string]time.Time{},
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

// busyLocked reports whether something other than snapshot holds the
// resource's suspended-user flag: an open window, a marker that is not
// yet resumed, or a resume of an unaccounted suspend. DRBD has one such
// flag per resource, so the resume of one would end the other. The
// caller holds set.mu and read markers from disk.
func (set *snapshotWindows) busyLocked(resource, snapshot string, markers []suspendMarker) bool {
	if set.resumingResources[resource] {
		return true
	}

	for name, w := range set.windows {
		if name != snapshot && w.marker.Resource == resource {
			return true
		}
	}

	for i := range markers {
		m := &markers[i]
		if m.Snapshot != snapshot && m.Resource == resource && !m.Resumed {
			return true
		}
	}

	return false
}

// heldByOther reports whether a live window of another snapshot holds
// the resource.
func (set *snapshotWindows) heldByOther(resource, snapshot string) bool {
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
	set.ended[snapshot] = true

	return w, nil
}

// claimLeftover claims the resume of a leftover marker; false when its
// window is open here or another caller holds the claim.
func (set *snapshotWindows) claimLeftover(snapshot string) bool {
	set.mu.Lock()
	defer set.mu.Unlock()

	if set.windows[snapshot] != nil || set.resumingMarkers[snapshot] {
		return false
	}

	set.resumingMarkers[snapshot] = true

	return true
}

func (set *snapshotWindows) releaseLeftover(snapshot string) {
	set.mu.Lock()
	defer set.mu.Unlock()

	delete(set.resumingMarkers, snapshot)
}

// spawn runs body on a goroutine the watchdog's shutdown waits for.
func (set *snapshotWindows) spawn(body func()) {
	set.work.Go(body)
}

// register claims the resource for a window that is about to open.
func (set *snapshotWindows) register(snapshot string, window *snapshotWindow, markers []suspendMarker) error {
	set.mu.Lock()
	defer set.mu.Unlock()

	switch {
	case set.stopping:
		return errSatelliteStopping
	case set.busyLocked(window.marker.Resource, snapshot, markers):
		return errResourceBusy
	}

	set.windows[snapshot] = window

	return nil
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

// windowEnded reports whether snap's window is over on this node: this
// process closed it, or a marker of an earlier window is still there.
func (r *SnapshotReconciler) windowEnded(snapshot string) bool {
	set := r.windowsFor()

	set.mu.Lock()
	ended := set.ended[snapshot]
	set.mu.Unlock()

	if ended {
		return true
	}

	_, ok := r.readMarker(snapshot)

	return ok
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

// rewriteMarker updates a marker that already exists; a failure only
// costs a retry the sweep would have made anyway.
func (r *SnapshotReconciler) rewriteMarker(marker *suspendMarker) {
	err := r.writeMarker(marker)
	if err != nil {
		log.Log.Info("update suspend marker", "snapshot", marker.Snapshot, "error", err.Error())
	}
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
		// An unreadable marker still names the snapshot; the resume
		// then works from the CR and the kernel's own minor list.
		return suspendMarker{Snapshot: snapshot}, true
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
		if ok {
			out = append(out, marker)
		}
	}

	return out
}

// openWindow suspends the resource's I/O for snap and records the
// window. The window is registered first, which claims the resource;
// the flush follows and is bounded; the marker is written before the
// suspend so no suspend exists that a restart cannot find.
func (r *SnapshotReconciler) openWindow(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) error {
	timing := r.windowTiming()
	resource := naming.Key(snap.Spec.ResourceDefinitionName)
	volumes := snapshotVolumeNumbers(snap)
	set := r.windowsFor()

	// Registered as opening, the window keeps the watchdog from taking
	// its marker for one an earlier process left or closing it before
	// its suspend has run, and keeps every other suspend off the
	// resource.
	window := &snapshotWindow{
		marker:  suspendMarker{Resource: resource, Snapshot: snap.Name},
		opening: true,
	}

	err := set.register(snap.Name, window, r.listMarkers())
	if err != nil {
		return err
	}

	marker := r.flushBeforeSuspend(ctx, snap.Name, resource, volumes)

	set.mu.Lock()
	if set.stopping {
		delete(set.windows, snap.Name)
		set.mu.Unlock()

		return errSatelliteStopping
	}

	window.marker = marker
	window.deadline = marker.SuspendedAt.Add(timing.windowDeadline)
	set.mu.Unlock()

	err = r.suspendRecorded(ctx, &marker)

	set.mu.Lock()
	if err != nil {
		// A marker kept by a failed suspend is now the sweep's.
		delete(set.windows, snap.Name)
		set.ended[snap.Name] = true
	} else {
		window.opening = false
	}
	set.mu.Unlock()

	return err
}

// flushBeforeSuspend runs the bounded flush and returns the marker of
// the suspend that follows it, with the minors the kernel reported.
func (r *SnapshotReconciler) flushBeforeSuspend(
	ctx context.Context, snapshot, resource string, volumes []int32,
) suspendMarker {
	state, stateErr := r.Config.Apply.ResourceSuspendState(ctx, resource)
	if stateErr != nil {
		log.FromContext(ctx).Info("suspend state unknown before suspend; skipping the flush",
			"resource", resource, "error", stateErr.Error())
	} else {
		r.runBounded(ctx, r.windowTiming().flushTimeout, func(flushCtx context.Context) {
			r.Config.Apply.FlushBeforeSuspend(flushCtx, resource, state, volumes)
		})
	}

	marker := suspendMarker{
		Resource:    resource,
		Snapshot:    snapshot,
		SuspendedAt: time.Now(),
	}

	for _, vol := range volumes {
		if minor, ok := state.Minors[vol]; ok {
			marker.Minors = append(marker.Minors, minor)
		}
	}

	return marker
}

// suspendRecorded writes the marker and suspends. A suspend that
// returned an error may still have frozen the device, so it is resumed
// before the failure is reported, and the marker stays for one more
// command timeout: a suspend-io killed at its timeout may have sent its
// request already, and the kernel can apply it after the resume read
// the device as running. The sweep resumes it then.
func (r *SnapshotReconciler) suspendRecorded(ctx context.Context, marker *suspendMarker) error {
	err := r.writeMarker(marker)
	if err != nil {
		return err
	}

	timing := r.windowTiming()

	cmdCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timing.commandTimeout)
	defer cancel()

	err = r.Config.Apply.SuspendResource(cmdCtx, marker.Resource)
	if err == nil {
		return nil
	}

	resumeErr := r.resumeAndVerify(ctx, marker)
	if resumeErr != nil {
		log.FromContext(ctx).Info("resume after a failed suspend not confirmed; the sweep retries it",
			"resource", marker.Resource, "error", resumeErr.Error())
	}

	marker.FailOwed = true
	marker.HoldUntil = time.Now().Add(timing.commandTimeout)
	r.rewriteMarker(marker)

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
// once the kernel reports it not suspended. It reads the state first,
// so a resource that is not suspended is left alone and the minors to
// fall back on are known when the .res is gone, and again afterwards so
// the answer is the kernel's, not the exit code's.
func (r *SnapshotReconciler) resumeAndVerify(ctx context.Context, marker *suspendMarker) error {
	return r.resumeVerified(ctx, marker, true)
}

// resumeVerified is resumeAndVerify with the first read optional. The
// close of a window this process opened skips it: the resource is
// suspended and the marker has its minors, so the read would only hold
// I/O frozen for one more command. LINSTOR resumes right after the take
// the same way, with resume-io alone.
func (r *SnapshotReconciler) resumeVerified(ctx context.Context, marker *suspendMarker, readFirst bool) error {
	if !r.Config.Apply.DRBDEnabled() {
		return nil
	}

	timing := r.windowTiming()

	cmdCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timing.commandTimeout)
	defer cancel()

	resource := marker.Resource

	var before drbd.SuspendState

	if readFirst {
		state, stateErr := r.Config.Apply.ResourceSuspendState(cmdCtx, resource)
		if stateErr == nil && (!state.Loaded || !state.SuspendedUser) {
			return nil
		}

		before = state
	}

	resumeErr := r.Config.Apply.ResumeResource(cmdCtx, resource)
	if resumeErr != nil {
		if !readFirst && len(marker.Minors) == 0 {
			before, _ = r.Config.Apply.ResourceSuspendState(cmdCtx, resource)
		}

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

// settleResumed records on the Snapshot that this node's window is
// over: Failed when failOwed, and the ack cleared, which is what the
// controller reads as I/O resumed. A Snapshot that is gone owes nothing.
func (r *SnapshotReconciler) settleResumed(ctx context.Context, snapshot string, failOwed bool) error {
	snap := &blockstoriov1alpha1.Snapshot{}
	snap.Name = snapshot

	if failOwed {
		err := r.stampSnapshotPerNodeFailed(ctx, snap)
		if err != nil {
			return err
		}
	}

	return r.stampSnapshotPerNodeSuspendCleared(ctx, snap)
}

// closeWindow resumes I/O for snapshot and forgets the window once the
// kernel confirms. failSnapshot stamps this node Failed first, for a
// window that ends without a snapshot the controller asked to keep.
// Returns the resume error so the caller can retry; a concurrent close
// of the same window reports nil and leaves the work to that caller.
// What the Snapshot's status still owes after the resume stays in the
// marker for the sweep, so it is retried and the window is not opened
// again meanwhile.
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
	failOwed := r.failOnClose(ctx, w, snapshot, failSnapshot, reason)

	err = r.resumeVerified(ctx, &marker, false)
	if err != nil {
		logger.Info("resume-io not confirmed; retrying", "error", err.Error())
		r.warnResumeFailed(ctx, snapshot, err)

		return err
	}

	set.mu.Lock()
	delete(set.windows, snapshot)
	delete(set.lastWarn, snapshot)
	set.mu.Unlock()

	settleErr := r.settleResumed(ctx, snapshot, failOwed)
	if settleErr != nil {
		logger.Info("status after resume not recorded; the sweep retries it", "error", settleErr.Error())

		marker.Resumed = true
		marker.FailOwed = failOwed
		r.rewriteMarker(&marker)

		return nil
	}

	r.removeMarker(snapshot)

	return nil
}

// failOnClose stamps this node Failed for a window that ends without a
// snapshot, unless its take already stamped Ready, and reports whether
// the stamp is still owed. The check and the stamp run under the
// window's stampMu, which the take holds across its own check and
// stamp.
func (r *SnapshotReconciler) failOnClose(
	ctx context.Context, w *snapshotWindow, snapshot string, failSnapshot bool, reason string,
) bool {
	if !failSnapshot {
		return false
	}

	w.stampMu.Lock()
	defer w.stampMu.Unlock()

	if w.reported {
		return false
	}

	logger := log.FromContext(ctx).WithValues("snapshot", snapshot)
	logger.Info("closing snapshot suspend window without a snapshot", "reason", reason)

	snap := &blockstoriov1alpha1.Snapshot{}
	snap.Name = snapshot

	err := r.stampSnapshotPerNodeFailed(ctx, snap)
	if err != nil {
		logger.Info("stamp Failed on window close; retried after the resume", "error", err.Error())

		return true
	}

	return false
}

// resumeLeftover resumes a window this process did not open: a marker
// left by an earlier process, a CR that says this node acked a suspend
// no window here remembers, or a marker whose status is still owed. The
// snapshot is failed, since the window it was taken in is gone, and the
// ack is cleared once the kernel confirms the resume. The resume comes
// first: a status write that fails must not keep I/O suspended. A live
// window of another snapshot on the same resource holds the one flag
// both share, so the marker is then settled without a resume.
func (r *SnapshotReconciler) resumeLeftover(ctx context.Context, marker *suspendMarker) error {
	set := r.windowsFor()
	if !set.claimLeftover(marker.Snapshot) {
		return nil
	}

	defer set.releaseLeftover(marker.Snapshot)

	snap, found, getErr := r.cachedSnapshot(ctx, marker.Snapshot)
	if found && marker.Resource == "" {
		marker.Resource = naming.Key(snap.Spec.ResourceDefinitionName)
	}

	// A marker that is already resumed records exactly what is owed; a
	// live one fails a snapshot the controller still wants taken.
	failOwed := marker.FailOwed || (!marker.Resumed && found && snap.Spec.SuspendIO)

	if !marker.Resumed {
		done, err := r.resumeLeftoverKernel(ctx, marker, getErr)
		if done || err != nil {
			return err
		}
	}

	if time.Now().Before(marker.HoldUntil) {
		// The failed suspend may still land; look again next tick.
		if failOwed != marker.FailOwed {
			marker.FailOwed = failOwed
			r.rewriteMarker(marker)
		}

		return nil
	}

	err := r.settleResumed(ctx, marker.Snapshot, failOwed)
	if err != nil {
		if !marker.Resumed || marker.FailOwed != failOwed {
			marker.Resumed = true
			marker.FailOwed = failOwed
			r.rewriteMarker(marker)
		}

		return err
	}

	r.removeMarker(marker.Snapshot)

	set.mu.Lock()
	delete(set.lastWarn, marker.Snapshot)
	set.mu.Unlock()

	return nil
}

// resumeLeftoverKernel is the kernel half of resumeLeftover. done is
// true when the marker is gone and nothing is left to settle.
func (r *SnapshotReconciler) resumeLeftoverKernel(
	ctx context.Context, marker *suspendMarker, getErr error,
) (bool, error) {
	if marker.Resource == "" {
		if getErr != nil {
			return false, getErr
		}

		// Neither the marker nor a CR names the resource; nothing can be
		// resumed by name, and keeping the marker would retry forever.
		// The sweep of unaccounted suspends still covers the resource.
		r.removeMarker(marker.Snapshot)

		return true, nil
	}

	if r.windowsFor().heldByOther(marker.Resource, marker.Snapshot) {
		log.FromContext(ctx).Info("leftover suspend marker on a resource another window holds; settled without a resume",
			"snapshot", marker.Snapshot, "resource", marker.Resource)

		return false, nil
	}

	err := r.resumeAndVerify(ctx, marker)
	if err != nil {
		r.warnResumeFailed(ctx, marker.Snapshot, err)

		return false, err
	}

	return false, nil
}

// warnResumeFailed puts a failed resume on the Snapshot's events so it
// reaches `kubectl describe`, not only the satellite log, at most once
// per resumeWarnInterval for each snapshot.
func (r *SnapshotReconciler) warnResumeFailed(ctx context.Context, snapshot string, err error) {
	if r.Config.Recorder == nil {
		return
	}

	set := r.windowsFor()

	set.mu.Lock()
	if last, ok := set.lastWarn[snapshot]; ok && time.Since(last) < resumeWarnInterval {
		set.mu.Unlock()

		return
	}

	set.lastWarn[snapshot] = time.Now()
	set.mu.Unlock()

	snap, found, _ := r.cachedSnapshot(ctx, snapshot)
	if !found {
		return
	}

	r.Config.Recorder.Eventf(snap, nil, corev1.EventTypeWarning, "ResumeIOFailed", "ResumeIO",
		"I/O of %s is still suspended on node %s: %s",
		snap.Spec.ResourceDefinitionName, r.Config.NodeName, err.Error())
}

// cachedSnapshot reads a Snapshot by CR name. found is false both when
// it is gone and when the read failed; err is set only for the latter.
func (r *SnapshotReconciler) cachedSnapshot(
	ctx context.Context, name string,
) (*blockstoriov1alpha1.Snapshot, bool, error) {
	var snap blockstoriov1alpha1.Snapshot

	err := r.Get(ctx, types.NamespacedName{Name: name}, &snap)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, errors.Wrapf(err, "get Snapshot %s", name)
	}

	return &snap, true, nil
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
	if w == nil || w.closing || set.takes[snap.Name] != nil {
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
	set.takes[snap.Name] = run
	deadline := w.deadline
	target := snap.DeepCopy()

	go r.runTake(context.WithoutCancel(ctx), target, w, deadline, run)
}

// runTake takes the snapshot under the window's deadline and reports
// the verdict, unless the window started closing meanwhile: a take that
// returns after the resume may hold writes from after the suspend.
func (r *SnapshotReconciler) runTake(
	parent context.Context, snap *blockstoriov1alpha1.Snapshot, w *snapshotWindow, deadline time.Time, run *takeRun,
) {
	set := r.windowsFor()

	defer func() {
		set.mu.Lock()
		if set.takes[snap.Name] == run {
			delete(set.takes, snap.Name)
		}
		set.mu.Unlock()

		close(run.done)
	}()

	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()

	logger := log.FromContext(parent).WithValues("snapshot", snap.Name)
	ctx = log.IntoContext(ctx, logger)

	verdict, createdAt, err := r.takeVerdict(ctx, snap, false)
	retry := verdict == takeTransient || err != nil

	w.stampMu.Lock()

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
		} else if verdict == takeOK {
			w.reported = true
		}
	}

	w.stampMu.Unlock()

	set.mu.Lock()
	run.transient = retry
	run.finishedAt = time.Now()
	set.mu.Unlock()
}

// takeInFlight reports whether a take for snapshot is still running,
// whether or not its window is still open.
func (r *SnapshotReconciler) takeInFlight(snapshot string) bool {
	set := r.windowsFor()

	set.mu.Lock()
	defer set.mu.Unlock()

	return set.takes[snapshot] != nil
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
// or the local deadline passes, and resumes whatever an earlier process
// left suspended and whatever nothing here accounts for. None of it
// goes through the Snapshot workqueue, so a queue busy with deletes
// cannot hold I/O suspended, and each resume runs on a goroutine of its
// own, so one stuck resume does not hold up the others.
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
	set := r.windowsFor()
	work := context.WithoutCancel(ctx)

	r.sweepLeftoverMarkers(work)
	r.maybeSweepUnownedSuspends(work)

	ticker := time.NewTicker(timing.watchdogTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			set.mu.Lock()
			set.stopping = true
			set.mu.Unlock()

			set.work.Wait()
			r.closeAllWindows(work)

			return nil
		case <-ticker.C:
			r.watchWindows(work)
			r.sweepLeftoverMarkers(work)
			r.maybeSweepUnownedSuspends(work)
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
	if w == nil || w.opening || w.resuming {
		set.mu.Unlock()

		return
	}

	closing := w.closing
	expired := time.Now().After(w.deadline)
	set.mu.Unlock()

	closeAsync := func(failSnapshot bool, reason string) {
		set.spawn(func() { _ = r.closeWindow(ctx, name, failSnapshot, reason) })
	}

	var snap blockstoriov1alpha1.Snapshot

	err := r.Get(ctx, types.NamespacedName{Name: name}, &snap)

	switch {
	case closing:
		closeAsync(false, "")
	case apierrors.IsNotFound(err):
		closeAsync(false, "")
	case err != nil:
		// The cache did not answer; the deadline still applies.
		if expired {
			closeAsync(true, "suspend window deadline passed")
		}
	case !snap.DeletionTimestamp.IsZero() || !snap.Spec.SuspendIO:
		closeAsync(false, "")
	case expired:
		closeAsync(true, "suspend window deadline passed")
	case perNodeStatusFailed(snap.Status.NodeStatus, r.Config.NodeName):
		closeAsync(false, "")
	case snap.Spec.TakeSnapshot && !perNodeStatusReady(snap.Status.NodeStatus, r.Config.NodeName):
		r.startTake(ctx, &snap)
	}
}

// sweepLeftoverMarkers resumes every marker without an open window,
// each on a goroutine of its own.
func (r *SnapshotReconciler) sweepLeftoverMarkers(ctx context.Context) {
	set := r.windowsFor()

	for _, marker := range r.listMarkers() {
		set.mu.Lock()
		skip := set.windows[marker.Snapshot] != nil || set.resumingMarkers[marker.Snapshot]
		set.mu.Unlock()

		if skip {
			continue
		}

		set.spawn(func() {
			err := r.resumeLeftover(ctx, &marker)
			if err != nil {
				log.FromContext(ctx).Info("resume of a suspend left by an earlier window not confirmed; retrying",
					"snapshot", marker.Snapshot, "resource", marker.Resource, "error", err.Error())
			}
		})
	}
}

// maybeSweepUnownedSuspends starts the sweep for unaccounted suspends
// once per unownedSweep, never two at a time.
func (r *SnapshotReconciler) maybeSweepUnownedSuspends(ctx context.Context) {
	if !r.Config.Apply.DRBDEnabled() {
		return
	}

	set := r.windowsFor()
	timing := r.windowTiming()

	set.mu.Lock()
	if set.unownedSweeping || time.Since(set.lastUnownedSweep) < timing.unownedSweep {
		set.mu.Unlock()

		return
	}

	set.unownedSweeping = true
	set.lastUnownedSweep = time.Now()
	set.mu.Unlock()

	set.spawn(func() {
		defer func() {
			set.mu.Lock()
			set.unownedSweeping = false
			set.mu.Unlock()
		}()

		r.sweepUnownedSuspends(ctx)
		r.pruneEnded(ctx)
	})
}

// sweepUnownedSuspends resumes every local resource the kernel reports
// suspended-user that no window or marker on this node accounts for,
// which is what LINSTOR's satellite does on every device-manager run
// (SuspendManager.manageSuspendIo, DrbdLayer.updateSuspendState): it
// resumes on any difference between the kernel's flag and the one it
// wants, with or without a record of who suspended. This covers a
// suspend an older satellite left without a marker and one whose
// marker was lost. Only resources this node holds a Resource for are
// touched, so a foreign DRBD resource on the same host is left alone.
func (r *SnapshotReconciler) sweepUnownedSuspends(ctx context.Context) {
	timing := r.windowTiming()
	logger := log.FromContext(ctx)

	cmdCtx, cancel := context.WithTimeout(ctx, timing.commandTimeout)
	states, err := r.Config.Apply.SuspendStates(cmdCtx)

	cancel()

	if err != nil {
		logger.Info("read suspend state of local resources", "error", err.Error())

		return
	}

	owned, err := r.localResourceNames(ctx)
	if err != nil {
		logger.Info("list local resources for the suspend sweep", "error", err.Error())

		return
	}

	set := r.windowsFor()

	for resource, state := range states {
		if !state.SuspendedUser || !owned[resource] {
			continue
		}

		markers := r.listMarkers()

		set.mu.Lock()
		if set.busyLocked(resource, "", markers) {
			set.mu.Unlock()

			continue
		}

		set.resumingResources[resource] = true
		set.mu.Unlock()

		marker := suspendMarker{Resource: resource}
		for _, minor := range state.Minors {
			marker.Minors = append(marker.Minors, minor)
		}

		logger.Info("resuming a suspend no snapshot window accounts for", "resource", resource)

		err := r.resumeAndVerify(ctx, &marker)
		if err != nil {
			logger.Info("resume of an unaccounted suspend not confirmed; retrying", "resource", resource,
				"error", err.Error())
		}

		set.mu.Lock()
		delete(set.resumingResources, resource)
		set.mu.Unlock()
	}
}

// localResourceNames lists the kernel names of the resources this node
// holds a replica of.
func (r *SnapshotReconciler) localResourceNames(ctx context.Context) (map[string]bool, error) {
	var list blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &list)
	if err != nil {
		return nil, errors.Wrap(err, "list Resources")
	}

	out := make(map[string]bool, len(list.Items))

	for i := range list.Items {
		if naming.Same(list.Items[i].Spec.NodeName, r.Config.NodeName) {
			out[naming.Key(list.Items[i].Spec.ResourceDefinitionName)] = true
		}
	}

	return out, nil
}

// pruneEnded forgets the ended snapshots whose CR is gone.
func (r *SnapshotReconciler) pruneEnded(ctx context.Context) {
	set := r.windowsFor()

	set.mu.Lock()
	names := make([]string, 0, len(set.ended))

	for name := range set.ended {
		names = append(names, name)
	}
	set.mu.Unlock()

	for _, name := range names {
		_, found, err := r.cachedSnapshot(ctx, name)
		if found || err != nil {
			continue
		}

		set.mu.Lock()
		if set.windows[name] == nil {
			delete(set.ended, name)
			delete(set.lastWarn, name)
		}
		set.mu.Unlock()
	}
}

// closeAllWindows resumes every open window on shutdown, all at once so
// the stop is bounded by one resume rather than by their sum. A window
// still opening is waited for. A window whose resume fails keeps its
// marker for the next start. ctx must outlive the stop that triggered
// it.
func (r *SnapshotReconciler) closeAllWindows(ctx context.Context) {
	var wg sync.WaitGroup

	for _, name := range r.windowsFor().names() {
		wg.Go(func() { r.closeOnShutdown(ctx, name) })
	}

	wg.Wait()
}

func (r *SnapshotReconciler) closeOnShutdown(ctx context.Context, name string) {
	timing := r.windowTiming()
	giveUp := time.Now().Add(2 * timing.commandTimeout)

	for {
		err := r.closeWindow(ctx, name, true, "satellite stopping")
		if errors.Is(err, errWindowOpening) && time.Now().Before(giveUp) {
			time.Sleep(timing.watchdogTick)

			continue
		}

		if err != nil {
			log.FromContext(ctx).Info("resume on shutdown not confirmed; the next start resumes it",
				"snapshot", name, "error", err.Error())
		}

		return
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
