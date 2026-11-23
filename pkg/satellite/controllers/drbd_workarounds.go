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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The satellite works around a few DRBD and environment bugs that leave a
// replica stuck where neither DRBD nor the apply gets it out by itself.
// In LINSTOR mode cozystack runs these checks as the `linstor-plunger`
// sidecar of every satellite
// (cozystack/cozystack packages/system/linstor/hack/plunger/plunger-satellite.sh);
// with blockstor that sidecar is not deployed, so the satellite does the
// same here. Each check is documented next to its code with the bug it
// works around, and each can be turned off on its own.
//
// The checks share one pass every WorkaroundsConfig.Interval. A pass reads
// `drbdsetup status --json` only when the observer's events2 cache shows a
// shape one of the enabled checks acts on (WorkaroundHints), so a node
// whose replicas are all Secondary and UpToDate, or Primary and in use,
// runs no command and makes no API call. A check acts only on a resource blockstor has a replica of
// on this node (the kernel is shared with LINSTOR during a migration),
// never on one an apply or delete is changing at that moment, and holds
// the resource while it looks and acts, so no apply or delete of it
// starts meanwhile (Hold). Every action it takes is logged and recorded
// as a Warning Event on the replica's Resource.

// WorkaroundsConfig turns the DRBD workarounds on and off.
type WorkaroundsConfig struct {
	// Interval is the time between passes. Zero turns every check off.
	Interval time.Duration

	// StalledResync reports a resync that made less progress than
	// StalledResyncMinRate KiB/s on StalledResyncPasses passes in a row
	// (checkStalledResync); zero is the default rate. It only reports,
	// with the command that would recover it, unless StalledResyncAct
	// is set too.
	StalledResync        bool
	StalledResyncPasses  int
	StalledResyncMinRate int

	// StalledResyncAct lets checkStalledResync disconnect the peer a
	// stalled resync waits on, or reconnect the one it pulls from, where
	// no Primary is in sight. Off by default: a reconnect can pick a
	// Secondary as SyncSource while a Primary writes, and that left a
	// replica UpToDate with data different from the Primary's on the
	// test cluster (see checkStalledResync).
	StalledResyncAct bool

	// IdlePrimary demotes a Primary nobody uses (checkIdlePrimary).
	IdlePrimary bool

	// ForcedIOFailures takes down and up a Secondary nobody uses that
	// still fails I/O (checkForcedIOFailures).
	ForcedIOFailures bool

	// StaleLoopDevices detaches loop devices whose backing file is gone
	// with its mount (detachStaleLoopDevices).
	StaleLoopDevices bool
}

// The plunger's cadence: a pass every 30 s, a resync stalled for 4 of
// them. A resync counts as stalled below 200 KiB/s: DRBD's defaults keep
// one at c-min-rate (250 KiB/s) or faster even under application I/O,
// and the trickle seen on the test cluster ran at 100-150 KiB/s. A
// cluster that lowers c-min-rate or sets a low fixed resync-rate lowers
// this too.
const (
	defaultWorkaroundInterval   = 30 * time.Second
	defaultStalledResyncPasses  = 4
	defaultStalledResyncMinRate = 200
)

// DefaultWorkaroundsConfig is every check on, at the plunger's cadence,
// with the stalled-resync check reporting only.
func DefaultWorkaroundsConfig() WorkaroundsConfig {
	return WorkaroundsConfig{
		Interval:             defaultWorkaroundInterval,
		StalledResync:        true,
		StalledResyncPasses:  defaultStalledResyncPasses,
		StalledResyncMinRate: defaultStalledResyncMinRate,
		IdlePrimary:          true,
		ForcedIOFailures:     true,
		StaleLoopDevices:     true,
	}
}

// enabled is whether a pass has anything to do: an interval and at
// least one check. With every check off a pass reads nothing and acts on
// nothing, so ticking it only costs a wakeup and reads as running.
func (c WorkaroundsConfig) enabled() bool {
	return c.Interval > 0 &&
		(c.StalledResync || c.IdlePrimary || c.ForcedIOFailures || c.StaleLoopDevices)
}

// stalledResyncMinRate is StalledResyncMinRate, or the default when it
// is not set.
func (c WorkaroundsConfig) stalledResyncMinRate() int {
	if c.StalledResyncMinRate <= 0 {
		return defaultStalledResyncMinRate
	}

	return c.StalledResyncMinRate
}

// logState logs at start which checks run and how often. A check logs
// nothing until it acts, so without this line nothing in the log tells
// whether a satellite runs a check at all.
func (c WorkaroundsConfig) logState(logger logr.Logger) {
	if !c.enabled() {
		reason := "no check enabled"
		if c.Interval <= 0 {
			reason = "interval is 0"
		}

		logger.Info("DRBD workarounds disabled", "interval", c.Interval.String(), "reason", reason)

		return
	}

	logger.Info("DRBD workarounds enabled",
		"interval", c.Interval.String(),
		"stalledResync", c.StalledResync,
		"stalledResyncPasses", c.StalledResyncPasses,
		"stalledResyncMinRate", c.stalledResyncMinRate(),
		"stalledResyncAct", c.StalledResyncAct,
		"idlePrimary", c.IdlePrimary,
		"forcedIOFailures", c.ForcedIOFailures,
		"staleLoopDevices", c.StaleLoopDevices)
}

// workaroundAdm is the part of drbd.Adm the workarounds drive.
type workaroundAdm interface {
	KernelStatus(ctx context.Context) ([]drbd.KernelResource, error)
	DisconnectPeer(ctx context.Context, resource, peer string) error
	ConnectPeer(ctx context.Context, resource, peer string) error
	Secondary(ctx context.Context, resource string) error
	Down(ctx context.Context, resource string) error
	Up(ctx context.Context, resource string) error
	Adjust(ctx context.Context, resource string) error
}

// DRBDWorkaroundsRunnable runs the DRBD workarounds on this node.
type DRBDWorkaroundsRunnable struct {
	Reader   client.Reader
	Adm      workaroundAdm
	Exec     storage.Exec
	Recorder events.EventRecorder
	NodeName string
	Config   WorkaroundsConfig

	// Hints gates the `drbdsetup status` read of a pass; nil reads it on
	// every pass.
	Hints func() WorkaroundHints

	// OpenUnknown reports a resource some volume of which the kernel
	// does not report as open or not; nil reports none. The checks that
	// act on an unused replica leave such a resource alone, since
	// `drbdsetup status --json` prints it as not open.
	OpenUnknown func(name string) bool

	// Hold claims a resource for the checks of one pass, so no apply or
	// delete of it starts until the returned func runs; false while one
	// is running, and the checks then leave the resource alone. nil
	// claims every resource.
	Hold func(name string) (func(), bool)

	// SysBlock is where the block devices are listed in sysfs; empty
	// means /sys/block.
	SysBlock string

	// StateDir is the satellite's state dir, where a reconnect owed to a
	// resource is recorded (satellite.ReconnectPendingSuffix) so it
	// outlives a restart. Empty keeps the debt in process memory only.
	StateDir string

	// stalls is the resync progress of each resource across passes.
	stalls map[string]resyncProgress

	// idle counts the passes in a row each resource was an idle Primary.
	idle map[string]int

	// failingIO counts the passes in a row each resource was an unused
	// Secondary failing I/O.
	failingIO map[string]int

	// Clock is the time of a pass; nil is time.Now.
	Clock func() time.Time

	// owedAdjust holds the resources a check left short of their
	// configuration, each with what the adjust owed to it does; every
	// pass retries them (adjustOwed).
	owedAdjust map[string]string

	// owedReconnect holds the resources whose paused peer a stalled
	// resync waited on was disconnected, each with whether the wait for
	// a Primary to go away was reported; every pass retries them
	// (reconnectOwed).
	owedReconnect map[string]bool

	// owedReconnectLoaded is whether the reconnects recorded in StateDir
	// were read into owedReconnect (loadOwedReconnects).
	owedReconnectLoaded bool

	// kernelRead is whether the last pass read the kernel status, and
	// kernelReadLogged whether any pass has logged it yet (logKernelRead).
	kernelRead       bool
	kernelReadLogged bool
}

// newDRBDWorkaroundsRunnable builds the runnable the manager runs, gated
// on the observer's cache and kept off the resources the apply is
// changing. Production caller: manager.go addBackgroundRunnables.
func newDRBDWorkaroundsRunnable(cli client.Client, cfg *Config, observer *ObserverRunnable) *DRBDWorkaroundsRunnable {
	return &DRBDWorkaroundsRunnable{
		Reader:      cli,
		Adm:         drbd.NewAdm(cfg.Exec),
		Exec:        cfg.Exec,
		Recorder:    cfg.Recorder,
		NodeName:    cfg.NodeName,
		Config:      cfg.Workarounds,
		Hints:       observer.WorkaroundHints,
		OpenUnknown: observer.OpenUnknown,
		Hold:        cfg.Apply.Hold,
		StateDir:    cfg.Apply.StateDir(),
	}
}

// NeedLeaderElection is false: every satellite looks after its own node.
func (*DRBDWorkaroundsRunnable) NeedLeaderElection() bool { return false }

// Start runs a pass every Config.Interval until ctx ends. The first pass
// waits one interval, so a satellite that just started has applied its
// replicas before anything here looks at them.
func (w *DRBDWorkaroundsRunnable) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("drbd-workarounds")

	ticker := time.NewTicker(w.Config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.pass(ctx, logger)
		}
	}
}

// RegisterWithManager adds the runnable to mgr when Config.Interval is
// set and a check is on, and logs which checks the satellite runs either
// way.
func (w *DRBDWorkaroundsRunnable) RegisterWithManager(mgr manager.Manager) error {
	w.Config.logState(mgr.GetLogger().WithName("drbd-workarounds"))

	if !w.Config.enabled() {
		return nil
	}

	err := mgr.Add(w)
	if err != nil {
		return errors.Wrap(err, "add DRBDWorkaroundsRunnable")
	}

	return nil
}

// pass runs every enabled check once.
func (w *DRBDWorkaroundsRunnable) pass(ctx context.Context, logger logr.Logger) {
	if w.Config.StaleLoopDevices {
		w.detachStaleLoopDevices(ctx, logger)
	}

	w.adjustOwed(ctx, logger)
	w.loadOwedReconnects(logger)

	wanted := w.kernelStatusWanted() || len(w.owedReconnect) > 0
	w.logKernelRead(logger, wanted)

	if !wanted {
		w.stalls, w.idle, w.failingIO = nil, nil, nil

		return
	}

	resources, err := w.Adm.KernelStatus(ctx)
	if err != nil {
		logger.Error(err, "read the DRBD kernel status")

		return
	}

	w.reconnectOwed(ctx, logger, resources)

	stalls := map[string]resyncProgress{}
	idle := map[string]int{}
	failingIO := map[string]int{}

	for i := range resources {
		res := &resources[i]

		release, ok := w.hold(res.Name)
		if !ok {
			if progress, seen := w.stalls[res.Name]; seen {
				stalls[res.Name] = progress
			}

			continue
		}

		w.checkResource(ctx, logger, res, stalls, idle, failingIO)
		release()
	}

	w.stalls, w.idle, w.failingIO = stalls, idle, failingIO
}

// logKernelRead logs at Info whether the pass reads the kernel status,
// on the first pass and then only when that changes, so the log shows an
// idle node running no command without a line every interval. Not V(1):
// the satellite's log sink drops everything below Info.
func (w *DRBDWorkaroundsRunnable) logKernelRead(logger logr.Logger, read bool) {
	if w.kernelReadLogged && w.kernelRead == read {
		return
	}

	w.kernelRead, w.kernelReadLogged = read, true

	logger.Info("DRBD workarounds pass", "kernelStatusRead", read)
}

// hold claims name for this pass's checks; see Hold.
func (w *DRBDWorkaroundsRunnable) hold(name string) (func(), bool) {
	if w.Hold == nil {
		return func() {}, true
	}

	return w.Hold(name)
}

// openUnknown reports whether the open state of some volume of name is
// unknown; see OpenUnknown.
func (w *DRBDWorkaroundsRunnable) openUnknown(name string) bool {
	return w.OpenUnknown != nil && w.OpenUnknown(name)
}

// checkResource runs the enabled checks on res while it is held.
func (w *DRBDWorkaroundsRunnable) checkResource(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	stalls map[string]resyncProgress, idle, failingIO map[string]int,
) {
	replica := w.replica(ctx, logger, res.Name)
	if replica == nil {
		return
	}

	if w.Config.StalledResync {
		w.checkStalledResync(ctx, logger, res, replica, stalls)
	}

	if w.Config.IdlePrimary {
		w.checkIdlePrimary(ctx, logger, res, replica, idle)
	}

	if w.Config.ForcedIOFailures {
		w.checkForcedIOFailures(ctx, logger, res, replica, failingIO)
	}
}

// kernelStatusWanted reports whether the observer's cache shows a shape an
// enabled check acts on.
func (w *DRBDWorkaroundsRunnable) kernelStatusWanted() bool {
	if w.Hints == nil {
		return true
	}

	hints := w.Hints()

	return (w.Config.StalledResync && hints.Inconsistent) ||
		(w.Config.IdlePrimary && hints.IdlePrimary) ||
		(w.Config.ForcedIOFailures && hints.ForcedIOFailures)
}

// replica is blockstor's Resource of the kernel resource name on this
// node, read from the cache; nil when blockstor has none here, the read
// failed or the replica is being deleted, and then the checks leave the
// resource alone. A replica being deleted is the satellite's delete to
// take down, not a workaround's to demote or restart in the meantime.
func (w *DRBDWorkaroundsRunnable) replica(ctx context.Context, logger logr.Logger, name string) *blockstoriov1alpha1.Resource {
	crName, err := replicaCRName(ctx, w.Reader, name, w.NodeName)
	if err != nil {
		logger.Error(err, "name the replica of a DRBD resource", "resource", name)

		return nil
	}

	var replica blockstoriov1alpha1.Resource

	err = w.Reader.Get(ctx, client.ObjectKey{Name: crName}, &replica)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "read the replica of a DRBD resource", "resource", name)
		}

		return nil
	}

	if !replica.DeletionTimestamp.IsZero() {
		return nil
	}

	return &replica
}

// acted logs a workaround's action and records it on obj as a Warning
// Event, so it shows in `kubectl describe` and not only in this log.
func (w *DRBDWorkaroundsRunnable) acted(logger logr.Logger, obj runtime.Object, resource, reason, note string) {
	logger.Info("DRBD workaround: "+note, "resource", resource, "reason", reason)

	if w.Recorder != nil && obj != nil {
		w.Recorder.Eventf(obj, nil, corev1.EventTypeWarning, reason, "DRBDWorkaround", "%s", note)
	}
}

// failed logs a workaround action that did not go through.
func (*DRBDWorkaroundsRunnable) failed(logger logr.Logger, err error, resource, what string) {
	logger.Error(err, "DRBD workaround: "+what, "resource", resource)
}

// Workaround for a DRBD 9 resync that stalls on one of several sources
// (https://github.com/LINBIT/drbd/issues/149; the older report
// https://github.com/LINBIT/drbd/issues/40 has the same shape). A replica
// that is SyncTarget of two peers resyncs from one at a time: DRBD picks
// one (Active) and pauses the other (Paused, resync-suspended:dependency;
// drbd_select_sync_target in drbd_state.c sets resync_susp_other_c).
// Resync requests this replica already sent the paused peer stay in its
// interval tree; a resync reply from the active peer, and a write from a
// Primary peer, that overlaps one of them defers to it (the
// drbd_should_defer_to_* rules in drbd_int.h), and when the paused one
// does not complete either, the active resync slows to a trickle or stops
// with its requests held: on the blockstor test cluster (v0.4.0-alpha.7,
// alpha.9, alpha.14, alpha.15) the active source held hundreds of
// replies unacknowledged (pending on the source, unacked here, the same
// count for minutes), the paused link held some too, and on alpha.15 the
// Primary's guest writes waited with it. Each time, disconnecting the
// paused peer on this node let the active resync finish within seconds:
// the disconnect cancels the requests toward that peer that wait on a
// conflict and drops their intervals (drain_resync_activity,
// drbd_cancel_conflicting_resync_requests in drbd_receiver.c), which
// releases everything deferring to them. `drbdadm adjust` afterwards
// connected the peer again. On alpha.7 a disconnect and connect of the
// active peer, which is what the LINSTOR plunger sidecar does, also
// cleared it.
//
// The reconnect is not harmless. On alpha.17, after a node reset, the
// returning node C was SyncTarget of the Primary B (active) and of the
// Secondary A (paused). The check disconnected A; B's resync finished and
// C turned UpToDate at the Primary's current UUID, which A had too. On
// the reconnect DRBD 9.3.4 found equal current UUIDs, ruled no-sync
// ("reconnected", uuid_compare in drbd_receiver.c), and then reconciled
// the out-of-sync bits C still held toward A: the leftover of the
// paused resync, which still carried MDF_PEER_BITMAP_AUTHORITATIVE, while
// A had cleared its side (maybe_reconcile_equal_uuid_bitmap). Both A and
// C had the Primary as an established neighbour, so neither sent
// UUID_FLAG_STABLE, primary_neighbor_decides abstained, and the holder
// of the bits, C, became SyncSource of A for 14.4 GiB while B kept
// writing. Afterwards A differed from B in 361 4-KiB blocks while DRBD
// reported every replica UpToDate and out-of-sync 0 (LINBIT/drbd#153,
// LINBIT/drbd#143: a resync from a third node races the Primary's
// writes on the target). Any reconnect between two Secondaries of a
// Primary can take that path; this node cannot read the peer's bitmap or
// its flags, so it cannot tell beforehand which side would be the
// source.
//
// So by default the check only reports: a Warning Event and a log line
// with the commands that would do what the workaround used to do, for
// an operator who has looked at the resource. With StalledResyncAct it
// runs them, and even then only where no Primary is in sight
// (drbd.KernelResource.PrimaryRuledOut: this node Secondary, every
// connection Connected to a Secondary), where the active peer does not
// hold the only UpToDate copy, as it does in an initial sync, and a
// paused peer it disconnected is connected again only while a Primary is
// ruled out on every other link (reconnectOwed).
//
// A pass counts as stalled when, since the previous pass, the out-of-sync
// count toward the active peer fell by less than StalledResyncMinRate,
// or the active link still holds exactly as many outstanding requests as
// before while less than that rate of data came in and out-of-sync did
// not fall fast either (heldRequestsMaxClearFactor). That catches the
// hard stop and the trickle. A pass on which out-of-sync grew starts the
// count over: application writes outrunning a resync that yields to them
// (c-min-rate, c-fill-target) grow it too, as an initial sync under a
// writing import did on alpha.17, and nothing there is stuck. After
// StalledResyncPasses stalled passes in a row the paused peer is
// disconnected and connected again on the next pass. When that did not
// help, the active peer is disconnected and connected instead.

// heldRequestsMaxClearFactor bounds the held-requests test: a resync that
// cleared out-of-sync at this many times the floor or faster is moving,
// whatever its other counters say. A thin volume (rs-discard-granularity)
// resyncs zeroed ranges as P_RS_DEALLOCATED, and a csums-alg resync
// answers matching blocks with P_RS_IS_IN_SYNC; neither adds to received
// (recv_cnt grows only for resync and peer data in drbd_receiver.c), so
// such a resync clears hundreds of MiB a pass with received hardly moving
// and a full pipeline of requests outstanding.
const heldRequestsMaxClearFactor = 8

// resyncProgress is how far the resync of a resource from Active got
// when it was last seen, and on how many passes in a row it stalled.
// PausedTried is the active peer a disconnect of the paused peer was
// already tried for, kept while the resource is Inconsistent.
type resyncProgress struct {
	drbd.ResyncDependency

	At          time.Time
	Passes      int
	PausedTried string
}

// stalled reports whether the resync moved too little from prev to dep
// in elapsed to count as moving at minRate KiB/s.
func (prev *resyncProgress) stalled(dep *drbd.ResyncDependency, elapsed time.Duration, minRate int) bool {
	floor := int64(elapsed.Seconds() * float64(minRate))

	cleared := prev.OutOfSync - dep.OutOfSync
	if cleared < floor {
		return true
	}

	// The held requests of the field stalls kept the same count for
	// minutes; a working pipeline's count moves between passes.
	return cleared < heldRequestsMaxClearFactor*floor &&
		dep.Outstanding > 0 && dep.Outstanding == prev.Outstanding &&
		dep.Received-prev.Received < floor
}

// now is the time of a pass.
func (w *DRBDWorkaroundsRunnable) now() time.Time {
	if w.Clock != nil {
		return w.Clock()
	}

	return time.Now()
}

// progress is dep as seen on this pass, with the stalled passes in a row
// counted on from prev when seen.
func (w *DRBDWorkaroundsRunnable) progress(prev *resyncProgress, seen bool, dep *drbd.ResyncDependency) resyncProgress {
	progress := resyncProgress{ResyncDependency: *dep, At: w.now(), PausedTried: prev.PausedTried}

	// The kernel zeroes received on a reconnect (drbd_nl.c), so a drop
	// means the link to the active peer is a new one: count afresh.
	if !seen || prev.Active != dep.Active || prev.Volume != dep.Volume || dep.Received < prev.Received {
		return progress
	}

	// Writes outran the resync; that is no stall to act on.
	if dep.OutOfSync > prev.OutOfSync {
		return progress
	}

	if prev.stalled(dep, progress.At.Sub(prev.At), w.Config.stalledResyncMinRate()) {
		progress.Passes = prev.Passes + 1
	}

	return progress
}

// checkStalledResync reports, and with StalledResyncAct acts on, a
// resync of res that stalled on StalledResyncPasses passes in a row.
func (w *DRBDWorkaroundsRunnable) checkStalledResync(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	replica *blockstoriov1alpha1.Resource, stalls map[string]resyncProgress,
) {
	prev, seen := w.stalls[res.Name]

	dep, ok := res.ResyncDependency()
	if !ok {
		if prev.PausedTried != "" && res.Inconsistent() {
			stalls[res.Name] = resyncProgress{PausedTried: prev.PausedTried}
		}

		return
	}

	progress := w.progress(&prev, seen, &dep)
	if progress.Passes < w.Config.StalledResyncPasses {
		stalls[res.Name] = progress

		return
	}

	// Counting starts over whatever happens: the action needs passes of
	// its own to show whether the resync moves again.
	progress.Passes = 0
	stalls[res.Name] = progress

	stuck := fmt.Sprintf("resync of volume %d from %s stalled for %d passes at %.2f%% "+
		"(out-of-sync %d KiB, %d requests outstanding) while %s waits on it",
		dep.Volume, dep.Active, w.Config.StalledResyncPasses, dep.PercentInSync, dep.OutOfSync, dep.Outstanding, dep.Paused)

	paused := progress.PausedTried != dep.Active

	if refusal := w.stalledResyncRefusal(res, &dep); refusal != "" {
		w.acted(logger, replica, res.Name, "DRBDResyncStalled", fmt.Sprintf(
			"%s: not acting, %s; by hand on %s: %s (LINBIT/drbd#149)",
			stuck, refusal, w.NodeName, stalledResyncCommand(res.Name, &dep, paused)))

		return
	}

	if !paused {
		w.reconnectActivePeer(ctx, logger, res, replica, &dep, stuck)

		return
	}

	if w.disconnectPausedPeer(ctx, logger, res, replica, &dep, stuck) {
		stalls[res.Name] = resyncProgress{ResyncDependency: dep, At: progress.At, PausedTried: dep.Active}
	}
}

// stalledResyncRefusal is why the check leaves the stall of dep in res to
// an operator, or "" when it may act. The reasons that make the action
// unsafe come first, so the report also tells an operator what to weigh
// before running the command by hand.
func (w *DRBDWorkaroundsRunnable) stalledResyncRefusal(res *drbd.KernelResource, dep *drbd.ResyncDependency) string {
	switch {
	case dep.ActiveOnlyCopy:
		return dep.Active + " holds the only UpToDate copy in sight, as in an initial sync"
	case !res.PrimaryRuledOut():
		return "a Primary is in sight or not ruled out, and a reconnect between two Secondaries of a Primary " +
			"can make a Secondary the SyncSource while the Primary writes, leaving them diverged (LINBIT/drbd#153)"
	case !w.Config.StalledResyncAct:
		return "report-only (--drbd-workaround-stalled-resync-act is off)"
	default:
		return ""
	}
}

// stalledResyncCommand is what an operator runs on this node to do by
// hand what the check would do: drop the paused peer and connect it again
// once the active resync is done, or reconnect the active peer.
func stalledResyncCommand(name string, dep *drbd.ResyncDependency, paused bool) string {
	if paused {
		return fmt.Sprintf("`drbdadm disconnect %s:%s`, then once the resync from %s is done `drbdadm adjust %s`",
			name, dep.Paused, dep.Active, name)
	}

	return fmt.Sprintf("`drbdadm disconnect %s:%s && drbdadm connect %s:%s`", name, dep.Active, name, dep.Active)
}

// disconnectPausedPeer drops this node's connection to the peer whose
// resync waits on the stalled one, and owes the resource a reconnect on
// a later pass (reconnectOwed). The pass in between gives the active
// resync time to finish without the paused one. The apply does not
// connect it sooner: it runs `adjust --skip-net` while a peer is
// StandAlone with its volumes still registered, which is how a
// disconnect leaves it (pkg/satellite reconciler.go
// shouldSkipNetOnAdjust). It reports whether the disconnect went through.
func (w *DRBDWorkaroundsRunnable) disconnectPausedPeer(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	replica *blockstoriov1alpha1.Resource, dep *drbd.ResyncDependency, stuck string,
) bool {
	w.acted(logger, replica, res.Name, "DRBDResyncStalled", fmt.Sprintf(
		"%s: disconnecting %s, connecting it again on a later pass with no Primary in sight (LINBIT/drbd#149)",
		stuck, dep.Paused))

	// Owed before the disconnect: a disconnect that went through but
	// returned an error, or a shutdown right after it, still leaves the
	// peer StandAlone, and no apply connects it (shouldSkipNetOnAdjust).
	// An adjust owed for nothing is a no-op.
	w.oweReconnect(logger, res.Name)

	err := w.Adm.DisconnectPeer(ctx, res.Name, dep.Paused)
	if err != nil {
		w.failed(logger, err, res.Name, "disconnect the peer a stalled resync waits on")

		return false
	}

	return true
}

// reconnectActivePeer disconnects and connects the peer the stalled
// resync pulls from, as the LINSTOR plunger does.
func (w *DRBDWorkaroundsRunnable) reconnectActivePeer(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	replica *blockstoriov1alpha1.Resource, dep *drbd.ResyncDependency, stuck string,
) {
	w.acted(logger, replica, res.Name, "DRBDResyncStalled", fmt.Sprintf(
		"%s: disconnecting and connecting %s (LINBIT/drbd#149)", stuck, dep.Active))

	// Owed until the connect went through, for the reason
	// disconnectPausedPeer gives; a reconnect already owed stays owed.
	_, owed := w.owedReconnect[res.Name]
	w.oweReconnect(logger, res.Name)

	err := w.Adm.DisconnectPeer(ctx, res.Name, dep.Active)
	if err == nil {
		err = w.Adm.ConnectPeer(ctx, res.Name, dep.Active)
	}

	if err != nil {
		w.failed(logger, err, res.Name, "reconnect the peer a resync stalled on")

		return
	}

	if !owed {
		w.settleReconnect(logger, res.Name)
	}
}

// reconnectPendingPerm is the mode of an owed-reconnect record: an empty
// file only the satellite reads.
const reconnectPendingPerm = 0o600

// reconnectPendingPath is the state file of name's owed reconnect, or ""
// without a state dir.
func (w *DRBDWorkaroundsRunnable) reconnectPendingPath(name string) string {
	if w.StateDir == "" {
		return ""
	}

	return filepath.Join(w.StateDir, name+satellite.ReconnectPendingSuffix)
}

// oweReconnect records that name owes the adjust that connects again a
// peer a stalled-resync action disconnected (reconnectOwed), in memory
// and in the state dir. A record that cannot be written is logged; the
// debt still holds until the satellite restarts.
func (w *DRBDWorkaroundsRunnable) oweReconnect(logger logr.Logger, name string) {
	if w.owedReconnect == nil {
		w.owedReconnect = map[string]bool{}
	}

	if _, ok := w.owedReconnect[name]; !ok {
		w.owedReconnect[name] = false
	}

	path := w.reconnectPendingPath(name)
	if path == "" {
		return
	}

	err := os.WriteFile(path, nil, reconnectPendingPerm)
	if err != nil {
		logger.Error(err, "record the reconnect owed to a resource", "resource", name)
	}
}

// settleReconnect drops name's owed reconnect.
func (w *DRBDWorkaroundsRunnable) settleReconnect(logger logr.Logger, name string) {
	delete(w.owedReconnect, name)

	path := w.reconnectPendingPath(name)
	if path == "" {
		return
	}

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		logger.Error(err, "drop the reconnect owed to a resource", "resource", name)
	}
}

// loadOwedReconnects reads, once, the reconnects an earlier run of the
// satellite recorded in the state dir and did not settle.
func (w *DRBDWorkaroundsRunnable) loadOwedReconnects(logger logr.Logger) {
	if w.owedReconnectLoaded || w.StateDir == "" {
		return
	}

	w.owedReconnectLoaded = true

	files, err := filepath.Glob(filepath.Join(w.StateDir, "*"+satellite.ReconnectPendingSuffix))
	if err != nil {
		logger.Error(err, "list the reconnects owed to resources")

		return
	}

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), satellite.ReconnectPendingSuffix)

		if w.owedReconnect == nil {
			w.owedReconnect = map[string]bool{}
		}

		if _, ok := w.owedReconnect[name]; !ok {
			w.owedReconnect[name] = false
		}
	}
}

// reconnectOwed connects again, with `drbdadm adjust`, the peers
// disconnectPausedPeer dropped, once a Primary is ruled out: a Primary
// may have come up since the disconnect, and the reconnect between this
// node and that Secondary is the handshake that can make a Secondary the
// SyncSource while the Primary writes. A link that is down or still
// connecting may lead to such a Primary as well
// (drbd.KernelResource.PrimaryRuledOutBesidesStandAlone). Such a
// resource is reported once and kept owed; it is connected again when no
// Primary can be in the way, or by an operator. One blockstor no longer has a replica of, or is deleting, is
// dropped, and one an apply is changing is left to the next pass.
// resources is this pass's kernel status.
func (w *DRBDWorkaroundsRunnable) reconnectOwed(ctx context.Context, logger logr.Logger, resources []drbd.KernelResource) {
	for name, reported := range w.owedReconnect {
		release, ok := w.hold(name)
		if !ok {
			continue
		}

		w.reconnect(ctx, logger, name, reported, resources)
		release()
	}
}

// reconnect is reconnectOwed for the resource name, held.
func (w *DRBDWorkaroundsRunnable) reconnect(
	ctx context.Context, logger logr.Logger, name string, reported bool, resources []drbd.KernelResource,
) {
	replica := w.replica(ctx, logger, name)
	if replica == nil {
		w.settleReconnect(logger, name)

		return
	}

	i := slices.IndexFunc(resources, func(r drbd.KernelResource) bool { return r.Name == name })
	if i >= 0 && !resources[i].PrimaryRuledOutBesidesStandAlone() {
		if !reported {
			w.owedReconnect[name] = true
			w.acted(logger, replica, name, "DRBDResyncStalled", fmt.Sprintf(
				"not connecting again the peer a stalled resync waited on while a Primary is in sight or not ruled out "+
					"(LINBIT/drbd#153); it is connected once none can be, or by hand on %s: `drbdadm adjust %s`",
				w.NodeName, name))
		}

		return
	}

	err := w.Adm.Adjust(ctx, name)
	if err != nil {
		w.failed(logger, err, name, "connect again the peer a stalled resync waited on")

		return
	}

	w.settleReconnect(logger, name)
	logger.Info("DRBD workaround: adjusted to connect again the peer a stalled resync waited on", "resource", name)
}

// Workaround for DRBD auto-demote not firing
// (https://github.com/piraeusdatastore/piraeus-operator/issues/942). DRBD
// promotes a replica when something opens it (auto-promote) and should
// demote it on the last close; sometimes it does not, and the replica
// stays Primary on a node where nothing uses it. Every other node then
// fails to promote, so the volume cannot be attached anywhere else
// (NodePublishVolume fails with "failed to set source device readwrite"),
// and deleting it fails too. No volume of the resource open means nobody
// uses it, so `drbdadm secondary` loses nothing; whoever opens it next is
// promoted again. Two passes in a row are required, and a resource an
// apply is changing is skipped (Hold): an auto-promote is Primary with
// nothing open until its mkfs opens the device. The demote the satellite
// owes after its own auto-promote is retried by the apply itself
// (pkg/satellite auto_demote.go); this check covers the kernel's.

// idlePrimaryPasses is how many passes in a row a resource must be an
// idle Primary before it is demoted.
const idlePrimaryPasses = 2

// checkIdlePrimary demotes res when it was an idle Primary on
// idlePrimaryPasses passes in a row.
func (w *DRBDWorkaroundsRunnable) checkIdlePrimary(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	replica *blockstoriov1alpha1.Resource, idle map[string]int,
) {
	if !res.IdlePrimary() || w.openUnknown(res.Name) {
		return
	}

	passes := w.idle[res.Name] + 1
	if passes < idlePrimaryPasses {
		idle[res.Name] = passes

		return
	}

	w.acted(logger, replica, res.Name, "DRBDIdlePrimary", fmt.Sprintf(
		"Primary with no volume open for %d passes: drbdadm secondary "+
			"(DRBD auto-demote did not fire, piraeus-operator#942)", passes))

	err := w.Adm.Secondary(ctx, res.Name)
	if err != nil {
		w.failed(logger, err, res.Name, "demote an idle Primary")
	}
}

// Workaround for a Secondary left failing I/O. When DRBD demotes a
// Primary that something still holds open, because the Primary must
// become SyncTarget of a peer with newer data ("I shall become
// SyncTarget, but I am primary! ... force secondary!", see
// https://github.com/LINBIT/drbd/issues/153) or because of a forced
// demote, it sets force-io-failures so the opener gets I/O errors rather
// than data that is no longer current. The kernel clears the flag when
// the last opener closes the device (drbd_release). A Secondary that
// nobody has open and still reports force-io-failures:yes missed that
// clear, and whoever opens it next gets I/O errors. `drbdadm down` and
// `up` of that Secondary clears it; only DRBD is restarted, the backing
// device is not touched, and a resync it was part of continues from its
// bitmap. Never done to a Primary or to a replica with a volume open, and
// only after two passes in a row, so the moment between the last close
// and the clear is not mistaken for it. Same check as the plunger, which
// looks at this node alone; here a replica a connected Primary peer
// depends on, as the only UpToDate disk it reaches, is also left up:
// taking it down could push that Primary into its no-quorum or
// no-data behaviour. It is unlikely, since force-io-failures comes from
// a demote towards SyncTarget and such a replica is normally the stale
// one, but it costs nothing to check.

// failingIOPasses is how many passes in a row a Secondary must fail I/O
// with nothing open before it is taken down and up.
const failingIOPasses = 2

// checkForcedIOFailures takes res down and up when it was an unused
// Secondary failing I/O on failingIOPasses passes in a row.
func (w *DRBDWorkaroundsRunnable) checkForcedIOFailures(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	replica *blockstoriov1alpha1.Resource, failingIO map[string]int,
) {
	if !res.UnusedSecondaryFailingIO() || w.openUnknown(res.Name) {
		return
	}

	passes := w.failingIO[res.Name] + 1
	if passes < failingIOPasses {
		failingIO[res.Name] = passes

		return
	}

	if res.SoleUpToDateOfAPrimaryPeer() {
		// Counted on, so the restart follows on the first pass the
		// Primary no longer depends on this replica.
		failingIO[res.Name] = passes

		logger.V(1).Info("DRBD workaround: a Secondary failing I/O is the only UpToDate disk a Primary peer reaches; left up",
			"resource", res.Name)

		return
	}

	w.acted(logger, replica, res.Name, "DRBDForcedIOFailures", fmt.Sprintf(
		"Secondary with no volume open still forces I/O failures for %d passes: "+
			"drbdadm down and up, backing device untouched (LINBIT/drbd#153)", passes))

	err := w.Adm.Down(ctx, res.Name)
	if err != nil {
		w.failed(logger, err, res.Name, "take down a Secondary failing I/O")

		return
	}

	err = w.Adm.Up(ctx, res.Name)
	if err != nil {
		// The replica is down now and nothing else brings it back
		// until some unrelated apply runs: the next passes retry.
		w.acted(logger, replica, res.Name, "DRBDWorkaroundUpFailed", fmt.Sprintf(
			"drbdadm up after the down failed, the replica is down: retrying with drbdadm adjust "+
				"every pass: %v", err))

		w.oweAdjust(res.Name, "bring up a Secondary whose up failed")
	}
}

// oweAdjust records that name needs a `drbdadm adjust` to do what, which
// adjustOwed runs from the next pass on.
func (w *DRBDWorkaroundsRunnable) oweAdjust(name, what string) {
	if w.owedAdjust == nil {
		w.owedAdjust = map[string]string{}
	}

	w.owedAdjust[name] = what
}

// adjustOwed runs `drbdadm adjust` of every resource a check left short of
// its configuration: one a forced-I/O restart took down and failed to
// bring up (adjust rather than up also completes an up that stopped half
// way). A resource an apply is changing is left to that apply and tried
// on the next pass; one blockstor no longer has a replica of, or is
// deleting, is dropped.
func (w *DRBDWorkaroundsRunnable) adjustOwed(ctx context.Context, logger logr.Logger) {
	for name, what := range w.owedAdjust {
		release, ok := w.hold(name)
		if !ok {
			continue
		}

		if w.replica(ctx, logger, name) == nil {
			delete(w.owedAdjust, name)
			release()

			continue
		}

		err := w.Adm.Adjust(ctx, name)

		release()

		if err != nil {
			w.failed(logger, err, name, what)

			continue
		}

		delete(w.owedAdjust, name)
		logger.Info("DRBD workaround: adjusted to "+what, "resource", name)
	}
}

// Workaround for loop devices left behind with their backing file gone.
// For a raw block volume kubelet binds the device node to a file under
// its plugin directory and attaches a loop device to that file to pin the
// device while a pod uses it; when the bind mount is gone and the loop
// device was not detached (kubelet leaving loop devices behind:
// https://github.com/kubernetes/kubernetes/issues/102408), the loop
// device keeps the DRBD device open, so the replica can neither be
// demoted nor taken down, and the volume cannot move or be deleted. The
// kernel then reports the loop device's backing file as "/" (or
// "/ (deleted)"): the path of a file that was a mount of its own, once
// that mount is gone (https://github.com/util-linux/util-linux/issues/4166).
// A backing file that still exists is never "/", so such a loop device is
// detached; the detach of one still open only marks it to go away on its
// last close (autoclear). A loop device already marked so is skipped: a
// detach adds nothing to it, and it would otherwise read "/" and be
// detached and reported again on every pass until its holder lets go.
// Same check as the plunger, which reads the same sysfs file through
// `losetup --json`; reading sysfs directly costs no command on a node
// without such a loop device. Like the plunger, it detaches any such
// loop device on the node, whether or not a DRBD device sits on it: a
// loop device whose backing file is gone serves nothing either way.

// defaultSysBlock is where sysfs lists the block devices.
const defaultSysBlock = "/sys/block"

// staleLoopDevices lists the loop devices under sysBlock whose backing
// file reads "/" or "/ (deleted)" and that are not yet marked to detach
// on their last close.
func staleLoopDevices(sysBlock string) []string {
	files, err := filepath.Glob(filepath.Join(sysBlock, "loop*", "loop", "backing_file"))
	if err != nil {
		return nil
	}

	var stale []string

	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			// Detached between the glob and the read.
			continue
		}

		switch strings.TrimRight(string(raw), "\n") {
		case "/", "/ (deleted)":
			if !autoclear(filepath.Dir(file)) {
				stale = append(stale, "/dev/"+filepath.Base(filepath.Dir(filepath.Dir(file))))
			}
		}
	}

	return stale
}

// autoclear reports whether the loop device whose sysfs loop directory is
// dir goes away on its last close, as a detach of an open one leaves it.
func autoclear(dir string) bool {
	raw, err := os.ReadFile(filepath.Join(dir, "autoclear"))

	return err == nil && strings.TrimSpace(string(raw)) == "1"
}

// detachStaleLoopDevices detaches every loop device whose backing file is
// gone with its mount.
func (w *DRBDWorkaroundsRunnable) detachStaleLoopDevices(ctx context.Context, logger logr.Logger) {
	sysBlock := w.SysBlock
	if sysBlock == "" {
		sysBlock = defaultSysBlock
	}

	stale := staleLoopDevices(sysBlock)
	if len(stale) == 0 {
		return
	}

	node := w.node(ctx)

	for _, device := range stale {
		w.acted(logger, node, device, "StaleLoopDevice", fmt.Sprintf(
			"loop device %s has lost its backing file (reads as \"/\"): losetup --detach "+
				"(kubernetes#102408)", device))

		_, err := w.Exec.Run(ctx, "losetup", "--detach", device)
		if err != nil {
			w.failed(logger, err, device, "detach a stale loop device")
		}
	}
}

// node is blockstor's Node of this satellite, read from the cache, to
// record node-wide actions on; nil when it cannot be read.
func (w *DRBDWorkaroundsRunnable) node(ctx context.Context) runtime.Object {
	var node blockstoriov1alpha1.Node

	err := w.Reader.Get(ctx, client.ObjectKey{Name: naming.Key(w.NodeName)}, &node)
	if err != nil {
		return nil
	}

	return &node
}
