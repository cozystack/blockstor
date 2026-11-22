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
// whose replicas are all Secondary and UpToDate runs no command and makes
// no API call. A check acts only on a resource blockstor has a replica of
// on this node (the kernel is shared with LINSTOR during a migration),
// never on one an apply or delete is changing at that moment, and holds
// the resource while it looks and acts, so no apply or delete of it
// starts meanwhile (Hold). Every action it takes is logged and recorded
// as a Warning Event on the replica's Resource.

// WorkaroundsConfig turns the DRBD workarounds on and off.
type WorkaroundsConfig struct {
	// Interval is the time between passes. Zero turns every check off.
	Interval time.Duration

	// StalledResync reconnects the peer a resync stalled on
	// (checkStalledResync), after StalledResyncPasses passes in a row
	// with no progress.
	StalledResync       bool
	StalledResyncPasses int

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
// them.
const (
	defaultWorkaroundInterval  = 30 * time.Second
	defaultStalledResyncPasses = 4
)

// DefaultWorkaroundsConfig is every check on, at the plunger's cadence.
func DefaultWorkaroundsConfig() WorkaroundsConfig {
	return WorkaroundsConfig{
		Interval:            defaultWorkaroundInterval,
		StalledResync:       true,
		StalledResyncPasses: defaultStalledResyncPasses,
		IdlePrimary:         true,
		ForcedIOFailures:    true,
		StaleLoopDevices:    true,
	}
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

	// Hold claims a resource for the checks of one pass, so no apply or
	// delete of it starts until the returned func runs; false while one
	// is running, and the checks then leave the resource alone. nil
	// claims every resource.
	Hold func(name string) (func(), bool)

	// SysBlock is where the block devices are listed in sysfs; empty
	// means /sys/block.
	SysBlock string

	// stalls is the resync progress of each resource across passes.
	stalls map[string]resyncProgress

	// idle counts the passes in a row each resource was an idle Primary.
	idle map[string]int

	// failingIO counts the passes in a row each resource was an unused
	// Secondary failing I/O.
	failingIO map[string]int

	// owedUp holds the resources checkForcedIOFailures took down and
	// failed to bring up again; every pass retries them (bringUpOwed).
	owedUp map[string]bool
}

// newDRBDWorkaroundsRunnable builds the runnable the manager runs, gated
// on the observer's cache and kept off the resources the apply is
// changing. Production caller: manager.go addBackgroundRunnables.
func newDRBDWorkaroundsRunnable(cli client.Client, cfg *Config, observer *ObserverRunnable) *DRBDWorkaroundsRunnable {
	return &DRBDWorkaroundsRunnable{
		Reader:   cli,
		Adm:      drbd.NewAdm(cfg.Exec),
		Exec:     cfg.Exec,
		Recorder: cfg.Recorder,
		NodeName: cfg.NodeName,
		Config:   cfg.Workarounds,
		Hints:    observer.WorkaroundHints,
		Hold:     cfg.Apply.Hold,
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

// RegisterWithManager adds the runnable to mgr when Config.Interval is set.
func (w *DRBDWorkaroundsRunnable) RegisterWithManager(mgr manager.Manager) error {
	if w.Config.Interval <= 0 {
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

	w.bringUpOwed(ctx, logger)

	if !w.kernelStatusWanted() {
		w.stalls, w.idle, w.failingIO = nil, nil, nil

		return
	}

	resources, err := w.Adm.KernelStatus(ctx)
	if err != nil {
		logger.Error(err, "read the DRBD kernel status")

		return
	}

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

// hold claims name for this pass's checks; see Hold.
func (w *DRBDWorkaroundsRunnable) hold(name string) (func(), bool) {
	if w.Hold == nil {
		return func() {}, true
	}

	return w.Hold(name)
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
		(w.Config.IdlePrimary && hints.Primary) ||
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
// resyncing from several peers at once stays SyncTarget toward one of them
// with percent-in-sync no longer moving, while its resync from the other
// peers waits on that one (resync-suspended:dependency), so the replica
// stays Inconsistent and the whole resync makes no progress. Hit on the
// blockstor test cluster after node resets (v0.4.0-alpha.5, alpha.7 and
// alpha.14). Disconnecting and connecting that one peer lets another peer
// take over and the resync completes; nothing on disk is touched. Same
// trigger as the plunger: the same percent on StalledResyncPasses passes
// in a row (4 x 30 s).

// resyncProgress is how far the resync of a resource from Peer got, and
// on how many passes in a row it stayed there.
type resyncProgress struct {
	Peer    string
	Percent float64
	Passes  int
}

// checkStalledResync reconnects the peer res's resync stalled on.
func (w *DRBDWorkaroundsRunnable) checkStalledResync(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	replica *blockstoriov1alpha1.Resource, stalls map[string]resyncProgress,
) {
	from, ok := res.ResyncSource()
	if !ok {
		return
	}

	progress := resyncProgress{Peer: from.Peer, Percent: from.PercentInSync, Passes: 1}
	if prev, seen := w.stalls[res.Name]; seen && prev.Peer == from.Peer && prev.Percent == from.PercentInSync {
		progress.Passes = prev.Passes + 1
	}

	if progress.Passes < w.Config.StalledResyncPasses {
		stalls[res.Name] = progress

		return
	}

	w.acted(logger, replica, res.Name, "DRBDResyncStalled", fmt.Sprintf(
		"resync from %s stalled at %.2f%% for %d passes while another peer waits on it: "+
			"disconnecting and connecting %s (LINBIT/drbd#149)",
		from.Peer, from.PercentInSync, progress.Passes, from.Peer))

	err := w.Adm.DisconnectPeer(ctx, res.Name, from.Peer)
	if err == nil {
		err = w.Adm.ConnectPeer(ctx, res.Name, from.Peer)
	}

	if err != nil {
		w.failed(logger, err, res.Name, "reconnect the peer a resync stalled on")
	}

	// Counting starts over either way: the reconnect needs passes of
	// its own to show whether the resync moves again.
	stalls[res.Name] = resyncProgress{Peer: from.Peer, Percent: from.PercentInSync}
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
	if !res.IdlePrimary() {
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
// and the clear is not mistaken for it. Same check as the plunger.

// failingIOPasses is how many passes in a row a Secondary must fail I/O
// with nothing open before it is taken down and up.
const failingIOPasses = 2

// checkForcedIOFailures takes res down and up when it was an unused
// Secondary failing I/O on failingIOPasses passes in a row.
func (w *DRBDWorkaroundsRunnable) checkForcedIOFailures(
	ctx context.Context, logger logr.Logger, res *drbd.KernelResource,
	replica *blockstoriov1alpha1.Resource, failingIO map[string]int,
) {
	if !res.UnusedSecondaryFailingIO() {
		return
	}

	passes := w.failingIO[res.Name] + 1
	if passes < failingIOPasses {
		failingIO[res.Name] = passes

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

		if w.owedUp == nil {
			w.owedUp = map[string]bool{}
		}

		w.owedUp[res.Name] = true
	}
}

// bringUpOwed brings up again every resource a forced-I/O restart took
// down and failed to bring up. `drbdadm adjust` rather than `up`: it
// also completes an up that stopped half way. A resource an apply is
// changing is left to that apply and tried on the next pass; one
// blockstor no longer has a replica of, or is deleting, is dropped.
func (w *DRBDWorkaroundsRunnable) bringUpOwed(ctx context.Context, logger logr.Logger) {
	for name := range w.owedUp {
		release, ok := w.hold(name)
		if !ok {
			continue
		}

		if w.replica(ctx, logger, name) == nil {
			delete(w.owedUp, name)
			release()

			continue
		}

		err := w.Adm.Adjust(ctx, name)

		release()

		if err != nil {
			w.failed(logger, err, name, "bring up a Secondary whose up failed")

			continue
		}

		delete(w.owedUp, name)
		logger.Info("DRBD workaround: brought up the Secondary whose up had failed", "resource", name)
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
// last close. Same check as the plunger, which reads the same sysfs file
// through `losetup --json`; reading sysfs directly costs no command on a
// node without such a loop device.

// defaultSysBlock is where sysfs lists the block devices.
const defaultSysBlock = "/sys/block"

// staleLoopDevices lists the loop devices under sysBlock whose backing
// file reads "/" or "/ (deleted)".
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
			stale = append(stale, "/dev/"+filepath.Base(filepath.Dir(filepath.Dir(file))))
		}
	}

	return stale
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
