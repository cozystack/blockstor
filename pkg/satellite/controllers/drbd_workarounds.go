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
// never on one an apply or delete is changing at that moment, and every
// action it takes is logged and recorded as a Warning Event on the
// replica's Resource.

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
	}
}

// workaroundAdm is the part of drbd.Adm the workarounds drive.
type workaroundAdm interface {
	KernelStatus(ctx context.Context) ([]drbd.KernelResource, error)
	DisconnectPeer(ctx context.Context, resource, peer string) error
	ConnectPeer(ctx context.Context, resource, peer string) error
	Secondary(ctx context.Context, resource string) error
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

	// Busy reports a resource an apply or delete is changing right now;
	// nil reports none.
	Busy func(name string) bool

	// stalls is the resync progress of each resource across passes.
	stalls map[string]resyncProgress

	// idle counts the passes in a row each resource was an idle Primary.
	idle map[string]int
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
		Busy:     cfg.Apply.Busy,
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
	if !w.kernelStatusWanted() {
		w.stalls, w.idle = nil, nil

		return
	}

	resources, err := w.Adm.KernelStatus(ctx)
	if err != nil {
		logger.Error(err, "read the DRBD kernel status")

		return
	}

	stalls := map[string]resyncProgress{}
	idle := map[string]int{}

	for i := range resources {
		res := &resources[i]

		if w.Busy != nil && w.Busy(res.Name) {
			if progress, ok := w.stalls[res.Name]; ok {
				stalls[res.Name] = progress
			}

			continue
		}

		replica := w.replica(ctx, logger, res.Name)
		if replica == nil {
			continue
		}

		if w.Config.StalledResync {
			w.checkStalledResync(ctx, logger, res, replica, stalls)
		}

		if w.Config.IdlePrimary {
			w.checkIdlePrimary(ctx, logger, res, replica, idle)
		}
	}

	w.stalls, w.idle = stalls, idle
}

// kernelStatusWanted reports whether the observer's cache shows a shape an
// enabled check acts on.
func (w *DRBDWorkaroundsRunnable) kernelStatusWanted() bool {
	if w.Hints == nil {
		return true
	}

	hints := w.Hints()

	return (w.Config.StalledResync && hints.Inconsistent) ||
		(w.Config.IdlePrimary && hints.Primary)
}

// replica is blockstor's Resource of the kernel resource name on this
// node, read from the cache; nil when blockstor has none here or the read
// failed, and then the checks leave the resource alone.
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
// apply is changing is skipped (Busy): an auto-promote is Primary with
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
