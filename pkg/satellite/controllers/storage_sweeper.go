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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/storage"
)

// StorageSweeperPeriod is the default cadence of the storage-orphan
// sweep — same ballpark as OrphanSweeperRunnable's drbd-side cadence
// so an operator who force-stripped a Resource finalizer mid-delete
// gets BOTH the kernel and the on-disk volume cleaned up inside one
// human-attention window (~5 minutes). Distinct constant from
// SweeperPeriod so the two cadences can drift independently if
// operational experience shows one wants tightening or loosening
// without affecting the other.
const StorageSweeperPeriod = 5 * time.Minute

// StorageSweeperMaxDeletePerCycle bounds the number of DeleteVolume
// calls the storage sweeper issues per tick — same rationale as
// SweeperMaxDownPerCycle: if a pathology produces a sudden batch of
// orphans, the sweeper logs them but only reaps a bounded few so
// the upstream bug remains visible AND the satellite never spends
// the whole tick budget on `zfs destroy` / `lvremove`. Three per
// cycle with the 5-min cadence = 36/hour — plenty for the bug 43
// failure mode it's designed for (one or two strays per
// force-strip event) but won't mask a runaway producer.
const StorageSweeperMaxDeletePerCycle = 3

// Event reasons the storage sweeper reports a volume it leaves alone
// under, on the local Node. Each is emitted once per volume and
// reason for the life of the satellite process; the log repeats it at
// V(1) every cycle.
const (
	// StorageSweeperReasonUnowned: no Resource claims the volume and
	// it does not carry blockstor's ownership marker, so it may be
	// data that was in the pool before blockstor (issue 264).
	StorageSweeperReasonUnowned = "UnownedStorageVolume"
	// StorageSweeperReasonSnapshots: the volume is blockstor's and no
	// Resource claims it, but it holds snapshots, which deleting it
	// would destroy.
	StorageSweeperReasonSnapshots = "OrphanStorageVolumeHasSnapshots"
)

// StorageSweeperSkipAnnotation is the per-Node opt-out for the
// storage sweeper. Setting it to "true" on the local Node CRD makes
// one cycle a no-op even with orphans present — useful when an
// operator wants to forensically inspect a stray ZVOL before letting
// the satellite reap it. Distinct from SweeperSkipAnnotation so an
// operator can pause storage GC without also pausing kernel-DRBD GC
// (and vice versa).
const StorageSweeperSkipAnnotation = "blockstor.org/skip-orphan-storage-sweeper"

// storageResourcePrefix is the name prefix the sweeper requires before
// it will consider a volume at all. blockstor's RD names always start
// with `pvc-` (CSI-driven) or `bs-` (CLI-driven); datasets / LVs in the
// same pool that match neither are skipped without a report, so a pool
// shared with other data does not fill the log. The prefix is not what
// keeps foreign data safe: LINSTOR names its volumes the same way
// (issue 264). The ownership marker is (see classifyStorageOrphan).
//
// Kept as a slice not a regex so adding a new convention is one entry
// without breaking grep-style debugging.
//
//nolint:gochecknoglobals // immutable allowlist, controller-package convention
var storageResourcePrefixes = []string{"pvc-", "bs-"}

// StorageOrphanSweeperRunnable is the per-satellite storage-side
// counterpart to OrphanSweeperRunnable. While the drbd sweeper looks
// for kernel resources without a Resource CRD, this sweeper looks
// for on-disk volumes (ZVOLs / LVs) without a Resource CRD — the
// failure mode Bug 43 documents.
//
// Bug 43 in a nutshell: REST controller force-strips the satellite
// finalizer after a hung apply; the satellite's handleDelete never
// runs; the kernel resource may already be down (the drbd sweeper
// catches that) but the storage volume survives because nobody
// called provider.DeleteVolume. This runnable closes that loop.
type StorageOrphanSweeperRunnable struct {
	Client    client.Client
	Providers ProvidersSnapshotFunc
	NodeName  string

	// Recorder receives the Warning Events for volumes the sweeper
	// leaves alone. Nil reports through the log only.
	Recorder events.EventRecorder

	// reported holds the volume+reason keys already reported in an
	// Event, so a volume that stays put does not raise one per cycle.
	// Touched only from the sweep goroutine.
	reported map[string]struct{}

	// Period overrides StorageSweeperPeriod (test-only — production
	// uses the default constant). A zero Period falls back.
	Period time.Duration

	// MaxDeletePerCycle overrides StorageSweeperMaxDeletePerCycle
	// (test-only). A zero value falls back. A negative value
	// disables the rate-limit entirely — useful in unit tests
	// asserting the full-list behaviour without juggling the bound.
	MaxDeletePerCycle int
}

// ProvidersSnapshotFunc returns a read-only snapshot of the
// satellite's pool→provider registry. Plumbed in as a callback (not
// a direct map) so the sweeper picks up any pool registered AFTER
// it started — StoragePoolReconciler can register providers at
// arbitrary times during runtime, and a cached map snapshot at
// construction would miss them.
type ProvidersSnapshotFunc func() map[string]storage.Provider

// NeedLeaderElection returns false — every satellite must sweep its
// own local storage, leader election would pick one pod to sweep
// the whole cluster which is structurally wrong (each node's local
// pool is opaque to peer satellites).
func (*StorageOrphanSweeperRunnable) NeedLeaderElection() bool { return false }

// Start runs the sweep loop until ctx cancels. Mirrors
// OrphanSweeperRunnable.Start — first sweep fires one period after
// startup so the c-r cache has a chance to warm; otherwise a fresh
// satellite would sweep against an empty CRD cache and incorrectly
// classify every legitimate volume as an orphan.
func (s *StorageOrphanSweeperRunnable) Start(ctx context.Context) error {
	period := s.Period
	if period == 0 {
		period = StorageSweeperPeriod
	}

	logger := log.FromContext(ctx).WithName("orphan-storage-sweeper").WithValues("node", s.NodeName)

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := s.sweepOnce(ctx, logger)
			if err != nil {
				logger.Error(err, "storage sweep cycle")
			}
		}
	}
}

// RegisterWithManager adds the runnable to the controller-runtime
// manager. Symmetrical to OrphanSweeperRunnable.RegisterWithManager
// so the wiring shape stays consistent.
func (s *StorageOrphanSweeperRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(s)
	if err != nil {
		return errors.Wrap(err, "add StorageOrphanSweeperRunnable")
	}

	return nil
}

// sweepOnce performs exactly one reconcile cycle.
//
// Algorithm:
//  1. Read the local Node; skip the cycle when it carries the
//     skip-annotation or cannot be read.
//  2. Build the set of (resource, vol) pairs the local node OWNS via
//     a Resource CRD list. NOTE: a CRD whose DeletionTimestamp is
//     non-zero is STILL counted as owned — the satellite's
//     handleDelete is responsible for that path, the sweeper must
//     not race it.
//  3. For each registered provider that implements VolumeLister:
//     list on-disk volumes; subtract the owned set; the remainder
//     is orphan candidates.
//  4. Skip candidates outside the storage-resource prefix allowlist.
//  5. Inspect each candidate: one with blockstor's ownership marker
//     and no snapshots is reaped, up to the per-cycle bound (the rest
//     waits for the next tick); any other is reported and kept.
//
// Exposed (lowercase) for unit tests pinning a single tick.
func (s *StorageOrphanSweeperRunnable) sweepOnce(ctx context.Context, logger logr.Logger) error {
	node, err := s.localNode(ctx)
	if err != nil {
		// The sweeper only ever deletes, and nothing is lost by
		// deleting a cycle later, so a Node it cannot read is one
		// whose opt-out it must assume is set. That includes a
		// satellite cold start, which sweeps before its Node exists
		// (issue 264).
		logger.Info("storage sweep skipped: cannot read the local Node, so cannot honour its opt-out",
			"annotation", StorageSweeperSkipAnnotation, "error", err.Error())

		return nil
	}

	if node.Annotations[StorageSweeperSkipAnnotation] == sweeperSkipValue {
		logger.V(1).Info("storage sweep skipped by Node annotation",
			"annotation", StorageSweeperSkipAnnotation)

		return nil
	}

	owned, err := s.listOwnedVolumeRefs(ctx)
	if err != nil {
		return errors.Wrap(err, "list local Resource CRDs")
	}

	limit := s.MaxDeletePerCycle
	if limit == 0 {
		limit = StorageSweeperMaxDeletePerCycle
	}

	providers := s.Providers()
	if len(providers) == 0 {
		// No pools registered yet — nothing to sweep. Common during
		// satellite cold-start before StoragePoolReconciler has
		// observed any pool CRDs.
		return nil
	}

	var deleted int

	for poolName, provider := range providers {
		stop := s.sweepPool(ctx, logger, node, poolName, provider, owned, limit, &deleted)
		if stop {
			return nil
		}
	}

	return nil
}

// sweepPool reaps orphan volumes on one pool. Returns true when the
// per-cycle rate-limit budget is exhausted and the caller should
// stop iterating remaining pools; false when the pool was processed
// (with any errors logged). Pulled out of sweepOnce so the parent
// stays under the gocyclo budget; the per-volume orphan-classification
// chain has nothing pool-routing-specific in it.
func (s *StorageOrphanSweeperRunnable) sweepPool(
	ctx context.Context,
	logger logr.Logger,
	node *blockstoriov1alpha1.Node,
	poolName string,
	provider storage.Provider,
	owned map[string]struct{},
	limit int,
	deleted *int,
) bool {
	lister, ok := provider.(storage.VolumeLister)
	if !ok {
		// Provider can't enumerate (or backend kind doesn't
		// support it); silently skip — the contract is opt-in.
		return false
	}

	refs, err := lister.ListVolumeNames(ctx)
	if err != nil {
		// Per-pool failure shouldn't sink the whole sweep — log
		// and try the next pool. A real backend outage will
		// surface on the next tick (and on real reconciles).
		logger.Error(err, "list volumes on pool", "pool", poolName)

		return false
	}

	for _, ref := range refs {
		ref.PoolName = poolName

		stop := s.reapIfOrphan(ctx, logger, node, poolName, provider, owned, limit, deleted, ref)
		if stop {
			return true
		}
	}

	return false
}

// reapIfOrphan classifies a single (pool, resource, volume) row and
// reaps it when it is a leak of blockstor's own: no local Resource
// claims it, it carries blockstor's ownership marker, and it holds no
// snapshots. Any other volume with no Resource is reported and left
// alone. Returns true when the per-cycle rate-limit budget has been
// hit and the caller should stop scanning the rest of the pool.
func (s *StorageOrphanSweeperRunnable) reapIfOrphan(
	ctx context.Context,
	logger logr.Logger,
	node *blockstoriov1alpha1.Node,
	poolName string,
	provider storage.Provider,
	owned map[string]struct{},
	limit int,
	deleted *int,
	ref storage.VolumeRef,
) bool {
	if _, ok := owned[ownedKey(ref.ResourceName, ref.VolumeNumber)]; ok {
		return false
	}

	// Wildcard match — a Resource CRD exists for this RD on this
	// node but the parent RD has already been deleted (cascade
	// out-of-order). The satellite finalizer is still responsible
	// for cleanup; don't race it.
	if _, ok := owned[ownedKey(ref.ResourceName, -1)]; ok {
		return false
	}

	if !hasStoragePrefix(ref.ResourceName) {
		// Not named like anything blockstor creates. Leave it alone
		// without a word: a pool shared with other datasets would
		// otherwise report all of them.
		return false
	}

	if limit >= 0 && *deleted >= limit {
		logger.Info("storage sweep rate-limit hit; deferring remainder",
			"limit", limit, "pool", poolName,
			"deferred_resource", ref.ResourceName,
			"deferred_volume", ref.VolumeNumber)

		return true
	}

	vol := storage.Volume{
		PoolName:     poolName,
		ResourceName: ref.ResourceName,
		VolumeNumber: ref.VolumeNumber,
	}

	if s.reapIfOwned(ctx, logger, node, provider, vol) {
		*deleted++
	}

	return false
}

// reapIfOwned inspects one volume no local Resource claims, reaps it
// when classifyStorageOrphan says so, and reports it otherwise.
// Returns true only when the volume was deleted. Pulled out of
// reapIfOrphan for the cyclop budget.
func (s *StorageOrphanSweeperRunnable) reapIfOwned(
	ctx context.Context,
	logger logr.Logger,
	node *blockstoriov1alpha1.Node,
	provider storage.Provider,
	vol storage.Volume,
) bool {
	reaper, ok := provider.(storage.OrphanReaper)
	if !ok {
		// A provider that cannot show the marker cannot prove the
		// volume is blockstor's, which is the same as not carrying it.
		s.report(logger, node, vol, StorageSweeperReasonUnowned, "its provider cannot show an ownership marker")

		return false
	}

	found, err := reaper.InspectVolume(ctx, vol)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			logger.Error(err, "inspect orphan storage volume; leaving it for the next cycle",
				"pool", vol.PoolName, "resource", vol.ResourceName, "volume", vol.VolumeNumber)
		}

		return false
	}

	switch classifyStorageOrphan(found) {
	case storageKeepUnowned:
		s.report(logger, node, vol, StorageSweeperReasonUnowned, "it does not carry blockstor's ownership marker")

		return false
	case storageKeepSnapshots:
		s.report(logger, node, vol, StorageSweeperReasonSnapshots, "it holds snapshots: "+strings.Join(found.Snapshots, ", "))

		return false
	case storageReap:
	}

	logger.Info("orphan storage volume detected; reaping it",
		"pool", vol.PoolName, "resource", vol.ResourceName, "volume", vol.VolumeNumber)

	err = reaper.ReapVolume(ctx, vol)

	switch {
	case errors.Is(err, storage.ErrNotOwned):
		s.report(logger, node, vol, StorageSweeperReasonUnowned, "it lost blockstor's ownership marker before it could be reaped")
	case errors.Is(err, storage.ErrHasSnapshots):
		s.report(logger, node, vol, StorageSweeperReasonSnapshots, "it gained a snapshot before it could be reaped")
	case err != nil:
		// Per-volume failure shouldn't abort the cycle — next tick
		// retries, and the rate-limit budget counts successful reaps
		// only.
		logger.Error(err, "reap orphan storage volume",
			"pool", vol.PoolName, "resource", vol.ResourceName, "volume", vol.VolumeNumber)
	default:
		return true
	}

	return false
}

// storageOrphanVerdict is what the sweeper does with a volume no local
// Resource claims.
type storageOrphanVerdict int

const (
	// storageReap: blockstor's own leak, delete it.
	storageReap storageOrphanVerdict = iota
	// storageKeepUnowned: no ownership marker, report and keep.
	storageKeepUnowned
	// storageKeepSnapshots: blockstor's, but deleting it would take
	// snapshots along; report and keep.
	storageKeepSnapshots
)

// classifyStorageOrphan decides from what the provider found on disk.
//
// A volume without the marker is never reaped, whatever it is named:
// it is either data that was in the pool before blockstor (issue 264:
// LINSTOR leftovers in an adopted pool) or a blockstor volume whose
// Resource was gone before blockstor wrote markers, and the two cannot
// be told apart.
//
// A volume with snapshots is never reaped either, tracked or not. A
// snapshot no Snapshot CR tracks may be the operator's rollback point;
// one a Snapshot CR tracks is still wanted, and its own reconciler
// deletes it when the CR goes, after which the volume is reaped.
func classifyStorageOrphan(found storage.VolumeOwnership) storageOrphanVerdict {
	if !found.Owned {
		return storageKeepUnowned
	}

	if len(found.Snapshots) > 0 {
		return storageKeepSnapshots
	}

	return storageReap
}

// report logs a volume the sweeper leaves alone and raises a Warning
// Event on the local Node, the Event once per volume and reason for
// the life of the process.
func (s *StorageOrphanSweeperRunnable) report(
	logger logr.Logger,
	node *blockstoriov1alpha1.Node,
	vol storage.Volume,
	reason, why string,
) {
	note := "volume " + vol.ResourceName + "/" + strconv.Itoa(int(vol.VolumeNumber)) +
		" in storage pool " + vol.PoolName + " has no Resource on this node and is left in place: " + why

	key := vol.PoolName + "/" + ownedKey(vol.ResourceName, vol.VolumeNumber) + "/" + reason
	if _, done := s.reported[key]; done {
		logger.V(1).Info("storage sweep: "+note, "reason", reason)

		return
	}

	if s.reported == nil {
		s.reported = map[string]struct{}{}
	}

	s.reported[key] = struct{}{}

	logger.Info("storage sweep: "+note, "reason", reason)

	if s.Recorder != nil {
		s.Recorder.Eventf(node, nil, corev1.EventTypeWarning, reason, "SweepOrphanStorage", "%s", note)
	}
}

// localNode reads the local Node CRD, whose annotations carry the
// per-node opt-out and which the sweeper's Events are recorded on.
func (s *StorageOrphanSweeperRunnable) localNode(ctx context.Context) (*blockstoriov1alpha1.Node, error) {
	var node blockstoriov1alpha1.Node

	err := s.Client.Get(ctx, client.ObjectKey{Name: naming.Key(s.NodeName)}, &node)
	if err != nil {
		return nil, errors.Wrap(err, "get Node")
	}

	return &node, nil
}

// listOwnedVolumeRefs returns the set of (resourceName, volumeNumber)
// the local node owns via a Resource CRD. The set includes Resources
// whose DeletionTimestamp is set — those are mid-delete via the
// satellite finalizer and the sweeper MUST NOT race them.
//
// Volume numbers come from the parent ResourceDefinition's
// VolumeDefinitions. A Resource without a parent RD (cascade-deleted
// out of order) contributes nothing — its on-disk volume IS an orphan
// the sweeper should reap.
func (s *StorageOrphanSweeperRunnable) listOwnedVolumeRefs(ctx context.Context) (map[string]struct{}, error) {
	var resList blockstoriov1alpha1.ResourceList

	err := s.Client.List(ctx, &resList)
	if err != nil {
		return nil, errors.Wrap(err, "list Resources")
	}

	// rdToVolumes caches each RD's volume-number set so we don't
	// re-fetch the same RD per Resource entry.
	rdToVolumes := map[string][]int32{}

	out := map[string]struct{}{}

	for i := range resList.Items {
		r := &resList.Items[i]
		if !naming.Same(r.Spec.NodeName, s.NodeName) {
			continue
		}

		// The volumes are laid down under the definition's CR name,
		// whatever spelling an older row keeps in spec.
		rdName := naming.Key(r.Spec.ResourceDefinitionName)

		vols, ok := rdToVolumes[rdName]
		if !ok {
			vols = s.lookupRDVolumes(ctx, rdName)
			rdToVolumes[rdName] = vols
		}

		for _, vn := range vols {
			out[ownedKey(rdName, vn)] = struct{}{}
		}

		// Edge case: RD already gone but the Resource CRD is still
		// in the apiserver mid-delete. Mark a wildcard owned entry
		// keyed only by resource name so the sweeper still
		// recognises the in-flight delete and doesn't race.
		if len(vols) == 0 {
			out[ownedKey(rdName, -1)] = struct{}{}
		}
	}

	return out, nil
}

// lookupRDVolumes returns the volume numbers for a given RD; an
// empty list on Get failure (RD already cascade-deleted) is the
// caller's signal to mark the resource as "owned but volume-count-
// unknown" via a sentinel key.
func (s *StorageOrphanSweeperRunnable) lookupRDVolumes(ctx context.Context, rdName string) []int32 {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := s.Client.Get(ctx, client.ObjectKey{Name: naming.Key(rdName)}, &rd)
	if err != nil {
		return nil
	}

	out := make([]int32, 0, len(rd.Spec.VolumeDefinitions))
	for i := range rd.Spec.VolumeDefinitions {
		out = append(out, rd.Spec.VolumeDefinitions[i].VolumeNumber)
	}

	return out
}

// ownedKey builds the lookup key used in listOwnedVolumeRefs's
// returned set. Volume number -1 is the wildcard sentinel for
// "owned but volume-count-unknown" (see lookupRDVolumes).
func ownedKey(resource string, volNumber int32) string {
	if volNumber < 0 {
		return resource + "/*"
	}

	return resource + "/" + strconv.Itoa(int(volNumber))
}

// hasStoragePrefix returns true when resource starts with one of
// the blockstor-managed name prefixes. Defence in depth so the
// sweeper never destroys an operator-managed dataset.
func hasStoragePrefix(resource string) bool {
	for _, p := range storageResourcePrefixes {
		if strings.HasPrefix(resource, p) {
			return true
		}
	}

	return false
}

// Compile-time check that the runnable satisfies the contract.
var _ manager.Runnable = (*StorageOrphanSweeperRunnable)(nil)
