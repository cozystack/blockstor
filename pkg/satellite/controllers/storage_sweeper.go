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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
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

// StorageSweeperSkipAnnotation is the per-Node opt-out for the
// storage sweeper. Setting it to "true" on the local Node CRD makes
// one cycle a no-op even with orphans present — useful when an
// operator wants to forensically inspect a stray ZVOL before letting
// the satellite reap it. Distinct from SweeperSkipAnnotation so an
// operator can pause storage GC without also pausing kernel-DRBD GC
// (and vice versa).
const StorageSweeperSkipAnnotation = "blockstor.io/skip-orphan-storage-sweeper"

// storageResourcePrefix is the name prefix the sweeper requires before
// it will consider a volume for deletion. blockstor's RD names always
// start with `pvc-` (CSI-driven) or `bs-` (CLI-driven); operator-
// created datasets / LVs in the same pool that don't match either
// prefix are left alone even if their name parses as `<x>_NNNNN`. A
// false-positive on this code path would destroy operator data — the
// prefix gate is the load-bearing safety belt.
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
//  1. Honour the skip-annotation.
//  2. Build the set of (resource, vol) pairs the local node OWNS via
//     a Resource CRD list. NOTE: a CRD whose DeletionTimestamp is
//     non-zero is STILL counted as owned — the satellite's
//     handleDelete is responsible for that path, the sweeper must
//     not race it.
//  3. For each registered provider that implements VolumeLister:
//     list on-disk volumes; subtract the owned set; the remainder
//     is orphan candidates.
//  4. Filter orphans by the storage-resource prefix allowlist
//     (defence in depth — operator-created volumes outside the
//     blockstor naming convention are left alone even if a future
//     refactor breaks the parser).
//  5. DeleteVolume each orphan up to the per-cycle bound; defer
//     the rest to the next tick.
//
// Exposed (lowercase) for unit tests pinning a single tick.
func (s *StorageOrphanSweeperRunnable) sweepOnce(ctx context.Context, logger logr.Logger) error {
	skip, err := s.shouldSkip(ctx)
	if err != nil {
		// Read failures on the Node CRD shouldn't block the sweep —
		// fall back to "not skipped" so a transient apiserver blip
		// doesn't silently disable the safety net. Log so operators
		// notice if it's persistent.
		logger.Error(err, "check skip-storage-sweeper annotation; proceeding without skip")
	} else if skip {
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
		lister, ok := provider.(storage.VolumeLister)
		if !ok {
			// Provider can't enumerate (or backend kind doesn't
			// support it); silently skip — the contract is opt-in.
			continue
		}

		refs, err := lister.ListVolumeNames(ctx)
		if err != nil {
			// Per-pool failure shouldn't sink the whole sweep —
			// log and try the next pool. A real backend outage
			// will surface on the next tick (and on real
			// reconciles).
			logger.Error(err, "list volumes on pool", "pool", poolName)

			continue
		}

		for _, ref := range refs {
			ref.PoolName = poolName

			if _, ok := owned[ownedKey(ref.ResourceName, ref.VolumeNumber)]; ok {
				continue
			}

			// Wildcard match — a Resource CRD exists for this RD on
			// this node but the parent RD has already been deleted
			// (cascade out-of-order). The satellite finalizer is
			// still responsible for cleanup; don't race it.
			if _, ok := owned[ownedKey(ref.ResourceName, -1)]; ok {
				continue
			}

			if !hasStoragePrefix(ref.ResourceName) {
				// Operator-owned volume that happens to share the
				// pool. Leave it alone — see prefix allowlist
				// rationale.
				continue
			}

			if limit >= 0 && deleted >= limit {
				logger.Info("storage sweep rate-limit hit; deferring remainder",
					"limit", limit, "pool", poolName,
					"deferred_resource", ref.ResourceName,
					"deferred_volume", ref.VolumeNumber)

				return nil
			}

			logger.Info("orphan storage volume detected; running DeleteVolume",
				"pool", poolName, "resource", ref.ResourceName, "volume", ref.VolumeNumber)

			delErr := provider.DeleteVolume(ctx, storage.Volume{
				PoolName:     poolName,
				ResourceName: ref.ResourceName,
				VolumeNumber: ref.VolumeNumber,
			})
			if delErr != nil {
				// Per-volume failure shouldn't abort the cycle —
				// next tick retries. We DON'T bump `deleted` so
				// the rate-limit budget reflects successful
				// reaps only.
				logger.Error(delErr, "DeleteVolume on orphan",
					"pool", poolName, "resource", ref.ResourceName, "volume", ref.VolumeNumber)

				continue
			}

			deleted++
		}
	}

	return nil
}

// shouldSkip mirrors OrphanSweeperRunnable.shouldSkip — checks the
// local Node CRD for the per-feature opt-out annotation.
func (s *StorageOrphanSweeperRunnable) shouldSkip(ctx context.Context) (bool, error) {
	var node blockstoriov1alpha1.Node

	err := s.Client.Get(ctx, client.ObjectKey{Name: s.NodeName}, &node)
	if err != nil {
		return false, errors.Wrap(err, "get Node")
	}

	v, ok := node.Annotations[StorageSweeperSkipAnnotation]
	if !ok {
		return false, nil
	}

	return v == sweeperSkipValue, nil
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
		if r.Spec.NodeName != s.NodeName {
			continue
		}

		vols, ok := rdToVolumes[r.Spec.ResourceDefinitionName]
		if !ok {
			vols = s.lookupRDVolumes(ctx, r.Spec.ResourceDefinitionName)
			rdToVolumes[r.Spec.ResourceDefinitionName] = vols
		}

		for _, vn := range vols {
			out[ownedKey(r.Spec.ResourceDefinitionName, vn)] = struct{}{}
		}

		// Edge case: RD already gone but the Resource CRD is still
		// in the apiserver mid-delete. Mark a wildcard owned entry
		// keyed only by resource name so the sweeper still
		// recognises the in-flight delete and doesn't race.
		if len(vols) == 0 {
			out[ownedKey(r.Spec.ResourceDefinitionName, -1)] = struct{}{}
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

	err := s.Client.Get(ctx, client.ObjectKey{Name: rdName}, &rd)
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
