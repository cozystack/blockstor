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

package satellite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// ReconcilerConfig parametrises a Reconciler.
//
// Providers maps the satellite's local storage-pool names to provisioned
// `storage.Provider` instances; an unknown pool fails the per-resource
// Apply with a surfaced error message.
//
// Adm, StateDir and NodeName drive the DRBD half: when set, Apply also
// renders the `.res` file under StateDir, runs `drbdadm create-md` on
// first activation, and `drbdadm adjust` on every reconcile.
type ReconcilerConfig struct {
	Providers map[string]storage.Provider

	// Adm is the drbdadm wrapper. nil → DRBD half is disabled (storage
	// only). Useful for unit tests of the storage path without DRBD.
	Adm *drbd.Adm

	// StateDir is where `.res` files land. Required when Adm is set.
	StateDir string

	// NodeName is this satellite's identifier; the reconciler uses it
	// to know which Peer entries describe local vs. remote.
	NodeName string

	// LocalAddress is the IP this satellite's DRBD layer should bind
	// to. Falls back into the .res file's `address` line on the local
	// `on <node>` block whenever the controller-supplied address is
	// the placeholder "0.0.0.0" (which it always is until controller
	// learns each satellite's pod IP).
	LocalAddress string

	// ShipExec runs the snapshot-ship subprocess (zfs send|recv,
	// thin-send-recv, …). Production wires storage.RealExec; tests
	// inject FakeExec to assert the command line without spinning up
	// a real ssh / zfs / thin tool.
	ShipExec storage.Exec

	// Cryptsetup is the LUKS-layer wrapper. nil → LUKS in the layer
	// stack is rejected (the satellite can't fulfil it). Production
	// wires luks.NewCryptsetup(storage.RealExec{}); tests inject
	// FakeExec.
	Cryptsetup *luks.Cryptsetup

	// CrossNodeFetcher pulls a snapshot from a peer satellite when
	// the local node doesn't host it. nil → no cross-node fallback,
	// materializeVolume falls through to a blank CreateVolume (the
	// pre-Phase-11 behaviour). The agent injects this post-manager
	// construction via SetCrossNodeFetcher because the implementation
	// needs the controller-runtime client only the manager owns.
	CrossNodeFetcher CrossNodeFetcher

	// MetadataCreatedStamper writes the `MetadataCreated=True`
	// Status Condition onto the parent Resource CRD after
	// `drbdmeta create-md` succeeds. nil → the satellite falls
	// back to the legacy on-disk `<rd>.md-created` marker for
	// firstActivation derivation (compatible with unit tests that
	// don't wire an apiserver). The agent injects this
	// post-manager construction via SetMetadataCreatedStamper —
	// the implementation needs the controller-runtime client only
	// the manager owns. Phase 11.3 Stage 1.
	MetadataCreatedStamper MetadataCreatedStamper

	// FilesystemFormattedStamper writes the
	// `FilesystemFormatted=True` Status Condition onto the parent
	// Resource CRD after `runAutoMkfs` reports every diskful
	// volume as carrying a filesystem (freshly mkfs'd or adopted
	// via blkid). nil → the satellite falls back to the legacy
	// on-disk `<rd>.mkfs.done` marker for both the
	// `needsAutoMkfsRetry` predicate and the `runAutoMkfs`
	// fast-path (compatible with unit tests that don't wire an
	// apiserver). The agent injects this post-manager construction
	// via SetFilesystemFormattedStamper — the implementation needs
	// the controller-runtime client only the manager owns. Phase
	// 11.3 Stage 2.
	FilesystemFormattedStamper FilesystemFormattedStamper

	// SkipDiskClearer releases the satellite's SSA claim on the
	// `DrbdOptions/SkipDisk` Spec.Props key when the kernel
	// re-emerges healthy (Bug 278: Talos kernel upgrade reattach).
	// nil → auto-clear path is disabled (compatible with unit tests
	// that don't wire an apiserver). The agent injects this
	// post-manager construction via SetSkipDiskClearer — the
	// implementation needs the controller-runtime client only the
	// manager owns.
	SkipDiskClearer SkipDiskClearer

	// Exec runs auxiliary shell-outs the reconciler owns directly
	// (currently: `mkfs.<type>` for the RG-driven auto-mkfs path,
	// scenario 9.W14). Production wires `storage.RealExec`; tests
	// inject `storage.FakeExec` and assert the exact command line.
	// nil disables auto-mkfs entirely — the seed path still promotes
	// and demotes via Adm, but a configured `FileSystem/Type` prop
	// becomes a no-op rather than panicking.
	Exec storage.Exec
}

// CrossNodeFetcher abstracts the "fetch a snapshot from a peer that
// hosts it locally" half of the cross-node clone path. Lives behind
// an interface so satellite.Reconciler stays free of a direct
// controller-runtime client dependency — the K8s lookup + peer-IP
// resolution + stream HTTP GET sits in pkg/satellite/controllers
// where the cached client already lives.
type CrossNodeFetcher interface {
	// Fetch opens a byte stream of (srcRD, snap, vol) from a peer
	// satellite. Returns the stream + the peer node name it came
	// from (for logging). On storage.ErrNotFound, NO peer hosts the
	// snapshot locally — the caller must decide whether to fall
	// through to a blank create or surface the error.
	Fetch(ctx context.Context, srcRD, snap string, vol int32) (io.ReadCloser, string, error)
}

// MetadataCreatedStamper abstracts the "stamp the
// `MetadataCreated=True` Status Condition on a Resource CRD" verb.
// Mirrors `CrossNodeFetcher`: the K8s SSA call lives in
// pkg/satellite/controllers (where the cached client owns the
// apiserver wire) while the satellite's apply chain stays free of
// a controller-runtime client dependency. Phase 11.3 Stage 1.
type MetadataCreatedStamper interface {
	// StampMetadataCreated SSA-patches a `MetadataCreated=True`
	// Condition onto Resource <resourceName>.Status.Conditions.
	// Idempotent — repeat calls converge on the same Condition
	// shape (LastTransitionTime moves forward on apiserver-side
	// transition only, not on every patch).
	StampMetadataCreated(ctx context.Context, resourceName string) error
}

// FilesystemFormattedStamper abstracts the "stamp the
// `FilesystemFormatted=True` Status Condition on a Resource CRD"
// verb. Mirrors `MetadataCreatedStamper`: the K8s SSA call lives
// in pkg/satellite/controllers (where the cached client owns the
// apiserver wire) while the satellite's apply chain stays free of
// a controller-runtime client dependency. Phase 11.3 Stage 2.
type FilesystemFormattedStamper interface {
	// StampFilesystemFormatted SSA-patches a
	// `FilesystemFormatted=True` Condition onto Resource
	// <resourceName>.Status.Conditions. Idempotent — repeat calls
	// converge on the same Condition shape (LastTransitionTime
	// moves forward on apiserver-side transition only, not on
	// every patch).
	StampFilesystemFormatted(ctx context.Context, resourceName string) error

	// StampFilesystemObserved SSA-patches the same
	// `FilesystemFormatted=True` Condition with a distinct
	// `Reason` (FilesystemObserved vs MkfsSucceeded) so the audit
	// trail distinguishes filesystems the satellite formatted
	// itself from filesystems it merely observed via blkid on a
	// device created by an external actor (Ganesha sidecar, manual
	// `mkfs.ext4 -F`, operator-recovery). Same FieldOwner + SSA
	// shape as StampFilesystemFormatted — only Reason/Message
	// differ — so the listMap merge on `type=FilesystemFormatted`
	// keeps both writers cleanly sharing one Condition entry
	// without disturbing `.status.volumes` ownership.
	StampFilesystemObserved(ctx context.Context, resourceName string) error
}

// SkipDiskClearer abstracts the "release the satellite's SSA claim
// on Spec.Props[DrbdOptions/SkipDisk]" verb used by the Bug 278
// auto-clear path. The clearer applies a Spec.Props document under
// the same FieldOwner the observer used to stamp SkipDisk
// defensively, but without the SkipDisk key — SSA's per-key map
// merge releases that owner's claim and, when nobody else owns the
// key, the apiserver deletes it from Spec.Props. The next
// dispatcher cycle re-resolves the Spec without SkipDisk, the
// reconciler's `isSkipDiskEnabled` gate flips false, and the next
// `drbdadm adjust` re-attaches the kernel-healthy lower disk.
//
// Lives behind an interface so satellite.Reconciler stays free of
// a controller-runtime client dependency — the K8s SSA call lives
// in pkg/satellite/controllers (where the cached client owns the
// apiserver wire). Mirrors MetadataCreatedStamper /
// FilesystemFormattedStamper. Bug 278.
type SkipDiskClearer interface {
	// ClearSkipDisk SSA-applies Resource <resourceName>.Spec.Props
	// without the SkipDisk key, under the observer's own
	// FieldOwner. Idempotent — repeat calls converge on the
	// "owner releases the key" state (the apiserver no-ops the
	// second apply because the claim is already gone). NotFound
	// on the Resource CRD is silently swallowed by the
	// implementation: the convergence-pending case is the same as
	// the observer's stamp path and surfacing it here would force
	// every caller to re-implement the same silence.
	ClearSkipDisk(ctx context.Context, resourceName string) error
}

// Reconciler turns a controller-pushed DesiredResource set into local
// state. Phase-3 cut: storage provisioning + DRBD .res / drbdadm.
//
// The Reconciler also keeps an in-memory map of which storage pool each
// resource lives in (last-seen from Apply). Snapshot RPCs use it to
// dispatch to the correct provider without the controller having to
// pass the pool on every call.
type Reconciler struct {
	cfg ReconcilerConfig

	mu             sync.Mutex
	resourceToPool map[string]string

	// seenStuckAt is the in-memory debounce table for the Bug 342
	// v3 proactive kernel cleanup (Pass 3 stuck-slot probe). Keyed
	// by "<rd>/<peerName>"; value is the first wall-clock time the
	// satellite observed that slot in the Connecting / StandAlone
	// state with no peer-device configured. Subsequent reconciles
	// compare time.Since(first) against the configurable grace
	// before tearing the slot down; any reconcile where the slot
	// is no longer stuck clears the entry so the timer resets on
	// the next healthy probe. Lives in process memory only — a
	// satellite restart resets the table, at worst delaying
	// recovery by `grace` (default 30s).
	seenStuckAt map[string]time.Time

	// lastRecoveryPromoteAt throttles the Bug 366 recovery-promote
	// self-heal (maybeRecoveryPromote). Keyed by resource name; value
	// is the last wall-clock time this node fired a recovery-promote
	// for it. runAutoPromote (`primary --force` → mkfs → `secondary`)
	// itself churns kernel state, generating events2 frames that
	// re-trigger the reconcile while the predicate still holds (peer
	// Inconsistent + no Primary mid-resync) — without a throttle the
	// self-heal hot-loops at ~3 Hz, starving the very resync it is
	// meant to drive (stand-measured: ~589 re-arms on one staggered
	// create). The throttle fires the promote at most once per
	// recoveryPromoteThrottle so the kernel-driven SyncTarget has room
	// to make progress between nudges. Process-memory only; a restart
	// resets it (at worst one extra promote).
	lastRecoveryPromoteAt map[string]time.Time
}

// recoveryPromoteThrottle bounds how often a single resource may fire
// the Bug 366 recovery-promote self-heal. Generous relative to a
// kernel resync's progress cadence but short enough that a genuine
// wedge (peer truly stuck Inconsistent with no Primary) still gets a
// fresh nudge promptly.
const recoveryPromoteThrottle = 10 * time.Second

// NewReconciler constructs a Reconciler from cfg.
//
//nolint:gocritic // value receiver matches the public constructor convention; ReconcilerConfig is the agent's flag bundle.
func NewReconciler(cfg ReconcilerConfig) *Reconciler {
	if cfg.Providers == nil {
		// ApplyStoragePools registers providers into this map at
		// runtime; nil-init would panic on the first dynamic pool.
		cfg.Providers = map[string]storage.Provider{}
	}

	return &Reconciler{
		cfg:                   cfg,
		resourceToPool:        map[string]string{},
		seenStuckAt:           map[string]time.Time{},
		lastRecoveryPromoteAt: map[string]time.Time{},
	}
}

// RegisterProvider adds (or replaces) a `storage.Provider` in the
// reconciler's pool registry under the given pool name. Phase 10.5:
// gives `ApplyStoragePools` a way to wire dynamic pools without
// restarting the satellite. Idempotent — re-registering the same
// pool overwrites the old Provider, which is what
// piraeus-operator-style "edit pool config" workflows expect.
//
// `nil` provider deregisters the pool (used for `DISKLESS` apply
// frames the controller pushes for completeness).
func (r *Reconciler) RegisterProvider(pool string, provider storage.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cfg.Providers == nil {
		r.cfg.Providers = map[string]storage.Provider{}
	}

	if provider == nil {
		delete(r.cfg.Providers, pool)

		return
	}

	r.cfg.Providers[pool] = provider
}

// SnapshotProviders returns a snapshot of the pool→provider map the
// reconciler currently holds. Used by the orphan-storage sweeper (Bug
// 43) which walks every registered provider for VolumeLister-capable
// backends. The map is copied under the same lock RegisterProvider
// takes so a concurrent registration can't tear the snapshot.
//
// Callers must treat the returned map as read-only — modifying it
// races every subsequent RegisterProvider call.
func (r *Reconciler) SnapshotProviders() map[string]storage.Provider {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[string]storage.Provider, len(r.cfg.Providers))
	maps.Copy(out, r.cfg.Providers)

	return out
}

// SetCrossNodeFetcher injects the cross-node fetcher post-construction.
// Called by the agent after the controller-runtime manager is built —
// the fetcher needs the manager's cached client to look up Snapshot +
// Node CRDs which doesn't exist at NewReconciler time. Safe to call
// before the first Apply: applyOne reads cfg.CrossNodeFetcher inside
// a single struct-field load, no extra synchronisation needed under
// "set once, then read many" semantics.
func (r *Reconciler) SetCrossNodeFetcher(f CrossNodeFetcher) {
	r.cfg.CrossNodeFetcher = f
}

// SetMetadataCreatedStamper injects the MetadataCreated stamper
// post-construction. Mirrors `SetCrossNodeFetcher`: the stamper
// needs the controller-runtime manager's cached client which doesn't
// exist at NewReconciler time. Safe to call before the first Apply.
// Phase 11.3 Stage 1.
func (r *Reconciler) SetMetadataCreatedStamper(s MetadataCreatedStamper) {
	r.cfg.MetadataCreatedStamper = s
}

// SetFilesystemFormattedStamper injects the FilesystemFormatted
// stamper post-construction. Mirrors `SetMetadataCreatedStamper`:
// the stamper needs the controller-runtime manager's cached client
// which doesn't exist at NewReconciler time. Safe to call before
// the first Apply. Phase 11.3 Stage 2.
func (r *Reconciler) SetFilesystemFormattedStamper(s FilesystemFormattedStamper) {
	r.cfg.FilesystemFormattedStamper = s
}

// SetSkipDiskClearer injects the SkipDisk clearer post-construction.
// Mirrors `SetMetadataCreatedStamper`: the clearer needs the
// controller-runtime manager's cached client which doesn't exist at
// NewReconciler time. Safe to call before the first Apply. Bug 278.
func (r *Reconciler) SetSkipDiskClearer(c SkipDiskClearer) {
	r.cfg.SkipDiskClearer = c
}

// StateDir returns the on-disk directory the reconciler uses for
// per-resource `.res` files and state markers. The OrphanSweeperRunnable
// consults it (Bug 299) to distinguish kernel-resident DRBD slots
// blockstor itself provisioned (`<StateDir>/<rsc>.res` present —
// `handleDelete` removes it only after a clean tear-down) from foreign
// slots written by a co-resident DRBD manager (e.g. an upstream
// piraeus / linstor-satellite running side-by-side on the same node).
// Without this distinction the sweeper used to issue `drbdsetup down`
// on every kernel slot that lacked a matching blockstor Resource CRD,
// which on a piraeus-coexistence stand reliably tore down LINSTOR's
// own resources between create and first-attach and surfaced as
// "Failed to adjust DRBD resource …" / "Cannot resize volume, because
// we have a non-UpToDate DRBD device" upstream.
//
// Empty string means "StateDir-based filtering disabled" — the sweeper
// then sweeps purely on CRD presence, the legacy behaviour. Tests use
// the empty default to keep assertions simple; production always
// passes the real on-disk path from cmd/satellite.
func (r *Reconciler) StateDir() string {
	return r.cfg.StateDir
}

// Apply walks res and brings local storage in line with each item.
// Each input gets a ResourceApplyResult — partial success is the norm
// (one missing pool shouldn't sink the rest of a batch).
//
// The signature returns an error too, but reserves it for context
// cancellation — per-resource failures land in the Result entries.
func (r *Reconciler) Apply(ctx context.Context, res []*intent.DesiredResource) ([]*intent.ResourceApplyResult, error) {
	results := make([]*intent.ResourceApplyResult, 0, len(res))

	for _, dr := range res {
		err := ctx.Err()
		if err != nil {
			return results, errors.Wrap(err, "apply: context cancelled")
		}

		results = append(results, r.applyOne(ctx, dr))
	}

	return results, nil
}

// CreateSnapshot dispatches to the storage provider that backs the
// resource (looked up via the resource→pool map populated by Apply).
// Returns ok=false in the response when the resource is unknown — the
// satellite never auto-creates snapshots of state it doesn't own.
//
// Terminal classification policy:
//   - providerForResource fails ⇒ Terminal=true. "Unknown resource"
//     means the parent volume never got materialised on this node;
//     a future Apply pass MIGHT change that, but the SnapshotReconciler
//     can still treat the snapshot as failed and rely on the operator
//     to delete + recreate once the parent lands. (Retrying forever on
//     an indefinitely-missing parent leaks Reconcile pressure.)
//   - provider.CreateSnapshot returns ErrTerminal (or wraps ErrNotFound)
//     ⇒ Terminal=true. Same logic.
//   - any other error ⇒ Terminal=false. Transient lvm/zfs noise.
func (r *Reconciler) CreateSnapshot(ctx context.Context, req *intent.CreateSnapshotRequest) (*intent.CreateSnapshotResponse, error) {
	provider, err := r.providerForResource(req.GetResourceName())
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &intent.CreateSnapshotResponse{Ok: false, Terminal: true, Message: err.Error()}, nil
	}

	err = provider.CreateSnapshot(ctx, storage.Snapshot{
		ResourceName: req.GetResourceName(),
		SnapshotName: req.GetSnapshotName(),
	})
	if err != nil {
		terminal := errors.Is(err, storage.ErrTerminal) || errors.Is(err, storage.ErrNotFound)

		return &intent.CreateSnapshotResponse{Ok: false, Terminal: terminal, Message: err.Error()}, nil
	}

	return &intent.CreateSnapshotResponse{
		Ok: true,
		// Upstream LINSTOR's OpenAPI declares
		// `create_timestamp` as **milliseconds** since unix
		// epoch in UTC (pkg/api/openapi/types.gen.go), and the
		// python CLI's `linstor s l` "CreatedOn" column divides
		// by 1000 before formatting. UnixMilli matches; Unix
		// (seconds) would render the stamp as 1970-01-21.
		CreateTimestampUnix: time.Now().UnixMilli(),
	}, nil
}

// DeleteSnapshot mirrors CreateSnapshot. Idempotency lives at the
// provider layer (lvremove on missing LV is non-fatal there).
func (r *Reconciler) DeleteSnapshot(ctx context.Context, req *intent.DeleteSnapshotRequest) (*intent.DeleteSnapshotResponse, error) {
	provider, err := r.providerForResource(req.GetResourceName())
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &intent.DeleteSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	err = provider.DeleteSnapshot(ctx, storage.Snapshot{
		ResourceName: req.GetResourceName(),
		SnapshotName: req.GetSnapshotName(),
	})
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &intent.DeleteSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	return &intent.DeleteSnapshotResponse{Ok: true}, nil
}

// SuspendResource freezes the local DRBD layer's I/O for `resName`
// via `drbdsetup suspend-io`. Phase 1 of the Bug-351 snapshot
// orchestration: every diskful peer suspend-io's the resource
// before any of them runs the local provider.CreateSnapshot, so
// the LV/zvol bytes the kernel captures reflect the same
// point-in-time DRBD-block-stream cursor on every node.
//
// nil Adm (DRBD half disabled — unit tests of the storage path
// without drbdadm wired) returns nil without erroring: the
// orchestration only matters for replicated resources, and a
// non-DRBD provider's `provider.CreateSnapshot` is already
// point-in-time on its own.
func (r *Reconciler) SuspendResource(ctx context.Context, resName string) error {
	if r.cfg.Adm == nil {
		return nil
	}

	err := r.cfg.Adm.SuspendIO(ctx, resName)
	if err != nil {
		return errors.Wrapf(err, "suspend-io %s", resName)
	}

	return nil
}

// ResumeResource is the counterpart to SuspendResource. MUST be
// called on every node SuspendResource fired on, even on the
// abort path — a partially-suspended cluster with no follow-up
// resume leaves application I/O hung forever. The controller-side
// orchestration unconditionally flips Spec.SuspendIO=false on
// terminal states (Phase 3 success / any per-node Failed) so this
// fires on every targeted satellite.
//
// nil Adm short-circuits to nil for the same reason
// SuspendResource does.
func (r *Reconciler) ResumeResource(ctx context.Context, resName string) error {
	if r.cfg.Adm == nil {
		return nil
	}

	err := r.cfg.Adm.ResumeIO(ctx, resName)
	if err != nil {
		return errors.Wrapf(err, "resume-io %s", resName)
	}

	return nil
}

// FlushBackingDevices drains the kernel writeback cache from the
// DRBD layer down to the backing block device for every volume of
// `resName`. P0 fix for the stale-snapshot bug: `drbdadm suspend-io`
// quiesces NEW writes at the DRBD layer but per the DRBD docs
// "inflight requests will still complete" — i.e. suspend-io
// returns BEFORE the in-flight queue between DRBD and the backing
// device has fully drained. Without an explicit flush the
// storage-layer snapshot is captured against a backing device that
// still has pending DRBD writes queued, so the snap (and any clone
// / `zfs send | recv` payload derived from it) carries empty /
// stale content.
//
// Empirical (ZFS-switch stand, 2026-05-23): a 256 KiB urandom
// payload written with `oflag=direct` through /dev/drbdN showed
// only ~16 KiB `used` on the post-snapshot ZFS zvol. Flushing the
// backing zvol alone via `blockdev --flushbufs` was insufficient
// because the writes hadn't even left DRBD's queue yet — DRBD's
// own writeback path runs asynchronously after the kthread acks
// the application bio. The fix mirrors upstream LINSTOR's
// `Controller/.../snapshot/*` barrier sequence: per-volume
// `blockdev --flushbufs` (drains the device's page cache) AND a
// global `sync` (drains DRBD's kthread queues that BLKFLSBUF on a
// single device doesn't reach because DRBD's writeback queue
// isn't associated with any particular /dev/.. fd's bdi).
//
// Per-device flush failures are logged but do NOT abort the whole
// flush: a missing volume (e.g. diskless peer) is fine — its
// VolumeStatus returns DevicePath="" and we skip it. Likewise, a
// non-zero `blockdev --flushbufs` / `sync` exit (kernel without
// the ioctl, hostpath sandboxing) is best-effort: degraded flush
// is better than failing the snapshot orchestration.
//
// nil Exec (unit tests of the storage path without shell-out
// wired) returns nil without erroring: the flush is a no-op on a
// fake-exec satellite, which is the same shape SuspendResource /
// ResumeResource use for their own nil-Adm short-circuit.
//
// volumeNumbers is the per-RD slice of volumes the orchestrator
// targets; mirrors the same slice
// `handleTakeSnapshotPhase` builds from `snap.Spec.VolumeDefinitions`
// so the per-device flush stays scoped to exactly the volumes the
// snapshot captures (the global sync drains everything regardless,
// which is unavoidable but acceptable because the satellite is
// already under a per-resource suspend-io window).
//
//nolint:funlen // three-step barrier (per-volume blockdev / global sync / zpool sync) is sequential by design; splitting hurts readability of the orchestration ordering.
func (r *Reconciler) FlushBackingDevices(ctx context.Context, resName string, volumeNumbers []int32) error {
	if r.cfg.Exec == nil {
		return nil
	}

	provider, err := r.providerForResource(resName)
	if err != nil {
		// Unknown resource on this node — nothing to flush. The
		// SnapshotReconciler's CreateSnapshot path will surface the
		// same lookup failure as a terminal error, so we don't
		// need to duplicate the routing here.
		//nolint:nilerr // best-effort flush; unknown-resource is handled by the caller.
		return nil
	}

	r.mu.Lock()
	pool := r.resourceToPool[resName]
	r.mu.Unlock()

	for _, volNum := range volumeNumbers {
		vol := storage.Volume{
			PoolName:     pool,
			ResourceName: resName,
			VolumeNumber: volNum,
		}

		status, statusErr := provider.VolumeStatus(ctx, vol)
		if statusErr != nil {
			log.FromContext(ctx).Info("flush: VolumeStatus failed; skipping",
				"resource", resName, "volume", volNum, "error", statusErr.Error())

			continue
		}

		if status.DevicePath == "" {
			// Volume not provisioned on this node (e.g. diskless
			// peer) — nothing to flush.
			continue
		}

		// `blockdev --flushbufs` issues a BLKFLSBUF ioctl which
		// drains the kernel page cache for the device.
		_, runErr := r.cfg.Exec.Run(ctx, "blockdev", "--flushbufs", status.DevicePath)
		if runErr != nil {
			log.FromContext(ctx).Info("flush: blockdev --flushbufs failed; continuing",
				"resource", resName, "volume", volNum, "device", status.DevicePath,
				"error", runErr.Error())

			continue
		}

		log.FromContext(ctx).Info("flush: backing-device page cache drained",
			"resource", resName, "volume", volNum, "device", status.DevicePath)
	}

	// Global `sync` is the host-wide barrier that drains DRBD's
	// kthread writeback queue. BLKFLSBUF on a single
	// /dev/zvol/... device doesn't reach DRBD's queue because
	// DRBD's writeback isn't anchored to any particular bdi fd.
	_, err = r.cfg.Exec.Run(ctx, "sync")
	if err != nil {
		log.FromContext(ctx).Info("flush: global sync failed; continuing",
			"resource", resName, "error", err.Error())
	} else {
		log.FromContext(ctx).Info("flush: global sync drained DRBD writeback queue",
			"resource", resName)
	}

	// ZFS-specific final step: `zpool sync <pool>` forces the
	// kernel ZFS module to commit all in-flight TXGs to disk
	// blocks. Without this the post-`sync` zvol page cache is
	// drained but the data may still be sitting in ZFS's
	// in-memory ARC/dnode tree waiting for the next periodic TXG
	// (default 5s). `zfs snapshot` is atomic with the next TXG
	// commit, but in the suspend-io window we want a barrier
	// that pushes the just-flushed bytes into the snapshotted
	// state BEFORE the snapshot dataset is created — otherwise
	// the snap captures an inconsistent point relative to the
	// caller's "I just wrote X bytes" expectation.
	//
	// Provider-agnostic: only fires for the ZFS / ZFS_THIN
	// kinds. LVM-thin / FILE rely on the prior `sync(1)` step,
	// which is sufficient for their kernel-page-cache-only
	// writeback model.
	kind := provider.Kind()
	if kind == "ZFS" || kind == "ZFS_THIN" {
		// The pool name lives behind the provider; extract it
		// via the optional `Pool() string` accessor (zfs.Provider
		// implements it). Fall back to the registry name when the
		// concrete type doesn't expose Pool() — production cmd/satellite
		// uses the same string for both.
		zpoolName := zpoolNameForProvider(provider, pool)
		if zpoolName != "" {
			_, err = r.cfg.Exec.Run(ctx, "zpool", "sync", zpoolName)
			if err != nil {
				log.FromContext(ctx).Info("flush: zpool sync failed; continuing",
					"resource", resName, "zpool", zpoolName, "error", err.Error())
			} else {
				log.FromContext(ctx).Info("flush: zpool sync committed pending TXGs",
					"resource", resName, "zpool", zpoolName)
			}
		}
	}

	return nil
}

// flushBackingPerVolume issues `blockdev --flushbufs <devPath>`
// for every volume of the resource. Best-effort: per-volume
// VolumeStatus / blockdev failures are logged and skipped so a
// missing replica or a kernel without BLKFLSBUF doesn't sink the
// whole snapshot orchestration. Extracted out of
// FlushBackingDevices to keep that function under the funlen
// budget (<60 lines).
// zpoolNameForProvider returns the underlying ZFS pool name for a
// ZFS / ZFS_THIN provider. The blockstor registry name (`pool`) is
// the logical pool name in CRDs, but the actual `zpool` is held by
// the provider's config. There's no public accessor on the
// storage.Provider interface, so we type-assert against an inline
// `Pool() string` interface — zfs.Provider implements it.
func zpoolNameForProvider(provider storage.Provider, registryName string) string {
	type zpoolNamer interface {
		Pool() string
	}

	if pp, ok := provider.(zpoolNamer); ok {
		return pp.Pool()
	}

	// Fall back to the registry name. The cmd/satellite wiring
	// uses the same string for both, so this is a sane default
	// even though it elides the indirection.
	return registryName
}

// DeleteResource tears down a resource: drbdadm down (best-effort —
// the kernel handles a missing one fine), DeleteVolume on every
// requested volume_number through the named Provider, then remove
// the .res file. Idempotent on a missing resource. Per-step errors
// land in the response body so the controller can surface granular
// status without aborting the rest of the cleanup.
func (r *Reconciler) DeleteResource(ctx context.Context, req *intent.DeleteResourceRequest) (*intent.DeleteResourceResponse, error) {
	var downMsg string

	if r.cfg.Adm != nil {
		// Bug 358 Step 3: force-resume I/O before `drbdadm down`. A
		// snapshot whose suspend-io was never matched by a resume-io
		// (force-deleted Snapshot CRD, controller crash between Phase 1
		// and Phase 3) leaves the device `suspended:user` with wedged
		// D-state writers pinning it open. `drbdadm down` then fails
		// "Device is held open" and the kernel slot leaks past CRD
		// deletion (issue 288 minor-leak). resume-io is idempotent —
		// the kernel folds it into a no-op on a non-suspended device —
		// so this is safe to fire unconditionally. Best-effort: a
		// "not configured" failure (resource unknown to the kernel)
		// is the no-op case and must not abort teardown.
		_ = r.cfg.Adm.ResumeIO(ctx, req.GetName())

		// Try `drbdadm down` first — it's the canonical teardown
		// path and exercises drbd-utils' full graceful sequence
		// (Secondary → Detach → Disconnect → Down).
		err := r.cfg.Adm.Down(ctx, req.GetName())
		if err != nil {
			// drbdadm fails with "not defined in your config (for
			// this host)" / "no resources defined!" whenever the
			// .res file in /etc/drbd.d is missing — which is the
			// state we land in when DeleteResource ran once already
			// (cleanup wiped the .res below) but the kernel slot
			// somehow survived. Fall back to `drbdsetup down`
			// (kernel-direct, no .res file needed) so the kernel
			// slot doesn't leak past CRD deletion (issue 288: the
			// leaked slot pins the resource's minor in the kernel,
			// blocking any subsequent RD re-using that minor with
			// "Device '<minor>' is configured!" on create-md).
			//
			// Best-effort either way: a "not configured" failure
			// on both is fine (kernel didn't know the resource).
			// Surface the original drbdadm error so operators can
			// see whether the fallback fired.
			downMsg = "drbdadm down: " + err.Error()

			setupErr := r.cfg.Adm.SetupDown(ctx, req.GetName())
			if setupErr != nil {
				downMsg += "; drbdsetup down: " + setupErr.Error()
			}
		}
	}

	// Tear down LUKS mappers BEFORE DeleteVolume — once the underlying
	// LV is gone, `cryptsetup luksClose` would either error out or hang
	// trying to flush the now-missing block device. Best-effort: a
	// missing mapper (delete-after-restart, never opened) is fine.
	if r.cfg.Cryptsetup != nil {
		for _, n := range req.GetVolumeNumbers() {
			_ = r.cfg.Cryptsetup.Close(ctx, luksMapperName(req.GetName(), n))
		}
	}

	if pool := req.GetStoragePool(); pool != "" {
		provider, ok := r.cfg.Providers[pool]
		if ok {
			for _, n := range req.GetVolumeNumbers() {
				err := provider.DeleteVolume(ctx, storage.Volume{
					ResourceName: req.GetName(),
					VolumeNumber: n,
				})
				if err != nil {
					//nolint:nilerr // surfaced as ok=false; gRPC error reserved for transport faults
					return &intent.DeleteResourceResponse{
						Ok:      false,
						Message: err.Error(),
					}, nil
				}
			}
		}
	}

	if r.cfg.StateDir != "" {
		// Drop the per-resource state files together. Leaving
		// `.md-created` behind would make a re-created RD with the
		// same name see firstActivation=false on its first apply,
		// skip create-md, and fail drbdadm adjust with
		// "No valid meta data found". `.owned` (Bug 432) is the
		// sweeper's ownership marker — must be removed AFTER the
		// kernel slot is gone so a concurrent sweeper tick sees a
		// clean post-state (kernel + marker both absent).
		for _, suffix := range []string{".res", ".md-created", ".mkfs.done", ".owned"} {
			err := os.Remove(filepath.Join(r.cfg.StateDir, req.GetName()+suffix))
			if err != nil && !os.IsNotExist(err) {
				return &intent.DeleteResourceResponse{
					Ok:      false,
					Message: err.Error(),
				}, nil
			}
		}
	}

	r.forgetPool(req.GetName())

	return &intent.DeleteResourceResponse{Ok: true, Message: downMsg}, nil
}

// forgetPool drops the resource from the resource→pool map so a
// future Apply with a different pool starts clean.
func (r *Reconciler) forgetPool(resourceName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.resourceToPool, resourceName)
}

// applyOne reconciles a single DesiredResource. Diskless replicas skip
// the storage path (they're memory-backed by the DRBD stack); everything
// else routes one CreateVolume per DesiredVolume. When the DRBD half is
// enabled (cfg.Adm != nil), also renders the `.res` file and runs
// drbdadm create-md / adjust.
func (r *Reconciler) applyOne(ctx context.Context, dr *intent.DesiredResource) *intent.ResourceApplyResult {
	res := &intent.ResourceApplyResult{
		Name:     dr.GetName(),
		NodeName: dr.GetNodeName(),
		Ok:       true,
	}

	diskless := isDiskless(dr.GetFlags())

	if isInactive(dr.GetFlags()) {
		r.applyInactive(ctx, dr, res)

		return res
	}

	devices, resized, cloned, err := r.applyStorageIfDiskful(ctx, dr, diskless)
	if err != nil {
		res.Ok = false
		res.Message = err.Error()

		return res
	}

	devices, luksGrew, err := r.maybeLUKS(ctx, dr, diskless, devices, resized)
	if err != nil {
		res.Ok = false
		res.Message = err.Error()

		return res
	}

	// LUKS layer may have driven a convergent resize even when the
	// storage layer did not (Bug LUKS-RESIZE-CONVERGE crash-recovery
	// path: prior reconcile widened the LV but the satellite restart
	// happened before cryptsetup-resize fired, so this reconcile sees
	// storage already at spec — resized=false — but the mapper is
	// still small). Fold the LUKS signal into the resize bool so the
	// downstream `drbdadm resize` fires.
	resized = resized || luksGrew

	// Skip DRBD when the layer_stack explicitly omits it. Empty
	// layer_stack defaults to ["DRBD","STORAGE"] so legacy clients
	// (and pre-Phase-9 dispatchers) keep getting full DRBD treatment.
	withDRBD := r.cfg.Adm != nil && needsDRBD(dr.GetLayerStack())
	if withDRBD {
		err := r.applyDRBD(ctx, dr, diskless, devices, resized, cloned)
		if err != nil {
			res.Ok = false
			res.Message = err.Error()

			return res
		}
	} else {
		// Storage-only resources (layerList=storage, the linstor-csi
		// "local" SC shape) bypass applyDRBD entirely, which means the
		// DRBD-side runAutoPromote → runAutoMkfs chain never runs and
		// `FileSystem/Type` set by linstor-csi on the RD would be
		// silently ignored. linstor-csi v1.10.1's NodePublishVolume
		// does `fsck` + plain `mount(2)` on the device (no
		// FormatAndMount fallback), so without satellite-side mkfs the
		// kubelet's fsck rejects the unformatted volume with exit 8.
		// Match the DRBD-stack contract by running the same auto-mkfs
		// path against the raw device map from applyStorage.
		err := r.runStorageOnlyMkfs(ctx, dr, diskless, devices)
		if err != nil {
			res.Ok = false
			res.Message = err.Error()

			return res
		}
	}

	res.Volumes = buildVolumeResults(dr, devices, diskless, withDRBD)

	return res
}

// maybeLUKS conditionally layers cryptsetup over the raw storage
// devices when the layer stack names "LUKS". Returns the (possibly
// rewritten) volume → device map and a bool reporting whether the
// LUKS layer just grew the dm-crypt mapper (so the caller can
// chain a downstream `drbdadm resize` even when the storage layer
// didn't change this reconcile — see Bug LUKS-RESIZE-CONVERGE).
// Skips entirely for diskless replicas — they never open the
// underlying disk.
func (r *Reconciler) maybeLUKS(ctx context.Context, dr *intent.DesiredResource, diskless bool, devices map[int32]string, resized bool) (map[int32]string, bool, error) {
	if diskless || !needsLUKS(dr.GetLayerStack()) {
		return devices, false, nil
	}

	return r.applyLUKS(ctx, dr, devices, resized)
}

// needsLUKS reports whether the satellite should layer cryptsetup
// over the storage device for this resource. Empty stack defaults to
// the no-LUKS legacy behaviour; LUKS only runs when explicitly named.
func needsLUKS(stack []string) bool {
	for _, s := range stack {
		if strings.EqualFold(s, "LUKS") {
			return true
		}
	}

	return false
}

// applyLUKS formats (first activation only) and opens every volume's
// raw device under /dev/mapper/<rd>-<volnum>, returning the new
// volNumber→DevicePath map for downstream layers (DRBD or direct
// consumer) and a bool reporting whether the dm-crypt mapper just
// grew during this reconcile (so the caller can chain a downstream
// `drbdadm resize` even when the storage layer didn't change).
//
// Resize trigger: cryptsetup resize fires when EITHER the storage
// layer just grew this reconcile (resized=true from applyStorage)
// OR the dm-crypt mapper was opened on a previous reconcile pass
// AND the underlying device has grown since (Bug LUKS-RESIZE-
// CONVERGE). The second trigger is the convergence path: if a prior
// reconcile widened the LV but the satellite crashed / lost its
// scheduler tick before reaching the cryptsetup-resize step, the
// `vol.SizeKib > status.UsableKib` predicate goes false on the next
// pass — applyStorage finds nothing to grow — and `resized` stays
// false. Without the convergence path the LUKS mapper would stay
// pinned at the old size forever, starving every consumer above it.
//
// To avoid an unconditional cryptsetup-resize shell-out on every
// steady-state reconcile (would spawn ~3 children per reconcile per
// volume), we only run resize when Open returned ErrAlreadyOpen
// (mapper carried over from a previous Apply) AND blockdev reports
// the underlying device is larger than the mapper. A fresh Open
// (Format-then-Open path) always lines up with the underlying size
// out of cryptsetup luksFormat's geometry so the probe is skipped.
//
// Passphrase source for this slice: dr.Props["LuksPassphrase"]. The
// controller folds it in from the RD's `DrbdOptions/Encryption/passphrase`
// prop via the resolver. Empty passphrase fails the apply — explicit
// rather than silently creating an unencrypted volume.
func (r *Reconciler) applyLUKS(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, resized bool) (map[int32]string, bool, error) {
	if r.cfg.Cryptsetup == nil {
		return nil, false, errors.New("LUKS in layer stack but no cryptsetup wrapper configured")
	}

	pass := dr.GetProps()["LuksPassphrase"]
	if pass == "" {
		return nil, false, errors.New("LUKS in layer stack but Props.LuksPassphrase empty")
	}

	out := make(map[int32]string, len(devices))
	key := []byte(pass)
	mapperGrew := false

	for vol, dev := range devices {
		dmName := luksMapperName(dr.GetName(), vol)

		err := r.cfg.Cryptsetup.Format(ctx, dev, key)
		if err != nil {
			return nil, false, errors.Wrapf(err, "luks format %s", dev)
		}

		openErr := r.cfg.Cryptsetup.Open(ctx, dev, dmName, key)
		alreadyOpen := errors.Is(openErr, luks.ErrAlreadyOpen)

		if openErr != nil && !alreadyOpen {
			// Non-EEXIST open failure: bubble. EEXIST is the every-
			// reconcile-after-first idempotent path — classified via
			// the typed luks.ErrAlreadyOpen sentinel so we are immune
			// to cryptsetup output locale (Bug 215): the prior
			// English-only substring match silently missed de_DE /
			// fr_FR / ru_RU satellites and would have triggered a
			// luksFormat retry against an already-formatted device.
			return nil, false, errors.Wrapf(openErr, "luks open %s -> %s", dev, dmName)
		}

		mapperPath := luks.DevicePath(dmName)

		// Decide whether to invoke cryptsetup resize. Two triggers:
		//
		//  1. resized=true from applyStorage — the LV/zvol/file just
		//     grew this reconcile. The mapper must catch up before
		//     DRBD's own resize fires.
		//
		//  2. The mapper carried over from a previous Apply
		//     (alreadyOpen=true) AND the underlying device is now
		//     larger than the mapper. Covers the crash-recovery path
		//     and any other reason applyStorage's grow predicate
		//     skipped this reconcile while the mapper still lags.
		//
		// On a fresh Open (no EEXIST) the dm-crypt geometry matches
		// the underlying device by definition — luksOpen sizes the
		// mapper from the device on every fresh attach — so we skip
		// the probe + resize to keep steady-state reconciles cheap.
		needResize := resized
		if !needResize && alreadyOpen && r.luksMapperBehindUnderlying(ctx, dev, mapperPath) {
			needResize = true
		}

		if needResize {
			err = r.cfg.Cryptsetup.Resize(ctx, dmName, key)
			if err != nil {
				return nil, false, errors.Wrapf(err, "luks resize %s", dmName)
			}

			mapperGrew = true
		}

		out[vol] = mapperPath
	}

	return out, mapperGrew, nil
}

// luksMapperBehindUnderlying reports whether the dm-crypt mapper at
// mapperPath is currently sized smaller than the underlying device
// at devicePath, accounting for the LUKS header carve-out.
//
// LUKS2 reserves a 16 MiB header by default (cryptsetup 2.x — older
// LUKS1 reserves ~2 MiB; older LUKS2 builds 4 MiB). The mapper is
// ALWAYS smaller than the underlying by at least that header amount,
// so a naive `underlying > mapper` comparison fires on every healthy
// LUKS device. We use a 32 MiB tolerance: anything beyond that gap
// is a real grow that the mapper hasn't picked up.
//
// Both probes shell out to `blockdev --getsize64`. Probe failures
// (Exec nil in unit tests, blockdev missing on a minimal image,
// transient EBUSY mid-attach) fall through to (false, nil): no probe
// → no convergence push, and the resized=true fast path still works
// for fresh resize-pending events. This mirrors the "best-effort
// drift detection" pattern used by readDeviceSizeMiB elsewhere in
// this file.
func (r *Reconciler) luksMapperBehindUnderlying(ctx context.Context, devicePath, mapperPath string) bool {
	if r.cfg.Exec == nil {
		return false
	}

	devSize, ok := readDeviceSizeBytes(ctx, r.cfg.Exec, devicePath)
	if !ok {
		return false
	}

	mapperSize, ok := readDeviceSizeBytes(ctx, r.cfg.Exec, mapperPath)
	if !ok {
		return false
	}

	// 32 MiB covers the largest possible LUKS2 header (16 MiB default,
	// some LUKS2 variants 4 MiB with extended secondary header) with
	// margin for cryptsetup's per-volume alignment rounding. A real
	// resize grows the underlying by at least an FS block (≥4 KiB) but
	// in practice operator-driven resizes move at least one MiB at a
	// time; 32 MiB is conservative without missing any real grow.
	// False negatives only delay the next reconcile's resize; false
	// positives cost one idempotent cryptsetup resize.
	const luksHeaderToleranceBytes int64 = 32 << 20 // 32 MiB

	return devSize > mapperSize+luksHeaderToleranceBytes
}

// readDeviceSizeBytes is the byte-precision counterpart of
// readDeviceSizeMiB used by the attach-wipe path. Pulled out so the
// LUKS-mapper drift probe can compare to the underlying device at
// byte granularity (a MiB-rounded comparison would lose any sub-MiB
// header carve-out and surface as a false-positive resize loop on
// every reconcile).
func readDeviceSizeBytes(ctx context.Context, exec storage.Exec, devicePath string) (int64, bool) {
	out, err := exec.Run(ctx, "blockdev", "--getsize64", devicePath)
	if err != nil {
		return 0, false
	}

	sizeBytes, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, false
	}

	return sizeBytes, true
}

// luksMapperName picks the dm-crypt name for an (rd, vol) pair. The
// satellite needs a stable identifier across reconciles so a re-Open
// after restart re-uses the existing mapping when present.
func luksMapperName(rdName string, vol int32) string {
	return fmt.Sprintf("%s-%d-luks", rdName, vol)
}

// needsDRBD reports whether the satellite should render a .res and
// run drbdadm for this resource. Empty stack → default-true (legacy
// + Phase-1..8 wire compatibility); explicit stack → only run DRBD
// when it's named in the stack.
func needsDRBD(stack []string) bool {
	if len(stack) == 0 {
		return true
	}

	for _, s := range stack {
		if strings.EqualFold(s, "DRBD") {
			return true
		}
	}

	return false
}

// applyStorage walks dr.Volumes and ensures each LV/zvol/loopfile
// exists. Returns a `volNumber → DevicePath` map the DRBD half uses
// to wire the `disk` line in the .res file — this is what the
// kernel actually opens, so we never want the satellite to guess
// (`/dev/<pool>/<rd>_<vol>` only works for LVM/ZFS, not loopfile).
//
// Records the resource→pool mapping (first volume's pool) so
// subsequent snapshot RPCs can route without the controller passing
// applyInactive runs the `drbdadm down` half of the INACTIVE flag
// path. Pulled out of applyOne to keep the latter under funlen.
// Storage + .res file are intentionally untouched — activate later
// brings the kernel resource back without losing port/node-id or
// triggering a re-sync.
func (r *Reconciler) applyInactive(ctx context.Context, dr *intent.DesiredResource, res *intent.ResourceApplyResult) {
	if r.cfg.Adm == nil {
		return
	}

	// Bug 350 down-veto (defense-in-depth behind the controller's
	// uncached authoritative-flag adoption). Probe the live kernel
	// before committing to `drbdadm down` and FAIL CLOSED: defer the
	// down whenever we cannot positively confirm the slot is a
	// genuinely-idle INACTIVE replica.
	//
	//   - DownVetoResync: a peer-device is mid-resync
	//     (SyncSource/SyncTarget). Downing now aborts that resync and
	//     strands the peer Inconsistent forever (out-of-sync=0, never
	//     finalizes) — the exact wedge a stale INACTIVE flag would
	//     inflict on a just-reactivated replica.
	//   - DownVetoInconclusive: the kernel probe failed/timed-out or
	//     returned ambiguous output that is NOT the verbatim
	//     "resource not loaded" branch — the cold-satellite timing
	//     window where a slot is most likely still loaded or mid-
	//     bring-up. The pre-fix code failed OPEN here and let the down
	//     proceed, which is exactly how the cold-start cycle-1 flake
	//     killed a resync at ~5%. We now defer instead.
	//
	// In BOTH cases refuse the down on this pass and surface a
	// transient failure so the reconcile requeues (applyFailureRequeue
	// in runApply). The next pass re-evaluates against authoritative
	// flags (cache caught up → no longer INACTIVE → no down at all) or
	// against a warm kernel (resync finished / slot conclusively gone →
	// down proceeds). EvaluateDownVeto maps a CONCLUSIVE not-loaded
	// probe (clean exit-10 "No such resource") to DownAllowed, so a
	// genuinely downed/idle resource is never deferred indefinitely —
	// the defer is bounded by the slot actually disappearing.
	switch r.cfg.Adm.EvaluateDownVeto(ctx, dr.GetName()) {
	case drbd.DownVetoResync:
		res.Ok = false
		res.Message = "deferring drbdadm down: resync in flight (Bug 350 veto)"

		return
	case drbd.DownVetoInconclusive:
		res.Ok = false
		res.Message = "deferring drbdadm down: kernel probe inconclusive, cannot confirm idle (Bug 350 fail-closed veto)"

		return
	case drbd.DownAllowed:
		// Fall through to the down.
	}

	err := r.cfg.Adm.Down(ctx, dr.GetName())
	if err != nil {
		res.Ok = false
		res.Message = err.Error()
	}
}

// applyStorageIfDiskful skips storage provisioning for diskless
// replicas (they have no backing disk) and routes diskful ones to
// applyStorage. Pulled out of applyOne to keep the latter under
// funlen.
//
// Bug 267 (HIGH, capacity leak): when a previously-diskful replica
// is toggled to diskless via `linstor r td <node> <rd> --diskless`,
// the REST handler flips Spec.Flags=[DISKLESS] but keeps Spec.
// StoragePool intact so the operator can toggle back. The dispatcher
// stamps the historical pool onto every DesiredVolume on the
// toggle-to-diskless path. THIS function detects that shape
// (diskless=true AND at least one Volume carries a non-empty
// StoragePool) and invokes provider.DeleteVolume to reclaim the
// backing LV / zvol — without this, the volume sits on disk forever
// counted against the pool's free-space budget; repeated
// demote-promote cycles compound the leak.
//
// Fresh DISKLESS replicas (no prior storage, every Volume's
// StoragePool empty) hit the no-op short-circuit at the top.
func (r *Reconciler) applyStorageIfDiskful(ctx context.Context, dr *intent.DesiredResource, diskless bool) (map[int32]string, bool, bool, error) {
	if diskless {
		// Bug 330 (P1, real stand): `linstor r td --diskless` returned
		// REST SUCCESS but `linstor r l` kept reporting the replica as
		// `UpToDate`. Root cause: the reconciler had no detach path —
		// the FSM dispatched plain `drbdadm adjust` against the loaded
		// slot, and drbd-utils' compare_volume does NOT cross the
		// kern->disk=<path> → conf->disk="none" boundary on its own
		// (the inverse of the Bug 319 attach direction). Without an
		// explicit `drbdadm detach`, the kernel never releases the
		// lower disk, and reclaimVolumesForDiskless below would then
		// destroy the LV out from under a still-attached DRBD slot.
		//
		// Match upstream LINSTOR's DrbdLayer.deactivateVolume sequence:
		// detach BEFORE the storage layer reclaims the backing volume.
		// detachIfStillAttached is a no-op when the kernel has already
		// dropped to Diskless on its own (idempotent re-entry on a
		// satellite restart mid-toggle).
		err := r.detachIfStillAttached(ctx, dr)
		if err != nil {
			return nil, false, false, err
		}

		// Bug-hunt v2 E.1b / E.1c: when LUKS sits between DRBD and
		// storage, `cryptsetup luksClose` MUST run between the DRBD
		// detach and the storage DeleteVolume — exactly the order
		// `DeleteResource` follows for full-RD teardown above. Before
		// this hook the toggle path skipped LUKS cleanup entirely;
		// the dm-crypt mapper survived on the now-diskless host
		// holding `/dev/zvol/...` open, so `reclaimVolumesForDiskless`
		// next-line either error'd ("dataset is busy") or — in the
		// observed dev-stand case — silently no-op'd because the ZFS
		// provider's DeleteVolume swallowed the leftover, leaving the
		// zvol AND the LUKS mapper resident on the host. The
		// subsequent `r d` then skipped the storage layer entirely
		// (Spec.Flags already DISKLESS) and the zvol leaked
		// permanently (bug-hunt2 E.1c).
		//
		// Best-effort, identical to DeleteResource's cryptsetup loop:
		// a mapper that was never opened (fresh toggle on a Resource
		// that hadn't yet promoted past `Created`) returns non-zero
		// and we don't care; the missing-mapper path can't strand
		// the toggle.
		if r.cfg.Cryptsetup != nil && needsLUKS(dr.GetLayerStack()) {
			for _, vol := range dr.GetVolumes() {
				_ = r.cfg.Cryptsetup.Close(ctx, luksMapperName(dr.GetName(), vol.GetVolumeNumber()))
			}
		}

		err = r.reclaimVolumesForDiskless(ctx, dr)
		if err != nil {
			return nil, false, false, err
		}

		return map[int32]string{}, false, false, nil
	}

	return r.applyStorage(ctx, dr)
}

// detachIfStillAttached invokes `drbdadm detach --force <rd>` when
// the kernel slot is currently loaded with a non-Diskless local
// volume. This is the satellite's response to `linstor r td
// --diskless` on a previously-diskful replica (Bug 330).
//
// Probe order matters: IsLoaded → HasDisklessVolume. A slot that
// isn't loaded at all (never brought up, or torn down by an earlier
// DeleteResource) has nothing to detach; a slot already reporting
// disk:Diskless has converged to the target state and re-issuing
// detach would be a no-op shell-out but still costs a netlink
// round-trip on every reconcile pass.
//
// Probe errors fall through to a best-effort detach: a transient
// netlink hiccup shouldn't strand the toggle — the kernel will
// either accept the detach (state already matches) or surface a
// real error the caller wraps. The detach itself runs with --force
// so the kernel doesn't block on outstanding I/O references when
// the satellite has already declared the replica diskless at the
// REST layer.
//
// Why not gate inside the FSM transition table: the FSM currently
// models Spec→Phase but not the diskful→Diskless intra-Running flip
// as a distinct edge (the Bug 319 diskless→diskful flip is the
// only intra-Running transition wired today). Detach is wired here
// at the storage-layer entry point so it runs BEFORE the LV is
// reclaimed, which is the load-bearing ordering constraint. A
// future Phase will retire this in favour of a proper FSM
// ActionDetach + Phase transition.
func (r *Reconciler) detachIfStillAttached(ctx context.Context, dr *intent.DesiredResource) error {
	if r.cfg.Adm == nil {
		return nil
	}

	loaded, err := r.cfg.Adm.IsLoaded(ctx, dr.GetName())
	if err != nil || !loaded {
		// Slot absent / netlink hiccup → nothing to detach; the LV
		// reclaim path is safe to proceed without a detach.
		return nil //nolint:nilerr // probe failure is "no-op" by design
	}

	disklessVol, err := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
	if err == nil && disklessVol {
		// Kernel has already converged to Diskless (operator-driven
		// detach, prior reconcile pass, or a peer-driven event). No
		// further work — re-issuing detach is harmless but the
		// shell-out cost adds up on every reconcile of a steady-
		// state diskless replica.
		return nil
	}

	detachErr := r.cfg.Adm.Detach(ctx, dr.GetName())
	if detachErr != nil {
		return errors.Wrapf(detachErr, "detach %s on diskless toggle", dr.GetName())
	}

	return nil
}

// reclaimVolumesForDiskless iterates the DesiredResource's volumes
// and calls provider.DeleteVolume on each that carries a non-empty
// StoragePool (the dispatcher's marker for a toggle-to-diskless
// transition — see applyStorageIfDiskful's godoc). Idempotent:
// the provider's DeleteVolume is a no-op on already-missing
// volumes, so a re-reconcile after a partial first pass safely
// finishes the cleanup.
//
// An unknown pool is silently skipped — the dispatcher may stamp a
// historical pool the satellite no longer has registered (e.g.
// after a pool rename). The orphan-storage sweeper backstops with
// its own scan in that edge case.
func (r *Reconciler) reclaimVolumesForDiskless(ctx context.Context, dr *intent.DesiredResource) error {
	for _, vol := range dr.GetVolumes() {
		pool := vol.GetStoragePool()
		if pool == "" {
			continue
		}

		provider, ok := r.cfg.Providers[pool]
		if !ok {
			continue
		}

		err := provider.DeleteVolume(ctx, storage.Volume{
			ResourceName: dr.GetName(),
			VolumeNumber: vol.GetVolumeNumber(),
			PoolName:     pool,
		})
		if err != nil {
			return errors.Wrapf(err,
				"reclaim volume %s/%d on diskless toggle",
				dr.GetName(), vol.GetVolumeNumber())
		}
	}

	return nil
}

// the pool.
func (r *Reconciler) applyStorage(ctx context.Context, dr *intent.DesiredResource) (map[int32]string, bool, bool, error) {
	devices := map[int32]string{}
	resized := false
	cloned := false

	for _, vol := range dr.GetVolumes() {
		provider, ok := r.cfg.Providers[vol.GetStoragePool()]
		if !ok {
			return nil, false, false, errors.Errorf("unknown storage pool %q", vol.GetStoragePool())
		}

		// Clone path: when DesiredVolume.SourceSnapshot is set (the
		// snapshot-restore-resource handler stamps it on the target
		// RD's Props, the dispatcher pipes it through), materialise
		// the volume via Provider.RestoreVolumeFromSnapshot instead
		// of CreateVolume so the new replica starts populated with
		// the snapshot's data. Idempotent: provider's clone op skips
		// when the target volume already exists.
		err := r.materializeVolume(ctx, provider, dr.GetName(), vol)
		if err != nil {
			return nil, false, false, errors.Wrapf(err, "create/restore volume %s/%d", dr.GetName(), vol.GetVolumeNumber())
		}

		if vol.GetSourceSnapshot() != "" {
			cloned = true
		}

		status, err := provider.VolumeStatus(ctx, storage.Volume{
			ResourceName: dr.GetName(),
			VolumeNumber: vol.GetVolumeNumber(),
		})
		if err != nil {
			return nil, false, false, errors.Wrapf(err, "volume status %s/%d", dr.GetName(), vol.GetVolumeNumber())
		}

		// Grow path: the controller's VolumeDefinition update set a
		// new size that's larger than what the provider has on disk.
		// Call ResizeVolume to extend the LV/zvol/file; the LUKS
		// layer (when present) and `drbdadm resize` are layered on
		// top by their own reconcile steps.
		if vol.GetSizeKib() > status.UsableKib && status.UsableKib > 0 {
			err = provider.ResizeVolume(ctx, storage.Volume{
				ResourceName: dr.GetName(),
				VolumeNumber: vol.GetVolumeNumber(),
				SizeKib:      vol.GetSizeKib(),
			})
			if err != nil {
				return nil, false, false, errors.Wrapf(err, "resize volume %s/%d to %d KiB",
					dr.GetName(), vol.GetVolumeNumber(), vol.GetSizeKib())
			}

			resized = true
		}

		devices[vol.GetVolumeNumber()] = status.DevicePath
	}

	if len(dr.GetVolumes()) > 0 {
		r.rememberPool(dr.GetName(), dr.GetVolumes()[0].GetStoragePool())
	}

	return devices, resized, cloned, nil
}

// materializeVolume picks the right provider call: clone from a
// snapshot when SourceSnapshot is set on the desired volume,
// otherwise create blank. Parses `<srcRD>:<snapName>` for the
// clone form — matches what the snapshot-restore-resource REST
// handler stamps onto the target RD's Props.
//
// Cross-node path: when SourceSnapshot is set but the snapshot
// doesn't physically exist on THIS node (autoplace landed the new
// replica on a node outside snap.Nodes), the local clone returns
// storage.ErrNotFound. With a configured CrossNodeFetcher we then
// stream the snapshot from a peer satellite that hosts it locally
// (upstream LINSTOR's `zfs send | zfs recv` shape). Without one,
// fall back to a blank CreateVolume — DRBD network resync will
// populate the data, at the cost of a known cloned-metadata vs
// fresh-metadata GI mismatch on the wire (see Phase 11 notes).
func (r *Reconciler) materializeVolume(ctx context.Context, provider storage.Provider, rdName string, vol *intent.DesiredVolume) error {
	target := storage.Volume{
		ResourceName: rdName,
		VolumeNumber: vol.GetVolumeNumber(),
		SizeKib:      vol.GetSizeKib(),
	}

	src := vol.GetSourceSnapshot()
	if src == "" {
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
	}

	// Cross-cluster ship guard (scenario 4.17). Upstream LINSTOR's
	// `BackupShip` payload references a remote-cluster snapshot via
	// `<remote_name>:<srcRD>:<snap>` (three colon-separated parts).
	// Cozystack's satellite knows only the local CrossNodeFetcher
	// pipeline — there is no wire shape for fetching a snapshot
	// from a different cluster's controller. Reject the 3-part form
	// up-front with an actionable error so it surfaces on the
	// resource's Status.Conditions instead of being silently mis-
	// parsed as a malformed 2-part srcRD that happens to contain
	// a colon.
	if remotePrefix, rest, hasRemote := strings.Cut(src, ":"); hasRemote {
		if _, _, hasSnap := strings.Cut(rest, ":"); hasSnap && remotePrefix != "" {
			return errors.Errorf(
				"SourceSnapshot %q references a cross-cluster remote (%q); "+
					"cluster-to-cluster ship via LINSTOR remote is not "+
					"implemented; use snapshot-restore-resource for "+
					"in-cluster ship", src, remotePrefix)
		}
	}

	srcRD, snapName, ok := strings.Cut(src, ":")
	if !ok || srcRD == "" || snapName == "" {
		return errors.Errorf("SourceSnapshot %q must be <srcRD>:<snapName>", src)
	}

	err := provider.RestoreVolumeFromSnapshot(ctx, target, storage.Snapshot{
		ResourceName: srcRD,
		SnapshotName: snapName,
		PoolName:     vol.GetStoragePool(),
	})
	if !errors.Is(err, storage.ErrNotFound) {
		return err //nolint:wrapcheck // caller wraps
	}

	// Local snapshot missing. Try the cross-node fetcher; if that
	// also doesn't pan out we fall through to a blank CreateVolume
	// so DRBD has something to resync into.
	if r.cfg.CrossNodeFetcher == nil {
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
	}

	return r.crossNodeClone(ctx, provider, target, srcRD, snapName, vol.GetVolumeNumber())
}

// crossNodeClone is materializeVolume's cross-node fallback branch.
// Fetches the snapshot byte stream from a peer satellite and pipes
// it into the local provider's RecvSnapshot. The provider must
// implement storage.SnapshotShipper — backends that can't ship
// (legacy file driver pre-Phase-11) fall through to a blank create
// so DRBD network resync still has somewhere to drop bytes.
func (r *Reconciler) crossNodeClone(
	ctx context.Context,
	provider storage.Provider,
	target storage.Volume,
	srcRD, snapName string,
	volNum int32,
) error {
	shipper, ok := provider.(storage.SnapshotShipper)
	if !ok {
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
	}

	body, peer, err := r.cfg.CrossNodeFetcher.Fetch(ctx, srcRD, snapName, volNum)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// No peer has the snapshot — DRBD resync is the last
			// resort. Returns wrong-data on receive (split-brain
			// from metadata mismatch); upstream behaviour with
			// FILE_THIN matches this for now.
			return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
		}

		return errors.Wrapf(err, "cross-node fetch %s/%s", srcRD, snapName)
	}

	defer func() { _ = body.Close() }()

	err = shipper.RecvSnapshot(ctx, target, body)
	if err != nil {
		return errors.Wrapf(err, "recv %s/%s from %s", srcRD, snapName, peer)
	}

	return nil
}

// tearDownRemovedPeers runs `drbdadm del-peer` AND `drbdmeta
// forget-peer` for every peer that was in the previous .res but
// is no longer in the new desired set.
//
// `drbdadm adjust` only adds / reconfigures peers; the kernel's
// connection slot for a dropped peer would otherwise stay alive
// in StandAlone forever. del-peer needs the peer's `on <node>`
// block still in the .res to resolve its node-id, so run it
// BEFORE overwriting the file.
//
// forget-peer clears the peer's per-peer GI / bitmap slot from
// every diskful volume's on-disk metadata block. Without it,
// DRBD-9 v09 metadata keeps the departed peer's slot occupied
// for the lifetime of the resource — after enough node-replace
// cycles the resource exhausts the MaxPeers-1 slot budget
// `drbdadm create-md --max-peers=15` carved at first activation,
// and the next replica add fails with drbdmeta running out of
// room. Errors on individual forget-peer calls are logged and
// not bubbled up: leaving a stale slot is a slow leak (recoverable
// at any point in the future), while wedging the entire reconcile
// on it would block the convergent steady-state path the dispatcher
// drives. del-peer failures still bubble — those leak a live
// kernel connection, which is a faster correctness issue.
func (r *Reconciler) tearDownRemovedPeers(ctx context.Context, dr *intent.DesiredResource, resPath string, devices map[int32]string) error {
	removed := computeRemovedPeers(resPath, dr, r.cfg.NodeName)
	if len(removed) == 0 {
		return nil
	}

	// Peer-name → node-id from the OLD .res. The desired bag may
	// no longer carry the removed peer's `peer.<name>.node-id`
	// entry (dispatcher already pruned the spec), so the .res
	// file we're about to overwrite is the only stable source.
	peerIDs := extractResFilePeerNodeIDs(resPath)

	for _, peer := range removed {
		err := r.cfg.Adm.DelPeer(ctx, dr.GetName(), peer)
		if err != nil {
			return errors.Wrapf(err, "del-peer %s from %s", peer, dr.GetName())
		}

		// forget-peer is per-volume because v09 metadata lives in
		// the per-volume block. Skip volumes without a device path
		// (DISKLESS local replica — no metadata to clean) and
		// peers without a resolvable node-id (.res malformed /
		// races a brand-new resource being torn down before its
		// peer ever rendered).
		peerID, hasID := peerIDs[peer]
		if !hasID {
			continue
		}

		for volNum, device := range devices {
			if device == "" {
				continue
			}

			// forget-peer errors are non-fatal: a stale on-disk
			// slot leaks one of the MaxPeers-1 budget entries but
			// the resource keeps serving I/O. The next reconcile
			// retries; if the leak persists, the eventual
			// create-md exhaustion surfaces a louder error than
			// any log line here could. del-peer errors still
			// bubble (above) — those leak a live kernel
			// connection, a faster correctness issue.
			_ = r.cfg.Adm.ForgetPeer(ctx, dr.GetName(), volNum, device, peerID)
		}
	}

	return nil
}

// computeRemovedPeers diffs the previously-rendered .res file against
// the new desired peer set. Returns peer node names that were present
// before but are NOT in the new layout. Empty when the .res file
// doesn't exist (first apply) or when the read fails — we'd rather
// skip the del-peer pass than wedge the reconcile.
func computeRemovedPeers(resPath string, dr *intent.DesiredResource, localNode string) []string {
	body, err := os.ReadFile(resPath)
	if err != nil {
		return nil
	}

	old := extractResFilePeers(string(body))
	if len(old) == 0 {
		return nil
	}

	want := make(map[string]struct{}, len(dr.GetPeers())+1)
	want[localNode] = struct{}{}

	for _, p := range dr.GetPeerNames() {
		want[p] = struct{}{}
	}

	var removed []string

	for _, p := range old {
		if _, keep := want[p]; !keep {
			removed = append(removed, p)
		}
	}

	return removed
}

// extractResFilePeers parses an `on <node> {` block list out of a
// rendered .res file. We don't need a full DRBD parser — only the
// peer node-name set, which writeOnBlock emits as `  on <name> {`.
func extractResFilePeers(body string) []string {
	var peers []string

	for line := range strings.SplitSeq(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "on ") {
			continue
		}

		rest := strings.TrimPrefix(trimmed, "on ")

		head, _, ok := strings.Cut(rest, "{")
		if !ok {
			continue
		}

		name := strings.TrimSpace(head)
		if name != "" {
			peers = append(peers, name)
		}
	}

	return peers
}

// extractResFilePeerNodeIDs parses the rendered .res file at
// resPath and returns the peer-name → DRBD-node-id map encoded
// in each `on <node> { ... node-id <N>; ... }` block. Used by
// tearDownRemovedPeers to resolve the node-id for a peer that
// was just dropped from the desired set: `drbdadm del-peer`
// reads node-id from the (still-present) `on <peer>` block, but
// `drbdmeta forget-peer` needs the raw integer, and we'd rather
// pull it from the file we're about to overwrite than guess from
// the desired bag (which the dispatcher may have already pruned).
//
// Missing file / unreadable / malformed block → empty map; the
// caller skips forget-peer for that peer rather than emit a
// bogus --node-id=0 collision against the local slot. Reads via
// os.ReadFile so a transient I/O hiccup degrades to no-op
// instead of wedging the reconcile.
// hasLateAddedVolume reports whether the desired-state Volumes[]
// includes at least one volume number that is NOT yet represented
// as a `volume <N> {` block in the OLD .res file at resPath.
//
// Bug 332: the `linstor vd c <rd> 1G` flow grows VolumeDefinitions[]
// after the RD has already passed first-activation. The dispatcher
// hands the satellite a DesiredResource with the new volume in
// Volumes[], but the on-disk .res still describes the smaller set —
// so a strict greater-than on the rendered block count is the
// late-VD signal. Returns false when the .res file is absent
// (cold-start path; the existing firstActivation gate owns
// metadata creation), when the file is unreadable (fail-safe to
// "no late vol → no extra work"), or when the desired set matches
// what's already rendered.
//
// Parser is intentionally simple: matches "volume <N> {" inside an
// `on <node> {` block. False positives across multi-host blocks are
// harmless — the helper de-duplicates by recording each volNumber
// once across the file.
func hasLateAddedVolume(resPath string, dr *intent.DesiredResource) bool {
	if dr == nil {
		return false
	}

	body, err := os.ReadFile(resPath)
	if err != nil {
		// No .res yet → cold start, existing firstActivation path
		// will create metadata for every volume via the standard
		// chain. Late-VD signal is "old file exists with fewer
		// volumes", not "no file at all".
		return false
	}

	rendered := map[int32]struct{}{}

	for line := range strings.SplitSeq(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "volume ") {
			continue
		}

		rest := strings.TrimPrefix(trimmed, "volume ")
		head, _, ok := strings.Cut(rest, "{")

		if !ok {
			continue
		}

		num, parseErr := strconv.ParseInt(strings.TrimSpace(head), 10, 32)
		if parseErr != nil {
			continue
		}

		rendered[int32(num)] = struct{}{}
	}

	for _, vol := range dr.GetVolumes() {
		if _, ok := rendered[vol.GetVolumeNumber()]; !ok {
			return true
		}
	}

	return false
}

func extractResFilePeerNodeIDs(resPath string) map[string]int32 {
	body, err := os.ReadFile(resPath)
	if err != nil {
		return nil
	}

	out := map[string]int32{}

	var currentPeer string

	for line := range strings.SplitSeq(string(body), "\n") {
		trimmed := strings.TrimSpace(line)

		// Block opener: `on <name> {`. Stash the name; the
		// matching `node-id` line follows within the block.
		if after, ok := strings.CutPrefix(trimmed, "on "); ok {
			rest := after

			head, _, ok := strings.Cut(rest, "{")
			if !ok {
				continue
			}

			currentPeer = strings.TrimSpace(head)

			continue
		}

		// node-id line shape: `node-id <N>;` (writeOnBlock emits
		// it as the second line of every on-block). Match
		// `node-id ` prefix to dodge `<peer>.node-id` style
		// option lines that might appear at the resource top
		// level.
		if currentPeer != "" && strings.HasPrefix(trimmed, "node-id ") {
			rest := strings.TrimPrefix(trimmed, "node-id ")
			rest = strings.TrimSuffix(rest, ";")
			rest = strings.TrimSpace(rest)

			id, parseErr := strconv.ParseInt(rest, 10, 32)
			if parseErr == nil {
				out[currentPeer] = int32(id)
			}

			currentPeer = ""
		}
	}

	return out
}

// stampOwnershipMarker writes an empty `<StateDir>/<rsc>.owned` file
// to claim the kernel slot for blockstor before any drbdadm verb
// runs. The OrphanSweeper uses the marker's presence as a robust
// signal that blockstor provisioned (or attempted to provision) this
// slot — robust even after aggressive cleanup paths wipe the other
// state files (force-strip, satellite crash mid-write, e2e harness
// preflight `rm -f /etc/drbd.d/*.res /etc/drbd.d/*.md-created`).
//
// Bug 432 cascade: without this marker the sweeper would mis-classify
// a force-stripped slot as a foreign (piraeus) coexistence slot
// (Bug 299 guard) and leave the leaked slot in place forever. The
// pinned minor/port then prevented every subsequent RD from reaching
// UpToDate, surfacing as the satellite-utils-smoke + 9-test cascade
// observed in Run 52.
//
// The marker is content-empty (presence is the whole signal),
// removed in DeleteResource together with the rest of the per-
// resource state files. Idempotent: a pre-existing marker from a
// previous reconcile pass is silently re-stamped. Skipped on empty
// StateDir (unit-test fixtures that wire no on-disk state).
func (r *Reconciler) stampOwnershipMarker(name string) error {
	if r.cfg.StateDir == "" {
		return nil
	}

	ownedPath := filepath.Join(r.cfg.StateDir, name+".owned")

	// O_CREATE only — never truncate, never error if it already
	// exists. We don't care about content; presence is the signal.
	f, err := os.OpenFile(ownedPath, os.O_CREATE|os.O_WRONLY, resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "stamp ownership marker %s", ownedPath)
	}

	err = f.Close()
	if err != nil {
		return errors.Wrapf(err, "close ownership marker %s", ownedPath)
	}

	return nil
}

// renderResFile builds and writes the per-node .res file content-
// idempotently. Bug 315 invariant: skips os.WriteFile when the
// rendered body matches what's already on disk so drbdadm's config-
// file-watcher does not see a spurious mtime bump. Pure file op —
// no kernel interaction, no peer probes.
//
// Extracted from applyDRBD so the FSM dispatch path (Phase 11.2.c
// Stage 2) can call the same writer the legacy chain uses without
// forking the apply flow. devices is the volNumber → DevicePath map
// applyStorage produced; buildResFile uses it as the disk path so a
// loopfile-backed volume gets `disk /dev/loopN` rather than the
// LVM-shaped guess.
func (r *Reconciler) renderResFile(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string) error {
	autoDisk := r.autoDiskOptionsForResource(ctx, dr, devices)

	body, err := buildResFile(dr, r.cfg.NodeName, r.cfg.LocalAddress, devices, autoDisk)
	if err != nil {
		return errors.Wrapf(err, "build .res for %s", dr.GetName())
	}

	// Bug 432: stamp the ownership marker every time we render
	// `.res`. The marker survives aggressive cleanup paths that
	// wipe `*.res` so the OrphanSweeper can still classify the
	// kernel slot as blockstor-managed and tear it down. Stamped
	// before the content-idempotent body check so a re-stamp also
	// happens on steady-state reconciles where `.res` is unchanged
	// — closing the window between a previous-pass `.owned` wipe
	// (e.g. by harness or operator) and the next change to `.res`.
	err = r.stampOwnershipMarker(dr.GetName())
	if err != nil {
		return err
	}

	resPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".res")

	current, _ := os.ReadFile(resPath)
	if bytes.Equal(current, []byte(body)) {
		return nil
	}

	return errors.Wrapf(os.WriteFile(resPath, []byte(body), resFilePerm), "write %s", resPath)
}

// applyDRBD renders the .res file from dr's metadata and (re)applies
// it via drbdadm. create-md runs only on first activation (we detect
// "first" by absence of the .res file before this run); diskless
// replicas skip create-md entirely.
//
// devices is the volNumber → DevicePath map applyStorage produced.
// buildResFile uses it as the disk path so a loopfile-backed volume
// gets `disk /dev/loopN` rather than the LVM-shaped guess.
func (r *Reconciler) applyDRBD(ctx context.Context, dr *intent.DesiredResource, diskless bool, devices map[int32]string, resized, cloned bool) error {
	// Bug 79: when the RD has no VolumeDefinitions yet (operator created
	// the RD and Resources before adding any VD), there is no backing
	// volume to bring DRBD up on. Returning early here keeps the
	// .md-created marker absent so a later VD-add reconcile sees
	// firstActivation=true and runs create-md against the now-present
	// backing storage. Without this guard, the empty-volume pass would
	// write the marker (runFirstActivation always writes it, even when
	// CreateMD is a no-op on zero volumes), pin firstActivation=false
	// for the lifetime of the resource, and the late VD would come up
	// with no DRBD metadata — the kernel then reports disk:Diskless
	// while Spec.Flags lacks DISKLESS, surfacing as "Unintentional
	// Diskless" in `linstor r l`.
	if len(dr.GetVolumes()) == 0 {
		return nil
	}

	resPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".res")
	mdMarkerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".md-created")

	// Refuse bring-up while the controller has not yet stamped the
	// per-replica Spec identity the satellite depends on: the DRBD
	// node-id (Bug 360) and the skip-init-sync decision (this branch).
	// Both are controller-allocated, append-only Spec fields; seeding /
	// bringing up before they land burns irrecoverable on-disk state
	// (node-id) or deadlocks a fresh deploy with no elected UpToDate
	// winner (skip-init-sync). Folded into one gate so applyDRBD stays
	// under the gocyclo budget — see gateBringUpReadiness for the full
	// rationale of each sub-gate.
	err := r.gateBringUpReadiness(ctx, dr, diskless, mdMarkerPath)
	if err != nil {
		return err
	}

	// Phase 11.2.b shadow: compute the FSM phase + expected action
	// from the current Observation and log it for divergence triage.
	// READ-ONLY: the historical apply path below is unchanged, and
	// the FSM never drives a transition until Phase 11.2.c flips
	// the switch. Probe errors fall through as zero-valued fields
	// inside observeForFsm; no retries, no failures bubble up here.
	r.logFsmShadow(ctx, dr, diskless)

	// tearDownRemovedPeers MUST run before the FSM dispatch block:
	// it reads the OLD .res to resolve node-ids for peers that have
	// departed from the spec, and then issues del-peer / forget-peer
	// for each one. The FSM dispatch's renderResFile preamble (Phase
	// 11.2.c Stage 4 step 1) overwrites .res with the new peer set,
	// so this tear-down step must observe the pre-render state to
	// avoid leaking kernel connections and on-disk GI slots.
	//
	// Bug 432: the `.owned` ownership marker is stamped by
	// renderResFile (called as the FSM dispatch preamble below) on
	// every reconcile pass, so the sweeper can recognise this kernel
	// slot as blockstor-managed even after aggressive cleanup paths
	// (force-strip, harness preflight `rm -f *.res *.md-created`,
	// satellite crash mid-write) have wiped the other state files.
	err = r.tearDownRemovedPeers(ctx, dr, resPath, devices)
	if err != nil {
		return err
	}

	// Bug 332 (regression of Bug 79): MetadataCreated=True is a per-RD
	// hint but the actual drbdmeta create-md must run per-volume. When
	// `vd c` adds a new volume to an existing RD (operator-observed
	// repro on a 3-replica cluster after vol-0 reached UpToDate), the
	// per-RD Condition is True so the legacy firstActivation predicate
	// flips false — yet the new volume has no on-disk metadata, and
	// the subsequent `drbdadm adjust` would bring it up as kernel
	// disk:Diskless while Spec.Flags lacks DISKLESS (the verbatim
	// "Unintentional Diskless" surface from `linstor r l`). Mirror
	// upstream LINSTOR DrbdLayer.adjustResource: per-volume
	// hasMetaData probe, per-volume createMd for those that lack it.
	//
	// Scope: NARROW to the "late-VD added" signal — desired Volumes[]
	// count strictly exceeds the OLD .res's `volume N {` block count.
	// Without this narrowing, the steady-state path would shell out
	// `drbdadm dump-md` on every reconcile, perturbing existing tests
	// that pin "no metadata work on retry" (e.g. mid-Apply abort
	// scenarios) and adding shell cost on every converged pass.
	// Skipped on diskless replicas (no lower disk to stamp) and on
	// the diskless→diskful flip path (Bug 319 owns that re-stamp).
	if !diskless && hasLateAddedVolume(resPath, dr) &&
		!r.isDisklessToDiskfulFlip(ctx, dr, diskless) {
		err = r.ensurePerVolumeMetadata(ctx, dr, devices, diskless)
		if err != nil {
			return err
		}
	}

	// Phase 11.2.c Stage 3d: shadow-dispatch every FSM action. Each
	// helper is content-idempotent — the legacy chain below will
	// re-run the same logic later in this Apply pass and detect that
	// the state already matches. The fsmShadowAgreeCount metric tags
	// each FSM-dispatched action with `:fsm-dispatched` so production
	// dashboards can prove every gate is FSM-reachable end-to-end.
	// Stage 4 will retire the legacy chain once the metric shows
	// every transition has been FSM-dispatched in steady state for
	// a full burnin window.
	//
	// Phase 11.2.c Stage 4 step 1: the FSM dispatch path now owns
	// renderResFile (legacy unconditional call below has been
	// retired). dispatchFsmAction invokes renderResFile as a preamble
	// for every action that consumes .res (createMd, up, adjust,
	// adjustSkipDisk), and the ActionRenderRes arm continues to
	// handle the cold-start PhaseUnprovisioned case.
	// Bug 360 self-heal + FSM shadow-dispatch. Extracted into
	// healAndDispatchFsm so applyDRBD stays under the gocyclo budget;
	// the self-heal MUST run before the dispatch (it `down`s a slot
	// whose kernel my-id diverged from the allocated id so the
	// dispatch's renderResFile-preamble + up reloads the correct id).
	err = r.healAndDispatchFsm(ctx, dr, diskless, devices)
	if err != nil {
		return err
	}

	// firstActivation is "did create-md succeed previously?" —
	// Phase 11.3 Stage 1 derives this from the
	// `MetadataCreated=True` Status Condition on the parent
	// Resource CRD (carried into the apply chain via
	// dr.MetadataCreated). The on-disk `.md-created` marker is a
	// belt-and-braces fallback for the migration window: if the
	// Condition is absent but the marker file is present (cluster
	// upgraded from a pre-11.3 build, Condition not yet
	// backfilled), firstActivation still flips false so we don't
	// re-run create-md on a metadata block that already exists.
	//
	// We can't gate on the .res-file existence alone: a previous
	// reconcile that wrote the .res but failed `drbdadm create-md`
	// (e.g. .res had a stale conflicting node-id from a race that
	// later got fixed) would otherwise report firstActivation=false
	// on every subsequent attempt → create-md is skipped → adjust
	// reports "No valid meta data found" forever.
	_, statErr := os.Stat(mdMarkerPath)
	firstActivation := !dr.GetMetadataCreated() && os.IsNotExist(statErr)

	// Phase 11.2.c Stage 4 step 1: legacy r.renderResFile call retired.
	// Why: the FSM shadow-dispatch above now owns renderRes — it
	// observes Phase==Unprovisioned (cold start) and dispatches
	// r.renderResFile through ActionRenderRes, and for every later
	// phase (MetadataPending / MetadataReady / Running) the dispatch
	// runs renderResFile as a preamble inside dispatchFsmAction
	// before the phase-specific action. The helper's Bug-315 content-
	// idempotent write guarantees no churn on converged state.
	// Removing the duplicate call here drops the idempotent
	// stat+compare overhead one Apply pass spent twice. Other
	// transitions (createMd, up, adjust) still run their legacy path
	// below; those legacy gates retire one-by-one in step 2-4.

	// Bug 319 (root-cause fix for Bug 303): probe BEFORE any bring-up
	// verbs whether the local kernel slot is `disk:Diskless client:yes`
	// (intentional diskless) on a Spec that has flipped to diskful
	// (`linstor r td --migrate-from`, `linstor r td --diskful`). The
	// upstream LINSTOR pattern (`DrbdLayer.createMetaData` → `drbdadm
	// adjust`) initialises metadata BEFORE every adjust and lets adjust
	// cross the diskless→diskful boundary via drbd-utils' compare_volume
	// (kern->disk=="none" + conf->disk="<path>" schedules attach_cmd).
	//
	// Match that pattern here: when the kernel reports a Diskless
	// volume and Spec is now diskful, re-enter the create-md path on
	// the now-present lower disk REGARDLESS of the .md-created marker.
	// The previous Bug 303 workaround (explicit `drbdadm attach` AFTER
	// adjust) papered over the missing create-md re-entry; removing it
	// in favour of the upstream-aligned pipeline.
	diskfulFlip := r.isDisklessToDiskfulFlip(ctx, dr, diskless)

	// Auto-promote (primary --force + auto-mkfs) and GI-seed are
	// gated on firstActivation: a Spec flag flip from diskless to
	// diskful is NOT a fresh activation — peers are already UpToDate,
	// so a primary --force here would regenerate the local Current
	// UUID out from under the cluster, and a GI-seed would corrupt
	// the in-flight handshake. Suppress firstActivation on the flip
	// so `ensureMetadata` skips GI-seed and `finishDRBDApply` skips
	// the auto-promote chain.
	//
	// Bug 356: the "peers are already UpToDate" assumption breaks for
	// the solo-replica case — a single-replica RD with one DISKLESS
	// peer that gets toggled to diskful has zero peers in the
	// desired list, so there is no peer UUID to inherit and no
	// in-flight handshake to corrupt. Without the auto-promote, DRBD
	// sits in Inconsistent forever (no sync source). Re-enable
	// firstActivation when diskfulFlip happens against an empty peer
	// set so runAutoPromote runs `drbdadm primary --force` and the
	// lone diskful slot transitions Inconsistent → UpToDate. Mirrors
	// upstream LINSTOR's `DrbdLayer.adjustResource` force-primary
	// unconditional on the mkfs path (peers may not be reachable
	// yet; --force is safe under quorum=majority on a 1-node cluster).
	soloDiskfulFlip := diskfulFlip && len(dr.GetPeerNames()) == 0
	effectiveFirstActivation := firstActivation && (!diskfulFlip || soloDiskfulFlip)

	// Phase 11.2.c Stage 3a: fresh-replica first-activation routes
	// through the dedicated createMetadata helper so Stage 3b can
	// FSM-shadow-dispatch it (mirror of the renderResFile shadow
	// landed in Stage 2). The diskless→diskful flip case (Bug 319)
	// stays on the historical ensureMetadata(..., false) call —
	// re-stamping metadata WITHOUT the fresh-replica GI-seed, since
	// the kernel slot is already handshaken via the diskless path.
	// Behaviour identical to the previous single ensureMetadata
	// branch parameterised by effectiveFirstActivation.
	err = r.maybeStampMetadata(ctx, dr, devices, mdMarkerPath, diskless, firstActivation, diskfulFlip)
	if err != nil {
		return err
	}

	err = r.runApplyDRBDVerb(ctx, dr, effectiveFirstActivation, diskfulFlip)
	if err != nil {
		return err
	}

	return r.finishDRBDApply(ctx, dr, diskless, effectiveFirstActivation, resized, cloned)
}

// healAndDispatchFsm runs the Bug 360 my-node-id self-heal and then
// the FSM shadow-dispatch for one Apply pass. Order matters: the
// self-heal `down`s a kernel slot whose burned-in my-id diverged from
// the controller-allocated id, and the dispatch's renderResFile
// preamble + `up` then reloads the slot with the correct id. Pulled
// out of applyDRBD so the orchestrator stays under the gocyclo budget.
func (r *Reconciler) healAndDispatchFsm(ctx context.Context, dr *intent.DesiredResource, diskless bool, devices map[int32]string) error {
	err := r.reconcileKernelMyNodeID(ctx, dr)
	if err != nil {
		return err
	}

	obs := r.observeForFsm(ctx, dr, diskless)

	phase := ObservePhase(obs)

	next := NextTransition(phase, obs)
	if next == nil {
		return nil
	}

	err = r.dispatchFsmAction(ctx, dr, devices, next.Action, obs)
	if err != nil {
		return errors.Wrapf(err, "fsm dispatch %s", next.Action)
	}

	fsmShadowAgreeCount.Add(next.Action+":fsm-dispatched", 1)

	return nil
}

// reconcileKernelMyNodeID is the Bug 360 self-heal: if the kernel
// already owns a slot for this resource whose my-node-id differs from
// the desired local node-id (the controller-allocated
// Status.DRBDNodeID rendered into dr's DrbdOptions["node-id"]), tear
// the slot down with `drbdsetup down` so the bring-up path below
// re-`up`s it with the correct my-id.
//
// Why a full down+up: DRBD burns the local node-id into kernel state
// at `new-resource`/`drbdadm up` time and provides NO way to rewrite
// a loaded resource's OWN my-id - `drbdadm adjust` only reconciles
// peers and disks, never the local id. A slot stuck at the wrong
// my-id (typically 0, leaked from a pre-allocation first-activation
// render) issues `new-peer <rd> 0 --_name=<peer-with-id-0>` on every
// adjust and dies with `peer node id cannot be my own node id`
// (exit 10) indefinitely. Down+up is the only recovery.
//
// Bounded + idempotent:
//   - desired id unparseable -> skip (defer to the allocation gate;
//     never act on a guess).
//   - kernel slot absent (KernelMyNodeID -> ok=false) -> skip; first
//     `up` will load the correct id directly.
//   - kernel my-id == desired -> no-op (steady state, every reconcile).
//   - mismatch -> single `drbdsetup down`; the FSM dispatch that runs
//     immediately after re-renders .res and `up`s with the right id.
//     A `down` failure is fatal to this Apply pass (return err) so the
//     next reconcile retries rather than proceeding to a doomed adjust.
func (r *Reconciler) reconcileKernelMyNodeID(ctx context.Context, dr *intent.DesiredResource) error {
	desiredID, err := strconv.Atoi(dr.GetDrbdOptions()["node-id"])
	if err != nil {
		// Why: no resolvable desired id (pre-allocation / malformed
		// DesiredResource). The controller-side allocation gate owns
		// blocking that case; here we just decline to act on a guess.
		return nil //nolint:nilerr // unresolved desired id ⇒ decline to act; allocation gate owns this case
	}

	kernelID, ok := r.cfg.Adm.KernelMyNodeID(ctx, dr.GetName())
	if !ok {
		// Kernel has no slot yet (or status unparseable) - the bring-up
		// path will `up` with the correct id directly; nothing to heal.
		return nil
	}

	if int(kernelID) == desiredID {
		return nil
	}

	log.FromContext(ctx).Info("Bug 360 self-heal: kernel my-node-id mismatch, recreating slot",
		"resource", dr.GetName(),
		"kernelMyNodeID", kernelID,
		"desiredNodeID", desiredID)

	err = r.cfg.Adm.Down(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "Bug 360 self-heal: drbdadm down %s (kernel my-id %d != desired %d)",
			dr.GetName(), kernelID, desiredID)
	}

	return nil
}

// finishDRBDApply runs the post-adjust steps: pickup-time resize and
// the first-activation auto-primary seed. Extracted from applyDRBD so
// the orchestrator stays under the project's gocyclo budget.
//
// Bug 319: an earlier revision called `drbdadm attach` here for the
// diskless→diskful flip (Bug 303 workaround). That step is gone —
// `ensureMetadata` now runs create-md on the new lower disk BEFORE
// adjust, and drbd-utils' compare_volume schedules attach_cmd
// automatically when kern->disk=="none" but conf->disk points at a
// real path. Matches upstream LINSTOR's DrbdLayer pipeline.
func (r *Reconciler) finishDRBDApply(ctx context.Context, dr *intent.DesiredResource, diskless, firstActivation, resized, cloned bool) error {
	// Pickup-time resize: the storage layer was just grown, drbdadm
	// resize tells the kernel to extend the replicated device to
	// match. Adjust on its own won't do this — only resize re-reads
	// the lower disk's size. Diskless replicas don't have a lower
	// disk to resize but they still need their internal state to
	// catch up; drbdadm resize handles that case too.
	if resized {
		// Bug 395 (P1, data integrity): gate `--assume-clean` on whether
		// the backing provider zero-fills the grown region. For thick
		// `LVM` (non-zero-fill) we MUST omit `--assume-clean` so DRBD
		// marks the grown region out-of-sync and resyncs it from the
		// UpToDate source — otherwise replicas silently disagree on
		// [old_size, new_size) (recycled VG extents differ per node).
		// Diskless replicas have no backing disk and no provider, so the
		// helper returns true (the notify-only resize there has no data
		// region to mark; the diskful peers drive the resync).
		assumeClean := r.resizeAssumeClean(dr)

		err := r.cfg.Adm.Resize(ctx, dr.GetName(), assumeClean)
		if err != nil {
			return errors.Wrapf(err, "resize %s", dr.GetName())
		}
	}

	// Force-primary trigger: only when the RD-prop `auto-primary` is
	// set (controller-initiated seed for fresh replicas).
	//
	// Do NOT auto-promote on clone. Local clone (zfs clone / lvcreate
	// -s / cp --reflink) copies the source's DRBD metadata byte-for-
	// byte, so every clone replica starts with the same Current UUID.
	// Running `drbdadm primary --force` on each replica regenerates
	// the Current UUID independently per node → peers see divergent
	// UUIDs on first handshake → split-brain (StandAlone).
	autoPrimaryReplica := !diskless &&
		dr.GetDrbdOptions()["auto-primary"] == drbdBoolPropTrue
	autoPromote := firstActivation && autoPrimaryReplica
	_ = cloned

	if autoPromote && !r.shouldForcePromote(ctx, dr) {
		// Bug 342 force-promote gate fired: a data-bearing peer exists,
		// so SKIP `drbdadm primary --force`. The fresh replica stays
		// Inconsistent and SyncTargets from the peer (full resync,
		// data-safe). Returning here also skips the mkfs-retry below —
		// correct, since the replica adopts the peer's filesystem via
		// the resync rather than formatting locally.
		return nil
	}

	// Reaching UpToDate no longer depends on this promote. The elected
	// winner already declared itself Consistent+UpToDate via the
	// `drbdmeta set-gi` seed (seedInitialGI's case-B winner slot), so
	// the kernel comes up UpToDate from metadata alone. `primary
	// --force` is now PURELY an mkfs concern: we promote only when the
	// RD requests a filesystem (and quorum may not be satisfiable yet
	// because peers haven't connected), run mkfs, then demote
	// immediately. When no filesystem is requested we skip the promote
	// entirely — which is exactly what avoids the old bug, where
	// `primary --force` minted a fresh current-UUID that diverged from
	// the day0 bitmap-base the peers carry, flipping them to SyncTarget
	// for a full initial sync even on empty thin/ZFS volumes.
	if autoPromote && needsMkfs(dr) {
		err := r.runAutoPromote(ctx, dr)
		if err != nil {
			return err
		}
	}

	// Bug 311: the auto-mkfs path used to live ONLY inside
	// runAutoPromote (above), wedged between `drbdadm primary --force`
	// and `drbdadm secondary`. That coupling meant any transient
	// failure in the promote/demote dance — primary --force racing the
	// initial-sync handshake, secondary racing an in-flight Open —
	// left `.mkfs.done` unwritten while `.md-created` persisted, so the
	// next reconcile saw firstActivation=false, skipped the whole
	// auto-promote branch, and mkfs never ran again. piraeus' NFS-
	// ganesha multi-volume RD (RWX PVC, two VDs, `FileSystem/Type=ext4`
	// on the RD) reproduced this every time: the resource bound but
	// `/dev/drbd/by-res/<pvc>/1` had no filesystem and ganesha's
	// `mount-recovery@<pvc>.service` failed with `fsck.ext2: Bad magic
	// number in super-block`.
	//
	// The retry path runs ONLY when firstActivation has already
	// happened (so we never double-promote a healthy fresh replica)
	// AND the `.mkfs.done` marker is still missing AND the RD asks
	// for a filesystem. It re-enters runAutoPromote which is
	// idempotent: primary --force on an already-Primary slot is a
	// kernel no-op, `runAutoMkfs` blkid-probes each device and skips
	// volumes that already carry a filesystem, and `secondary`
	// matches the regular post-mkfs demote. Once every diskful
	// volume passes the blkid probe (either freshly-mkfs'd here or
	// already populated from a previous attempt), runAutoMkfs writes
	// the marker and this branch becomes a no-op for the rest of the
	// resource's life.
	if !autoPromote && autoPrimaryReplica && r.needsAutoMkfsRetry(dr) {
		err := r.runAutoPromote(ctx, dr)
		if err != nil {
			return err
		}
	}

	// NOTE: observeExistingFilesystem helper is intentionally NOT
	// called here. The original intent (close the ~28 Hz hot loop on
	// RDs whose filesystem the satellite never formatted itself —
	// NFS-Ganesha RWX PVCs, etc.) regressed DRBD bring-up convergence
	// across multiple recovery scenarios when wired into
	// finishDRBDApply: probing the device via blkid (and even
	// gating-probes via `drbdsetup status`) on every reconcile during
	// the bring-up window stalled the apply path long enough for
	// upstream e2e timeouts to fire. The helper + StampFilesystemObserved
	// stamper remain in the package as the API surface for a follow-up
	// that wires the stamp from the observer event path (after the
	// kernel reports an established+UpToDate frame), which does not
	// race the per-RD apply lane.

	// Bug 366 recovery-promote self-heal — re-arm the auto-primary seed
	// when a fresh RD wedged with the late replica stuck Inconsistent and
	// no Primary anywhere. See maybeRecoveryPromote for the full why.
	err := r.maybeRecoveryPromote(ctx, dr, autoPromote, autoPrimaryReplica)
	if err != nil {
		return err
	}

	return nil
}

// resizeAssumeClean decides whether the pickup-time `drbdadm resize`
// may pass `--assume-clean` (skip resync of the grown region) for this
// resource (Bug 395, P1 data integrity).
//
// `--assume-clean` is sound ONLY when every diskful volume's backing
// provider zero-fills the grown region [old_size, new_size) — i.e. the
// grown bytes are deterministically zero on every replica. Classic
// thick `LVM` does NOT (recycled VG extents differ per node), so for it
// we return false and let DRBD mark the grown region out-of-sync and
// resync it from the UpToDate source.
//
// A provider that does not implement storage.ResizeZeroFiller is
// treated as non-zero-fill (the safe default → return false). A volume
// whose pool is unknown to this satellite (e.g. a diskless replica with
// no StoragePool, or a historical pool after a rename) is skipped: it
// has no local backing disk to mark dirty, and the diskful peers'
// reconcilers drive the cluster-wide resync decision. With no diskful
// volume on this node the helper returns true (the notify-only resize
// has nothing local to resync).
func (r *Reconciler) resizeAssumeClean(dr *intent.DesiredResource) bool {
	for _, vol := range dr.GetVolumes() {
		pool := vol.GetStoragePool()
		if pool == "" {
			continue
		}

		provider, ok := r.cfg.Providers[pool]
		if !ok {
			continue
		}

		zf, ok := provider.(storage.ResizeZeroFiller)
		if !ok || !zf.ResizeZeroFills() {
			// Non-zero-fill (or capability not declared): the grown
			// region must be resynced — omit `--assume-clean`.
			return false
		}
	}

	return true
}

// maybeRecoveryPromote re-arms the auto-primary seed on a steady-state
// reconcile to unstick a fresh RD whose initial sync wedged (Bug 366).
//
// Why: a brand-new 3-diskful RD reaches all-UpToDate via two mechanisms
// BOTH gated on "a data-bearing diskful peer exists" (anyDiskfulPeerHasData
// / PeerHasData): the day0 skip-initial-sync GI seed and the lowest-node-id
// seed-primary election. When the three first-activation reconciles stagger,
// the replicas that seed first flip UpToDate; that flips the gate true for
// the LATE replica, which then (a) declines its own day0 seed → comes up
// Inconsistent, and (b) vetoes the seed-primary election → NO replica is
// ever promoted Primary. With no Primary the late replica's resync stalls
// (dual SyncSource collide into resync-suspended:peer, done:0.00) and it
// sits Inconsistent forever (~2 in 7 cold creates pre-fix).
//
// The one-shot first-activation autoPromote can never recover this —
// firstActivation has latched false by the time the wedge is observable. So
// this re-arms the SAME auto-primary action, modelled on the
// needsAutoMkfsRetry re-entry (same runAutoPromote promote→mkfs→demote
// lifecycle). NeedsRecoveryPromote reads live drbdsetup status (not latched
// flags) and fires ONLY when: the local replica is diskful+UpToDate, at
// least one diskful peer is Inconsistent, NO replica anywhere is Primary,
// and this node holds the lowest my-node-id among the UpToDate diskful
// replicas — so EXACTLY ONE deterministic node promotes (no split-brain).
// runAutoPromote then makes this node the authoritative SyncSource, driving
// the stalled Inconsistent peer to UpToDate. Data-safe: every replica shares
// the synthetic day0 Current-UUID, so primary --force mints no unrelated
// UUID. Bounded / self-limiting: once the peer reaches UpToDate the predicate
// no longer holds, so it cannot re-fire. We deliberately do NOT route through
// shouldForcePromote — its AnyConnectedPeerHasData veto is exactly what
// wedged us (a peer IS UpToDate by now); NeedsRecoveryPromote is the correct,
// narrower gate for this state.
func (r *Reconciler) maybeRecoveryPromote(ctx context.Context, dr *intent.DesiredResource, autoPromote, autoPrimaryReplica bool) error {
	if autoPromote || !autoPrimaryReplica || !r.cfg.Adm.NeedsRecoveryPromote(ctx, dr.GetName()) {
		return nil
	}

	// Throttle: runAutoPromote churns kernel state (promote → mkfs →
	// demote) which re-triggers this reconcile while the predicate
	// still holds mid-resync. Firing on every such reconcile hot-loops
	// at several Hz and starves the SyncTarget. Skip if we already
	// promoted this resource within recoveryPromoteThrottle — the
	// kernel resync the previous promote kicked off needs time to make
	// progress, and a still-genuine wedge gets a fresh nudge once the
	// window elapses.
	if !r.recoveryPromoteDue(dr.GetName()) {
		return nil
	}

	log.FromContext(ctx).Info("Bug 366 recovery-promote: re-arming auto-primary to unstick wedged initial sync",
		"resource", dr.GetName())

	return r.runAutoPromote(ctx, dr)
}

// recoveryPromoteDue reports whether enough time has elapsed since this
// node last fired a recovery-promote for `name` to fire another, and
// records the fire time when it returns true. Serialised by r.mu so
// concurrent reconciles of the same resource can't both pass the gate.
func (r *Reconciler) recoveryPromoteDue(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if last, ok := r.lastRecoveryPromoteAt[name]; ok && now.Sub(last) < recoveryPromoteThrottle {
		return false
	}

	r.lastRecoveryPromoteAt[name] = now

	return true
}

// needsAutoMkfsRetry probes whether an auto-primary replica must
// re-enter the promote-mkfs-demote chain on a steady-state reconcile.
// Returns true only when (a) the RD asks for a filesystem
// (`FileSystem/Type` prop set), (b) the `.mkfs.done` marker is
// absent, and (c) the satellite has both an Exec wrapper and a
// StateDir wired (production always does; tests that omit them
// disable auto-mkfs entirely, matching the runAutoMkfs no-Exec branch).
//
// The marker file is the same one `runAutoMkfs` drops after every
// volume reaches a filesystem (either by mkfs or by adopting an
// existing one via blkid). Reading the marker here is a cheap
// fs.Stat — cheaper than re-running blkid on every volume just to
// decide whether we need to do anything at all.
func (r *Reconciler) needsAutoMkfsRetry(dr *intent.DesiredResource) bool {
	if strings.TrimSpace(dr.GetProps()["FileSystem/Type"]) == "" {
		return false
	}

	if r.cfg.Exec == nil || r.cfg.StateDir == "" {
		return false
	}

	// Phase 11.3 Stage 2: Condition first. When the dispatcher
	// observed `FilesystemFormatted=True` on the Resource CRD, the
	// auto-mkfs path has already finished for every diskful volume
	// — no retry needed even if the file marker happened to be
	// removed (host rebuild, operator `rm`). The per-volume blkid
	// probe inside runAutoMkfs stays as the double-mkfs safety net,
	// so a stale Condition cannot cause data loss; this read is a
	// hot-path stat-skip.
	if dr.GetFilesystemFormatted() {
		return false
	}

	// Belt-and-braces file fallback: pre-11.3-Stage-2 clusters
	// have populated `.mkfs.done` markers but no Condition stamped
	// until the next reconcile.
	markerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".mkfs.done")
	_, err := os.Stat(markerPath)

	return os.IsNotExist(err)
}

// isDisklessToDiskfulFlip probes whether the local kernel slot is
// currently `disk:Diskless client:yes` (intentional diskless) on a
// Resource whose Spec has flipped to diskful (`linstor r td
// --migrate-from`, `linstor r td --diskful`).
//
// Bug 319: this is the trigger for re-entering the create-md path
// on a flag flip even when the satellite's .md-created marker is
// already present (the previous diskless apply may not have written
// it, but it may also have been written by a prior diskful incarnation
// of the same name — we must re-stamp metadata on the newly-carved
// lower disk either way). Upstream LINSTOR's DrbdLayer always runs
// createMetaData before adjust on every reconcile pass; drb-utils'
// compare_volume then schedules attach_cmd via the
// kern->disk=="none" + conf->disk="<path>" diff. Matching that flow
// is what makes the explicit Bug 303 `drbdadm attach` unnecessary.
//
// Probe BEFORE any bring-up verbs run because adjust / CreateMD /
// etc may shift kernel state mid-flight and we'd lose the signal.
// Errors fall through to false: a netlink hiccup shouldn't strand
// the apply chain, and the next reconcile pass (driven by Status
// updates / events2) will retry the probe.
//
// Returns false when:
//   - the spec is still diskless (no boundary crossing),
//   - the kernel slot isn't loaded (the bring-up path will Up the
//     resource with the new .res, which DOES attach the disk
//     because new-resource sees a disk path),
//   - the kernel slot is loaded with no Diskless volume (already
//     diskful — re-running create-md would be a HasMD-gated no-op
//     but we skip the probe to avoid the shell-out cost).
func (r *Reconciler) isDisklessToDiskfulFlip(ctx context.Context, dr *intent.DesiredResource, diskless bool) bool {
	if diskless {
		return false
	}

	loaded, err := r.cfg.Adm.IsLoaded(ctx, dr.GetName())
	if err != nil || !loaded {
		return false
	}

	disklessVol, err := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
	if err != nil {
		return false
	}

	return disklessVol
}

// ensureMetadata is the upstream-aligned create-md entry point. It
// runs in three cases:
//
//  1. firstActivation: the resource has never had `.md-created`
//     stamped (fresh diskful replica). Behaves exactly like the
//     historical runFirstActivation — HasMD-gated CreateMD, marker
//     write, GI-seed.
//  2. diskless→diskful Spec flag flip with pre-existing metadata
//     (Bug 319): the resource was previously diskful on this node,
//     went diskless, and is now flipping back. The lower disk
//     still carries a valid DRBD-9 superblock from the prior
//     diskful incarnation, and the kernel slot is already
//     handshaken with peers via the diskless path. Re-enter
//     create-md as a no-op (HasMD=true), write the marker, SKIP
//     the GI-seed — re-stamping the GI would corrupt the
//     in-flight session.
//  3. tieB→diskful promotion with fresh metadata (Bug 347): the
//     resource was a tiebreaker on this node — no backing storage,
//     no DRBD-9 superblock anywhere. `linstor r c <tieB-node>
//     <rd>` drops the Diskless flag, applyStorage carves a fresh
//     zvol/LV, HasMD returns false → CreateMD writes zero-GI
//     metadata. Without the GI-seed the peer handshake sees a GI
//     mismatch and triggers a full resync. Case 2's
//     "slot-already-handshaken" reasoning does NOT apply here: the
//     tiebreaker had no superblock to inherit GI from. Gate the
//     seed on `metadataFreshlyCreated := !hasMD` (captured
//     pre-CreateMD) so the seed runs whenever a new superblock
//     just landed — fresh-first-replica AND tieB-promotion alike
//     — and skips only when the kernel already had a valid GI to
//     inherit.
//
// Idempotent on both axes: HasMD short-circuits CreateMD when the
// metadata block already exists (e.g. satellite restart between
// CreateMD and marker write), and the marker write is a one-shot
// OS truncate that doesn't churn on repeat. firstActivation is
// retained as a parameter for caller-side branching and dispatch
// gating but no longer drives the GI-seed decision — the
// pre-CreateMD HasMD probe is the more accurate signal.
func (r *Reconciler) ensureMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, mdMarkerPath string, firstActivation bool) error {
	// Bug B.4 (P0): probe + create-md per-volume rather than per-
	// resource. The legacy single-shot `drbdadm dump-md <rd>` /
	// `drbdadm create-md <rd>` walks every volume in .res and bails
	// on the FIRST one that doesn't match the requested operation —
	// for `dump-md` that means "no metadata" if ANY volume lacks it,
	// and for `create-md` it means EBUSY against vol-0 if vol-0 is
	// already attached. Mirrors upstream LINSTOR's
	// DrbdLayer.adjustResource per-volume hasMetaData + createMd
	// pattern (satellite/.../DrbdLayer.java).
	//
	// Triggered by `linstor vd c <rd> N` adding vol-1 to a 2-replica
	// RD where vol-0 has already reached UpToDate: the new kernel
	// slot brings vol-1 up as `disk:Diskless` (no metadata yet),
	// which trips `isDisklessToDiskfulFlip` (HasDisklessVolume=true)
	// and routes through here with firstActivation=false. The
	// per-resource HasMD probe then sees "vol-1 has no metadata" →
	// returns false → CreateMD against the per-resource target →
	// fails EBUSY on vol-0's attached minor → reconciler hot-loops
	// at ~10 Hz and the WHOLE resource enters `suspended:quorum`
	// because vol-1 cannot achieve quorum, blocking vol-0 I/O.
	//
	// Per-volume scoping is also the SAFETY invariant for the
	// historical (single-volume) path: an RD-scoped `create-md
	// --force` walks every volume and would wipe a sibling volume's
	// existing GI + bitmap state. Targeting `<rd>/<volNumber>` keeps
	// drbdmeta away from already-stamped lower disks.
	metadataFreshlyCreated := false

	for _, vol := range dr.GetVolumes() {
		target := fmt.Sprintf("%s/%d", dr.GetName(), vol.GetVolumeNumber())

		hasMD, probeErr := r.cfg.Adm.HasMD(ctx, target)
		if probeErr != nil {
			return errors.Wrapf(probeErr, "dump-md %s", target)
		}

		if hasMD {
			continue
		}

		// Why (Bug 347): capture "at least one volume's metadata is
		// about to be freshly created" so the downstream GI-seed
		// gate fires. Mirrors the pre-CreateMD HasMD signal the
		// previous per-resource probe captured. `firstActivation`
		// alone is too narrow — it's false on tieB→diskful even
		// though the tiebreaker has no DRBD-9 superblock to inherit
		// GI from, so the seed must still run to dodge a full
		// resync.
		metadataFreshlyCreated = true

		createErr := r.createMDWithCollisionRecovery(ctx, dr.GetName(), target)
		if createErr != nil {
			return errors.Wrapf(createErr, "create-md %s", target)
		}
	}

	err := os.WriteFile(mdMarkerPath, nil, resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "write %s", mdMarkerPath)
	}

	// Phase 11.3 Stage 1: stamp the `MetadataCreated=True` Status
	// Condition on the parent Resource CRD. Belt-and-braces with
	// the file marker write above: future reconciles read the
	// Condition first to derive `firstActivation`, falling back to
	// the file presence only when the Condition is absent (cluster
	// upgrade window before the satellite's startup backfill
	// pass). The stamp failure does NOT fail the apply — the file
	// marker is the transitional source of truth, so a transient
	// apiserver hiccup here just defers Condition stamping to the
	// next reconcile.
	if r.cfg.MetadataCreatedStamper != nil {
		// Why (Bug 344): the stamper SSA-patches a `Resource`
		// object whose Name is the CRD object name. Real Resource
		// CRDs are named `<rd>.<node>` (per-node sharding); passing
		// the RD-only name made the apiserver return 404 on every
		// stamp attempt, polluting ERROR logs since Phase 11.3
		// Stage 1 (#489). Best-effort tolerated (file marker is the
		// source of truth) so no functional regression, just noise.
		resourceCRDName := dr.GetName() + "." + dr.GetNodeName()

		stampErr := r.cfg.MetadataCreatedStamper.StampMetadataCreated(ctx, resourceCRDName)
		if stampErr != nil {
			log.FromContext(ctx).Error(stampErr, "stamp MetadataCreated Condition; will retry next reconcile",
				"resource", resourceCRDName)
		}
	}

	// GI-seed pre-stamps the per-peer bitmap slots with a peer's
	// UpToDate GI so the initial-sync handshake skips a full resync.
	// Gate combines `firstActivation` with `metadataFreshlyCreated`
	// (i.e. `!hasMD` captured pre-CreateMD):
	//
	//   - firstActivation=true → ALWAYS seed. Either fresh replica
	//     (CreateMD just ran, zero-GI superblock) OR adopted metadata
	//     (W09 disk-replace recipe — Bug 319 invariant: stamp the
	//     day0 GI tuple even when metadata adoption skipped CreateMD,
	//     because the per-peer bitmap slots still need to declare
	//     "in-sync from day0").
	//   - firstActivation=false + metadataFreshlyCreated=true →
	//     tieB→diskful promotion (Bug 347). The resource already has
	//     `.md-created` from its tiebreaker incarnation (so
	//     firstActivation is false), but the freshly-carved zvol/LV
	//     gets a zero-GI superblock from CreateMD. MUST seed,
	//     otherwise DRBD sees a GI mismatch with peers and triggers a
	//     full resync.
	//   - firstActivation=false + metadataFreshlyCreated=false →
	//     Bug 319 diskless→diskful flip with pre-existing superblock.
	//     Kernel slot already handshaken with peers via the diskless
	//     path; re-seeding would corrupt the in-flight session.
	//
	// Why (Bug 347): the previous gate (`!firstActivation`) only ran
	// seedInitialGI on firstActivation=true, which produced a full
	// resync on every `linstor r c <tieB-node> <rd>` because tieB→
	// diskful arrives with firstActivation=false + HasMD=false. The
	// HasMD probe captures the "fresh superblock" signal that
	// `firstActivation` alone cannot distinguish.
	if !firstActivation && !metadataFreshlyCreated {
		return nil
	}

	err = r.seedInitialGI(ctx, dr, devices, isInitialUpToDateWinner(dr, firstActivation))
	if err != nil {
		return errors.Wrapf(err, "seed initial-sync GI %s", dr.GetName())
	}

	return nil
}

// isInitialUpToDateWinner reports whether THIS node is the elected
// initial-UpToDate source for a not-yet-initialized resource — the
// "winner" of the day0 seed race. When true, seedInitialGI writes the
// case-B winner seed (random current + day0 bitmap-base + Consistent +
// UpToDate on the local slot) so the source reaches UpToDate purely
// from metadata, replacing the old `drbdadm primary --force` that
// minted a divergent current-UUID and forced a full initial sync.
//
// The three gates together are the "elected winner AND volume not yet
// initialized" condition from the spec:
//
//   - firstActivation: this is the resource's first create-md on this
//     node (metadata never created before). Once the winner is
//     UpToDate, every subsequent reconcile sees firstActivation=false,
//     closing the winner path forever — a late-added / relocated
//     replica can NEVER take case B (it falls to case A day0 or case C
//     Inconsistent), which is what keeps relocate split-brain-free.
//   - auto-primary: the dispatcher's lowest-diskful-node-id election
//     (BuildDesired) stamps this flag on exactly ONE replica per fresh
//     RD — the elected source. It is the "winner node" marker.
//   - !PeerHasData: the dispatcher already withholds auto-primary when
//     any diskful peer holds data, but we re-check here as a belt-and-
//     braces gate so a relocate target never seeds itself UpToDate.
//
// A diskless replica is never a winner (no lower disk / metadata to
// seed); the auto-primary flag is only ever stamped on diskful
// replicas, so the flag check already excludes diskless.
func isInitialUpToDateWinner(dr *intent.DesiredResource, firstActivation bool) bool {
	return firstActivation &&
		dr.GetDrbdOptions()["auto-primary"] == drbdBoolPropTrue &&
		!dr.GetPeerHasData()
}

// ensurePerVolumeMetadata stamps DRBD-9 metadata on every diskful
// volume of `dr` that lacks it. Mirrors upstream LINSTOR's
// DrbdLayer.adjustResource (satellite/.../DrbdLayer.java L702-723):
// hasMetaData per-volume, createMd per-volume for the ones missing.
//
// Bug 332 (regression of Bug 79): the per-RD `MetadataCreated=True`
// Status Condition (Phase 11.3 Stage 1) caches "this RD has had
// create-md before". But the actual `drbdmeta create-md` is
// per-volume — when `linstor vd c <rd> 1G` adds a new volume to an
// existing RD, the Condition is True yet the new volume's lower
// disk carries no metadata. Without this helper the subsequent
// `drbdadm adjust` brings the new volume up as kernel disk:Diskless
// while Spec.Flags lacks DISKLESS (the verbatim "Unintentional
// Diskless" surface from `linstor r l`).
//
// Renders .res before probing so drbdadm dump-md / create-md can
// resolve the new volume's lower disk path. renderResFile is
// content-idempotent (Bug 315) so the redundant call when the FSM
// dispatch's renderResFile preamble runs afterwards is a stat+
// compare no-op.
//
// Per-volume scoping is the SAFETY invariant: a bare
// `drbdadm create-md --force <rd>` would walk every volume and
// wipe vol-0's existing GI + bitmap state (the W09 disk-replace
// safety guard exists for exactly this reason). We pass
// `<rd>/<volNumber>` so drbdadm targets only the missing volume.
//
// Callers MUST gate this on "the legacy firstActivation predicate
// would skip create-md" (MetadataCreated=True OR `.md-created`
// marker present). On a true first activation the existing
// createMetadata path handles every volume via a single RD-scoped
// call; running this helper in that branch duplicates work and
// races the FSM dispatch's bring-up shape.
//
// Skipped on diskless replicas (no lower disk to stamp). Errors
// bubble up — a stuck per-vol create-md must surface, not silently
// degrade to Diskless.
func (r *Reconciler) ensurePerVolumeMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, diskless bool) error {
	if diskless {
		return nil
	}

	if r.cfg.Adm == nil {
		return nil
	}

	// Render .res first — drbdadm dump-md / create-md need an
	// up-to-date .res to resolve the per-volume lower disk path.
	// Content-idempotent on converged state.
	err := r.renderResFile(ctx, dr, devices)
	if err != nil {
		return err
	}

	freshlyCreated := map[int32]struct{}{}

	for _, vol := range dr.GetVolumes() {
		target := fmt.Sprintf("%s/%d", dr.GetName(), vol.GetVolumeNumber())

		hasMD, probeErr := r.cfg.Adm.HasMD(ctx, target)
		if probeErr != nil {
			return errors.Wrapf(probeErr, "dump-md %s", target)
		}

		if hasMD {
			continue
		}

		createErr := r.createMDWithCollisionRecovery(ctx, dr.GetName(), target)
		if createErr != nil {
			return errors.Wrapf(createErr, "create-md %s", target)
		}

		freshlyCreated[vol.GetVolumeNumber()] = struct{}{}
	}

	if len(freshlyCreated) == 0 {
		return nil
	}

	// Bug B.4: seed the day0 GI tuple on volumes whose metadata was
	// just freshly created so the subsequent FSM-dispatched adjust
	// brings the new volume up Consistent (case-B winner shape) or
	// at least with a clean per-peer bitmap (case-A skip-init-sync).
	// Without the seed both diskful peers handshake at zero
	// current-UUID, neither is force-promoted (the late-add path
	// runs with firstActivation=false on the parent RD), and the
	// volume latches Inconsistent forever.
	//
	// Per-volume scoping: only the freshly-created volumes get
	// touched. vol-0 (already attached, HasMD=true) is skipped
	// before the loop ever fires — drbdmeta set-gi against an
	// attached lower disk would EBUSY just like create-md would.
	// The per-volume `resolveVolumeSeed` already gates on the
	// per-volume `AnyConnectedPeerHasDataForVolume` probe so the
	// late-add can take the day0 skip-init-sync seed even when
	// sibling volumes have UpToDate peers.
	return r.seedFreshVolumes(ctx, dr, devices, freshlyCreated)
}

// seedFreshVolumes runs seedPerPeerGI for the subset of volumes
// listed in `fresh`. Mirrors the loop in seedInitialGI but limits
// touch to the volumes whose metadata was just freshly created,
// so already-stamped sibling volumes (vol-0 in the Bug B.4
// late-add scenario) are never re-seeded.
//
// Bug 384 (P0, regression of Bug 79/332): the late-add path runs with
// firstActivation=false on the parent RD, so the dispatcher's
// first-activation winner election (auto-primary, gated on
// !rdInitialized) never fires — the RD is already Initialized by the
// time `linstor vd c` lands. The previous code therefore passed
// isWinner=false unconditionally, so EVERY diskful replica took the
// case-A skip-init-sync seed (current=day0, clean bitmap, NO UpToDate
// flag). With no replica declaring itself the UpToDate source and no
// primary --force on this path, the freshly-carved volume came up
// Inconsistent on ALL replicas and never converged (verbatim operator
// repro: `vd c test 1G` → vol-1 Inconsistent on every node).
//
// Fix: re-run the SAME lowest-node-id winner election the dispatcher
// runs at first activation, but locally per fresh volume. Exactly one
// diskful replica (the lowest allocated node-id) takes the case-B
// winner seed (Consistent+UpToDate) and becomes the SyncSource; the
// rest take case-A. resolveVolumeSeed's per-volume
// AnyConnectedPeerHasDataForVolume + PeerHasData vetoes still fire
// FIRST, so a relocate / migrate-disk target (a peer already holds
// data on this volume) never wins itself UpToDate-empty — the winner
// seed is byte-identical to the first-activation winner and shares the
// day0 lineage anchor, so a staggered dual election agrees rather than
// split-brains.
func (r *Reconciler) seedFreshVolumes(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, fresh map[int32]struct{}) error {
	isWinner := isLateAddWinner(dr)

	for _, vol := range dr.GetVolumes() {
		if _, ok := fresh[vol.GetVolumeNumber()]; !ok {
			continue
		}

		device := devices[vol.GetVolumeNumber()]
		if device == "" {
			continue
		}

		seed, ok := r.resolveVolumeSeed(ctx, dr.GetName(), vol, dr.GetPeerHasData(), isWinner, dr.GetSkipInitialSync())
		if !ok {
			continue
		}

		err := r.seedPerPeerGI(ctx, dr, vol, device, seed)
		if err != nil {
			return err
		}
	}

	return nil
}

// isLateAddWinner reports whether THIS node is the elected initial-
// UpToDate source for a late-added volume — the lowest-node-id diskful
// replica, the same election the dispatcher runs at first activation
// (lowestDiskfulID). It returns true iff this node's allocated DRBD
// node-id is strictly the lowest among the local node-id and every
// peer's allocated node-id.
//
// Why local (not auto-primary): the dispatcher only stamps the
// auto-primary winner flag on a NOT-yet-Initialized RD (Bug 356 /
// respawn-StandAlone guard). A `linstor vd c` always lands AFTER the RD
// is Initialized, so no replica carries auto-primary on this path and
// isInitialUpToDateWinner can never elect one. Recomputing the election
// from the node-id set the dispatcher already rendered into the wire
// payload restores the missing winner without re-arming the unsafe
// respawn-time auto-primary.
//
// Deterministic + split-brain-safe: node-id is stable across reconciles
// so the SAME replica wins every pass, and the strict-lowest comparison
// elects EXACTLY ONE node. Peers whose node-id is not yet allocated
// (NodeID==nil) are skipped — the election waits implicitly for the
// next reconcile once every peer id is stamped, the same shape
// diskfulPeersAllocated enforces on the dispatcher side. The result is
// only ever consumed by resolveVolumeSeed AFTER its PeerHasData /
// AnyConnectedPeerHasDataForVolume vetoes, so a winner verdict on a
// volume whose peer already holds data is suppressed before it can
// seed.
func isLateAddWinner(dr *intent.DesiredResource) bool {
	localID, ok := localNodeIDFromOpts(dr)
	if !ok {
		return false
	}

	for _, peer := range dr.GetPeers() {
		if peer.NodeID == nil {
			continue
		}

		if *peer.NodeID <= localID {
			return false
		}
	}

	return true
}

// createMDWithCollisionRecovery wraps Adm.CreateMD with a one-shot
// recovery path for the "Device '<minor>' is configured!" failure
// mode. The collision fires when the kernel already owns the target
// minor for a different DRBD resource — typically a zombie slot left
// behind by a torn-down RD whose `drbdsetup down` was blocked by a
// process stuck in D-state inside the DRBD path (the
// `blockstor_drbd_stuck_state` pattern). The cluster-wide minor
// allocator only sees CRD-tracked minors and cannot observe these
// kernel-only zombies, so multi-volume RDs that need consecutive
// minors (`vol-0=base, vol-1=base+1, …`) deterministically collide on
// the satellite that hosts the orphan.
//
// Recovery is best-effort and conservative:
//  1. Parse the offending minor out of the drbdmeta error.
//  2. Resolve the kernel resource currently owning that minor.
//  3. Confirm the owner is NOT one of OUR volumes for `selfRD` —
//     refusing to tear down our own in-flight metadata work. selfRD
//     is the parent RD name; `<selfRD>/<volNum>` targets are not
//     visible in drbdsetup status (which keys by RD), so the safety
//     check compares the kernel owner to selfRD itself.
//  4. `drbdsetup down` the stale resource. Failure here is non-fatal
//     (lvs holding the device open in D-state means only a node
//     reboot can recover — see blockstor_drbd_stuck_state); we still
//     surface the original create-md error in that case.
//  5. Retry CreateMD exactly once. Any second collision indicates
//     a transient kernel state we cannot resolve from userspace and
//     bubbles up to the reconciler's normal requeue path.
//
// `target` is the drbdadm-style `<rd>[/<volNum>]` argument the caller
// would have passed to Adm.CreateMD directly. `selfRD` is the bare
// parent RD name used by the safety-check above; for a single-volume
// CreateMD call (`target=="<rd>"`) the two are equal.
func (r *Reconciler) createMDWithCollisionRecovery(ctx context.Context, selfRD, target string) error {
	err := r.cfg.Adm.CreateMD(ctx, target)
	if err == nil {
		return nil
	}

	minor, ok := drbd.ParseConfiguredDeviceMinor(err)
	if !ok {
		return err //nolint:wrapcheck // caller wraps with create-md context
	}

	owner, lookupErr := r.cfg.Adm.ResourceOwningMinor(ctx, minor)
	if lookupErr != nil || owner == "" {
		// No kernel owner (or probe failed) → cannot identify the
		// zombie. Surface the original create-md error so the
		// reconciler's retry budget still observes the failure.
		return err //nolint:wrapcheck // caller wraps with create-md context
	}

	if owner == selfRD {
		// The colliding minor is ours. This means a previous
		// reconcile pass already brought the resource up; the
		// caller's HasMD probe race-lost to the legacy chain. Do
		// NOT tear down our own kernel slot — surface the original
		// error so the higher-level idempotent path handles it.
		return err //nolint:wrapcheck // caller wraps with create-md context
	}

	log.FromContext(ctx).Info("create-md collided with foreign kernel slot; attempting recovery",
		"resource", selfRD, "target", target, "minor", minor, "stale", owner)

	downErr := r.cfg.Adm.SetupDown(ctx, owner)
	if downErr != nil {
		// Best-effort: the zombie is genuinely stuck (D-state
		// holder, see blockstor_drbd_stuck_state). Surface the
		// original create-md error — the operator gets the
		// actionable "node X needs reboot" signal upstream.
		log.FromContext(ctx).Info("collision-recovery drbdsetup down failed",
			"stale", owner, "err", downErr.Error())

		return err //nolint:wrapcheck // caller wraps with create-md context
	}

	return r.cfg.Adm.CreateMD(ctx, target) //nolint:wrapcheck // caller wraps with create-md context
}

// maybeStampMetadata is the create-md decision branch lifted out of
// applyDRBD so the orchestrator stays under the gocyclo budget.
// Routes the fresh-replica first-activation path through
// createMetadata (Phase 11.2.c Stage 3a) and the diskless→diskful
// flip path through ensureMetadata(..., firstActivation=false)
// (Bug 319 invariant: re-stamp metadata WITHOUT GI-seed, since the
// kernel slot is already handshaken via the diskless path).
//
// Pure dispatch — every reachable mutation is one of the two
// helpers' existing side-effects. No-op when diskless, or when
// neither firstActivation nor diskfulFlip fires.
func (r *Reconciler) maybeStampMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, mdMarkerPath string, diskless, firstActivation, diskfulFlip bool) error {
	if diskless {
		return nil
	}

	if firstActivation && !diskfulFlip {
		return r.createMetadata(ctx, dr, devices)
	}

	if diskfulFlip {
		return r.ensureMetadata(ctx, dr, devices, mdMarkerPath, false)
	}

	return nil
}

// createMetadata runs drbdadm create-md + per-peer drbdmeta set-gi
// + writes the .md-created file marker + stamps the MetadataCreated
// Condition. Idempotent re-entry: if drbdadm dump-md already shows
// metadata, skips create-md but still seeds set-gi for any peer
// slots without a matching GI line (Bug 319 invariant).
//
// Caller must have already verified firstActivation==true. The
// helper does NOT re-check the gate — moving it inside would
// change ordering vs adjust later in applyDRBD. The
// MetadataCreated Status-Condition stamp lives INSIDE this helper
// so the caller doesn't need to know about the stamper plumbing;
// any per-call .md-created marker path math is also internal.
//
// Phase 11.2.c Stage 3a: pure extract, no behaviour change. Stage 3b
// will FSM-shadow-dispatch this helper at the top of applyDRBD,
// mirror of the renderResFile shadow landed in Stage 2.
func (r *Reconciler) createMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string) error {
	mdMarkerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".md-created")

	return r.ensureMetadata(ctx, dr, devices, mdMarkerPath, true)
}

// runAutoPromote orchestrates the first-activation seed:
//
//  1. `drbdadm primary --force` — promote out of Inconsistent so the
//     kernel accepts writes.
//  2. RG-driven `mkfs.<type>` (scenario 9.W14) — runs ONLY while we
//     hold Primary; mkfs on a Secondary deadlocks on EROFS.
//  3. `drbdadm secondary` — demote so the consumer (CSI / external
//     mounter) can promote at its own discretion.
//
// Pulled out of applyDRBD so the orchestration function stays under
// the project's gocyclo budget.
// shouldForcePromote is the Bug 342 force-promote gate. It returns true
// only when `drbdadm primary --force` is data-safe on this fresh
// replica — i.e. NO other replica already holds committed data. Forcing
// primary mints a brand-new Current UUID; doing so when a peer already
// owns the real data + UUID makes the peer decline the GI handshake
// (`uuid_compare()=unrelated-data` → `Unrelated data, aborting!`) and
// wedges this replica StandAlone (the relocate / physical-`r d`-then-
// `r c` case). The safe alternative is "no force → Inconsistent →
// SyncTarget from the peer" (full resync).
//
// Two independent signals, EITHER of which vetoes the promote:
//
//  1. dr.PeerHasData — the dispatcher's view of peer CRD
//     Status.DiskState (UpToDate/Consistent/Outdated). Fast, but the
//     apiserver cache can lag a freshly-recreated peer, so it can miss.
//  2. A bounded kernel-truth probe (drbdsetup status --json): the .res
//     was adjusted+connected before this runs, but the peer-connect
//     handshake is async, so the peer's disk state may not be visible
//     the instant we probe. Poll briefly so a peer that is about to
//     connect with data is seen BEFORE we force-primary — this closes
//     the connect-timing race the dispatcher gate alone can't (the
//     dmesg evidence showed force-primary firing ~0.5s before the peer
//     handshake landed).
//
// A genuinely-fresh RD has no peer with data: PeerHasData is false and
// the kernel probe finds nothing (peers fresh/Inconsistent or just a
// diskless tiebreaker), so this returns true and the Bug 77
// first-replica seed still force-primaries. When the replica has no
// configured peers at all (true solo, e.g. Bug 356 diskless→diskful
// flip on a 1-node RD) we skip the wait entirely and promote at once.
func (r *Reconciler) shouldForcePromote(ctx context.Context, dr *intent.DesiredResource) bool {
	if dr.GetPeerHasData() {
		return false
	}

	// No peers configured → sole replica, nothing to sync from, promote
	// immediately (no wait — there is no connection to ever establish).
	if len(dr.GetPeerNames()) == 0 {
		return true
	}

	// Bounded kernel-truth wait: give the async peer handshake a chance
	// to surface a data-bearing peer-disk before we commit to forcing
	// primary. ~6s ceiling (12 × 500ms) comfortably exceeds the observed
	// adjust→handshake latency while staying well under the reconcile
	// budget. The instant ANY connected peer exposes committed data we
	// veto; if the window elapses with no data peer, the RD is genuinely
	// fresh and we proceed.
	for range 12 {
		if r.cfg.Adm.AnyConnectedPeerHasData(ctx, dr.GetName()) {
			return false
		}

		select {
		case <-ctx.Done():
			// Context cancelled — be conservative and DON'T force
			// (full resync is always safe; a missed promote retries
			// on the next reconcile).
			return false
		case <-time.After(500 * time.Millisecond):
		}
	}

	return true
}

// needsMkfs reports whether the RD requests an on-creation filesystem
// (the `FileSystem/Type` prop the controller folds from the RG's
// effective props). It is the sole remaining reason to `drbdadm
// primary --force` a fresh replica: mkfs must run while Primary, and
// quorum may not be satisfiable yet because peers haven't connected.
// When false, the elected winner reaches UpToDate from its set-gi
// seed alone and is never promoted at create time.
func needsMkfs(dr *intent.DesiredResource) bool {
	return strings.TrimSpace(dr.GetProps()["FileSystem/Type"]) != ""
}

func (r *Reconciler) runAutoPromote(ctx context.Context, dr *intent.DesiredResource) error {
	err := r.cfg.Adm.PrimaryForce(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "auto-primary %s", dr.GetName())
	}

	err = r.runAutoMkfs(ctx, dr, drbdDevicesForMkfs(dr))
	if err != nil {
		return errors.Wrapf(err, "auto-mkfs %s", dr.GetName())
	}

	err = r.cfg.Adm.Secondary(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "auto-secondary %s", dr.GetName())
	}

	return nil
}

// drbdDevicesForMkfs builds the {volNumber → /dev/drbd<minor>} map
// runAutoMkfs feeds into mkfs for DRBD-stacked resources. mkfs on a
// DRBD volume MUST go through the kernel device (writes are mirrored
// to peers via initial-sync); writing directly to the lower disk
// would diverge from the kernel's view and corrupt the replica.
// Mirrors the per-volume minor resolution buildVolumeResults uses
// for the same fan-out.
func drbdDevicesForMkfs(dr *intent.DesiredResource) map[int32]string {
	minor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

	out := make(map[int32]string, len(dr.GetVolumes()))
	for _, vol := range dr.GetVolumes() {
		out[vol.GetVolumeNumber()] = fmt.Sprintf("/dev/drbd%d", volMinorOrBase(vol, minor))
	}

	return out
}

// runStorageOnlyMkfs handles the auto-mkfs path for resources whose
// layer stack omits DRBD (`layerList=storage`, the linstor-csi
// "local" SC shape). For those there is no DRBD slot to promote and
// no peer mirroring — writes land directly on the lower disk, so
// mkfs targets the raw device path applyStorage produced. Skips
// silently when (a) the resource is diskless (no lower disk to
// format), (b) DRBD is in the stack (runAutoPromote already owns
// the promote→mkfs→demote ordering), or (c) FileSystem/Type is
// unset (no FS requested — pure block-mode PVC).
//
// linstor-csi v1.10.1's NodePublishVolume runs `fsck` then plain
// `mount(2)` on the device — there is no FormatAndMount fallback
// (see pkg/client/linstor.go lines 2287-2293). The satellite is the
// only place a CSI-provisioned local PVC can get a filesystem
// before the kubelet tries to mount it.
func (r *Reconciler) runStorageOnlyMkfs(ctx context.Context, dr *intent.DesiredResource, diskless bool, devices map[int32]string) error {
	if diskless {
		return nil
	}

	if needsDRBD(dr.GetLayerStack()) {
		return nil
	}

	if strings.TrimSpace(dr.GetProps()["FileSystem/Type"]) == "" {
		return nil
	}

	return r.runAutoMkfs(ctx, dr, devices)
}

// runAutoMkfs handles the RG-driven auto-mkfs path of scenario
// 9.W14. The controller folds `FileSystem/Type` (and the optional
// `FileSystem/MkfsParams`) from the RG's effective props into the
// per-RD wire Props map; the satellite consumes them here on the
// primary replica.
//
// Idempotency has two layers:
//
//  1. A per-RD `<rd>.mkfs.done` marker under StateDir (sibling to
//     `.md-created`) records the durable "we already finished mkfs
//     for every diskful volume" state. Cheap stat-only fast path.
//  2. Per-volume `blkid -o export /dev/drbd<minor>` probe (mirroring
//     upstream LINSTOR's `MkfsUtils.hasFileSystem`). When a volume
//     already carries a filesystem we skip mkfs on that volume and
//     adopt the existing fs — exactly upstream's behaviour. This
//     closes Bug 311: a previous reconcile that dropped `.md-created`
//     but failed to write `.mkfs.done` (e.g. `drbdadm primary
//     --force` raced the initial-sync handshake and returned a
//     transient error) would otherwise permanently skip mkfs on
//     subsequent passes, since firstActivation goes false. The new
//     retry gate in finishDRBDApply re-enters this function; the
//     blkid probe makes that retry safe even on a volume that was
//     partially mkfs'd before the failure.
//
// SAFETY: mkfs on a populated filesystem silently destroys data. The
// blkid probe is what protects an already-formatted volume from
// double-mkfs when the marker file is absent (manual `rm`, host
// rebuild that wipes /etc/drbd.d). DeleteResource removes the marker
// together with `.res` / `.md-created` so a re-created RD with the
// same name correctly mkfs-s again — the blkid probe sees an empty
// (freshly-carved) volume and lets mkfs run.
//
// The devices parameter maps volNumber → device path mkfs should
// target. DRBD-stacked callers pass `/dev/drbd<minor>` (mkfs MUST go
// through the kernel so writes mirror to peers); storage-only
// callers pass the raw LV/zvol/loopfile path applyStorage
// produced. A volume missing from the map is silently skipped — the
// only legitimate "missing" case is a diskless replica caller,
// which has already been early-returned by runStorageOnlyMkfs.
func (r *Reconciler) runAutoMkfs(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string) error {
	fsType := strings.TrimSpace(dr.GetProps()["FileSystem/Type"])
	if fsType == "" {
		return nil
	}

	if r.cfg.Exec == nil || r.cfg.StateDir == "" {
		// No exec wrapper or no state dir → can't run mkfs / can't
		// drop a marker. Skip rather than fail; production always
		// wires both. The unit test that pins the no-Exec branch
		// would otherwise need to mock half a Reconciler.
		return nil
	}

	markerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".mkfs.done")

	// Phase 11.3 Stage 2: Condition-first fast-path. When the
	// dispatcher already observed `FilesystemFormatted=True` on the
	// Resource CRD, every diskful volume of this RD has already
	// passed the auto-mkfs path (either freshly mkfs'd or adopted
	// via blkid) and we can skip the per-volume blkid round-trip
	// entirely. Belt-and-braces: a stale Condition still leaves the
	// blkid probe per-volume below as the authoritative safety net
	// against double-mkfs — but with the Condition set we never
	// reach that branch.
	if dr.GetFilesystemFormatted() {
		return nil
	}

	_, statErr := os.Stat(markerPath)
	if statErr == nil {
		// Marker present → mkfs already ran on a previous activation.
		// Re-running would wipe a populated filesystem. File-marker
		// fallback for the migration window: clusters upgraded from
		// pre-11.3 Stage 2 may have a populated marker but no
		// Condition stamped yet.
		return nil
	}

	args := []string{}

	if extra := strings.TrimSpace(dr.GetProps()["FileSystem/MkfsParams"]); extra != "" {
		args = append(args, strings.Fields(extra)...)
	}

	for _, vol := range dr.GetVolumes() {
		device := devices[vol.GetVolumeNumber()]
		if device == "" {
			// No device path for this volume → caller is a path that
			// has nothing to mkfs (diskless replica, missing pickup).
			// Storage-only callers always populate devices via
			// applyStorage; DRBD callers always populate via
			// drbdDevicesForMkfs. Skip rather than fail so this is a
			// hot-path safety net, not a behavioural change.
			continue
		}

		if r.deviceHasFilesystem(ctx, device) {
			// Volume already carries a filesystem. Two cases land here:
			// (a) a previous reconcile mkfs'd this volume but crashed
			// before writing the marker — adopt the fs and continue;
			// (b) the operator manually formatted the device — same
			// treatment. Matches upstream LINSTOR's MkfsUtils.
			// makeFileSystemOnMarked which short-circuits on a
			// non-empty hasFileSystem result.
			continue
		}

		cmdArgs := append(slices.Clone(args), device)

		_, err := r.cfg.Exec.Run(ctx, "mkfs."+fsType, cmdArgs...)
		if err != nil {
			return errors.Wrapf(err, "mkfs.%s %s", fsType, device)
		}
	}

	err := os.WriteFile(markerPath, nil, resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "write %s", markerPath)
	}

	// Phase 11.3 Stage 2: stamp the `FilesystemFormatted=True` Status
	// Condition on the parent Resource CRD. Belt-and-braces with the
	// file marker write above: future reconciles read the Condition
	// first to short-circuit the auto-mkfs path, falling back to the
	// file presence only when the Condition is absent (cluster
	// upgraded from a pre-11.3-Stage-2 build). Stamp failure does NOT
	// fail the apply — the file marker is the transitional source of
	// truth, so a transient apiserver hiccup here just defers
	// Condition stamping to the next reconcile.
	if r.cfg.FilesystemFormattedStamper != nil {
		// Bug 344 (regression of Bug 311 followup #501): the stamper
		// SSA-patches a `Resource` object whose Name is the CRD object
		// name. Real Resource CRDs are named `<rd>.<node>` (per-node
		// sharding); passing the RD-only name made the apiserver
		// return 404 on every stamp attempt, so `FilesystemFormatted`
		// never landed on the Condition list and the cli-matrix
		// `rwx-ganesha-data-vol-mkfs` cell's Condition-assertion FAIL
		// reproduced even after the unit-test-level retry path landed.
		// Mirror the Bug 344 fix in maybeStampMetadata's caller.
		// Best-effort tolerated (file marker is the source of truth)
		// so no functional regression on a transient apiserver hiccup.
		resourceCRDName := dr.GetName() + "." + dr.GetNodeName()

		stampErr := r.cfg.FilesystemFormattedStamper.StampFilesystemFormatted(ctx, resourceCRDName)
		if stampErr != nil {
			log.FromContext(ctx).Error(stampErr, "stamp FilesystemFormatted Condition; will retry next reconcile",
				"resource", resourceCRDName)
		}
	}

	return nil
}

// deviceHasFilesystem reports whether the given DRBD device already
// carries a recognised filesystem. Wraps `blkid -o export <device>`
// the same way upstream LINSTOR's MkfsUtils.hasFileSystem does:
// presence of a `TYPE=` line in the export-format output means the
// kernel's libblkid detected a known filesystem signature. blkid's
// exit-2 (no signature found) is folded into the FakeExec /
// RealExec "non-zero exit → wrapped error" contract; we treat that
// as "no filesystem" rather than propagating the error because the
// caller's only sensible response is exactly the same: skip mkfs on
// a populated volume, run it on an empty one.
//
// A real I/O failure (device gone, kernel returned EIO) also lands
// in the error branch, but the subsequent mkfs.<type> on the same
// device would fail just as loudly with a more actionable message
// ("No such file or directory" / "Input/output error"), so the
// fall-through to mkfs preserves the failure mode operators
// already expect.
func (r *Reconciler) deviceHasFilesystem(ctx context.Context, device string) bool {
	out, err := r.cfg.Exec.Run(ctx, "blkid", "-o", "export", device)
	if err != nil {
		// Treat any blkid failure as "no recognised filesystem". The
		// most common shape is exit-code 2 (no signature) which
		// RealExec wraps into a generic error — the caller's only
		// sensible reaction is to run mkfs, which is what the
		// no-filesystem branch already does.
		return false
	}

	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "TYPE=") {
			return true
		}
	}

	return false
}

// runApplyDRBDVerb is the per-reconcile dispatch for the bring-up
// chain. First activation falls through to the SkipDisk-aware
// `drbdadm adjust` (or `adjust --skip-disk`): the .res + freshly-
// created metadata are the canonical bring-up path on master and
// existing tests pin that behaviour. Bug 319 (diskless→diskful
// flip) also routes through this arm via `diskfulFlip=true` so the
// compare_volume attach_cmd schedule has a chance to fire on a
// freshly re-stamped lower disk — even though firstActivation is
// false on the flip (metadata pre-existed from the diskless slot).
//
// The historical steady-state arm (`runBringUpOrAdjust` — kernel-
// state probe + `drbdadm up` / `drbdadm adjust` dispatch) has been
// retired in Phase 11.2.c Stage 4 step 3. The FSM shadow-dispatch
// at the top of applyDRBD now owns ActionAdjust / ActionAdjust
// SkipDisk for every non-firstActivation, non-flip pass. See the
// inline Why-comment on the steady-state branch below.
//
// Split out of applyDRBD so the orchestration function stays under
// the gocyclo budget.
func (r *Reconciler) runApplyDRBDVerb(ctx context.Context, dr *intent.DesiredResource, firstActivation, diskfulFlip bool) error {
	// Bug 319 flip stays on the legacy adjustResource call until step
	// 4 (retire createMetadata legacy) lands and the FSM owns the
	// flip transition end-to-end. The flip routes here with
	// firstActivation=false (metadata pre-existed from the diskless
	// slot) but still needs `drbdadm adjust` to fire so drb-utils'
	// compare_volume schedules attach_cmd on the freshly re-stamped
	// lower disk. The diskfulFlip=true argument suppresses the Bug
	// 280 kernel-Diskless → --skip-disk coercion inside runAdjust so
	// the attach can land.
	if firstActivation || diskfulFlip {
		return r.adjustResource(ctx, dr, diskfulFlip)
	}

	// Phase 11.2.c Stage 4 step 3: legacy runBringUpOrAdjust /
	// r.adjustResource call retired on the steady-state arm.
	//
	// Why: the FSM shadow-dispatch at the top of applyDRBD owns
	// ActionAdjust / ActionAdjustSkipDisk. The renderResFile preamble
	// inside dispatchFsmAction's adjust arm (Stage 4 step 1) ensures
	// .res is current before drbdadm adjust runs, so kernel state
	// matches the declarative spec without a second pass through this
	// site.
	//
	// drbdadm adjust is the canonical "make kernel match .res" verb —
	// naturally idempotent (Bug-287 fallback inside runAdjust still
	// re-attempts via drbdadm up on `(158) Unknown resource`). We do
	// not add any additional closed-loop recovery here; DRBD-9's own
	// resync / auto-promote logic owns post-adjust convergence, and
	// the Bug 47 / scenario 5.32 operator-down recovery loop is
	// closed by the FSM's Phase==MetadataReady → ActionUp transition
	// (retired in step 2) which observes the unloaded slot ahead of
	// any adjust attempt.
	//
	// Step 4 (retire createMetadata legacy) is the final step and
	// will also collapse the firstActivation + diskfulFlip arms
	// above into FSM dispatch once the ActionCreateMd → ActionAdjust
	// transition is wired for the flip phase.
	return nil
}

// bringUpResource runs `drbdadm up <name>` to load the kernel slot
// from the rendered .res file. Caller has already ensured .res
// exists (renderResFile) and create-md has run for diskful replicas
// (createMetadata); this helper is the third bring-up verb in the
// per-reconcile sequence, distinct from `adjustResource` which
// reconciles already-loaded kernel state.
//
// Bug 319 re-entry: if the operator flipped Spec.Flags Diskless→
// diskful but the on-disk `.md-created` marker (or
// `MetadataCreated=True` Status Condition) says metadata was
// already laid down, the kernel will refuse to up because there's
// no metadata on the LV. Detection of that flip lives at the gate
// level in applyDRBD (suppress firstActivation, force re-entry to
// createMetadata) — bringUpResource itself is ONLY the
// `drbdadm up <name>` invocation + error wrapping.
//
// The Bug-287 `(158) Unknown resource` fallback to `drbdadm up`
// inside `runAdjust` is a distinct call site and intentionally
// stays inline: it's the recovery verb in the half-torn
// kernel-slot window, not the first-load path, and its error
// wrap ("drbdadm up %s (after adjust 158 fallback)") needs to
// preserve that context.
//
// Phase 11.2.c Stage 3c: pure extract, no behaviour change. Stage 3d
// (or later) will FSM-shadow-dispatch this helper for ActionUp
// transitions at the top of applyDRBD, mirror of the renderResFile
// (Stage 2), createMetadata (Stage 3a), and adjustResource (Stage
// 3b) shadows.
func (r *Reconciler) bringUpResource(ctx context.Context, dr *intent.DesiredResource) error {
	err := r.cfg.Adm.Up(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "drbdadm up %s", dr.GetName())
	}

	// Talos / minimal-distro fix: no udev daemon means DRBD's bundled
	// udev rules never fire and `/dev/drbd<minor>` is never created
	// after `drbdadm up`. The CSI publish path, e2e tests, and any
	// `drbdadm primary` consumer that opens the device via plain
	// open(2) would otherwise create a regular file in tmpfs at that
	// path and silently land all I/O in tmpfs (never touching DRBD
	// or the backing zvol/loop/lvm). EnsureDeviceNode is the
	// satellite-side replacement for the missing udev rule: idempotent,
	// safe on real-udev nodes (no-op when path is already correct).
	ensureDeviceNodes(ctx, dr)

	return nil
}

// ensureDeviceNodes invokes drbd.EnsureDeviceNode for every kernel
// minor the resource owns, derived from the DrbdOptions["minor"] base
// + per-volume offset (mirrors buildResVolumes' minor allocation). A
// per-volume failure is logged and otherwise swallowed: the next
// reconcile re-attempts, and a transient EPERM / I/O hiccup must not
// strand the bring-up path. Real /dev/drbd<N> creation is best-effort
// — if the satellite can't mknod (e.g. the container lacks CAP_MKNOD),
// the consumer's open will fail loudly rather than silently writing to
// a tmpfs file.
func ensureDeviceNodes(ctx context.Context, dr *intent.DesiredResource) {
	baseMinor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

	for _, vol := range dr.GetVolumes() {
		// Per-volume minor is authoritative; fall back to
		// base+volumeNumber when unset (mid-upgrade / in-flight).
		minor := int(vol.GetMinor())
		if minor == 0 {
			minor = baseMinor + int(vol.GetVolumeNumber())
		}

		if minor <= 0 {
			// Base minor unset (DesiredResource may carry no minor
			// when the controller hasn't allocated one yet — pre-first-
			// activation). Skip: the bring-up that just succeeded can't
			// have been about THIS volume, the kernel didn't get a
			// minor to bind to.
			continue
		}

		err := drbd.EnsureDeviceNode(minor)
		if err != nil {
			log.FromContext(ctx).Error(err, "ensure /dev/drbd device node",
				"resource", dr.GetName(),
				"volume", vol.GetVolumeNumber(),
				"minor", minor)
		}
	}
}

// adjustResource runs `drbdadm adjust <name>` with the right
// SkipDisk coercion: bare adjust when neither the operator prop nor
// kernel state asks for skip-disk, `--skip-disk` form when either
// signal is present. The SkipDisk arm covers both Bug 280 (kernel
// Diskless without operator prop) and operator-pinned downgrade
// (scenario 5.11).
//
// Idempotent: `drbdadm adjust` is the canonical "make kernel state
// match .res" call; safe to re-run. The caller has already ensured
// .res exists (renderResFile) and create-md has run if
// firstActivation (createMetadata). The Bug-287 fallback to
// `drbdadm up` for the `(158) Unknown resource` race lives inside
// the helper so callers don't have to know about the half-torn
// kernel-slot window.
//
// Gate computation (SkipDisk prop check + kernel-Diskless probe)
// stays inside the helper — it determines which variant to run and
// the caller must not pass that decision. `diskfulFlip` is an
// input (not an internal probe) because Bug 319 needs to suppress
// the Diskless-probe-driven SkipDisk coercion on the
// diskless→diskful transition where compare_volume must see the
// kern->disk=="none" + conf->disk path diff to schedule attach_cmd.
//
// Phase 11.2.c Stage 3b: pure extract, no behaviour change. Stage 3c
// (or later) will FSM-shadow-dispatch this helper for ActionAdjust
// and ActionAdjustSkipDisk transitions at the top of applyDRBD,
// mirror of the renderResFile (Stage 2) and createMetadata (Stage
// 3a) shadows.
func (r *Reconciler) adjustResource(ctx context.Context, dr *intent.DesiredResource, diskfulFlip bool) error {
	return r.runAdjust(ctx, dr, diskfulFlip)
}

// runAdjust dispatches to the plain `drbdadm adjust` or the
// `--skip-disk` variant based on the `DrbdOptions/SkipDisk` prop
// (scenario 5.11).
//
// Bug 280 (P1): the prop-only gate races the observer's
// SkipDisk-stamp path. When an operator runs `drbdadm detach
// --force` against the satellite shell:
//
//  1. Kernel transitions UpToDate → Diskless and emits
//     `change device disk:Diskless` on events2.
//  2. The observer's UpToDate→Diskless gate writes
//     `DrbdOptions/SkipDisk=True` onto Spec.Props.
//  3. The Diskless event also causes a Status update which fires a
//     parallel reconcile.
//
// A reconcile already in flight when the operator's command landed
// loaded `res` from the watch cache BEFORE the prop write hit the
// apiserver. Its `dr.Props` view has SkipDisk absent, the
// prop-only gate dispatches plain `drbdadm adjust`, and the disk
// re-attaches in sub-second — the operator's poll never observes
// Diskless.
//
// Probe the kernel directly via `HasDisklessVolume`: the kernel is
// the authority on the disk's current state, independent of any
// apiserver cache trail. When the kernel reports a not-attached
// state (Diskless, Detaching, or Failed) on a slot that's already
// loaded (so we're past first activation), we coerce the adjust
// onto `--skip-disk` regardless of the prop's cache visibility.
// The operator's SkipDisk-stamp is a hint that will catch up via
// the apiserver; the kernel probe closes the race window in the
// meantime. The Detaching arm closes a sub-second race window
// where `drbdsetup status` lags `drbdadm detach --force`'s kernel
// transition by reporting `disk:Detaching` rather than
// `disk:Diskless`, which the old probe missed and the next
// reconcile re-attached through.
//
// Errors from the probe fall through to the prop-only gate (the
// pre-Bug-280 behaviour) so a transient netlink hiccup doesn't
// strand the reconciler.
//
// Bug 287 / scenario 5.32 race: even when the FSM `KernelLoaded`
// observation reads as true (or when this path runs on first
// activation), the kernel slot can be torn down between the probe
// and the `drbdadm adjust` shell-out — that's the half-torn window
// right after an operator's `drbdadm down` finishes its kernel-side
// teardown. `drbdadm adjust` in that state issues
// `drbdsetup new-minor` without `new-resource` first and bails with
// `Failure: (158) Unknown resource`. Catch that exact error string,
// fall back to `drbdadm up <rsc>` (which always issues
// new-resource + new-minor + attach + connect), and let the next
// reconcile re-converge.
func (r *Reconciler) runAdjust(ctx context.Context, dr *intent.DesiredResource, diskfulFlip bool) error {
	skipDisk := isSkipDiskEnabled(dr)

	// Bug 319: on the diskless→diskful Spec flag flip we DELIBERATELY
	// want plain `drbdadm adjust` to attach the freshly create-md'd
	// lower disk via drb-utils' compare_volume (kern->disk=="none" +
	// conf->disk path diff schedules attach_cmd). Coercing
	// `--skip-disk` here — which Bug 280's kernel probe would
	// otherwise do because the kernel still reports Diskless — would
	// suppress exactly the attach we just created the metadata for.
	if !skipDisk && !diskfulFlip {
		diskless, probeErr := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
		if probeErr == nil && diskless {
			skipDisk = true
		}
	}

	// Bug 278: Talos kernel upgrade leaves SkipDisk pinned from the
	// pre-upgrade defensive stamp. Upstream LINSTOR's SkipDisk is
	// operator-only; we stamped it defensively (Phase 11.3 territory
	// — Failed→Diskless trigger in the observer) and now must
	// un-stamp when the kernel re-emerges healthy after the satellite
	// reattaches.
	//
	// Why this isn't "auto-recovery beyond upstream": SkipDisk on a
	// healthy slot is an artifact OF our defensive stamping (the
	// observer's writeSkipDiskProp under observerSkipDiskFieldOwner).
	// Removing our own stamp is symmetric with stamping it — not new
	// behavior. Operator-set SkipDisk (via `linstor r prop set ...
	// SkipDisk=True` on the controller's FieldOwner) survives the
	// SSA release because the observer's owner only ever claimed its
	// own apply, not the operator's. DRBD's own resync /
	// auto-promote logic owns post-adjust convergence; we do not
	// add a closed-loop recovery here — the clear is a one-shot SSA
	// release that lets the existing FSM transition
	// (PhaseSkipDisk→PhaseRunning on !obs.SkipDiskProp) fire on the
	// next reconcile.
	//
	// Gate: SkipDisk-from-prop AND kernel NOT in Diskless state
	// (HasDisklessVolume==false). The diskful-flip arm doesn't
	// reach this since `!skipDisk && !diskfulFlip` is the only path
	// that flips skipDisk via the kernel probe — and we explicitly
	// scope the clear to the "prop-set" origin so a freshly probed
	// kernel-Diskless does NOT trigger a clear. We also gate on
	// !diskfulFlip so the Bug 319 flip path (kernel still Diskless,
	// SkipDisk prop unset) never enters the clear path.
	if isSkipDiskEnabled(dr) && !diskfulFlip && r.cfg.SkipDiskClearer != nil {
		diskless, probeErr := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
		if probeErr == nil && !diskless {
			// Kernel reports the local volume as non-Diskless (UpToDate
			// / Inconsistent / Outdated — all are "backing storage
			// attached"). SkipDisk on the prop is an artifact of the
			// pre-upgrade defensive stamp; release the observer's SSA
			// claim so the next dispatcher cycle re-resolves Spec.Props
			// without SkipDisk and the next reconcile dispatches plain
			// `drbdadm adjust`.
			//
			// Best-effort: a clearer error doesn't strand the reconciler
			// (the worst case is the same as not having the clearer at
			// all — the prop stays pinned and the next pass re-tries).
			_ = r.cfg.SkipDiskClearer.ClearSkipDisk(ctx, dr.GetName())
		}
	}

	skipNet := r.shouldSkipNetOnAdjust(ctx, dr.GetName())

	err := r.dispatchAdjust(ctx, dr.GetName(), skipDisk, skipNet)
	if err == nil {
		// Talos / minimal-distro fix: same rationale as bringUpResource.
		// Adjust may have added a new volume (multi-volume scenario 6.5)
		// or flipped a diskless replica diskful (Bug 319) — both create
		// a new kernel minor on this node. Without udev the matching
		// /dev/drbd<minor> never appears; consumers then open the path
		// via open(2) and the kernel creates a regular file in tmpfs.
		// EnsureDeviceNode is idempotent on already-correct nodes so
		// covering the steady-state-adjust path here costs nothing.
		ensureDeviceNodes(ctx, dr)

		return nil
	}

	// Recover from the Bug-287 race: the kernel slot the probe just
	// saw vanished before adjust ran. `drbdadm up` is the only verb
	// that bootstraps a missing slot from a valid .res + on-disk
	// metadata; surface its error directly so the reconciler retry
	// loop can re-converge if up also fails.
	if isUnknownResourceErr(err) {
		upErr := r.cfg.Adm.Up(ctx, dr.GetName())
		if upErr != nil {
			return errors.Wrapf(upErr, "drbdadm up %s (after adjust 158 fallback)", dr.GetName())
		}

		ensureDeviceNodes(ctx, dr)

		return nil
	}

	// Recover from the tiebreaker-relocate StandAlone wedge:
	// when the controller re-allocates DRBDNodeID for a returning
	// TIE_BREAKER onto a fresh peer-slot, the surviving diskful's
	// v09 metadata for that slot reads back as
	// `peer-disk:Outdated` (default-initialised bitmap-uuid). The
	// failing adjust hits `peer-device-options --bitmap=no` and
	// the kernel refuses with `(162) Can not drop the bitmap when
	// both sides have a disk`. recoverFromBitmapBothDisks below
	// runs a full down + up + adjust cycle so the next adjust
	// reads peer-disk from a fresh handshake (DUnknown) rather
	// than from the stale metadata default — see
	// tests/e2e/tiebreaker-r-d-r-c-other-node.sh.
	if drbd.IsBitmapDropBothDisksErr(err) {
		return r.recoverFromBitmapBothDisks(ctx, dr, skipDisk, skipNet)
	}

	return errors.Wrapf(err, "adjust %s", dr.GetName())
}

// recoverFromBitmapBothDisks executes the down + up + adjust
// recovery for the tiebreaker-relocate StandAlone wedge.
// Without this recovery the first failed adjust leaves the
// returning TB's kernel slot StandAlone with peer-device entries
// kernel-registered (the new-peer call ran before peer-device-
// options failed); shouldSkipNetOnAdjust then permanently
// latches `--skip-net` for that slot and the slot stays
// StandAlone forever.
//
// Bounded to a single bounce per adjust pass: the recovery
// retries dispatchAdjust once; if the second adjust still
// fails, bubble the error so the reconciler retry loop
// re-converges on the next tick instead of looping in-place.
//
// Down on a Primary-Open resource would block, but the
// signature only fires on the moment a returning TB joins —
// the wedge surfaces on the surviving diskful side which is
// Secondary (unmounted at the moment the TB respawns).
func (r *Reconciler) recoverFromBitmapBothDisks(ctx context.Context, dr *intent.DesiredResource, skipDisk, skipNet bool) error {
	log.FromContext(ctx).Info("adjust failed with bitmap-bothdisks; recovering via down + up + adjust",
		"resource", dr.GetName())

	err := r.cfg.Adm.Down(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "drbdadm down %s (recovery from bitmap-bothdisks)", dr.GetName())
	}

	err = r.cfg.Adm.Up(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "drbdadm up %s (recovery from bitmap-bothdisks)", dr.GetName())
	}

	err = r.dispatchAdjust(ctx, dr.GetName(), skipDisk, skipNet)
	if err != nil {
		return errors.Wrapf(err, "adjust %s (after bitmap-bothdisks recovery)", dr.GetName())
	}

	ensureDeviceNodes(ctx, dr)

	return nil
}

// dispatchAdjust picks the right `drbdadm adjust` variant for the
// observed (skipDisk, skipNet) signal combination and shells out.
// Pulled out of runAdjust so the orchestration stays under gocyclo.
func (r *Reconciler) dispatchAdjust(ctx context.Context, resource string, skipDisk, skipNet bool) error {
	switch {
	case skipDisk && skipNet:
		return r.cfg.Adm.AdjustSkipNetSkipDisk(ctx, resource) //nolint:wrapcheck // caller wraps
	case skipDisk:
		return r.cfg.Adm.AdjustSkipDisk(ctx, resource) //nolint:wrapcheck // caller wraps
	case skipNet:
		return r.cfg.Adm.AdjustSkipNet(ctx, resource) //nolint:wrapcheck // caller wraps
	default:
		return r.cfg.Adm.Adjust(ctx, resource) //nolint:wrapcheck // caller wraps
	}
}

// shouldSkipNetOnAdjust probes the kernel for operator-initiated
// `StandAlone` peer connection slots. When any peer is in
// `StandAlone` AND the kernel has peer-device entries registered
// for it, the caller dispatches `drbdadm adjust --skip-net` rather
// than plain adjust — preserving the operator's manual disconnect
// across the reconcile.
//
// W12 + network-partition guard: when the operator runs
// `drbdadm disconnect <rd>` or `drbdsetup disconnect --force=yes <rd>
// <peerID>` (the documented split-brain recovery recipe + the
// iptables-partition test pre-amble), the affected peer's kernel
// connection state becomes `StandAlone` — a state the kernel will
// NOT recover from on its own (it requires operator action:
// `drbdadm connect` or one of the --discard-my-data variants).
//
// blockstor's observer-trigger channel wakes the reconciler on the
// connection-state change, the reconciler runs `drbdadm adjust`,
// and adjust's net-attach pass re-issues `drbdsetup connect` —
// undoing the operator's disconnect within ~1 s. That kills both
// the W12 recipe (the subsequent `drbdadm -- --discard-my-data
// connect` fails with `(125) Device has a net-config`) and any
// split-brain provocation a test wants to set up.
//
// Upstream LINSTOR sidesteps this by only invoking adjust when
// `drbdadm list-adjustable` reports a config difference (DrbdLayer
// .java L181-185 + L1207-1209). DRBD 9.22 ships without
// list-adjustable, so we use the equivalent kernel-state probe:
// if any peer's connection slot is currently in StandAlone, treat
// this reconcile as operator-controlled and append `--skip-net` to
// adjust. Disk-level drift convergence (volume resize, SkipDisk
// prop changes) still runs; net-attach is left for the operator to
// restore via `drbdadm connect`.
//
// Scenario 5.32 / recovery-down-reverses guard: the same `StandAlone`
// connection-state token shows up in a SECOND, semantically distinct
// case — the post-`drbdadm down` recovery window. When an operator
// runs `drbdadm down <rd>` and the reconciler revives the kernel
// slot via the Bug-287 `drbdadm up` fallback (ActionUp via FSM), a
// transient or failed handshake can leave fresh peer connection
// slots stuck in StandAlone WITHOUT peer-device entries — the
// kernel allocated the slot but the connect handshake never
// registered the per-volume peer-device table. The operator-
// disconnect case ALWAYS retains peer-device entries (kernel keeps
// the configured volumes around after `drbdadm disconnect`, only
// the connection-state flips), so the presence of at least one
// peer-device entry is the disambiguator: it is the kernel's
// "this slot was successfully negotiated at some point" marker.
//
// We therefore require BOTH StandAlone connection-state AND at
// least one peer-device entry for any volume in the desired set
// before coercing `--skip-net`. A fresh-revive StandAlone (no
// peer-devices) falls through to the bare adjust path — which
// re-issues `drbdsetup connect` and unwedges scenario 5.32
// (`tests/e2e/recovery-down-reverses.sh`).
//
// Best-effort: a probe error returns false so adjust falls through
// to the existing full-adjust path (failing closed would freeze
// adjust on any transient drbdsetup hiccup). The probe is
// per-resource, so a healthy peer on a multi-peer RD still gets
// reconnected when its own slot is in Connecting/Timeout/etc.
func (r *Reconciler) shouldSkipNetOnAdjust(ctx context.Context, resource string) bool {
	slots, probeErr := r.cfg.Adm.Show(ctx, resource)
	if probeErr != nil {
		return false
	}

	for _, slot := range slots {
		if slot.ConnectionState != string(drbd.ConnectionStateStandAlone) {
			continue
		}
		// Recovery-down-reverses disambiguator: a StandAlone slot with
		// NO peer-device entry is a fresh slot that has not completed a
		// connect handshake (post-`drbdadm up` revive window). Treat
		// that as "needs full adjust" so the next `drbdadm adjust`
		// re-issues `drbdsetup connect`. Only StandAlone slots that
		// retain peer-device entries (the operator-disconnect signal)
		// trigger `--skip-net`.
		if len(slot.PeerDevicesByVolNum) == 0 {
			continue
		}

		return true
	}

	return false
}

// isUnknownResourceErr reports whether a drbdadm error is the
// `(158) Unknown resource` failure mode — adjust saw the kernel
// slot vanish between the satellite's probe and adjust's own
// `drbdsetup new-minor` shell-out (Bug 287 / scenario 5.32 race).
// We grep the wrapped error text rather than introducing a typed
// errno because drbdadm surfaces 158 via a textual message; the
// caller's wrap chain already preserves the verbatim stderr from
// `pkg/storage/exec.go`.
//
// Bug 291 (P1): the original predicate also accepted the bare
// substring `"unknown resource"` (case-sensitive but unanchored)
// as a fallback. That substring appears verbatim in DRBD's
// `additional info from kernel: unknown resource` diagnostic — but
// also in unrelated drbdsetup errors (`drbdsetup new-path …
// unknown resource`, `drbdsetup detach … unknown resource`, even
// LINSTOR's `ApiCallRc: unknown resource <name>` when the rest
// adapter surfaces a not-found through the same wrap chain). Any
// of those false-positive matches triggers an unconditional
// `drbdadm up`, which races a partial teardown and leaves kernel
// state half-up; the next reconcile pass loops on the same
// failure mode while peers stay Connecting/StandAlone. Tightened
// to a single canonical regex anchored on the `(158)` errno + the
// `Unknown resource` verb drbdadm-9 emits (verified verbatim
// against `drbdadm adjust` on a slot-less resource).
//
// Phase 11.4.b P1: delegates to the package-level numeric exit-code
// parser (`pkg/drbd.IsErrCode`) so future call sites can switch on
// stable drbdsetup err numbers instead of duplicating ad-hoc string
// regexes. Behaviour is preserved — the previous regex anchored on
// `(158) Unknown resource` and the numeric predicate anchors on the
// `(158)` errno; existing test fixtures cover both.
func isUnknownResourceErr(err error) bool {
	return drbd.IsUnknownResourceErr(err)
}

// seedInitialGI pre-stamps each diskful volume's freshly-created
// DRBD metadata block with the GI the controller picked from an
// UpToDate peer (Phase 8.1). When SeedFromGI is empty (fresh
// cluster, no peer to seed from) the volume is skipped — DRBD will
// fall through to the full initial-sync on first connect, which is
// the acceptable cost for the first replica in a new RD.
//
// Must be called between create-md (which writes the metadata
// block this then mutates) and drbdadm adjust (which reads the
// metadata into kernel state).

func (r *Reconciler) seedInitialGI(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, isWinner bool) error {
	for _, vol := range dr.GetVolumes() {
		device := devices[vol.GetVolumeNumber()]
		if device == "" {
			continue
		}

		seed, ok := r.resolveVolumeSeed(ctx, dr.GetName(), vol, dr.GetPeerHasData(), isWinner, dr.GetSkipInitialSync())
		if !ok {
			continue
		}

		err := r.seedPerPeerGI(ctx, dr, vol, device, seed)
		if err != nil {
			return err
		}
	}

	return nil
}

// seedPerPeerGI stamps the day0 GI tuple into EVERY DRBD-9 v09
// metadata node-id slot (0..drbd.NodeIDMax) for one (resource,
// volume) pair — the local node's own current_uuid slot AND every
// possible peer slot, occupied or not. DRBD 9.2+ stores current/
// bitmap UUIDs per-peer (one slot per node-id) plus the local
// `self` slot; the GI handshake on first connect compares the
// initiating node's local-slot current_uuid against the responder's
// matching peer-slot bitmap_uuid. Any slot we leave un-stamped keeps
// whatever bitmap_uuid was last written there — for a relocated /
// deleted / not-yet-visible peer that is a STALE value from a prior
// incarnation, which the handshake reads as "owe a resync toward
// that slot" → the replica enters SyncTarget for an often-0-byte
// resync that then latches behind the diskless tiebreaker's
// connection dependency and NEVER finishes (the relocate / cold-
// start "0 bits set, SyncTarget forever" stall, Bug 342 family /
// DRBD #40 Mode B).
//
// The fix mirrors upstream LINSTOR's DrbdLayer.createMetaData, which
// after `create-md --max-peers <N>` seeds GI by looping over EVERY
// node-id slot (nodeId = 0..NODE_ID_MAX) and stamps the day0 tuple
// into each — local builder for the local id, "other" builder for
// the rest — regardless of whether a peer currently occupies that
// slot. Blanketing all slots guarantees no slot is ever left with a
// stale per-peer bitmap-UUID, so the handshake can never invent a
// phantom resync.
//
// Upper bound = drbd.NodeIDMax (31), verified empirically on
// drbd-utils 9.22: `drbdmeta ... set-gi --node-id N` succeeds for
// N in 0..31 on a v09 block created with `create-md 15`
// (--max-peers=15) and hard-errors `node-id out of range (0...31)`
// for N>=32. So 0..31 covers every slot create-md sized AND every
// id the allocator (0..MaxPeers-1) could ever hand out, while never
// tripping the out-of-range failure. Stamping the 16..31 slots that
// the allocator never uses is a harmless overwrite of an unused
// zero-GI slot; the safety it buys is that a late-allocated node-id
// (Bug 87) or a peer whose Status.DRBDNodeID is not yet visible at
// apply time can NEVER find its slot left stale.
//
// Bug 284 (local slot): the previous shape looped only over the
// currently-VISIBLE peers (dr.GetPeerNames()) and `continue`d on any
// peer whose node-id wasn't allocated yet — leaving the local
// current_uuid (and every unseen / late peer slot) at the random
// value `drbdadm create-md` generated. When such a peer later joined
// and connected, the handshake compared its day0 current_uuid
// against our random one → `uuid_compare()=unrelated-data` →
// `Unrelated data, aborting!` → permanent StandAlone. The blanket
// loop subsumes the explicit local-slot stamp that fix added and
// extends the guarantee to every slot.
//
// Returns the first non-nil error from drbdmeta. Every call carries
// `--node-id <X>`, so the legacy "set-gi requires --node-id" failure
// mode is structurally unreachable.
//
// One uniform seed per (resource, volume): the SAME GI string is
// stamped into every node-id slot 0..NodeIDMax. This is correct AND
// necessary because of how v09 metadata is laid out (drbd-utils
// m_set_v9_uuid / md_cpu_to_disk_09):
//
//   - the current-UUID and the consistent/up-to-date FLAGS are
//     DEVICE-LEVEL (md->current_uuid, md->flags) — shared, not
//     per-peer. Every `set-gi --node-id N` call rewrites them, so the
//     LAST call wins. Stamping the same current+flags on every slot
//     makes the loop order irrelevant and leaves the device with the
//     intended current+flags (a winner seed with a per-slot-varying
//     current would be silently clobbered by the highest node-id).
//   - only the bitmap-base UUID is PER-PEER (md->peers[N].bitmap_uuid).
//     The day0 seed paths leave it EMPTY, so the uniform stamp writes
//     bitmap-uuid 0x0 into every slot — matching upstream LINSTOR's
//     working-skip metadata (captured via `drbdmeta dump-md`: every
//     per-peer slot bitmap-uuid 0x0, resource skips the resync).
//
// So the winner seed (`current=day0, bitmap-base EMPTY, Consistent,
// UpToDate`) applied uniformly yields: shared current=day0 +
// Consistent+UpToDate flags, and every peer slot's bitmap-uuid 0x0.
// The skip-init-sync seed (`current=day0, bitmap-base EMPTY`, no flags)
// is likewise uniform. A NON-zero bitmap-base (a previous iteration
// stamped day0 there) is read by DRBD as a live out-of-sync anchor and
// triggers a full SyncTarget — the bug this seeding now avoids.
//
// Blanketing all slots (not just dr.GetPeerNames()) is the Bug 284 /
// Bug 342 invariant: it wipes stale per-peer bitmap-UUIDs out of slots
// no currently-visible peer occupies (relocated/deleted prior
// incarnation, not-yet-visible peer, late-allocated node-id) so the
// handshake can never invent a phantom resync. Upper bound NodeIDMax
// (31) is the drbdmeta hard ceiling.
func (r *Reconciler) seedPerPeerGI(ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, device string, seed drbd.GISeed) error {
	// Validate once before touching metadata so a malformed seed (e.g.
	// up-to-date with empty current) fails fast rather than mid-loop
	// with half the slots written.
	validateErr := seed.Validate()
	if validateErr != nil {
		return errors.Wrapf(validateErr, "validate GI seed vol %d", vol.GetVolumeNumber())
	}

	gi := seed.String()

	for nodeID := int32(0); nodeID <= drbd.NodeIDMax; nodeID++ {
		err := r.cfg.Adm.SetGIString(ctx, dr.GetName(), vol.GetVolumeNumber(), device, nodeID, gi)
		if err != nil {
			return errors.Wrapf(err, "set-gi vol %d node-id %d",
				vol.GetVolumeNumber(), nodeID)
		}
	}

	return nil
}

// localNodeIDFromOpts extracts this satellite's own DRBD node-id
// from the DesiredResource's flat DrbdOptions bag. The dispatcher
// writes `node-id` (no peer prefix) for the target replica from
// `Resource.Status.DRBDNodeID`. Returns ok=false when the entry is
// missing / malformed — callers then skip the local-slot stamp;
// DRBD falls through to a real initial-sync, slow but correct.
func localNodeIDFromOpts(dr *intent.DesiredResource) (int32, bool) {
	raw, ok := dr.GetDrbdOptions()["node-id"]
	if !ok || raw == "" {
		return 0, false
	}

	id, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false
	}

	return int32(id), true
}

// errUnresolvedLocalNodeID is the sentinel returned by
// refuseUnresolvedLocalNodeID when the controller has not yet
// allocated this node's DRBD node-id. It is wrapped into the
// per-resource Apply result so runApply requeues (applyFailureRequeue)
// rather than treating it as a hard, non-retryable failure — the id
// always arrives on a subsequent reconcile once the controller's
// allocator stamps Status.DRBDNodeID.
var errUnresolvedLocalNodeID = errors.New("local DRBD node-id not yet allocated by controller")

// refuseUnresolvedLocalNodeID is the Bug 360 prevention gate: it
// returns errUnresolvedLocalNodeID when the DesiredResource carries no
// resolvable local `node-id` in DrbdOptions. The dispatcher OMITS that
// key (rather than rendering an ambiguous `node-id 0`) whenever the
// controller has not yet stamped this node's Status.DRBDNodeID, so an
// absent/malformed key is the unambiguous "unresolved" signal.
//
// Callers MUST invoke this before ANY verb that renders the .res or
// loads it into the kernel (create-md, drbdadm up, adjust). Letting
// create-md run with a zero-defaulted id permanently burns `node-id 0`
// into the on-disk v09 metadata; once the kernel my-id is later fixed,
// `drbdsetup attach` fails `(119) ambiguous node id` forever. Refusing
// up-front — and requeuing — is the only no-data-loss prevention.
//
// A present `node-id 0` is honoured: 0 is a legitimate allocation
// (LowestFreeNodeID hands out the lowest free id, which is 0 for the
// first/freed slot), and localNodeIDFromOpts distinguishes a present
// "0" (ok=true) from an absent/empty key (ok=false).
func refuseUnresolvedLocalNodeID(dr *intent.DesiredResource) error {
	if _, ok := localNodeIDFromOpts(dr); !ok {
		return errUnresolvedLocalNodeID
	}

	return nil
}

// errUnresolvedSkipInitialSync is the sentinel returned by
// refuseUnresolvedSkipInitialSync when the controller has not yet
// stamped Resource.Spec.SkipInitialSync. Wrapped into the per-resource
// Apply result so runApply requeues (applyFailureRequeue) — the stamp
// always arrives on a subsequent reconcile once the controller's
// allocateResourceSpecFields / ensureSkipInitSyncDecision pass commits
// it. Mirrors errUnresolvedLocalNodeID.
var errUnresolvedSkipInitialSync = errors.New("Resource.Spec.SkipInitialSync not yet stamped by controller")

// refuseUnresolvedSkipInitialSync is the skip-init-sync prevention
// gate: it returns errUnresolvedSkipInitialSync when this satellite is
// about to seed a fresh diskful replica's metadata (freshActivation)
// but the controller has not yet stamped Resource.Spec.SkipInitialSync
// (GetSkipInitialSync() == nil).
//
// SkipInitialSync is a controller-allocated, append-only Spec field
// (set to !RD.Spec.Initialized once at creation), exactly like the DRBD
// node-id / port / minor the satellite already waits for before
// bring-up. The seed decision in resolveVolumeSeed is undefined until
// it lands: a nil read forces the conservative "refuse every day0 skip"
// branch, which on a genuinely-fresh deployment elects NO UpToDate
// winner and deadlocks both diskful replicas Inconsistent. Gating the
// seed until the flag is non-nil removes the nil read entirely.
//
// Caller MUST gate this only on the fresh-diskful-first-activation case
// (freshActivation && !diskless): a diskless replica carries no
// metadata to seed, and a replica whose metadata already exists has
// already committed its seed decision — neither must be stalled.
//
// A present false (controller decided "must SyncTarget") is honoured:
// GetSkipInitialSync() returns a non-nil *bool for both true and false,
// so the gate distinguishes "stamped false" (proceed → SyncTarget) from
// "not stamped yet" (requeue).
func refuseUnresolvedSkipInitialSync(dr *intent.DesiredResource, freshActivation bool) error {
	if freshActivation && dr.GetSkipInitialSync() == nil {
		return errUnresolvedSkipInitialSync
	}

	return nil
}

// gateBringUpReadiness refuses (requeues) the DRBD apply while a
// controller-allocated per-replica Spec field the satellite depends on
// is still unstamped. Extracted out of applyDRBD so the orchestrator
// stays under the gocyclo budget; runs the two prevention gates in
// order, each surfacing its own sentinel + log line.
//
// Gate 1 (Bug 360, node-id): refuse the entire first-activation — and
// every other .res-consuming verb (createMd, up, adjust) — while the
// controller has NOT yet allocated this node's DRBD node-id. The
// dispatcher omits the `node-id` key from DrbdOptions when the local
// Status.DRBDNodeID is still nil, so an absent key is the unambiguous
// "unresolved" signal (a present "0" is a legitimate allocation from
// LowestFreeNodeID and must be honoured). This gate exists even though
// waitForControllerAllocation already gates runApply: that
// controller-side gate has been observed to let a premature Apply
// through during the auto-place initial-create burst (stale informer
// cache / sibling-Watches-driven reconcile before the local Status
// patch lands). If create-md runs with my-node-id 0 it burns the kernel
// slot my-id (self-healed by reconcileKernelMyNodeID) AND —
// irrecoverably without data loss — the on-disk v09 metadata, which
// records `node-id 0`; `drbdsetup attach` then fails `(119) ambiguous
// node id` forever. The only safe cure is to never let create-md/up run
// before the allocated id is known.
//
// Gate 2 (skip-init-sync): refuse a fresh diskful replica's
// first-activation metadata seed while the controller has NOT yet
// stamped Resource.Spec.SkipInitialSync. The satellite reconciles on
// its own controller-runtime Watch, independently of the controller's
// allocate→stamp pass, so on a fresh deployment it can observe a
// Resource whose node-id/port are already stamped (Gate 1 passes) but
// whose SkipInitialSync is still nil — the allocateResourceSpecFields
// stamp lags when the parent RD's `initialized` latch isn't observable
// yet, and ensureSkipInitSyncDecision lands it only on a later
// controller pass. If the seed runs in that window, resolveVolumeSeed
// reads nil → REFUSES every day0 skip (case A AND the case-B winner
// UpToDate seed) → NO replica is seeded as the UpToDate winner → both
// diskful replicas come up Inconsistent, each PausedSyncT waiting on the
// other with no sync source → permanent deadlock. Requeuing here until
// the flag is non-nil removes the nil read entirely: resolveVolumeSeed
// is only ever reached once the controller has committed a real
// true/false decision, so the PR #20 winner-election path fires under
// SkipInitialSync==true and the offline-safe SyncTarget path fires under
// false.
//
// Gate 2 scope is minimal — never stalls a converged or non-seeding
// replica: diskless replicas carry no metadata to seed (never gated),
// and a replica whose metadata already exists (firstActivation=false:
// MetadataCreated Condition stamped or `.md-created` marker present) has
// already taken its seed decision (never gated), so steady-state
// reconciles and the diskless→diskful flip path are untouched. Only the
// fresh-diskful-first-activation case — the exact case whose seed
// decision resolveVolumeSeed makes — waits for the stamp.
func (r *Reconciler) gateBringUpReadiness(ctx context.Context, dr *intent.DesiredResource, diskless bool, mdMarkerPath string) error {
	err := refuseUnresolvedLocalNodeID(dr)
	if err != nil {
		log.FromContext(ctx).Info("Bug 360: deferring DRBD bring-up until controller allocates local node-id",
			"resource", dr.GetName())

		return err
	}

	if diskless {
		return nil
	}

	_, mdStatErr := os.Stat(mdMarkerPath)
	freshActivation := !dr.GetMetadataCreated() && os.IsNotExist(mdStatErr)

	err = refuseUnresolvedSkipInitialSync(dr, freshActivation)
	if err != nil {
		log.FromContext(ctx).Info("skip-init-sync: deferring fresh-replica DRBD seed until controller stamps Spec.SkipInitialSync",
			"resource", dr.GetName())

		return err
	}

	return nil
}

// resolveVolumeSeed decides the single GI seed to stamp uniformly into
// every per-node-id metadata slot of a fresh replica so the resource
// reaches its initial state WITHOUT a runtime `drbdadm primary
// --force` (which mints a divergent current-UUID and forces a full
// initial sync). The seed's device-level current/flags and per-peer
// bitmap-base are applied to all slots (see seedPerPeerGI for why
// uniform is both correct and required). It maps onto the spec cases:
//
//   - peerHasData → no seed (ok=false). Bug 342 data-integrity gate:
//     a fresh replica joining an RD where a diskful peer already holds
//     committed data MUST come up Inconsistent and SyncTarget from that
//     peer. Seeding any GI here lets DRBD's handshake skip the resync
//     against a peer whose current-UUID is unrelated → `uuid_compare()=
//     unrelated-data` → permanent StandAlone (the relocate / physical
//     `r d` then `r c` case). Checked FIRST; wins over every branch,
//     including the winner branch.
//
//   - Controller-supplied SeedFromGI (Phase 8.1: copied from an
//     existing UpToDate peer) → `current = bitmap = seed`, no flags.
//     DRBD's handshake sees a true match and skips the sync. Preferred
//     over the winner branch because a real peer GI is a stronger
//     lineage anchor than the synthetic day0.
//
//   - isWinner (case B: this node is the elected initial-UpToDate
//     source and the volume is not yet initialized) → `current =
//     bitmap = day0`, Consistent + UpToDate. The flags make the
//     kernel read the replica as UpToDate after adjust — UpToDate
//     purely from metadata, no promote (that promote is the bug; it
//     minted a divergent current-UUID).
//
//     The current-UUID is day0 (NOT a fresh random GID): the winner
//     stays on the shared day0 lineage so (a) a staggered late replica
//     observing this winner's CurrentGI == day0 is recognised by the
//     seed-safety gate as a fresh day0 sibling (dispatcher
//     isDay0SeededVolume) and may take its OWN day0 skip instead of a
//     needless SyncTarget; and (b) a dual-winner election race produces
//     two identical day0 generations that agree at handshake rather
//     than two divergent random siblings that split-brain. Relocate
//     stays safe because any real write advances the source's current
//     past day0 (normal DRBD behaviour), after which the gate counts it
//     as data-bearing and a new replica correctly SyncTargets.
//
//   - Else skip-init-sync (case A: thin-backed or all-ZFS, force-
//     initial-sync not requested) → `current = bitmap = day0`, no
//     flags. Both peers present equal current-UUIDs with clean bitmaps
//     → DRBD reads "no data difference" → no sync; both reach UpToDate
//     at the kernel handshake. (Preserves blockstor's existing,
//     stand-validated skip-init-sync shape.) GATED on the kernel-truth
//     AnyConnectedPeerHasData probe: a migrate/relocate destination
//     flips diskless→diskful on a slot that is ALREADY connected to an
//     UpToDate peer, and since PR #20 that peer sits at day0 so the
//     CRD-status PeerHasData gate can't see it as data-bearing — the
//     probe catches it from kernel state and refuses the skip (→ case C
//     SyncTarget). On a fresh replica the slot is not connected yet at
//     seed time, so the probe finds nothing and the skip proceeds.
//
//   - Else (case C: thick LVM, opaque file, unknown provider, non-
//     winner, OR a flip dst with a connected data peer) → ok=false. The
//     freshly-created Inconsistent metadata is left untouched so this
//     replica resyncs from the source.
//
// Race-free: peerHasData is recomputed by the dispatcher every
// reconcile from live peer status, not a controller stamp that must
// land in Spec in time; the case-A AnyConnectedPeerHasData gate reads
// live kernel state, immune to the day0 CRD-classification ambiguity.
func (r *Reconciler) resolveVolumeSeed(ctx context.Context, resourceName string, vol *intent.DesiredVolume, peerHasData, isWinner bool, skipInitialSync *bool) (drbd.GISeed, bool) {
	if peerHasData {
		return drbd.GISeed{}, false
	}

	// Skip-init-sync hardening — the AUTHORITATIVE, controller-decided,
	// persisted, OFFLINE-SAFE gate (replaces the live-kernel probe that
	// follows as authority). The controller stamps Resource.Spec.
	// SkipInitialSync = !RD.Spec.Initialized ONCE at creation:
	//
	//   - nil  → not yet stamped (or a pre-upgrade CRD). Conservative
	//     default: REFUSE every day0 skip (case A AND the case-B winner
	//     UpToDate seed). The replica comes up Inconsistent and either
	//     SyncTargets a peer or waits — never falsely UpToDate-empty. A
	//     genuinely-fresh replica re-reconciles within ~1s once the
	//     controller stamps true, then takes the skip. Invariant 5.
	//   - false → controller decided this replica MUST sync (relocate /
	//     migrate-disk / extra replica joining an already-initialized
	//     RD). REFUSE the skip → SyncTarget. This holds EVEN IF the sole
	//     data-holder is offline at seed time, because the decision was
	//     read from the persisted RD.Spec.Initialized latch, not from
	//     live kernel/peer state — the core offline-safety fix.
	//   - true  → controller decided this replica may skip (fresh initial
	//     set). Fall through to the winner / day0 skip seeds below.
	//
	// The SeedFromGI case (an explicit GID the controller copied from a
	// real UpToDate peer) is NOT gated here: it anchors against a live
	// peer's real generation and is only ever stamped when no data-
	// bearing peer exists, so it remains the legitimate Phase 8.1
	// relocate-skip path.
	skipAllowed := skipInitialSync != nil && *skipInitialSync

	day0 := day0GiFor(resourceName, vol.GetVolumeNumber())

	if seed := vol.GetSeedFromGI(); seed != "" {
		// Phase 8.1 relocate-skip: the controller copied this GID from a
		// real UpToDate peer. current == bitmap-base == peer's GID means
		// "I am at the peer's generation and have no dirty bits relative
		// to it" — the long-standing, separately-validated seed-from-
		// real-peer shape (Adm.SetGI). This is NOT the fresh day0 skip
		// path; it anchors against a live peer's real generation, so the
		// bitmap-base carries that generation rather than being empty.
		return drbd.GISeed{Current: seed, BitmapBase: seed}, true
	}

	if isWinner && skipAllowed {
		// Winner seed, matched byte-for-byte to upstream LINSTOR's
		// working-skip metadata captured live via `drbdmeta dump-md` on
		// a piraeus thin resource that reaches UpToDate with ZERO
		// resync:
		//
		// Gated on skipAllowed: the case-B winner is the elected
		// initial-UpToDate source, but it must only mint the day0
		// Consistent+UpToDate metadata when the controller has confirmed
		// this is a genuinely-fresh RD (Spec.SkipInitialSync==true). If
		// the controller said "must sync" (false) or hasn't stamped yet
		// (nil), the winner falls through to case C (Inconsistent) and
		// reaches UpToDate only via a real handshake/resync — so an
		// election that races an offline data-holder can never declare
		// the empty winner authoritatively UpToDate.
		//
		//   current-uuid <day0>; flags Consistent+UpToDate;
		//   EVERY peer slot bitmap-uuid 0x0  (clean — NOT day0).
		//
		// The decisive field is the per-peer BITMAP-BASE, left EMPTY
		// (→ drbdmeta writes bitmap-uuid 0x0). DRBD's connection
		// handshake reads a zero bitmap-uuid as "no out-of-sync bits
		// relative to this peer", so with both replicas sharing the
		// same current-uuid and a clean (zero) bitmap, it concludes
		// "nothing to copy" and both go straight to UpToDate.
		//
		// The earlier shape stamped bitmap-base = day0 (a NON-zero
		// value equal to current) into every slot. DRBD then read a
		// non-zero bitmap-base as a live out-of-sync bitmap anchor and
		// flipped the peer to SyncTarget for a full ~1 GiB resync of an
		// empty thin volume — the exact bug. (Captured: ours had
		// bitmap-uuid=day0 in every slot and SyncTargeted; upstream had
		// bitmap-uuid=0x0 in every slot and skipped, same kernel.)
		//
		// current-uuid stays day0 (the shared deterministic lineage
		// anchor) — NOT a fresh random GID — so the dispatcher's
		// isDay0SeededVolume discriminator still recognises this winner
		// as a fresh day0 sibling (staggered late replica may take its
		// own skip; a dual-winner race agrees rather than split-brains).
		// Upstream uses a shared random current; day0 is the
		// blockstor-equivalent shared value and works identically at the
		// handshake (both replicas present the same current-uuid).
		return drbd.GISeed{
			Current:    day0,
			Consistent: true,
			UpToDate:   true,
		}, true
	}

	provider, ok := r.cfg.Providers[vol.GetStoragePool()]
	if !ok || provider == nil {
		return drbd.GISeed{}, false
	}

	if !IsThinOrZFS(provider.Kind()) {
		return drbd.GISeed{}, false
	}

	// Skip-init-sync hardening — AUTHORITATIVE gate (case A). The
	// controller-decided, persisted, offline-safe Spec.SkipInitialSync
	// is the source of truth: skip ONLY when it explicitly says true.
	// nil (unstamped / pre-upgrade) and false (relocate / migrate-disk /
	// extra replica joining an already-initialized RD) both REFUSE the
	// skip → the replica stays Inconsistent and SyncTargets the real
	// data, even when the sole data-holder is offline at seed time
	// (because the decision came from the persisted RD latch, not live
	// peer/kernel state). This closes the unsafe-probe hole the
	// kernel-truth backstop below could not: the probe only sees
	// CONNECTED peers, so an offline data-holder made it return false →
	// a fresh replica wrongly skipped → falsely UpToDate while empty.
	if !skipAllowed {
		return drbd.GISeed{}, false
	}

	// Migrate / relocate destination gate (kernel-truth backstop;
	// DEFENSE-IN-DEPTH, no longer the authoritative skip decision —
	// Spec.SkipInitialSync above owns that). Retained as a secondary
	// veto: if a peer connection already exposes committed data the
	// instant we seed, refuse the skip regardless of the Spec flag.
	//
	// The day0 skip-init-sync seed below is ONLY data-safe when this
	// replica is part of a genuinely-fresh resource — no peer is an
	// established UpToDate authority that holds the real (even if empty)
	// copy this replica must derive from. A diskless→diskful flip (the
	// `linstor r td --migrate-from` / relocate destination: a replica
	// that was a connected diskless client and is now attaching a fresh
	// lower disk) joins a resource whose Primary peer is ALREADY UpToDate.
	//
	// Since PR #20 the elected winner reaches UpToDate via `set-gi` at
	// current-uuid == day0 and STAYS there (no runtime write advances it
	// on an empty thin/ZFS volume, and `primary --force` mints no new UUID
	// on an already-UpToDate node — verified on DRBD 9.3.2 via get-gi:
	// the Primary source sits at `<day0>:0:0:0:1:1`). So the CRD-status
	// gate (dispatcher.anyDiskfulPeerHasData → PeerHasData) can no longer
	// tell that data-bearing day0 Primary apart from a never-written day0
	// sibling: isDay0SeededVolume sees CurrentGI == day0 on BOTH and
	// classifies the real authority as a fresh sibling → PeerHasData is
	// false → control reaches here. Stamping the day0 skip seed then gives
	// the destination a clean (zero) bitmap + no UpToDate flag, so DRBD
	// has neither out-of-sync bits to drive a SyncTarget nor a metadata
	// flag to declare it UpToDate → it latches Inconsistent forever (the
	// lifecycle-toggle-migrate regression).
	//
	// Use kernel truth, immune to the day0 ambiguity: if a peer connection
	// already exposes committed data (peer-disk UpToDate/Consistent/
	// Outdated), REFUSE the skip and leave the freshly-created metadata
	// Inconsistent so DRBD runs a real SyncTarget from that peer (case C).
	// On a genuinely-fresh replica the kernel slot is NOT connected yet at
	// seed time (the connection establishes during the later `drbdadm
	// adjust`), so the probe finds nothing and the skip proceeds — the
	// fresh 2-/3-replica skip-init-sync is preserved. Mirrors the
	// shouldForcePromote AnyConnectedPeerHasData backstop.
	// Bug B.4: per-volume probe. The RD-scoped variant returns
	// true if ANY peer-device on the resource is UpToDate; for a
	// `vd c` adding vol-1 to an RD whose vol-0 is already
	// UpToDate on both peers, the RD-scoped check would refuse
	// the seed for vol-1 even though vol-1's peer-devices are
	// Inconsistent. Per-volume scoping surfaces the truth about
	// the NEW volume's peer state in isolation from any
	// already-UpToDate sibling volumes.
	if r.cfg.Adm.AnyConnectedPeerHasDataForVolume(ctx, resourceName, vol.GetVolumeNumber()) {
		return drbd.GISeed{}, false
	}

	// Skip-init-sync (case A): current = day0 (shared lineage anchor),
	// bitmap-base EMPTY (zero) — same clean-bitmap shape as the winner,
	// minus the Consistent/UpToDate flags. Both peers present the same
	// current-uuid with a zero bitmap → DRBD reads "no data difference"
	// → no sync; both reach UpToDate at the kernel handshake. (Stamping
	// day0 into bitmap-base here was the same SyncTarget-triggering bug
	// the winner branch documents.)
	return drbd.GISeed{Current: day0}, true
}

// providerForResource resolves the provider that owns the named
// resource using the in-memory pool map. Returns an error when the
// resource isn't known or its pool isn't registered.
func (r *Reconciler) providerForResource(name string) (storage.Provider, error) {
	r.mu.Lock()
	pool, ok := r.resourceToPool[name]
	r.mu.Unlock()

	if !ok {
		return nil, errors.Errorf("resource %q not known on this satellite", name)
	}

	provider, ok := r.cfg.Providers[pool]
	if !ok {
		return nil, errors.Errorf("storage pool %q not registered", pool)
	}

	return provider, nil
}

// rememberPool records the pool that backs a resource, so subsequent
// snapshot RPCs can route to the right provider. Multi-pool resources
// are not yet a thing — we record the first volume's pool only.
func (r *Reconciler) rememberPool(resourceName, pool string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resourceToPool[resourceName] = pool
}

// buildResFile assembles a drbd.Resource from dr's flat option map.
// The proto carries DRBD config as a string→string map for now (the
// schema solidifies once the controller-side autoplacer feeds it); we
// honour the documented keys: `port`, `node-id`, `address`, `minor` for
// the local node, and `peer.<name>.{port,node-id,address}` per peer.
//
// localAddr is the satellite's own IP — when the controller-supplied
// `address` is the placeholder "0.0.0.0" we substitute localAddr so
// drbd-9 has a real interface to bind to.
//
// devices is volNumber → DevicePath; when present we use it as the
// disk path. Empty / missing → fall back to the LVM/ZFS-shaped
// `/dev/<pool>/<rd>_<vol>` guess, which is what works for those
// providers.
func buildResFile(dr *intent.DesiredResource, localNode, localAddr string, devices map[int32]string, autoDisk map[string]string) (string, error) {
	opts := dr.GetDrbdOptions()
	port, _ := strconv.Atoi(opts["port"])
	nodeID, _ := strconv.Atoi(opts["node-id"])
	minor, _ := strconv.Atoi(opts["minor"])

	hosts := make([]drbd.Host, 0, 1+len(dr.GetPeers()))
	hosts = append(hosts, drbd.Host{
		NodeName: localNode,
		Address:  resolveAddr(opts["address"], localAddr),
		Port:     port,
		NodeID:   nodeID,
		IsLocal:  true,
		Diskless: isDiskless(dr.GetFlags()),
	})

	for _, peer := range dr.GetPeerNames() {
		peerPort, _ := strconv.Atoi(opts["peer."+peer+".port"])
		peerNodeID, _ := strconv.Atoi(opts["peer."+peer+".node-id"])

		hosts = append(hosts, drbd.Host{
			NodeName: peer,
			Address:  resolveAddr(opts["peer."+peer+".address"], ""),
			Port:     peerPort,
			NodeID:   peerNodeID,
			Diskless: opts["peer."+peer+".diskless"] == drbdBoolPropTrue,
		})
	}

	vols := buildResVolumes(dr, devices, minor)

	sections := splitDRBDOptions(opts)
	mergeAutoDiskOptions(sections.Disk, autoDisk)

	out, err := drbd.Build(drbd.Resource{
		Name:        dr.GetName(),
		Net:         drbd.Net{ProtocolC: true, Options: sections.Net},
		Hosts:       hosts,
		Volumes:     vols,
		Options:     sections.Resource,
		Disk:        sections.Disk,
		Handlers:    sections.Handlers,
		PeerDevice:  sections.PeerDevice,
		Connections: buildResConnections(dr),
	})
	if err != nil {
		return "", errors.Wrap(err, "drbd.Build")
	}

	return out, nil
}

// buildResConnections translates the DesiredResource's logical
// connection overrides (scenario 3.7 multi-path) into the .res
// renderer's drbd.ResourceConnection shape. Empty input returns nil
// — the renderer then falls back to the default single-host-pair
// connection block.
func buildResConnections(dr *intent.DesiredResource) []drbd.ResourceConnection {
	src := dr.GetConnections()
	if len(src) == 0 {
		return nil
	}

	out := make([]drbd.ResourceConnection, 0, len(src))

	for _, conn := range src {
		paths := make([]drbd.ResourcePath, 0, len(conn.Paths))

		for _, p := range conn.Paths {
			paths = append(paths, drbd.ResourcePath{
				Name:     p.Name,
				AddressA: p.AddressA,
				AddressB: p.AddressB,
			})
		}

		out = append(out, drbd.ResourceConnection{
			NodeA: conn.NodeA,
			NodeB: conn.NodeB,
			Paths: paths,
		})
	}

	return out
}

// buildResVolumes turns the per-RD DesiredVolumes into the
// drbd.Volume slice the .res renderer consumes. Pulled out of
// buildResFile to keep that function under the funlen budget.
//
// `minor` is the base /dev/drbd<N> minor for the resource; each
// volume offsets from it (volumeNumber 0 → minor, vol 1 → minor+1,
// …). The disk path follows applyStorage's devices map when set
// (the kernel actually opens that path); empty falls through to
// the LVM/ZFS-shaped `/dev/<pool>/<rd>_<vol5digits>` guess so
// providers that don't surface a devicePath still get a working
// .res. The meta-disk path is the scenario 6.18
// `StorPoolNameDrbdMeta` carve — see Volume.MetaPool godoc.
// volMinorOrBase returns the volume's authoritative per-volume minor
// (DesiredVolume.Minor, sourced from RD.Spec.VolumeDefinitions), or
// the legacy base+volumeNumber derivation when the controller hasn't
// stamped a per-volume minor yet (mid-upgrade / in-flight reconcile).
func volMinorOrBase(vol *intent.DesiredVolume, base int) int {
	if m := int(vol.GetMinor()); m != 0 {
		return m
	}

	return base + int(vol.GetVolumeNumber())
}

func buildResVolumes(dr *intent.DesiredResource, devices map[int32]string, minor int) []drbd.Volume {
	vols := make([]drbd.Volume, 0, len(dr.GetVolumes()))

	for _, vol := range dr.GetVolumes() {
		disk := devices[vol.GetVolumeNumber()]
		if disk == "" {
			disk = fmt.Sprintf("/dev/%s/%s_%05d", vol.GetStoragePool(), dr.GetName(), vol.GetVolumeNumber())
		}

		// External-metadata path (scenario 6.18). When MetaPool is set
		// we emit `meta-disk <path>;` against a sibling LV named
		// `<rd>_<vol5digits>_meta` on that pool. Path shape matches
		// the data volume's LVM/ZFS guess — `/dev/<pool>/<lv>` — so
		// the renderer doesn't need a second devices-map round trip.
		// applyStorage carves the LV (or its provider equivalent)
		// before this renders, so the file is always rendered with a
		// path that resolves on disk; create-md fails fast otherwise.
		metaDisk := ""
		if mp := vol.GetMetaPool(); mp != "" {
			metaDisk = fmt.Sprintf("/dev/%s/%s_%05d_meta", mp, dr.GetName(), vol.GetVolumeNumber())
		}

		// Per-volume minor is authoritative (RD.Spec.VolumeDefinitions
		// → DesiredVolume.Minor). Fall back to base+volumeNumber only
		// when the controller hasn't stamped a per-volume minor yet
		// (mid-upgrade / in-flight reconcile).
		volMinor := int(vol.GetMinor())
		if volMinor == 0 {
			volMinor = minor + int(vol.GetVolumeNumber())
		}

		vols = append(vols, drbd.Volume{
			Number:   int(vol.GetVolumeNumber()),
			Device:   fmt.Sprintf("/dev/drbd%d", volMinor),
			Disk:     disk,
			MetaDisk: metaDisk,
			Minor:    volMinor,
		})
	}

	return vols
}

// drbdOptionSections holds the per-section maps splitDRBDOptions
// produces. Each map corresponds to one `.res` block; the renderer
// consumes them in writeNet / writeOptions / writeNamedBlock /
// per-connection disk{}. See SectionFor for the routing decision.
type drbdOptionSections struct {
	Net        map[string]string
	Resource   map[string]string
	Disk       map[string]string
	PeerDevice map[string]string
	Handlers   map[string]string
}

// splitDRBDOptions partitions the satellite-received drbd_options bag
// into per-section maps. Per-replica wiring (port/node-id/peer.*.…)
// is dropped — those are not user-tunable knobs.
//
// Routing uses `drbd.SectionFor`, which maps each
// `DrbdOptions/<Section>/<Key>` to the right `.res` block:
//
//   - `DrbdOptions/Net/*`     → `net { }`         (Net)
//   - `DrbdOptions/Disk/*`    → `disk { }`        at resource scope
//   - `DrbdOptions/Handlers/*` → `handlers { }`   at resource scope
//   - `DrbdOptions/PeerDevice/*` → `disk { }`     inside each connection
//   - `DrbdOptions/Resource/*` (and unknown sections) → `options { }`
//     (drbd's catch-all top-level block)
//
// The renderer writes the keys verbatim with the
// `DrbdOptions/<Section>/` prefix stripped — that's the form drbdadm
// expects.
//
// Section-less keys (`DrbdOptions/<Key>` with nothing after the
// prefix beyond a single segment) are LINSTOR-controller-only props
// — e.g. `DrbdOptions/AutoEvictAllowEviction` is consumed by the
// LINSTOR controller's auto-eviction logic, NOT by DRBD. Writing
// those into the .res file makes drbdadm fail with "expected:
// cpu-mask | on-no-data-accessible | ... but got: <name>". Drop
// them on the satellite side; the convention upstream is the same.
//
// Bug 258: prior to this routing rewrite, `Disk`, `Handlers` and
// `PeerDevice` keys all collapsed onto the resource-level options{}
// map, where drbd-9 rejected them at parse time ("expected: …
// got: on-io-error") — wedging the reconciler on any
// `linstor rd sp <rd> DrbdOptions/Disk/on-io-error detach` (a common
// operator action).
func splitDRBDOptions(opts map[string]string) drbdOptionSections {
	out := drbdOptionSections{
		Net:        map[string]string{},
		Resource:   map[string]string{},
		Disk:       map[string]string{},
		PeerDevice: map[string]string{},
		Handlers:   map[string]string{},
	}

	for key, value := range opts {
		rest, ok := strings.CutPrefix(key, drbd.PropPrefix)
		if !ok {
			continue
		}

		_, rawKey, hasSection := strings.Cut(rest, "/")
		if !hasSection {
			// LINSTOR-only key (no DRBD section subpath); these
			// don't belong in the rendered .res. See doc comment.
			continue
		}

		switch drbd.SectionFor(key) {
		case drbd.SectionNet:
			out.Net[rawKey] = value
		case drbd.SectionDisk:
			out.Disk[rawKey] = value
		case drbd.SectionPeerDevice:
			out.PeerDevice[rawKey] = value
		case drbd.SectionHandlers:
			out.Handlers[rawKey] = value
		default:
			// SectionOptions — drbd's catch-all top-level block.
			// Covers `DrbdOptions/Resource/*` plus any unknown
			// section so a future upstream key still lands
			// somewhere sensible (matches SectionFor's fallback).
			out.Resource[rawKey] = value
		}
	}

	return out
}

// mergeAutoDiskOptions folds the satellite-derived thin-aware-resync
// disk options (autoDiskOptions: rs-discard-granularity /
// discard-zeroes-if-aligned) into the operator-supplied `disk { }`
// section. An OPERATOR-set value ALWAYS wins — if the operator pinned
// `DrbdOptions/Disk/rs-discard-granularity` (already present in dst
// after splitDRBDOptions), we leave it untouched. The auto value only
// fills keys the operator left unset. Nil/empty auto map is a no-op.
//
// Operator-override precedence mirrors upstream LINSTOR, where the
// auto-* managers (CtrlRscDfnApiCallHelper) skip a volume whose
// rs-discard-granularity was set by hand.
func mergeAutoDiskOptions(dst, auto map[string]string) {
	for k, v := range auto {
		if _, set := dst[k]; set {
			continue
		}

		dst[k] = v
	}
}

// drbdAddrPlaceholder is what the controller stamps on a Resource
// before it learns each satellite's pod IP — `resolveAddr`
// substitutes the satellite's own IP whenever it sees this value.
const drbdAddrPlaceholder = "0.0.0.0"

// drbdBoolPropTrue mirrors dispatcher.boolPropTrue — the literal
// `true` the dispatcher stamps on flag-like drbd_options keys. We
// inline rather than re-export to keep `pkg/satellite` from
// importing `pkg/dispatcher` just for one constant.
const drbdBoolPropTrue = "true"

// skipDiskPropKey and skipDiskPropValue mirror upstream linstor's
// `ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_SKIP_DISK`
// and `ApiConsts.VAL_TRUE` constants. Scenario 5.11: the
// satellite-side observer stamps `DrbdOptions/SkipDisk=True` onto
// Resource.Spec.Props when the kernel reports `disk:Failed`; this
// reconciler reads the prop and gates `drbdadm adjust --skip-disk`
// onto its presence. Constants kept here (rather than re-exported
// from `pkg/satellite/controllers`) so the reconciler's gate
// doesn't pick up a controllers-package import cycle.
const (
	skipDiskPropKey   = "DrbdOptions/SkipDisk"
	skipDiskPropValue = "True"
)

// isSkipDiskEnabled reports whether the observer (or an operator
// via `linstor r sp <n> <r> DrbdOptions/SkipDisk True`) has marked
// this replica's lower disk as failed. The check covers both
// landing spots:
//
//   - `dr.DrbdOptions`: the dispatcher pulls every `DrbdOptions/...`
//     key out of `Spec.Props` and folds it into the per-replica
//     DrbdOptions bag before calling Apply. The production path
//     therefore reads the prop from here.
//   - `dr.Props`: the satellite reconciler unit tests build
//     DesiredResource directly without running through the
//     dispatcher's split; tests that pin the SkipDisk gate need
//     a shape that doesn't require re-implementing dispatcher
//     internals.
//
// Case-insensitive compare to mirror upstream's
// `VAL_TRUE.equalsIgnoreCase` so operators who set the prop
// manually with lower-case "true" get the same behaviour the
// observer's canonical "True" produces.
func isSkipDiskEnabled(dr *intent.DesiredResource) bool {
	if strings.EqualFold(dr.GetDrbdOptions()[skipDiskPropKey], skipDiskPropValue) {
		return true
	}

	return strings.EqualFold(dr.GetProps()[skipDiskPropKey], skipDiskPropValue)
}

// resolveAddr substitutes the satellite's own IP whenever the
// controller-supplied address is the placeholder (which it is until
// the controller starts learning each satellite's pod IP and passing
// it down). Empty fallback returns the placeholder unchanged so unit
// tests don't blow up the way a missing override would.
func resolveAddr(supplied, fallback string) string {
	if supplied == "" || supplied == drbdAddrPlaceholder {
		if fallback != "" {
			return fallback
		}
	}

	return supplied
}

// isInactive returns true when the operator has called
// `linstor r deactivate` on this replica. The reconciler keeps
// storage and the .res file intact and just drops the kernel
// resource via `drbdadm down`. Activation reverses it without
// losing port/node-id allocations.
func isInactive(flags []string) bool {
	return slices.Contains(flags, "INACTIVE")
}

// isDiskless returns true when the DRBD-layer "DISKLESS" flag is set.
// Diskless replicas live entirely in DRBD memory and have no backing
// storage, so the reconciler must skip the storage path for them.
func isDiskless(flags []string) bool {
	return slices.Contains(flags, "DISKLESS")
}

// buildVolumeResults assembles per-volume devicePath entries for
// the ResourceApplyResult, choosing the path the consumer should
// see:
//
//   - When DRBD is in the layer stack, the consumer-facing device
//     is `/dev/drbd<minor>` regardless of the lower-disk path
//     (loop/LV/zvol/dm-crypt). drbdMinor + volumeNumber follow
//     the dispatcher's per-replica allocation.
//   - When DRBD is not in the stack (LayerStack=["STORAGE"] or
//     ["LUKS","STORAGE"]), the consumer sees the raw storage /
//     dm-crypt device — that's exactly what `devices` already
//     holds after applyStorage + maybeLUKS.
//   - DISKLESS replicas have no consumer-facing device; we emit
//     no Volumes entries.
func buildVolumeResults(dr *intent.DesiredResource, devices map[int32]string, diskless, withDRBD bool) []*intent.ResourceApplyVolumeResult {
	if diskless {
		return nil
	}

	out := make([]*intent.ResourceApplyVolumeResult, 0, len(dr.GetVolumes()))

	if withDRBD {
		minor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

		for _, vol := range dr.GetVolumes() {
			out = append(out, &intent.ResourceApplyVolumeResult{
				VolumeNumber: vol.GetVolumeNumber(),
				DevicePath:   fmt.Sprintf("/dev/drbd%d", volMinorOrBase(vol, minor)),
			})
		}

		return out
	}

	for _, vol := range dr.GetVolumes() {
		dev, ok := devices[vol.GetVolumeNumber()]
		if !ok {
			continue
		}

		out = append(out, &intent.ResourceApplyVolumeResult{
			VolumeNumber: vol.GetVolumeNumber(),
			DevicePath:   dev,
		})
	}

	return out
}

// resFilePerm is the on-disk mode for /etc/drbd.d/<name>.res. drbd is
// happy with 0o644; the file does not contain secrets the way auth-keys
// would (shared-secret is in /etc/drbd.d/global_common.conf, written
// once at install time).
const resFilePerm = 0o644
