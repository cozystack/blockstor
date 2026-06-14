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

package drbd

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Adm is a thin wrapper around the `drbdadm` CLI. It exists so the
// satellite reconciler can be unit-tested without a real DRBD kernel
// module present: production injects storage.RealExec, tests inject
// storage.FakeExec and assert the exact command lines.
type Adm struct {
	exec storage.Exec
}

// NewAdm constructs an Adm with the given Exec.
func NewAdm(ex storage.Exec) *Adm {
	return &Adm{exec: ex}
}

// Up activates the resource: `drbdadm up <res>`. Idempotent on the DRBD
// side (already-up resources return 0 with a noisy warning); we don't
// try to suppress that here.
func (a *Adm) Up(ctx context.Context, resource string) error {
	return a.run(ctx, "up", resource)
}

// Down deactivates the resource: `drbdadm down <res>`. Counterpart to Up.
func (a *Adm) Down(ctx context.Context, resource string) error {
	return a.run(ctx, "down", resource)
}

// SetupDown tears down a kernel-resident DRBD resource via
// `drbdsetup down <res>`, bypassing drbdadm's .res-file lookup.
//
// 288 P1: the orphan sweeper used to call `drbdadm down`
// on resources it discovered via `drbdsetup status` but for which
// no Resource CRD existed on this node. drbdadm refuses with
// `'<rsc>' not defined in your config (for this host)` /
// `no resources defined!` whenever the corresponding .res file
// in /etc/drbd.d is missing — which is precisely the state we
// land in after `DeleteResource` removed the .res file but its
// `drbdadm down` step never reached the kernel (e.g. the resource
// was already torn down once and a subsequent restart wiped the
// .res via cleanStateDir; or the controller raced the satellite
// and CRD delete fired before drbdadm down propagated).
//
// `drbdsetup down` reads kernel state directly (the resource
// name is the kernel-side handle, not the config) so it works
// in the .res-less state the sweeper exists to clean up.
// Mirrors `cleanKernelState` in cmd/satellite/main.go (issue 285)
// for runtime use rather than startup.
func (a *Adm) SetupDown(ctx context.Context, resource string) error {
	_, err := a.exec.Run(ctx, "drbdsetup", "down", resource)
	if err != nil {
		return errors.Wrapf(err, "drbdsetup down %s", resource)
	}

	return nil
}

// Adjust reconciles kernel state to the on-disk .res file. Called after
// the ConfFileBuilder writes a new file and we need DRBD to pick up
// changes (added/removed peers, new options).
func (a *Adm) Adjust(ctx context.Context, resource string) error {
	return a.run(ctx, "adjust", resource)
}

// AdjustSkipDisk is the Failed-replica variant of Adjust that
// appends drbd-utils' `--skip-disk` flag. Used after the observer
// detected `disk:Failed` and stamped `DrbdOptions/SkipDisk=True`
// on the Resource: a plain `drbdadm adjust` on a Failed/Diskless
// replica would try to re-attach the dead lower disk and fail; the
// `--skip-disk` flag tells drbdadm to leave the disk attachment
// alone and only reconcile network/peer state. Mirrors upstream
// linstor's `DrbdAdm.adjust` behaviour when its `skipDisk` flag is
// set (satellite/.../DrbdAdm.java:124).
//
// Operator clears the SkipDisk prop with
// `linstor r sp <node> <rsc> DrbdOptions/SkipDisk` (no value);
// next reconcile falls back to plain Adjust and re-attaches when
// the lower disk is back.
func (a *Adm) AdjustSkipDisk(ctx context.Context, resource string) error {
	return a.run(ctx, "adjust", "--skip-disk", resource)
}

// AdjustSkipNet is the operator-controlled-disconnect variant of
// Adjust that appends drbd-utils' `--skip-net` flag. Used when the
// reconciler detects a peer in operator-initiated StandAlone (the
// `drbdadm disconnect` / `drbdsetup disconnect --force=yes` state)
// AND .res content has not changed since the last apply — i.e. this
// reconcile is observer-trigger-driven, not Spec-driven. Plain
// `drbdadm adjust` would re-issue `drbdsetup connect` and undo the
// operator's disconnect within ~1 s, defeating split-brain recovery
// recipes (scenario 5.W12) and other manual-intervention paths that
// rely on StandAlone surviving long enough to run subsequent commands.
//
// The `--skip-net` flag tells drbdadm to leave the connection slots
// alone and only reconcile disk-level state. Mirrors upstream
// LINSTOR's `listAdjustable`-gated adjust dispatch, which simply
// doesn't call adjust at all when nothing in the .res differs from
// kernel.
//
// Operator restores connectivity with the documented W12 recipe
// (`drbdadm connect <rd>` or `drbdadm -- --discard-my-data connect
// <rd>`); after that the kernel reports Connected again, the next
// observer-triggered reconcile sees no StandAlone peer, and the
// full Adjust path resumes for any subsequent drift convergence.
func (a *Adm) AdjustSkipNet(ctx context.Context, resource string) error {
	return a.run(ctx, "adjust", "--skip-net", resource)
}

// AdjustSkipNetSkipDisk runs `drbdadm adjust --skip-net --skip-disk`.
// The combination fires when both signals are active: a SkipDisk pin
// (operator prop or kernel-Diskless coercion) AND a peer in operator-
// initiated StandAlone. Each flag's invariant still holds — disk
// attachment is left alone (SkipDisk), connection slots are left alone
// (SkipNet) — so the only reconcile work that lands is options /
// volume-shape drift; the safe-rest of adjust's idempotent passes.
func (a *Adm) AdjustSkipNetSkipDisk(ctx context.Context, resource string) error {
	return a.run(ctx, "adjust", "--skip-net", "--skip-disk", resource)
}

// CreateMD initialises on-disk metadata for the resource. We always use
// --force: a freshly-allocated LV may carry leftover signature bytes
// from its previous tenant, and DRBD bails without --force.
//
// `--max-peers` is pinned to `MaxPeers - 1` (the kernel counts the
// local node separately from peers, so a 16-replica RD needs
// `--max-peers=15`). Without this we'd inherit drbd-utils' default
// of 7, which silently caps every RD at 8 nodes total regardless of
// what the allocator says — and a later `drbdadm adjust` on the 9th
// replica would fail with a confusing "peer-id out of range" error.
//
// DANGER: `--force` overwrites whatever metadata is on the underlying
// disk. Callers MUST guarantee no valid DRBD metadata is already there
// — `--force` will happily wipe a healthy replica's GI/bitmap state,
// dropping the node's claim on its replicated data. The satellite's
// `runFirstActivation` gates the call behind a `HasMD` pre-check so
// this stays safe across satellite restarts / failed first attempts.
func (a *Adm) CreateMD(ctx context.Context, resource string) error {
	return a.run(ctx,
		"create-md",
		"--force",
		fmt.Sprintf("--max-peers=%d", MaxPeers-1),
		resource)
}

// HasMD reports whether DRBD-9 metadata already exists for the
// resource. `drbdadm dump-md <res>` exits 0 + prints a multi-line
// dump when there's a parseable metadata block on the lower disk;
// exit non-zero (with a "No valid meta data found" message) when
// there isn't. Used as the safety guard before re-running CreateMD:
// if metadata exists, the satellite must keep it (recreating with
// --force destroys the local generation identifier + dirty bitmap).
//
// Requires BOTH zero exit AND non-empty stdout to count as "present"
// — real drbdadm never returns success with no output, but a faked
// exec in unit tests can, and we'd rather err on the side of
// "missing → safe to create-md".
//
// Bug B.4 carve-out: when the volume's lower disk is already
// attached to a running DRBD kernel slot, drbdmeta refuses to open
// it ("Device or resource busy" / "Device 'X' is configured!") and
// `dump-md` exits non-zero. Treating that as "missing" would route
// the caller into create-md against an attached minor, which
// EBUSY-loops at ~10 Hz. A volume that's attached BY DEFINITION
// has metadata — the kernel could not have brought it up otherwise
// — so map the busy/configured error string to `hasMD=true` and
// skip the create-md call entirely.
func (a *Adm) HasMD(ctx context.Context, resource string) (bool, error) {
	out, err := a.exec.Run(ctx, "drbdadm", "dump-md", resource)
	if err == nil {
		return len(out) > 0, nil
	}

	// Attached-lower-disk surface: dump-md cannot exclusive-
	// open the device because the kernel holds it. The device
	// is attached, therefore metadata exists. Surface
	// hasMD=true so the caller skips create-md. (Bug B.4)
	errStr := err.Error()
	if strings.Contains(errStr, "Device or resource busy") ||
		strings.Contains(errStr, "is configured!") ||
		strings.Contains(string(out), "Device or resource busy") ||
		strings.Contains(string(out), "is configured!") {
		return true, nil
	}

	// `No valid meta data found` / drbdmeta "missing image" / etc.
	// all bubble up as non-zero exit. Treat as "not yet
	// initialised" — the caller's create-md will either succeed
	// (truly missing) or surface a more specific failure.
	return false, nil
}

// Primary flips the resource to Primary role so it can be opened
// read-write (mounted, exported via NBD, etc.).
func (a *Adm) Primary(ctx context.Context, resource string) error {
	return a.run(ctx, "primary", resource)
}

// PrimaryForce promotes a resource to Primary even when local disk is
// Inconsistent and no peer is UpToDate. Used as the initial-sync seed
// on a brand-new diskful replica — without --force, drbd refuses to
// promote, leaving the resource permanently "Inconsistent".
func (a *Adm) PrimaryForce(ctx context.Context, resource string) error {
	return a.run(ctx, "primary", "--force", resource)
}

// Secondary flips the resource back to Secondary role. Used after the
// consumer unmounts and before another peer takes Primary.
func (a *Adm) Secondary(ctx context.Context, resource string) error {
	return a.run(ctx, "secondary", resource)
}

// Detach drops the local lower-disk binding without bringing the
// resource down. The replica becomes Diskless on this node — peers
// stay UpToDate, the consumer keeps doing I/O via DRBD's network
// path. Used when the storage layer fails (LV evicted, zvol
// destroyed, file inode gone) and we want the kernel to stop bashing
// the dead block device. `--force` is required when the disk is
// already in a transient state.
func (a *Adm) Detach(ctx context.Context, resource string) error {
	return a.run(ctx, "detach", "--force", resource)
}

// Attach binds the lower disk(s) named in the .res file to the
// already-loaded kernel slot, transitioning a replica from
// `disk:Diskless intentional` (i.e. brought up with no backing disk)
// to diskful. This is the missing piece for the diskless→diskful
// conversion path (`linstor r td --migrate-from`, `linstor r td
// --diskful`): `drbdadm adjust` reconciles network/peer state and
// resource options against the .res file, but for an intentionally-
// diskless kernel slot it does NOT add the backing disk because the
// kernel treats the current diskless state as deliberate. Only an
// explicit `drbdadm attach` (which shells out to `drbdsetup attach`
// per volume) crosses that boundary.
//
// Idempotent: calling Attach on a slot that's already diskful is a
// no-op at the kernel level (the attach request finds the disk
// already bound and returns success). Callers gate on
// HasDisklessVolume so the no-op case still avoids the shell-out
// cost.
func (a *Adm) Attach(ctx context.Context, resource string) error {
	return a.run(ctx, "attach", resource)
}

// Resize rescans the lower disk's size and tells DRBD to grow the
// replicated volume to match. The lower disk (LV / zvol / dm-crypt
// target) must already be the target size — this is a notify-only
// command.
//
// assumeClean selects the post-grow reconciliation strategy for the new
// region [old_size, new_size):
//
//   - assumeClean=true: pass `--assume-clean` so DRBD marks the grown
//     region UpToDate on every replica WITHOUT a resync. Sound ONLY
//     for zero-on-allocate providers (ZFS/thin/file), where the grown
//     bytes are deterministically zero on every replica — skipping the
//     resync there is a correct fast path that avoids serialising the
//     grow on every replica.
//   - assumeClean=false: omit the flag so DRBD marks the grown region
//     out-of-sync on the non-source peers and resyncs it from the
//     UpToDate source. Required for classic thick `LVM` (Bug 395, P1
//     data integrity): `lvextend` exposes recycled VG extents whose
//     prior content differs per node, so `--assume-clean` would
//     silently leave replicas disagreeing on the grown region with no
//     out-of-sync flag.
//
// The caller derives assumeClean from the provider's
// storage.ResizeZeroFiller capability.
func (a *Adm) Resize(ctx context.Context, resource string, assumeClean bool) error {
	if assumeClean {
		return a.run(ctx, "resize", "--assume-clean", resource)
	}

	return a.run(ctx, "resize", resource)
}

// SetGI pre-seeds the per-peer GI slot in this replica's DRBD
// metadata with the GI tuple of an existing UpToDate peer (or a
// deterministic day0 seed for fresh thin/ZFS-backed RDs), so DRBD's
// GI handshake on first connect recognises the new replica as
// already-in-sync against that specific peer and skips the full
// initial-sync.
//
// Must be called AFTER `create-md` (which writes the fresh metadata
// block this then mutates) and BEFORE `drbdadm up` (which reads the
// metadata into kernel state).
//
// The GI tuple format DRBD's `set-gi` accepts is
// `<current>:<bitmap>:<history0>:<history1>`. We set both
// current_uuid and bitmap_uuid to peerCurrentGI so the new replica
// claims "I'm at the peer's generation; I have no dirty bits relative
// to the peer". History is zeroed — DRBD's handshake never matches
// against history when current+bitmap match, so it doesn't matter.
//
// DRBD 9.2+ requires `--node-id <peerNodeID>` because the current/
// bitmap UUID tuple lives in a per-peer slot in the modern v09
// metadata layout. Without `--node-id`, drbdmeta refuses the call
// with "The set-gi command requires the --node-id option" (the
// e2e regression guard pins the failure shape). The caller MUST
// invoke SetGI once per peer node-id of the resource so every
// peer's bitmap slot carries the matching tuple; this is what makes
// the day0 skip-sync optimisation actually take effect on DRBD 9.2+.
//
// peerNodeID is the DRBD node-id of the peer whose slot is being
// stamped. The value is the one the controller-side allocator wrote
// onto `Resource.Status.DRBDNodeID` for that peer and that
// dispatcher.BuildDesired propagated into
// `DrbdOptions["peer.<name>.node-id"]` — keeping the .res render
// and the set-gi call reading from the same authoritative map (so
// the two satellites can't disagree about which bitmap slot a given
// peer occupies).
//
// Tested via FakeExec capture in pkg/drbd/drbdadm_test.go and
// pinned end-to-end in pkg/satellite/reconciler_drbd_test.go's
// first-activation case.
func (a *Adm) SetGI(ctx context.Context, resource string, volume int32, device string, peerNodeID int32, peerCurrentGI string) error {
	gi := fmt.Sprintf("%s:%s:0:0", peerCurrentGI, peerCurrentGI)

	return a.SetGIString(ctx, resource, volume, device, peerNodeID, gi)
}

// SetGIString is the general form of SetGI: it stamps an arbitrary,
// already-rendered GI string into the per-node-id v09 metadata slot.
// SetGI is the special case where current == bitmap == peerCurrentGI
// with no flags (the all-day0 skip-init-sync / SeedFromGI shape).
//
// Used by the satellite to reach the initial UpToDate state by
// writing metadata directly (the elected winner's local slot carries
// a random current-UUID + day0 bitmap-base + Consistent + UpToDate;
// the winner's peer slots carry day0 bitmap-base only) instead of
// force-promoting the device, which would mint a divergent current-
// UUID and force a full initial sync. Build the GI string with
// (GISeed).String() so the field/flag layout stays in one place.
//
// Same call-ordering contract as SetGI: AFTER create-md, BEFORE
// drbdadm up. `--node-id` is mandatory on DRBD 9.2+.
func (a *Adm) SetGIString(ctx context.Context, resource string, volume int32, device string, peerNodeID int32, gi string) error {
	target := fmt.Sprintf("%s/%d", resource, volume)

	_, err := a.exec.Run(ctx,
		"drbdmeta", "--force", target, "v09", device, "internal",
		"set-gi", "--node-id", strconv.Itoa(int(peerNodeID)), gi)
	if err != nil {
		return errors.Wrapf(err, "drbdmeta set-gi %s --node-id %d", target, peerNodeID)
	}

	return nil
}

// ForgetPeer clears a peer's GI / bitmap slot from this replica's
// on-disk DRBD metadata via `drbdmeta <res>/<vol> v09 <device>
// internal forget-peer <peer-node-id>`. Must run AFTER DelPeer
// (which clears the kernel-side connection slot) on a per-volume
// basis — DRBD-9 v09 metadata stores per-peer slots in the
// per-volume metadata block, one slot per peer node-id.
//
// Why this matters: DelPeer only severs the kernel connection.
// The on-disk slot keeps the peer's last-known GI and dirty
// bitmap forever — eating one of the MaxPeers-1 metadata slots
// `drbdadm create-md --max-peers=15` carved out at first
// activation. After enough permanent-node-removal cycles the
// resource exhausts its slot pool and the next replica add
// fails with `drbdmeta create-md` running out of room. Calling
// forget-peer in the per-node-removal path keeps the slot pool
// recyclable.
//
// Idempotent on a slot that's already empty: drbdmeta exits zero
// with a no-op warning. A missing metadata block (resource never
// fully initialised) bubbles up as an error so the caller can
// log and continue — the slot leakage we're trying to prevent
// can't have accumulated on a resource that has no metadata to
// begin with.
func (a *Adm) ForgetPeer(ctx context.Context, resource string, volume int32, device string, peerNodeID int32) error {
	target := fmt.Sprintf("%s/%d", resource, volume)

	_, err := a.exec.Run(ctx,
		"drbdmeta", "--force", target, "v09", device, "internal",
		"forget-peer", strconv.Itoa(int(peerNodeID)))
	if err != nil {
		return errors.Wrapf(err, "drbdmeta forget-peer %s --node-id %d", target, peerNodeID)
	}

	return nil
}

// DelPeer disconnects and forgets a peer node for the given resource.
// Run this BEFORE rewriting the .res file with the peer removed —
// drbdadm needs the peer's `on <peer>` block in the .res to resolve
// its node-id. Running adjust on a .res that no longer mentions the
// peer leaves the kernel's connection object alive in StandAlone
// state forever (drbdadm adjust never tears down connections, only
// adds / reconfigures them).
//
// `disconnect` first so a connected peer is quiesced; `del-peer`
// then removes the kernel-side connection slot entirely. Both
// commands are idempotent on already-detached peers; del-peer's
// "not defined in your config" failure mode is swallowed because
// it means there's nothing to delete (the .res was rewritten
// without the peer before drbdadm saw it — del-peer is a clean
// no-op in that branch).
func (a *Adm) DelPeer(ctx context.Context, resource, peerNode string) error {
	target := peerNode + ":" + resource

	// Best-effort disconnect — a peer that's already StandAlone
	// returns non-zero, which we don't care about here.
	_, _ = a.exec.Run(ctx, "drbdadm", "disconnect", target)

	out, err := a.exec.Run(ctx, "drbdadm", "del-peer", target)
	if err == nil {
		return nil
	}

	// drbdadm prints "'<peer>:<rd>' not defined in your config (for
	// this host)." when the .res no longer mentions the peer block.
	// The kernel slot we wanted to drop also wouldn't exist in that
	// state, so the operation has already converged.
	if strings.Contains(string(out), "not defined in your config") ||
		strings.Contains(err.Error(), "not defined in your config") {
		return nil
	}

	return errors.Wrapf(err, "drbdadm del-peer %s", target)
}

// Verify runs `drbdadm verify <resource>` to schedule an online
// data scan against peers. Out-of-sync blocks discovered during
// the scan surface in subsequent peer-device events2 frames as
// `out-of-sync:<KiB>`. Idempotent for already-verifying resources
// (drbdadm exits zero with a warning). Operator-recovery tool —
// no business-logic caller in the satellite, this exists so the
// operator-recovery surface can be wired up without re-shelling
// from arbitrary callers.
func (a *Adm) Verify(ctx context.Context, resource string) error {
	return a.run(ctx, "verify", resource)
}

// Invalidate runs `drbdadm invalidate <resource>` — marks local
// data Inconsistent and forces a full resync from a peer. The
// recovery counterpart to PrimaryForce: when the local replica
// is suspected corrupt (silent bit-rot, lower-disk fsck reported
// damage, etc.) the operator uses this to throw the local copy
// away and pull a fresh one. Requires at least one UpToDate peer
// — drbdadm refuses if no peer can be the resync source.
func (a *Adm) Invalidate(ctx context.Context, resource string) error {
	return a.run(ctx, "invalidate", resource)
}

// NewCurrentUUID runs `drbdadm new-current-uuid <resource>` —
// bumps the current generation UUID. Used in split-brain recovery
// (UG9 §7.4.1): after manually picking a survivor and connecting,
// the operator stamps a fresh current-UUID on the survivor so
// the other side recognises it as the new generation source on
// the next handshake. No business-logic caller; pure operator
// recovery tool.
func (a *Adm) NewCurrentUUID(ctx context.Context, resource string) error {
	return a.run(ctx, "new-current-uuid", resource)
}

// SuspendIO runs `drbdadm suspend-io <resource>` — freezes the
// resource's block-I/O path on the local satellite so a backing
// snapshot (LVM-thin / ZFS / file) captures bytes at a stable
// point. Mirrors upstream LINSTOR's CtrlSnapshotCrtApiCallHandler
// suspend-io broadcast (controller/.../CtrlSnapshotCrtApiCallHandler.java
// around setSuspendIO(true) → updateSatellites → ack); the
// per-satellite SnapshotReconciler invokes this in Phase 1 of the
// `suspend → take → resume` orchestration so two diskful replicas
// don't capture divergent bytes while the application writer
// streams traffic. Bug 351.
//
// Why drbdadm not drbdsetup: drbdsetup's `suspend-io` subcommand
// takes a minor number or `/dev/drbdN` path, not a resource name —
// passing a resource name yields `exit 20: Cannot determine minor
// device number of device '<res>'`. drbdadm resolves the resource
// name to its kernel minor via the local .res file and forwards to
// drbdsetup correctly. Idempotent on a freshly-suspended resource
// — the kernel folds a second suspend-io into a no-op.
func (a *Adm) SuspendIO(ctx context.Context, resource string) error {
	_, err := a.exec.Run(ctx, "drbdadm", "suspend-io", resource)
	if err != nil {
		return errors.Wrapf(err, "drbdadm suspend-io %s", resource)
	}

	return nil
}

// ResumeIO runs `drbdadm resume-io <resource>` — the
// counterpart to SuspendIO. MUST be called on every node the
// controller broadcast SuspendIO to, even on the abort path: a
// partially-acked suspend followed by no resume leaves the
// remaining peers' I/O frozen forever (application traffic
// hangs). The controller-side SnapshotReconciler unconditionally
// flips Spec.SuspendIO=false on Phase 3 (or on any per-node
// Failed) so this fires on every targeted node. Bug 351.
//
// Why drbdadm not drbdsetup: same as SuspendIO above — drbdsetup
// resume-io takes a kernel minor, not a resource name. drbdadm
// resolves res→minor via .res file and forwards to drbdsetup.
//
// Idempotent on a resource that's already running — the kernel
// folds a second resume-io into a no-op, so a retry after a
// crashed satellite never wedges anything.
func (a *Adm) ResumeIO(ctx context.Context, resource string) error {
	_, err := a.exec.Run(ctx, "drbdadm", "resume-io", resource)
	if err != nil {
		return errors.Wrapf(err, "drbdadm resume-io %s", resource)
	}

	return nil
}

// PauseSync runs `drbdadm pause-sync <resource>` — temporarily
// halts an in-flight resync without tearing down the connection.
// Used as an operator throttle: long initial-sync on a fresh
// replica monopolises lower-disk + network I/O; the operator
// pauses it during business hours and resumes it overnight.
// Idempotent: an already-paused resource stays paused.
func (a *Adm) PauseSync(ctx context.Context, resource string) error {
	return a.run(ctx, "pause-sync", resource)
}

// ResumeSync runs `drbdadm resume-sync <resource>` — counterpart
// to PauseSync. Lets a paused resync resume from its checkpoint;
// no work is repeated.
func (a *Adm) ResumeSync(ctx context.Context, resource string) error {
	return a.run(ctx, "resume-sync", resource)
}

// Outdate runs `drbdadm outdate <resource>` — explicitly marks
// the local replica's disk state as Outdated. Used in fencing
// patterns (UG9 §7.6): an external fence agent observes that
// this node lost quorum or got partitioned and stamps Outdated
// so the kernel refuses to serve I/O until a peer brings it back
// UpToDate via resync. No business-logic caller; the satellite
// relies on automatic quorum-driven outdating today, but the
// operator-recovery surface needs a manual override too.
func (a *Adm) Outdate(ctx context.Context, resource string) error {
	return a.run(ctx, "outdate", resource)
}

// ApplyAL runs `drbdadm apply-al <resource>` — manually applies
// the on-disk activity log to the lower disk. Needed before
// promote-after-crash when the kernel surfaces `ERR_NEED_APPLY_AL`
// (drbdsetup exit 167): a dirty activity log from a non-clean
// shutdown must be replayed onto the backing storage before the
// resource can be promoted to Primary, otherwise stale extents
// in the AL would be read as authoritative bytes.
func (a *Adm) ApplyAL(ctx context.Context, resource string) error {
	return a.run(ctx, "apply-al", resource)
}

// WipeMd runs `drbdmeta --force <res>/<vol> v09 <device> internal
// wipe-md` — the deliberate-wipe counterpart to CreateMD. Zeroes
// the metadata block on the lower disk so a subsequent CreateMD
// starts from a clean slate. Operator-recovery use case: a
// permanently-removed peer's lower disk is being recycled for a
// new replica and the stale GI/bitmap state must be erased
// before the new metadata is written.
//
// `--force` is required because the in-place mutation rejects
// in-use metadata without it. Caller MUST guarantee the resource
// is not currently loaded in the kernel; running wipe-md on a
// live replica destroys the GI tuple the kernel needs and the
// resource will refuse to come up afterwards.
func (a *Adm) WipeMd(ctx context.Context, resource string, volume int32, device string) error {
	target := fmt.Sprintf("%s/%d", resource, volume)

	_, err := a.exec.Run(ctx,
		"drbdmeta", "--force", target, "v09", device, "internal",
		"wipe-md")
	if err != nil {
		return errors.Wrapf(err, "drbdmeta wipe-md %s", target)
	}

	return nil
}

// ShowGI runs `drbdmeta --force <res>/<vol> v09 <device> internal
// show-gi` and returns the raw stdout — the on-disk generation
// UUID tuple, peer slot table, and bitmap-UUID per peer. Used
// for verification (compare against a peer's view to triage
// split-brain) and as the source data for GetGI.
//
// Output shape (drbdmeta v09 show-gi):
//
//	+--<  Current data generation UUID  >-
//	| 78A0DDDABCDEF000
//	+--<  Bitmap's base data generation UUID  >-
//	| 78A0DDDABCDEF000
//	+--<  Historical generation UUIDs  >-
//	| 0000000000000000
//	...
//
// Callers wanting just the current UUID should prefer GetGI, which
// returns the parsed scalar.
func (a *Adm) ShowGI(ctx context.Context, resource string, volume int32, device string) ([]byte, error) {
	target := fmt.Sprintf("%s/%d", resource, volume)

	out, err := a.exec.Run(ctx,
		"drbdmeta", "--force", target, "v09", device, "internal",
		"show-gi")
	if err != nil {
		return nil, errors.Wrapf(err, "drbdmeta show-gi %s", target)
	}

	return out, nil
}

// GetGI is the parsed counterpart to ShowGI — runs the same
// `drbdmeta ... internal get-gi` subcommand (the terser variant
// that emits just the GI tuple, suitable for scripted comparison)
// and returns the trimmed string. The tuple shape is
// `<current>:<bitmap>:<history0>:<history1>` matching the format
// SetGI accepts, so callers can round-trip through SetGI after a
// fix-up. Useful for split-brain triage: compare GetGI output on
// each replica, pick a survivor, SetGI the others against it.
func (a *Adm) GetGI(ctx context.Context, resource string, volume int32, device string) (string, error) {
	target := fmt.Sprintf("%s/%d", resource, volume)

	out, err := a.exec.Run(ctx,
		"drbdmeta", "--force", target, "v09", device, "internal",
		"get-gi", "--node-id", "0")
	if err != nil {
		return "", errors.Wrapf(err, "drbdmeta get-gi %s", target)
	}

	return strings.TrimSpace(string(out)), nil
}

// CurrentGI returns just the current-UUID (the first colon field of
// the GI tuple) for one volume, read from on-disk metadata via
// GetGI. Empty string (not an error) when the tuple can't be parsed,
// so callers can leave an observed CurrentGI unclaimed and keep the
// seed-safety gate conservative.
//
// Why the observer needs this: on DRBD 9.3.2 `drbdsetup events2
// --full` / `status --json` do NOT emit the current-uuid on device
// frames, so the events2-sourced CurrentGI is always empty there. The
// seed-safety gates' day0-sibling discriminator (isDay0SeededVolume)
// compares a peer's observed CurrentGI against the deterministic day0
// value to let a staggered late replica skip the initial sync;
// without an observed CurrentGI it can never fire. The `get-gi
// --node-id 0` read works on an active (kernel-attached) device via
// `--force`; the device-level current-UUID is the same in every
// node-id slot, so node-id 0 is representative.
func (a *Adm) CurrentGI(ctx context.Context, resource string, volume int32, device string) (string, error) {
	tuple, err := a.GetGI(ctx, resource, volume, device)
	if err != nil {
		return "", err
	}

	current, _, found := strings.Cut(tuple, ":")
	if !found || current == "" {
		return "", nil
	}

	return strings.ToUpper(current), nil
}

// StatusResources runs `drbdsetup status` and returns the names of
// every resource the local kernel currently owns. Used by the
// orphan-diskless sweeper (Scenario 5.34) to cross-reference
// kernel-resident resources against Resource CRDs placed on this
// node; anything in the kernel but missing from the CRD set is a
// candidate for `drbdadm down` cleanup.
//
// drbdsetup status output convention: every resource starts at
// column 0 with `<name> role:<role> [...]`; per-volume / per-peer
// lines are indented. We parse the first non-empty whitespace-token
// of every column-0 line — robust against drbdsetup format additions
// (new tail fields don't affect the resource-name slot).
//
// A non-zero exit from drbdsetup with the typical "no resources
// defined" message returns an empty slice + nil error: a kernel
// with no DRBD resources is a valid steady state, not a failure.
func (a *Adm) StatusResources(ctx context.Context) ([]string, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "status")
	if err != nil {
		// `No currently configured DRBD found.` (kernel module loaded
		// but zero resources) and friends all bubble up non-zero. Treat
		// as "empty kernel" so the sweeper just runs a no-op cycle.
		if strings.Contains(string(out), "No currently configured DRBD") ||
			strings.Contains(err.Error(), "No currently configured DRBD") {
			return nil, nil
		}

		return nil, errors.Wrap(err, "drbdsetup status")
	}

	var names []string

	for line := range strings.SplitSeq(string(out), "\n") {
		// Indented lines describe connections / volumes inside a
		// resource block — skip. Blank lines separate resources.
		if line == "" || line[0] == ' ' || line[0] == '\t' {
			continue
		}

		// First whitespace-token is the resource name.
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		// Bug 264 (P3): drbdsetup text output emits `# ...` banner
		// or comment lines in some environments (wrapper scripts,
		// kernel-side configuration hints). Without this guard the
		// column-0 `#` token was misread as a resource named "#"
		// and the orphan-sweeper looped every 5 minutes on
		// `drbdadm down #` — which always failed with
		// `no resources defined!`. Comments have always been the
		// documented convention for drbdsetup text output; the
		// JSON variant has no such ambiguity.
		if strings.HasPrefix(fields[0], "#") {
			continue
		}

		names = append(names, fields[0])
	}

	return names, nil
}

// IsLoaded reports whether the kernel currently owns a DRBD slot
// for the named resource. Used to detect the post-`drbdadm down`
// state where the on-disk .res + `.md-created` marker still
// describe a resource but the kernel slot is gone — running
// `drbdadm adjust` in that state fails with `(158) Unknown
// resource` because adjust only reconciles already-loaded
// kernel state, it doesn't bootstrap missing resources. The
// reconciler consults this probe and falls back to `drbdadm up`
// (which performs new-resource + new-path + attach + connect)
// to revive the slot, then proceeds with adjust as normal.
//
// Convention:
//   - zero exit + non-empty stdout → loaded (true)
//   - non-zero exit OR empty stdout → not loaded (false)
//   - error-text / stdout containing "No currently configured
//     DRBD found" is folded into the not-loaded case too — that's
//     drbdsetup's verbatim message when the kernel module is
//     present but the named resource isn't.
//
// Returning false + nil error is the dominant "absent" signal so
// callers don't need to branch on the error type; a true error
// surfaces only for genuinely-unexpected failures (binary
// missing, exec/IO error) that the caller should bubble up.
func (a *Adm) IsLoaded(ctx context.Context, resource string) (bool, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource)
	if err != nil {
		// Any non-zero exit is treated as "absent": the dominant
		// failure mode is `drbdsetup status` returning exit 10 +
		// "No currently configured DRBD found", but other non-zero
		// codes (e.g. transient netlink hiccups) also indicate the
		// kernel doesn't have a usable view of the slot, which is
		// the trigger for the `drbdadm up` recovery path. Surface
		// false + nil so the caller doesn't need to branch.
		return false, nil //nolint:nilerr // non-zero exit is the "kernel slot absent" signal, not a bubble-up error
	}

	return strings.TrimSpace(string(out)) != "", nil
}

// KernelMyNodeID returns the kernel resource's OWN DRBD node-id
// (the my-node-id burned into the slot at `drbdadm up` time) by
// parsing `drbdsetup status <res> --json`. Returns (-1, false) when
// the kernel has no slot for the resource (not loaded) or the status
// is unparseable - callers treat "unknown" as "nothing to reconcile"
// rather than acting on a guess.
//
// Why (Bug 360): `drbdadm up`/`new-resource` burns the .res's local
// `node-id` into kernel state permanently. If first-activation rendered
// the .res with node-id 0 (because the controller had not yet stamped
// Status.DRBDNodeID for the local node), the kernel my-id sticks at 0
// even after the .res is later re-rendered with the correct id -
// `drbdadm adjust` cannot rewrite a loaded resource's my-id. The only
// fix is `down` + re-`up`; the self-heal path needs this reader to
// detect the mismatch (kernel my-id != allocated Status.DRBDNodeID).
func (a *Adm) KernelMyNodeID(ctx context.Context, resource string) (int32, bool) {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return -1, false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil {
		return -1, false
	}

	if len(status) == 0 || status[0].NodeID == nil {
		return -1, false
	}

	return *status[0].NodeID, true
}

// AnyConnectedPeerHasData probes `drbdsetup status <res> --json` and
// reports whether ANY peer connection currently exposes committed data
// — a peer-device in `peer-disk-state` UpToDate / Consistent / Outdated.
//
// Bug 342 force-promote gate: the satellite calls this immediately
// before `drbdadm primary --force` on a fresh replica's first
// activation. Force-primary mints a brand-new Current UUID; if a peer
// already holds the real data + UUID (the relocate / physical-`r d`-
// then-`r c` case) the peer then declines the GI handshake
// (`uuid_compare()=unrelated-data` → `Unrelated data, aborting!`) and
// the new replica wedges StandAlone. When this returns true the
// satellite SKIPS the force-primary so the fresh replica stays
// Inconsistent and SyncTargets from the data-bearing peer (full resync,
// always data-safe).
//
// This is the RACE-FREE backstop to the dispatcher's CRD-status gate
// (anyDiskfulPeerHasData): the .res has already been adjusted+connected
// by the time finishDRBDApply runs, so the peer's disk state is
// observable directly from the kernel — immune to the apiserver
// cache-trail that can leave a freshly-recreated replica's peer Status
// looking empty/Inconsistent at BuildDesired time.
//
// Conservative: returns false on any probe/parse failure (kernel slot
// absent, status non-zero, malformed JSON) so a genuinely-fresh RD
// — where no peer has data and the probe legitimately finds nothing —
// still force-primaries (Bug 77 / first-replica seed preserved). The
// only states that gate the promote are the three committed-data ones;
// a fresh, never-promoted peer reports Inconsistent (or no connection
// yet) and does NOT block.
func (a *Adm) AnyConnectedPeerHasData(ctx context.Context, resource string) bool {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 {
		return false
	}

	for _, conn := range status[0].Connections {
		for _, pd := range conn.PeerDevices {
			switch DiskState(pd.PeerDiskState) {
			case DiskStateUpToDate, DiskStateConsistent, DiskStateOutdated:
				return true
			case DiskStateDiskless, DiskStateAttaching, DiskStateDetaching,
				DiskStateFailed, DiskStateNegotiating, DiskStateInconsistent,
				DiskStateDUnknown:
				// No committed data on this peer-device; keep scanning.
			default:
				// Unknown/empty state — treat as "no data".
			}
		}
	}

	return false
}

// AnyConnectedPeerHasDataForVolume is the per-volume variant of
// AnyConnectedPeerHasData. It returns true only when at least one
// connected peer's peer-device for the given volume number is in
// `UpToDate` / `Consistent` / `Outdated`. Used by Bug B.4 path:
// `linstor vd c <rd> N` adds a NEW volume to a running RD whose
// existing volumes are already UpToDate on every peer — the
// per-RD probe sees UpToDate peer-devices for the OLD volumes
// and falsely refuses the day0 skip-init-sync seed for the NEW
// volume, leaving it Inconsistent forever. Per-volume scoping
// surfaces the truth: the new volume's peer-devices are
// Inconsistent / not-yet-attached, so the seed proceeds.
//
// Identical conservative semantics to the RD-scoped variant:
// returns false on any probe / parse failure or when the volume
// has no matching peer-devices in any connection (i.e. nobody
// is connected with the new minor yet — that is the "fresh
// volume" steady state, no peer holds data for it).
func (a *Adm) AnyConnectedPeerHasDataForVolume(ctx context.Context, resource string, volNumber int32) bool {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 {
		return false
	}

	for _, conn := range status[0].Connections {
		for _, pd := range conn.PeerDevices {
			if pd.VolumeNumber != volNumber {
				continue
			}

			switch DiskState(pd.PeerDiskState) {
			case DiskStateUpToDate, DiskStateConsistent, DiskStateOutdated:
				return true
			case DiskStateDiskless, DiskStateAttaching, DiskStateDetaching,
				DiskStateFailed, DiskStateNegotiating, DiskStateInconsistent,
				DiskStateDUnknown:
				// No committed data on this peer-device; keep scanning.
			default:
				// Unknown/empty state — treat as "no data".
			}
		}
	}

	return false
}

// SafeForMkfsRetryPromote probes `drbdsetup status <res> --json` and
// reports whether a promote→mkfs→demote retry is provably safe to run
// RIGHT NOW on this node without the dispatcher's auto-primary
// blessing (the BUG-028 latch-free mkfs retry; see the satellite's
// latchFreeMkfsRetryAllowed for the full story of the false
// RD.Spec.Initialized latch that kills the auto-primary election).
//
// Returns true ONLY when ALL hold:
//
//   - the local role is NOT Primary (we are about to promote; an
//     already-Primary local slot means some consumer or a previous
//     dance holds the device — let it finish);
//   - every local volume is diskful UpToDate (the retry exists to add
//     a missing filesystem to a HEALTHY converged replica set, never
//     to promote an Inconsistent local copy);
//   - NO peer is Primary (an external promoter — drbd-reactor's RWX
//     mount loop — may briefly hold the device; the caller simply
//     retries on a later reconcile once it has demoted again);
//   - every connected peer-device is UpToDate or an intentional
//     Diskless witness. UpToDate-while-Connected means the peer is in
//     the SAME data generation as the local volume (bit-identical), so
//     `primary --force` mints nothing unrelated and the subsequent
//     mkfs writes replicate to copies that already equal ours. ANY
//     other peer-disk state (Inconsistent, DUnknown of a disconnected
//     peer, Negotiating, …) vetoes — a disconnected diskful peer could
//     be an offline data holder, and forcing primary against one is
//     exactly the Bug 342 unrelated-data wedge.
//
// Conservative on any probe / parse failure: returns false, the retry
// just waits for the next reconcile.
func (a *Adm) SafeForMkfsRetryPromote(ctx context.Context, resource string) bool {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 {
		return false
	}

	res := status[0]

	if Role(res.Role).IsPrimary() {
		return false
	}

	if !localIsUpToDate(res.Devices) {
		return false
	}

	for _, conn := range res.Connections {
		if Role(conn.PeerRole).IsPrimary() {
			return false
		}

		for _, pd := range conn.PeerDevices {
			switch DiskState(pd.PeerDiskState) {
			case DiskStateUpToDate, DiskStateDiskless:
				// Lock-step sibling or intentional witness — safe.
			case DiskStateConsistent, DiskStateOutdated, DiskStateAttaching,
				DiskStateDetaching, DiskStateFailed, DiskStateNegotiating,
				DiskStateInconsistent, DiskStateDUnknown:
				return false
			default:
				// Unknown/empty token — refuse, conservative.
				return false
			}
		}
	}

	return true
}

// Day0SiblingSetConnected probes `drbdsetup status <res> --json` and
// reports whether the ENTIRE configured replica set is currently
// visible to the kernel as a promote-safe day0 candidate set (the
// BUG-028 first-activation mkfs bypass; the GI-level day0 proof is the
// satellite's, this is only the connectivity/coverage half):
//
//   - the local role is NOT Primary and every local volume is diskful
//     UpToDate (the elected winner seeded UpToDate via set-gi);
//   - NO peer is Primary (an external promoter mid-grab defers the
//     bypass to the latch-free retry, which handles foreign Primaries);
//   - every connected peer-device is UpToDate or Diskless;
//   - a peer-device whose state is still unknown (DUnknown — the
//     connection has not handshaken) is tolerated ONLY when the peer is
//     named in disklessPeers (an intentional diskless witness carries
//     no data by construction). An un-handshaken DISKFUL peer refuses:
//     it could be an offline data holder, and both `primary --force`
//     and mkfs against it are the Bug 342 unrelated-data / data-loss
//     wedge.
//
// Why this exists: the dispatcher's CRD-level PeerHasData treats an
// UpToDate sibling whose CurrentGI has not been OBSERVED yet (the
// get-gi backfill is best-effort) as data-bearing. On a fresh day0
// race that conservatism is FALSE and would permanently cost the
// one-shot first-activation mkfs. The kernel coverage here, combined
// with the satellite's local-GI==day0 proof (a Connected+UpToDate peer
// necessarily shares the local data generation), strictly supersedes
// the CRD signal: every case PeerHasData correctly protects is also
// refused here (a real connected data peer forces local GI != day0; a
// disconnected diskful peer is DUnknown).
//
// Conservative on any probe / parse failure: returns false.
func (a *Adm) Day0SiblingSetConnected(ctx context.Context, resource string, disklessPeers map[string]bool) bool {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 {
		return false
	}

	res := status[0]

	if Role(res.Role).IsPrimary() || !localIsUpToDate(res.Devices) {
		return false
	}

	for _, conn := range res.Connections {
		if Role(conn.PeerRole).IsPrimary() {
			return false
		}

		for _, pd := range conn.PeerDevices {
			switch DiskState(pd.PeerDiskState) {
			case DiskStateUpToDate, DiskStateDiskless:
				// Lock-step sibling or intentional witness — safe.
			case DiskStateDUnknown:
				if !disklessPeers[conn.PeerName] {
					return false
				}
			case DiskStateConsistent, DiskStateOutdated, DiskStateAttaching,
				DiskStateDetaching, DiskStateFailed, DiskStateNegotiating,
				DiskStateInconsistent:
				return false
			default:
				// Unknown/empty token — refuse, conservative.
				return false
			}
		}
	}

	return true
}

// NeedsRecoveryPromote probes the live kernel via `drbdsetup status
// <res> --json` and reports whether THIS node should re-arm the
// auto-primary seed to unstick a fresh RD whose initial sync wedged
// (Bug 366). It is the kernel-truth predicate behind the satellite's
// steady-state recovery-promote self-heal — modelled on the existing
// AnyConnectedPeerHasData backstop, reading observed peer/disk state
// rather than any latched CRD flag.
//
// Why (Bug 366): on a brand-new 3-diskful RD blockstor reaches
// all-UpToDate via two mechanisms BOTH gated on the same predicate
// (a data-bearing diskful peer exists): the day0 skip-initial-sync GI
// seed and the lowest-node-id seed-primary election. When the three
// first-activation reconciles stagger, the replicas that seed first
// flip UpToDate; that flips the predicate true for the late replica,
// which then (a) declines its own day0 seed → comes up Inconsistent,
// and (b) vetoes the seed-primary election → NO replica is ever
// promoted Primary. With no Primary the late replica's resync stalls
// (dual SyncSource collide into resync-suspended:peer / done:0.00) and
// it sits Inconsistent forever (~2 in 7 cold creates).
//
// Returns true ONLY when ALL hold, so EXACTLY ONE node acts and the
// promote is data-safe + self-limiting:
//   - the local replica is diskful AND disk:UpToDate (a viable, fully
//     populated SyncSource — never promote an Inconsistent local);
//   - at least one connected diskful peer is disk:Inconsistent (the
//     wedge symptom — there is a peer that still needs a source);
//   - NO replica anywhere (local role nor any peer-role) is Primary
//     (a Primary already drives the sync; never disturb it);
//   - this node's my-node-id is the LOWEST among the UpToDate diskful
//     replicas (local + every UpToDate peer), so on a multi-UpToDate
//     RD a single deterministic node promotes — no split-brain race.
//
// Data-safety: every replica of a fresh RD shares the synthetic day0
// Current-UUID (the seed gate guarantees no divergence), so
// `drbdadm primary --force` here mints no unrelated UUID and the
// Inconsistent peer simply SyncTargets from this UpToDate source.
//
// Self-limiting: once the peer reaches UpToDate it is no longer
// Inconsistent, so the predicate stops holding and the self-heal never
// re-fires. Conservative on any probe/parse failure (returns false) —
// a missed promote just retries on the next reconcile.
func (a *Adm) NeedsRecoveryPromote(ctx context.Context, resource string) bool {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 || status[0].NodeID == nil {
		return false
	}

	res := status[0]

	// Local must be a fully-populated SyncSource: diskful + UpToDate.
	if !localIsUpToDate(res.Devices) {
		return false
	}

	// Never disturb an existing Primary anywhere in the RD.
	if Role(res.Role).IsPrimary() {
		return false
	}

	// Scan peers for the wedge symptom (a diskful peer Inconsistent), a
	// peer already Primary (veto), and whether any UpToDate peer outranks
	// us (only the lowest my-node-id among UpToDate replicas promotes).
	anyPeerInconsistent, weAreLowestUpToDate, peerPrimary := scanRecoveryPromotePeers(res.Connections, *res.NodeID)
	if peerPrimary {
		return false
	}

	return anyPeerInconsistent && weAreLowestUpToDate
}

// scanRecoveryPromotePeers inspects a resource's peer connections for the
// Bug 366 recovery-promote decision and reports: whether any diskful peer
// is WEDGED Inconsistent (the symptom that needs a forced SyncSource),
// whether this node still holds the lowest my-node-id among the UpToDate
// diskful replicas (so a single deterministic node promotes), and whether
// any peer is already Primary (a veto — a Primary already drives the sync).
//
// An Inconsistent peer that is being ACTIVELY resynced from this node
// (replication-state SyncSource — or WFBitMapS, the bitmap-exchange step
// immediately before it — with resync-suspended "no") is NOT the wedge:
// the sync machinery is already running and will finish on its own.
// Firing the recovery-promote there made every real initial sync
// (e.g. a fresh FILE_THIN 512M create, ~2 min on the loop substrate)
// churn a pointless `primary --force` → `secondary` cycle every throttle
// window for the whole duration. The genuine Bug 366 wedge state — dual
// SyncSource collapsed into resync-suspended:peer at done:0.00 — still
// qualifies, because there resync-suspended is NOT "no".
func scanRecoveryPromotePeers(conns []drbdsetupStatusConnection, myID int32) (bool, bool, bool) {
	var (
		anyPeerInconsistent bool
		weAreLowestUpToDate = true
		peerPrimary         bool
	)

	for _, conn := range conns {
		if Role(conn.PeerRole).IsPrimary() {
			peerPrimary = true
		}

		for _, pd := range conn.PeerDevices {
			switch DiskState(pd.PeerDiskState) {
			case DiskStateInconsistent:
				if !peerDeviceActivelySyncing(pd) {
					anyPeerInconsistent = true
				}
			case DiskStateUpToDate:
				// Another UpToDate diskful replica. The lowest
				// my-node-id among UpToDate replicas is the sole
				// promoter — defer if this peer outranks us.
				if conn.PeerNodeID < myID {
					weAreLowestUpToDate = false
				}
			case DiskStateConsistent, DiskStateOutdated, DiskStateDiskless,
				DiskStateAttaching, DiskStateDetaching, DiskStateFailed,
				DiskStateNegotiating, DiskStateDUnknown:
				// Not the wedge symptom and not a competing UpToDate
				// promoter; ignore.
			default:
				// Unknown/empty state — ignore.
			}
		}
	}

	return anyPeerInconsistent, weAreLowestUpToDate, peerPrimary
}

// peerDeviceActivelySyncing reports whether an Inconsistent peer device
// is already being driven to UpToDate by a live, unsuspended resync
// from this node: replication-state SyncSource (or WFBitMapS — the
// bitmap-exchange handshake step that immediately precedes SyncSource)
// with resync-suspended "no" (an empty token is treated as "no" for
// drbd-utils versions that omit the field when nothing is suspended).
// Such a peer needs no recovery-promote — the kernel finishes the sync
// on its own; promoting mid-sync only churns Primary/Secondary state.
func peerDeviceActivelySyncing(peerDev drbdsetupStatusPeerDevice) bool {
	switch peerDev.ReplicationState {
	case "SyncSource", "WFBitMapS":
	default:
		return false
	}

	return peerDev.ResyncSuspended == "no" || peerDev.ResyncSuspended == ""
}

// NeedsSoloPromote probes the live kernel via `drbdsetup status <res>
// --json` and reports whether THIS node is a lone, peerless diskful
// replica wedged below UpToDate — the case where a force-primary is the
// ONLY way to reach UpToDate because there is no peer to SyncTarget
// from.
//
// Why (solo diskless→diskful toggle wedge): when the operator flips the
// LAST/ONLY replica of an RD from diskless to diskful (r-full Phase 6:
// every prior diskful was deleted, a diskless witness re-added, then
// `r td -s <pool>`), the satellite carves a fresh lower disk and the
// replica comes up Inconsistent. Two upstream-aligned data-safety gates
// then conspire to leave it Inconsistent forever:
//
//   - the dispatcher suppresses the `auto-primary` seed on an
//     INITIALIZED RD (BuildDesired's `!rdInitialized(rd)` gate, the
//     respawn-StandAlone fix), so no force-primary / case-B UpToDate
//     winner seed fires;
//   - resolveVolumeSeed refuses the day0 skip when SkipInitialSync==false
//     (the offline-safety fix), so the replica is seeded to SyncTarget a
//     peer rather than declare itself UpToDate.
//
// Both are correct in their multi-replica intent: never fabricate an
// UpToDate-empty replica while a real data peer exists. But for a SOLO
// replica with ZERO peers there is no other copy to diverge from and no
// SyncSource to wait for — the operator's explicit toggle to diskful IS
// the instruction to make this the authoritative copy. NeedsRecoveryPromote
// cannot cover it: that predicate requires the local to be ALREADY
// UpToDate and a PEER to be Inconsistent — the exact inverse of the solo
// case. Hence this dedicated peerless predicate.
//
// Returns true ONLY when ALL hold, so the promote is data-safe and
// self-limiting:
//   - the kernel slot exists and reports a my-node-id;
//   - there are ZERO peer connections (a genuinely solo replica — never
//     act when any peer slot exists, where the recovery / SyncTarget
//     paths own convergence and a force-primary could mint a divergent
//     Current UUID);
//   - the local role is not already Primary (nothing to do);
//   - the local replica is diskful but NOT UpToDate (Inconsistent /
//     Consistent / Outdated): a diskless local has no disk to promote,
//     and an already-UpToDate local needs no promote.
//
// Self-limiting: once `primary --force` flips the lone slot to UpToDate
// the predicate stops holding, so it never re-fires. Conservative on any
// probe/parse failure (returns false) — a missed promote just retries on
// the next reconcile.
func (a *Adm) NeedsSoloPromote(ctx context.Context, resource string) bool {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 || status[0].NodeID == nil {
		return false
	}

	res := status[0]

	// Solo only: any peer connection means another replica exists, and
	// the recovery-promote / SyncTarget paths own convergence there. A
	// force-primary against a peer could mint a divergent Current UUID
	// and split-brain.
	if len(res.Connections) != 0 {
		return false
	}

	// Never disturb an already-Primary slot.
	if Role(res.Role).IsPrimary() {
		return false
	}

	// Promote only a diskful-but-not-UpToDate local: a diskless local
	// has no disk to promote, an already-UpToDate one needs none.
	return localIsDiskfulBelowUpToDate(res.Devices)
}

// localIsDiskfulBelowUpToDate reports whether the local replica has at
// least one diskful volume and EVERY diskful volume sits below UpToDate
// (Inconsistent / Consistent / Outdated). A Diskless device disqualifies
// the replica (nothing to promote); an empty device list (slot mid-
// negotiation) yields false — conservative. Used by NeedsSoloPromote to
// confirm a lone replica genuinely needs the force-primary nudge.
func localIsDiskfulBelowUpToDate(devices []drbdsetupStatusDevice) bool {
	if len(devices) == 0 {
		return false
	}

	for _, d := range devices {
		switch DiskState(d.DiskState) {
		case DiskStateInconsistent, DiskStateConsistent, DiskStateOutdated:
			// A diskful volume below UpToDate — the promote target.
		case DiskStateUpToDate, DiskStateDiskless, DiskStateAttaching,
			DiskStateDetaching, DiskStateFailed, DiskStateNegotiating,
			DiskStateDUnknown:
			// UpToDate (no promote needed), diskless (nothing to
			// promote), or a transient/failed state — do not act.
			return false
		default:
			return false
		}
	}

	return true
}

// localIsUpToDate reports whether at least one local diskful volume is
// UpToDate and none is in a non-UpToDate diskful state. A diskless
// local replica (no disk to be a SyncSource) yields false. Empty
// device list (slot mid-negotiation) yields false — conservative.
func localIsUpToDate(devices []drbdsetupStatusDevice) bool {
	if len(devices) == 0 {
		return false
	}

	for _, d := range devices {
		switch DiskState(d.DiskState) {
		case DiskStateUpToDate:
			// good
		case DiskStateDiskless, DiskStateInconsistent, DiskStateConsistent,
			DiskStateOutdated, DiskStateAttaching, DiskStateDetaching,
			DiskStateFailed, DiskStateNegotiating, DiskStateDUnknown:
			// Any non-UpToDate diskful volume disqualifies the local
			// replica as a clean SyncSource.
			return false
		default:
			return false
		}
	}

	return true
}

// NeedsLateAddPromote probes the live kernel via `drbdsetup status
// <res> --json` and reports whether THIS node must `primary --force`
// to unstick a LATE-ADDED volume that wedged Inconsistent on every
// diskful replica with no SyncSource (BUG-048, the concurrent two-VD
// add on a ≥3-diskful RD).
//
// Why a separate gate from NeedsRecoveryPromote / NeedsSoloPromote:
//   - NeedsRecoveryPromote requires the LOCAL to be already UpToDate and
//     a PEER Inconsistent — but in this wedge the local volume is ALSO
//     Inconsistent (no replica ever won the volume's UpToDate seed), so
//     that predicate can never fire. It is also gated by the dispatcher's
//     `auto-primary`, which is suppressed on an INITIALIZED RD (every
//     late-add lands on one) so the satellite-side maybeRecoveryPromote
//     short-circuits before even probing.
//   - NeedsSoloPromote requires ZERO peers; here peers exist.
//
// SPLIT-BRAIN SAFETY (the two gates that make this distinct from a day0
// bootstrap — without them the predicate misfired on a fresh RD's vol-0
// and two non-lowest nodes simultaneously force-primaried into
// split-brain):
//
//  1. A LATE-add wedge means the RD is PAST first activation, so at
//     least one local volume is already UpToDate (the earlier
//     volumes). A pure day0 bootstrap has NO UpToDate volume yet —
//     EVERY volume is transiently Inconsistent while the normal winner
//     election + auto-promote run. Requiring a local UpToDate sibling
//     means this predicate can NEVER fire during day0.
//  2. EVERY peer connection must be fully Connected with the wedged
//     volume's peer-disk OBSERVED. The lowest-node-id election is only
//     sound with COMPLETE peer information; at day0 t+1s peers are still
//     StandAlone / Connecting, so each node would see a partial peer set
//     and several could each conclude "I am lowest" → simultaneous
//     force-primary. Deferring until every peer is connected guarantees
//     every node computes the election over the same full set, so
//     EXACTLY ONE (the true lowest id) promotes.
//
// Beyond those, returns true ONLY when ALL hold, so the force-primary is
// data-safe + self-limiting:
//   - the kernel slot exists and reports a my-node-id;
//   - the local role is NOT Primary and NO peer is Primary;
//   - at least one LOCAL diskful volume is Inconsistent, and for EVERY
//     such volume NO connected peer exposes committed data
//     (peer-disk UpToDate / Consistent / Outdated) — the defining wedge;
//     a peer with data means SyncTarget instead (Bug 342 guard);
//   - none of those wedged volumes is already being actively resynced;
//   - this node's my-node-id is the LOWEST among the wedged volume's
//     diskful replicas.
//
// Data-safety: a fresh late-added volume's metadata was seeded at the
// deterministic day0 current-UUID on every replica (the seed path runs
// before bring-up), so `primary --force` here mints no UNRELATED UUID;
// the Inconsistent peers simply SyncTarget from this now-Primary source.
// Self-limiting: once the peers reach UpToDate the predicate stops
// holding. Conservative on any probe/parse failure → false.
func (a *Adm) NeedsLateAddPromote(ctx context.Context, resource string) bool {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		return false
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 || status[0].NodeID == nil {
		return false
	}

	res := status[0]

	// Never disturb an existing Primary anywhere in the RD.
	if Role(res.Role).IsPrimary() {
		return false
	}

	for _, conn := range res.Connections {
		if Role(conn.PeerRole).IsPrimary() {
			return false
		}
	}

	// Gate 1 (split-brain safety): the RD must be PAST day0 — at least one
	// local volume already UpToDate. A pure day0 bootstrap has none, so
	// the predicate cannot misfire there.
	if !localHasUpToDateVolume(res.Devices) {
		return false
	}

	// Gate 2 (split-brain safety): every peer must be fully connected so
	// the lowest-node-id election runs over COMPLETE peer information.
	if !allPeersConnected(res.Connections) {
		return false
	}

	// Which local diskful volumes are wedged Inconsistent?
	wedged := localInconsistentVolumes(res.Devices)
	if len(wedged) == 0 {
		return false
	}

	// For every wedged volume: no peer may hold data (else SyncTarget),
	// none may be actively resyncing (else let it finish), and we must be
	// the lowest node-id among its non-diskless diskful replicas.
	for vol := range wedged {
		ok := lateAddVolumeNeedsLocalPromote(res.Connections, vol, *res.NodeID)
		if !ok {
			return false
		}
	}

	return true
}

// localHasUpToDateVolume reports whether at least one local device is
// UpToDate — the proof that the RD is past first activation (a day0
// bootstrap has every volume still Inconsistent). Gate 1 of the
// split-brain-safe NeedsLateAddPromote.
func localHasUpToDateVolume(devices []drbdsetupStatusDevice) bool {
	for _, d := range devices {
		if DiskState(d.DiskState) == DiskStateUpToDate {
			return true
		}
	}

	return false
}

// allPeersConnected reports whether EVERY peer connection is in the
// Connected connection-state. Gate 2 of NeedsLateAddPromote: the
// lowest-node-id promoter election is only sound with complete peer
// information, so any StandAlone / Connecting / unconnected peer (the
// day0 t+1s state) defers the self-heal. A resource with zero peer
// connections is NOT covered here (that is NeedsSoloPromote's domain) —
// returns false so the late-add gate never acts peerless.
func allPeersConnected(conns []drbdsetupStatusConnection) bool {
	if len(conns) == 0 {
		return false
	}

	for _, conn := range conns {
		if conn.ConnectionStr != "Connected" {
			return false
		}
	}

	return true
}

// localInconsistentVolumes returns the set of local volume numbers whose
// disk-state is Inconsistent. A volume in any other state (UpToDate,
// Diskless, Negotiating, …) is excluded — only a stuck-Inconsistent
// local volume is a late-add-promote candidate.
func localInconsistentVolumes(devices []drbdsetupStatusDevice) map[int32]struct{} {
	out := map[int32]struct{}{}

	for _, d := range devices {
		if DiskState(d.DiskState) == DiskStateInconsistent {
			out[d.VolumeNumber] = struct{}{}
		}
	}

	return out
}

// lateAddVolumeNeedsLocalPromote reports whether `vol` is a genuine
// late-add wedge that THIS node (myID) must force-primary: no connected
// peer exposes committed data for it, no peer is actively resyncing it,
// and myID is the lowest node-id among the volume's non-diskless diskful
// replicas. Conservative: any peer with data, any active resync, or any
// lower-id non-diskless peer returns false.
func lateAddVolumeNeedsLocalPromote(conns []drbdsetupStatusConnection, vol, myID int32) bool {
	weAreLowest := true

	for _, conn := range conns {
		for _, peerDev := range conn.PeerDevices {
			if peerDev.VolumeNumber != vol {
				continue
			}

			switch DiskState(peerDev.PeerDiskState) {
			case DiskStateUpToDate, DiskStateConsistent, DiskStateOutdated:
				// A peer holds real data — must SyncTarget from it, never
				// force-primary (Bug 342 unrelated-data guard).
				return false
			case DiskStateInconsistent:
				// A fellow wedged diskful replica. It competes for the
				// lowest-id promoter election; if it outranks us, defer.
				if conn.PeerNodeID < myID {
					weAreLowest = false
				}

				if peerDeviceActivelySyncing(peerDev) {
					// A live resync is already driving this volume — let
					// it finish rather than churn a promote.
					return false
				}
			case DiskStateAttaching, DiskStateNegotiating, DiskStateDUnknown:
				// BUG-048 (≥3-replica double-promoter wedge): a freshly
				// late-added volume on a LOWER-id diskful peer passes
				// through Attaching/Negotiating/DUnknown while its kernel
				// slot brings the new volume up — the peer has NOT yet
				// settled to Inconsistent. The old code treated these
				// transient states as "not a competing diskful promoter;
				// ignore", so a HIGHER-id node observing a lower-id peer
				// mid-bring-up wrongly concluded "I am lowest" and force-
				// primaried. Both the higher-id node AND the true-lowest
				// node (once it finished bring-up) then promoted, minting
				// divergent current-UUIDs → the volume wedged PausedSyncS /
				// StandAlone split-brain with no convergence (stand-observed
				// on 3-diskful: node-id 1 and node-id 0 both force-primaried
				// the same late volume). A lower-id peer in a transient
				// bring-up state is a real diskful replica that WILL win the
				// election, so defer to it. DUnknown is also the
				// connection-not-fully-negotiated state of a diskful peer —
				// deferring is the conservative, split-brain-safe choice
				// (the next reconcile re-evaluates once the peer settles).
				if conn.PeerNodeID < myID {
					weAreLowest = false
				}
			case DiskStateDiskless, DiskStateDetaching, DiskStateFailed:
				// Diskless witness (steady-state of a tiebreaker — never a
				// diskful promoter, deferring to it would deadlock the
				// promote) or a failed/detaching local-disk peer that holds
				// no data. Not a competing diskful promoter; ignore.
			default:
				// Unknown/empty — ignore.
			}
		}
	}

	return weAreLowest
}

// DownVeto is the tri-state outcome of the Bug 350 kernel-truth probe
// the satellite consults before committing a `drbdadm down` on the
// INACTIVE path. It separates "definitely safe to down" from "must
// defer the down" so the caller can fail CLOSED on inconclusive
// cold-satellite timing instead of barrelling through.
type DownVeto int

const (
	// DownAllowed: the kernel was probed conclusively and the slot is
	// either genuinely absent (down is a no-op) or loaded-but-quiescent
	// (no peer-device mid-resync). Safe to `drbdadm down`.
	DownAllowed DownVeto = iota

	// DownVetoResync: a peer-device is currently SyncSource/SyncTarget.
	// Downing now aborts the resync and strands the peer Inconsistent
	// forever (out-of-sync=0, never finalizes). Defer.
	DownVetoResync

	// DownVetoInconclusive: the kernel probe failed or returned an
	// ambiguous/empty result that is NOT the verbatim "resource not
	// loaded" branch — the dominant cold-satellite timing failure mode
	// (truncated JSON, netlink hiccup, status timeout) while a slot may
	// still be loaded or mid-bring-up. We cannot confirm INACTIVE is the
	// true desired state, so we fail CLOSED and defer; the next reconcile
	// (warm cache + warm kernel) re-evaluates. NOT returned for a clean
	// not-loaded slot — that yields DownAllowed so a genuinely idle
	// resource still downs and the defer can never spin forever.
	DownVetoInconclusive
)

// EvaluateDownVeto probes the live kernel via `drbdsetup status
// <res> --json` and classifies whether the INACTIVE-path `drbdadm
// down` is safe RIGHT NOW (Bug 350). Unlike the original fail-OPEN
// IsResyncInFlight, this fails CLOSED on an inconclusive probe so a
// transient cold-satellite hiccup can never let a spurious down abort
// a just-reactivated replica's resync.
//
// Classification:
//   - probe error matching the verbatim not-loaded branch
//     (No such resource / Unknown resource / …) → DownAllowed: the slot
//     is conclusively gone, down is a no-op. This is the anti-infinite-
//     defer guard — once the slot is actually down, the probe is
//     conclusive and the caller stops deferring.
//   - any OTHER probe error (timeout, netlink hiccup) → DownVetoInconclusive.
//   - exit 0 but unparseable / empty array → DownVetoInconclusive
//     (drbdsetup emitted partial output for a slot that almost certainly
//     still exists; a truly-absent slot exits non-zero with a not-loaded
//     message, handled above).
//   - parsed, some peer-device SyncSource/SyncTarget → DownVetoResync.
//   - parsed, all peer-devices quiescent → DownAllowed.
func (a *Adm) EvaluateDownVeto(ctx context.Context, resource string) DownVeto {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
	if err != nil {
		// Conclusive absence (clean not-loaded) → down is a no-op, allow
		// it so a genuinely idle/downed resource is never deferred
		// indefinitely. Every other error is an inconclusive probe
		// failure → fail closed.
		if isResourceNotLoadedErr(err, out) {
			return DownAllowed
		}

		return DownVetoInconclusive
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 {
		// Exit 0 yet no usable resource block: under cold-satellite
		// timing drbdsetup can emit truncated/partial JSON for a slot
		// that is in fact loaded. A truly-absent slot exits non-zero
		// (handled above), so reaching here means "ambiguous, slot
		// likely present" → fail closed.
		return DownVetoInconclusive
	}

	for _, conn := range status[0].Connections {
		for _, pd := range conn.PeerDevices {
			if ReplicationState(pd.ReplicationState).IsSyncing() {
				return DownVetoResync
			}
		}
	}

	return DownAllowed
}

// HasDisklessVolume reports whether any of the named resource's
// volumes are currently in a "not-attached" disk state in the
// kernel — specifically `Diskless`, `Detaching`, or `Failed`. Used
// by the reconciler's runAdjust dispatch to detect the Bug 280
// race window:
//
//   - Operator runs `drbdadm detach --force <rsc>` against the
//     satellite shell. The kernel transitions UpToDate →
//     Detaching → Diskless and emits `change device disk:Diskless`
//     on its events2 stream.
//   - The observer's UpToDate→Diskless gate writes
//     `DrbdOptions/SkipDisk=True` onto Spec.Props and the kernel's
//     Status update fires a parallel reconcile.
//   - A reconcile already in flight when the operator's detach
//     command landed loaded `res` from cache BEFORE the prop write
//     hit the apiserver. Its `dr.Spec.Props` view has SkipDisk
//     absent, runAdjust dispatches plain `drbdadm adjust`, and the
//     disk re-attaches before the operator's poll can observe
//     Diskless.
//
// `HasDisklessVolume` lets runAdjust probe the kernel directly
// — the kernel is the authority on the disk's current state,
// independent of any apiserver cache trail. When the kernel reports
// a not-attached state we coerce the adjust onto `--skip-disk`
// regardless of what the prop view says. Safe because:
//
//   - The first-activation path goes through `drbdadm up`, not
//     adjust; this probe is only consulted by runAdjust, so a
//     not-yet-attached resource (kernel slot absent → status
//     returns non-zero → IsLoaded false → runApplyDRBDVerb routes
//     to Up) never reaches here.
//   - On a healthy steady-state diskful replica the kernel reports
//     disk:UpToDate, the probe returns false, and runAdjust
//     continues onto plain adjust as before.
//   - `--skip-disk` on an already-UpToDate kernel is a no-op for
//     the disk portion (it only skips disk-level reconfig; the
//     connections/peers half still adjusts), so an over-zealous
//     coerce-to-SkipDisk wouldn't break anything either.
//
// Why include `Detaching` (Bug 280 follow-up):
//   - There is a sub-second window after `drbdadm detach --force`
//     where `drbdsetup status` reports `disk:Detaching` rather
//     than `disk:Diskless`. A reconcile that probes during this
//     window would otherwise miss the signal, fall through to
//     plain `drbdadm adjust`, and re-attach via drbd-utils'
//     compare_volume(kern->disk=="none" + conf->disk=<path>) →
//     attach_cmd, racing the operator's recipe and flipping the
//     replica back to UpToDate before any external poll can see
//     Diskless. Detaching is unambiguously "kernel is mid-tear-
//     down of the disk binding" — there is no legitimate reason
//     to schedule attach_cmd in this state.
//
// Why include `Failed`:
//   - The observer already stamps SkipDisk on Failed → Diskless,
//     but until that prop write propagates the kernel probe is
//     the only safety net. A `Failed` lower disk is by definition
//     not safe to reattach via plain adjust — coerce to
//     `--skip-disk` until the operator clears the prop or replaces
//     the disk.
//
// `Attaching` is intentionally NOT included: it's a transient
// state on the way INTO UpToDate, so we're already past the
// detach window. Coercing skip-disk there would be a benign
// no-op for the disk portion but adds nothing of value.
//
// Convention (matches IsLoaded):
//   - non-zero exit from drbdsetup → false + nil (slot absent;
//     not our race window)
//   - parses each indented volume line (`disk:<state>`) and returns
//     true on the first match for any of the not-attached states.
func (a *Adm) HasDisklessVolume(ctx context.Context, resource string) (bool, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", "--verbose", resource)
	if err != nil {
		// Slot absent / netlink hiccup → not in the race window we
		// care about. Same convention IsLoaded uses (zero value +
		// nil error) so the caller never has to branch on err.
		_ = err

		return false, nil
	}

	// `drbdsetup status --verbose` emits one block per resource;
	// per-volume lines are indented and carry `disk:<state>`. The
	// not-attached states are `Diskless` (steady state post-detach),
	// `Detaching` (sub-second transient during `drbdadm detach
	// --force`), and `Failed` (lower disk returned I/O errors —
	// observer is about to stamp SkipDisk but we can't wait for
	// the prop trail).
	notAttached := []string{"disk:Diskless", "disk:Detaching", "disk:Failed"}

	for line := range strings.SplitSeq(string(out), "\n") {
		for _, token := range notAttached {
			if !strings.Contains(line, token) {
				continue
			}

			// Skip `peer-disk:<state>` lines — that's the remote
			// peer's disk state, not ours. The local-disk line
			// carries the `disk:` token without the `peer-` prefix;
			// match on a leading space (' disk:') to disambiguate
			// from `peer-disk:`.
			peerToken := "peer-" + token

			if strings.Contains(line, peerToken) &&
				!strings.Contains(line, " "+token) {
				continue
			}

			return true, nil
		}
	}

	return false, nil
}

// ParseConfiguredDeviceMinor inspects an error returned by `drbdadm
// create-md` (or `drbdsetup new-minor`) and, when the failure was caused
// by the kernel already owning that minor for a different resource,
// returns the offending minor number and ok=true. Otherwise returns
// (-1, false).
//
// Drbdmeta surfaces this as the line `Device '<minor>' is configured!`
// (typically immediately before `Command 'drbdmeta <minor> ... create-md
// ...' terminated with exit code 20`). This collision is the kernel's
// safety guard: it refuses to overwrite metadata for a minor currently
// attached to another in-flight DRBD resource.
//
// In production the collision happens when a foreign or zombie kernel
// slot from a torn-down test (or a stuck `drbdsetup down` from a
// `blockstor_drbd_stuck_state` D-state holder) still owns the minor the
// allocator just handed to a new multi-volume RD. The allocator scans
// CRD-tracked minors but cannot see kernel-only zombies, so
// LowestFreeMinor returns a value that collides with the orphan on the
// satellite's local node. The caller pairs this signal with
// `ResourceOwningMinor` to identify the zombie and a best-effort
// `drbdsetup down` to free it before retrying.
func ParseConfiguredDeviceMinor(err error) (int32, bool) {
	if err == nil {
		return -1, false
	}

	const marker = "Device '"

	_, rest, ok := strings.Cut(err.Error(), marker)
	if !ok {
		return -1, false
	}

	num, tail, ok := strings.Cut(rest, "'")
	if !ok {
		return -1, false
	}

	// Pattern continues "... is configured!" — confirm to avoid
	// false positives on unrelated `Device '...'` strings.
	if !strings.HasPrefix(tail, " is configured") {
		return -1, false
	}

	minor, parseErr := strconv.ParseInt(num, 10, 32)
	if parseErr != nil {
		return -1, false
	}

	return int32(minor), true
}

// ResourceOwningMinor returns the kernel-resident DRBD resource name
// that currently owns the given device minor, or "" + nil when no
// kernel slot holds it (steady state on a freshly-rebooted node).
//
// Parses `drbdsetup status --verbose` which emits one resource block
// at column 0 followed by indented per-volume lines that include
// `minor:<N>`. The function pairs the most recent column-0 resource
// name with each indented `minor:<N>` token: when the requested minor
// matches, the paired resource name wins.
//
// Used by the create-md collision recovery path: when CreateMD fails
// with "Device '<minor>' is configured!" (see ParseConfiguredDeviceMinor),
// the caller asks who owns the minor and, if that owner is NOT the
// resource being initialised, best-effort `drbdsetup down`s it to free
// the minor and retries create-md.
func (a *Adm) ResourceOwningMinor(ctx context.Context, minor int32) (string, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", "--verbose")
	if err != nil {
		// `No currently configured DRBD found.` — kernel has no
		// resources, so no minor is owned. Surface "" + nil.
		if strings.Contains(string(out), "No currently configured DRBD") ||
			strings.Contains(err.Error(), "No currently configured DRBD") {
			return "", nil
		}

		return "", errors.Wrap(err, "drbdsetup status --verbose")
	}

	needle := fmt.Sprintf("minor:%d", minor)

	var currentResource string

	for line := range strings.SplitSeq(string(out), "\n") {
		// Column-0 line opens a new resource block.
		if line != "" && line[0] != ' ' && line[0] != '\t' &&
			!strings.HasPrefix(strings.TrimSpace(line), "#") {
			fields := strings.Fields(line)
			if len(fields) > 0 {
				currentResource = fields[0]
			}

			continue
		}

		// Indented volume line — look for the minor token. Use a
		// space-bounded match so `minor:200` doesn't accidentally
		// fire on `minor:20001`.
		if slices.Contains(strings.Fields(line), needle) {
			return currentResource, nil
		}
	}

	return "", nil
}

// run is the single shell-out site so every drbdadm error gets
// uniform context (subcommand + resource) for log triage.
func (a *Adm) run(ctx context.Context, args ...string) error {
	_, err := a.exec.Run(ctx, "drbdadm", args...)
	if err != nil {
		return errors.Wrapf(err, "drbdadm %s", args[0])
	}

	return nil
}
