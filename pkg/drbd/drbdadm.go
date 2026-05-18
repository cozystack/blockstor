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
	"fmt"
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
func (a *Adm) HasMD(ctx context.Context, resource string) (bool, error) {
	out, err := a.exec.Run(ctx, "drbdadm", "dump-md", resource)
	if err != nil {
		// `No valid meta data found` / drbdmeta "missing image" / etc.
		// all bubble up as non-zero exit. Treat as "not yet
		// initialised" — the caller's create-md will either succeed
		// (truly missing) or surface a more specific failure.
		return false, nil //nolint:nilerr // non-zero exit is the "metadata absent" signal, not a bubble-up error
	}

	return len(out) > 0, nil
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
// command. `--assume-clean` skips the resync of the new bytes since
// they were just allocated, which would otherwise serialise growing
// every replica.
func (a *Adm) Resize(ctx context.Context, resource string) error {
	return a.run(ctx, "resize", "--assume-clean", resource)
}

// SetGi pre-seeds the per-peer GI slot in this replica's DRBD
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
// current_uuid and bitmap_uuid to peerCurrentGi so the new replica
// claims "I'm at the peer's generation; I have no dirty bits relative
// to the peer". History is zeroed — DRBD's handshake never matches
// against history when current+bitmap match, so it doesn't matter.
//
// DRBD 9.2+ requires `--node-id <peerNodeID>` because the current/
// bitmap UUID tuple lives in a per-peer slot in the modern v09
// metadata layout. Without `--node-id`, drbdmeta refuses the call
// with "The set-gi command requires the --node-id option" (the
// e2e regression guard pins the failure shape). The caller MUST
// invoke SetGi once per peer node-id of the resource so every
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
func (a *Adm) SetGi(ctx context.Context, resource string, volume int32, device string, peerNodeID int32, peerCurrentGi string) error {
	target := fmt.Sprintf("%s/%d", resource, volume)
	gi := fmt.Sprintf("%s:%s:0:0", peerCurrentGi, peerCurrentGi)

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

// HasDisklessVolume reports whether any of the named resource's
// volumes are currently in disk:Diskless state in the kernel. Used
// by the reconciler's runAdjust dispatch to detect the Bug 280
// race window:
//
//   - Operator runs `drbdadm detach --force <rsc>` against the
//     satellite shell. The kernel emits `change device disk:Diskless`
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
// Diskless we coerce the adjust onto `--skip-disk` regardless of
// what the prop view says. Safe because:
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
// Convention (matches IsLoaded):
//   - non-zero exit from drbdsetup → false + nil (slot absent;
//     not our race window)
//   - parses each indented volume line (`disk:<state>`) and returns
//     true on the first match for `disk:Diskless`.
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
	// per-volume lines are indented and carry `disk:<state>`.
	// `disk:Diskless` is the post-detach steady state; we don't
	// match transient `disk:Detaching` / `disk:Attaching` because
	// the kernel may bounce through those during a healthy reconcile
	// and we'd false-trip the SkipDisk coerce.
	for line := range strings.SplitSeq(string(out), "\n") {
		if !strings.Contains(line, "disk:Diskless") {
			continue
		}

		// Skip `peer-disk:Diskless` lines — that's the remote
		// peer's disk state, not ours. The local-disk line carries
		// the `disk:` token without the `peer-` prefix.
		if strings.Contains(line, "peer-disk:Diskless") &&
			!strings.Contains(line, " disk:Diskless") {
			continue
		}

		return true, nil
	}

	return false, nil
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
