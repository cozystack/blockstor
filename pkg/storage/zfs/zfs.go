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

// Package zfs is the ZFS-pool storage backend. It supports both the
// LINSTOR `ZFS` (thick) and `ZFS_THIN` (sparse zvols) provider kinds via
// the same Provider; Config.Thin toggles between them.
package zfs

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Config parametrises a Provider with the ZFS pool name and whether it
// runs in thin (sparse zvol) mode.
type Config struct {
	Pool string
	Thin bool
}

// Provider implements storage.Provider against ZFS on the host.
type Provider struct {
	cfg  Config
	exec storage.Exec
	now  func() time.Time
}

// NewProvider constructs a Provider. The Exec is injected so unit tests
// can drive it without ZFS installed.
func NewProvider(cfg Config, ex storage.Exec) *Provider {
	return &Provider{cfg: cfg, exec: ex, now: time.Now}
}

// Kind returns "ZFS" or "ZFS_THIN" depending on Config.Thin.
func (p *Provider) Kind() string {
	if p.cfg.Thin {
		return "ZFS_THIN"
	}

	return "ZFS"
}

// ResizeZeroFills reports true for both ZFS and ZFS_THIN: ZFS reads
// zeros for never-written blocks (thick uses a refreservation but the
// blocks are still zero-on-read until written), so a grown region
// [old_size, new_size) is byte-identical (all zeros) across replicas.
// The `drbdadm resize --assume-clean` fast path is sound — no resync of
// the grown region is needed (Bug 395). See storage.ResizeZeroFiller.
func (*Provider) ResizeZeroFills() bool { return true }

// Pool returns the underlying ZFS pool name (`zpool list` identifier).
// The satellite Reconciler's FlushBackingDevices uses this to issue
// `zpool sync <pool>` between drbdadm suspend-io and `zfs snapshot`
// — without that TXG-commit barrier the snapshot captures a
// pre-commit dataset state and misses up to one TXG (default 5s)
// worth of writes (P0 stale-snapshot bug, 2026-05-23).
func (p *Provider) Pool() string {
	return p.cfg.Pool
}

// CreateVolume creates a zvol. Idempotent: existing dataset → still
// reconciles refreservation (Bug 255) so a crash between `zfs create`
// and the implicit thick-refreservation pass leaves no permanently
// sparse-thick state across reconciles.
//
// Bug 255 (P2, capacity safety): the pre-fix idempotency check returned
// early BEFORE ensureRefreservation, so any path that observed an
// existing-but-historically-sparse dataset silently downgraded the
// thick guarantee. Same root cause as Bug 253/254 — every path that may
// observe an existing dataset must end on ensureRefreservation. Thin
// mode no-ops inside the helper.
func (p *Provider) CreateVolume(ctx context.Context, vol storage.Volume) error {
	dataset := p.volumeDataset(vol)

	if p.datasetExists(ctx, dataset) {
		// Bug 255 fix: do NOT short-circuit before ensuring refreservation.
		// A prior crash may have left the dataset sparse-thick.
		return p.ensureRefreservation(ctx, dataset)
	}

	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	args := []string{"create"}
	if p.cfg.Thin {
		args = append(args, "-s")
	}

	args = append(args, "-V", strconv.FormatInt(sizeMiB, 10)+"M", dataset)

	_, err := p.exec.Run(ctx, "zfs", args...)
	if err != nil {
		return errors.Wrapf(err, "zfs create %s", dataset)
	}

	// THICK mode: `zfs create -V` (no `-s`) reserves the volume size up
	// front, but explicitly re-set refreservation so a future
	// ensureRefreservation pre-check observes the value rather than the
	// inherited default (`zfs get refreservation` on a thick `zfs create
	// -V` zvol can report "none" on some ZFS versions because the
	// reservation is implicit). Thin mode no-ops inside the helper.
	err = p.ensureRefreservation(ctx, dataset)
	if err != nil {
		return errors.Wrapf(err, "ensure refreservation on created %s", dataset)
	}

	return nil
}

// ResizeVolume sets the zvol's volsize to vol.SizeKib (rounded up
// to MiB). zfs is happy to set the same size twice, so the call is
// idempotent. Shrinks are rejected by ZFS itself when the existing
// volume already holds more data — the CSI grow-only contract makes
// the no-shrink invariant a non-issue here.
//
// Bug 252 (P2, capacity safety): `zfs set volsize=` does NOT auto-grow
// refreservation. In thick mode, a post-restore resize would silently
// leave the grown range sparse and could hit ENOSPC mid-write —
// defeating the thick contract on the new space. After the volsize
// grow, call ensureRefreservation so the refreservation tracks the
// new volsize. Thin mode no-ops inside the helper.
func (p *Provider) ResizeVolume(ctx context.Context, vol storage.Volume) error {
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)
	dataset := p.volumeDataset(vol)

	_, err := p.exec.Run(ctx, "zfs", "set",
		"volsize="+strconv.FormatInt(sizeMiB, 10)+"M",
		dataset)
	if err != nil {
		return errors.Wrapf(err, "zfs set volsize %s", dataset)
	}

	err = p.ensureRefreservation(ctx, dataset)
	if err != nil {
		return errors.Wrapf(err, "ensure refreservation after resize %s", dataset)
	}

	return nil
}

// VolumeStatus parses `zfs list -p` output (bytes, no suffixes). Bug
// 273: bounded ctx so a SUSPENDED zpool that wedges `zfs list` in
// kernel I/O-wait cannot consume the satellite reconcile worker
// indefinitely. See withProbeTimeout below.
func (p *Provider) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	ctx, cancel := withProbeTimeout(ctx)
	defer cancel()

	out, err := p.exec.Run(ctx, "zfs",
		"list", "-H", "-p",
		"-o", "name,volsize,used",
		p.volumeDataset(vol))
	if err != nil {
		return storage.VolumeStatus{State: stateNotProvisioned}, nil
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.VolumeStatus{State: stateNotProvisioned}, nil
	}

	parts := strings.Split(line, "\t")
	if len(parts) != zfsListCols {
		return storage.VolumeStatus{}, errors.Errorf("zfs list: unexpected line %q", line)
	}

	volSizeBytes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "parse volsize")
	}

	usedBytes, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "parse used")
	}

	return storage.VolumeStatus{
		DevicePath:   "/dev/zvol/" + p.volumeDataset(vol),
		UsableKib:    volSizeBytes / bytesPerKib,
		AllocatedKib: usedBytes / bytesPerKib,
		State:        "PROVISIONED",
	}, nil
}

// PoolStatus reads `zpool list -p` for free/total bytes.
//
// An empty `zpool list` response (operator ran `zpool destroy` out
// of band) is surfaced as a `pool %q not found` error so the
// satellite's writeCapacity loop flips Status.PoolMissing=true and
// the wire view in `linstor sp l` lands state=Faulty. Without this
// check, an empty stdout would slip through as success-with-no-data
// and the pool would silently report state=Ok with zeroed capacity.
func (p *Provider) PoolStatus(ctx context.Context) (storage.PoolStatus, error) {
	// Bug 271: bounded ctx so a suspended zpool cannot block the
	// every-30-seconds writeCapacity loop. Without this the
	// controller-runtime reconcile worker stays parked in `zpool
	// list` indefinitely and `linstor sp l` returns nothing for the
	// affected node — same symptom class as the LVM Bug 270 wedge.
	ctx, cancel := withProbeTimeout(ctx)
	defer cancel()

	out, err := p.exec.Run(ctx, "zpool", "list", "-H", "-p", "-o", "size,free", p.cfg.Pool)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrapf(err, "zpool list %s", p.cfg.Pool)
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		// Bug 282: tag with ErrPoolGone so writeCapacity flips
		// PoolMissing=true. Transient zpool errors (timeout, etc.)
		// MUST NOT carry this tag.
		return storage.PoolStatus{}, errors.Wrapf(storage.ErrPoolGone,
			"zpool %s not found", p.cfg.Pool)
	}

	parts := strings.Split(line, "\t")
	if len(parts) != zpoolListCols {
		return storage.PoolStatus{}, errors.Errorf("zpool list: unexpected output %q", out)
	}

	sizeBytes, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse size")
	}

	freeBytes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse free")
	}

	return storage.PoolStatus{
		FreeCapacityKib:   freeBytes / bytesPerKib,
		TotalCapacityKib:  sizeBytes / bytesPerKib,
		SupportsSnapshots: true,
	}, nil
}

// CreateSnapshot is `zfs snapshot <pool>/<rd>_00000@<snap>`.
//
// Idempotent: pre-existing snapshot dataset → no-op (Bug 216).
// Without the fold the real `zfs` would reject the second
// `zfs snapshot` with "dataset already exists" on every reconcile
// pass, looping the satellite-side reconciler forever on an
// already-materialised snapshot. Mirrors the datasetExists pre-check
// the delete path uses (Bug 212).
func (p *Provider) CreateSnapshot(ctx context.Context, snap storage.Snapshot) error {
	dataset := p.snapshotDataset(snap)

	if p.datasetExists(ctx, dataset) {
		// A receive interrupted before its cleanup left the source's
		// snapshot on the zvol, and it may carry this name: skipping
		// would keep the restore's data under the new snapshot.
		zvol, _, _ := strings.Cut(dataset, "@")

		received, err := p.receivedSnapshot(ctx, zvol)
		if err != nil || received != dataset {
			return err
		}

		err = p.finishReceive(ctx, zvol)
		if err != nil {
			return err
		}
	}

	_, err := p.exec.Run(ctx, "zfs", "snapshot", dataset)
	if err != nil {
		return errors.Wrapf(err, "zfs snapshot %s", dataset)
	}

	return nil
}

// HasSnapshot reports whether this pool holds the snapshot.
func (p *Provider) HasSnapshot(ctx context.Context, snap storage.Snapshot) (bool, error) {
	return p.datasetExists(ctx, p.snapshotDataset(snap)), nil
}

// RestoreVolumeFromSnapshot materialises target as a `zfs clone` of
// the source snapshot. ZFS clones are CoW — instant create, lazy
// allocation as writes diverge from the snapshot.
//
// Bug 246 (P2, capacity safety): `zfs clone` always produces a sparse
// zvol regardless of the origin's reservation. In thick mode the
// cloned volume would silently lose its space guarantee and could
// hit ENOSPC mid-write — defeating the whole point of thick. After
// the clone, in thick mode, restore `refreservation=<volsize-bytes>`
// so the cloned zvol is fully reserved (same contract as
// `zfs create -V` without `-s`). The clone inherits volsize from
// origin; setting refreservation to that value makes it thick again.
//
// If the pool free space at clone time is < volsize, the
// `zfs set refreservation=…` fails with ENOSPC — propagated as the
// thick-provisioning contract working as intended.
//
// Upstream LINSTOR equivalent: `zfs clone <pool>/<src>@<snap> <pool>/<tgt>`.
//
// Idempotent: target dataset present → treat as resumed reconcile and
// skip the `zfs clone`. Bug 253 (P2, capacity safety): the pre-fix
// idempotency check returned early BEFORE the refreservation step. A
// crash between `zfs clone` and `zfs set refreservation=` left the
// dataset permanently sparse-thick across reconciles. Even on the
// idempotent-skip path, ensureRefreservation is called so the thick
// guarantee is restored on every reconcile, not just on the first
// happy-path invocation.
func (p *Provider) RestoreVolumeFromSnapshot(ctx context.Context, target storage.Volume, src storage.Snapshot) error {
	tgtDS := p.volumeDataset(target)

	if p.datasetExists(ctx, tgtDS) {
		// A cross-node receive into this zvol may have stopped before it
		// removed the snapshot it brought.
		err := p.finishReceive(ctx, tgtDS)
		if err != nil {
			return err
		}

		// Bug 253 fix: do NOT short-circuit before ensuring refreservation.
		// A prior crash may have left the dataset sparse-thick.
		return p.ensureRefreservation(ctx, tgtDS)
	}

	// The snapshot may outlive its replica: a replica delete renames a
	// zvol that has snapshots, and they stay restorable under the new name.
	srcDS, err := p.locateSnapshot(ctx, src)
	if err != nil {
		return err
	}

	if srcDS == "" {
		return errors.Wrapf(storage.ErrNotFound, "snapshot %s for clone", p.snapshotDataset(src))
	}

	_, err = p.exec.Run(ctx, "zfs", "clone", srcDS, tgtDS)
	if err != nil {
		return errors.Wrapf(err, "zfs clone %s → %s", srcDS, tgtDS)
	}

	// THICK mode: restore the space guarantee that `zfs clone` always
	// drops. Thin mode no-ops inside the helper — sparse-everywhere is
	// the thin contract by design.
	err = p.ensureRefreservation(ctx, tgtDS)
	if err != nil {
		return errors.Wrapf(err, "ensure refreservation on cloned %s", tgtDS)
	}

	return nil
}

// SendSnapshot streams `zfs send -p <pool>/<rd>_00000@<snap>` so a peer
// satellite can pipe it into its own `zfs recv` and reproduce the
// dataset byte-for-byte (including the DRBD metadata block embedded
// in the zvol). The returned ReadCloser wraps the running zfs
// process's stdout — Close kills the process via the wrapped
// context cancel, so callers can abort mid-stream.
//
// Bypasses storage.Exec because Exec.Run buffers the full output in
// memory; multi-GB snapshot streams need a pipe. FakeExec tests
// don't cover this path — the integration test on the stand drives
// it through a real peer Fetch. SendSnapshotArgs is the test seam
// for the argv shape (Bug 251).
//
// Bug 251 (P2, capacity safety, cross-node): `-p` forces zfs to
// include dataset properties (notably `refreservation`) in the stream
// so the receiver's `zfs recv` reproduces the thick guarantee.
// Without `-p`, the recv produced a sparse-by-inheritance dataset and
// the cross-node-cloned thick zvol lost its space reservation — same
// blast as Bug 246 but on the wire path. The recv-side defensively
// re-sets refreservation too (in case the peer sender is an old
// un-patched binary), see RecvSnapshot.
func (p *Provider) SendSnapshot(ctx context.Context, snap storage.Snapshot) (io.ReadCloser, error) {
	argv, err := p.SendSnapshotArgs(ctx, snap)
	if err != nil {
		return nil, err
	}

	srcDS := argv[len(argv)-1]

	cmd := exec.CommandContext(ctx, "zfs", argv...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "stdout pipe %s", srcDS)
	}

	err = cmd.Start()
	if err != nil {
		return nil, errors.Wrapf(err, "start zfs send %s", srcDS)
	}

	return &zfsSendReader{cmd: cmd, stdout: stdout}, nil
}

// SendSnapshotArgs returns the argv (excluding the leading "zfs"
// binary) that SendSnapshot would pass to `exec.CommandContext` for
// the given snapshot. Exposed so unit tests can assert command-line
// shape (notably the `-p` flag from Bug 251) without spawning a real
// `zfs send` process. The snapshot is looked up where a replica delete
// may have left it, under the renamed zvol, the same way a restore and a
// delete find it.
func (p *Provider) SendSnapshotArgs(ctx context.Context, snap storage.Snapshot) ([]string, error) {
	srcDS, err := p.locateSnapshot(ctx, snap)
	if err != nil {
		return nil, err
	}

	if srcDS == "" {
		return nil, errors.Wrapf(storage.ErrNotFound, "snapshot %s for send", p.snapshotDataset(snap))
	}

	return []string{"send", "-p", srcDS}, nil
}

// zfsSendReader bundles the zfs-send process with its stdout pipe
// so Close terminates both. Reading hits EOF when zfs send exits;
// the caller still has to Close to reap the process.
type zfsSendReader struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
}

// Read forwards to the underlying stdout pipe.
func (r *zfsSendReader) Read(p []byte) (int, error) {
	return r.stdout.Read(p) //nolint:wrapcheck // io contract preserves err shape
}

// Close kills the running zfs-send process (no-op if it already
// exited) and reaps it.
func (r *zfsSendReader) Close() error {
	_ = r.stdout.Close()

	if r.cmd.Process != nil {
		_ = r.cmd.Process.Kill()
	}

	_ = r.cmd.Wait()

	return nil
}

// RecvSnapshot reads the stream produced by a peer satellite's
// SendSnapshot and replays it through `zfs recv <pool>/<rd>_<vol>`.
// After this call the target dataset exists and is mountable; the
// embedded DRBD metadata block carries the source's UUIDs, so the
// caller follows up with drbdmeta drop-md + create-md to stamp the
// local node-id before drbdadm adjust.
//
// Idempotent on a pre-existing target dataset: the recv is skipped
// and any leftover staging artifacts cleaned up. matches
// FILE backend's resumed-reconcile semantic.
//
// Bug 251 (P2, capacity safety, cross-node): after recv completes
// successfully, in thick mode, defensively re-set `refreservation`
// on the dataset. The send side (Bug 251) now uses `zfs send -p` to
// embed props in the stream, but an old un-patched peer might still
// send a stream without `-p`, which leaves the recv'd dataset
// sparse-by-inheritance and silently downgrades the thick guarantee.
// Re-setting refreservation here defends against the peer-version
// skew window during a partial cluster upgrade.
//
// Bug 254 (P2, capacity safety): the pre-fix idempotency check
// returned early BEFORE the refreservation step. A crash between
// `zfs recv` and `zfs set refreservation=` left the recv'd dataset
// permanently sparse-thick across reconciles. Even on the
// idempotent-skip path, ensureRefreservation is called so the thick
// guarantee is restored on every reconcile.
//
// The stream is a full send of the source snapshot, so `zfs recv` leaves a
// copy of that snapshot on the new zvol. No Snapshot of the restored
// resource names it, so nothing would ever delete it, and a zvol that has a
// snapshot is renamed rather than destroyed when its replica goes: the
// restored volume's delete left `_deleted_<zvol>_<time>` and its snapshot
// behind for good. It is removed once the receive is done, as LINSTOR's
// clone service removes every snapshot of a volume it filled by send and
// receive (CloneService.doZFSCloneTargetCleanup). The zvol records its name
// in receivedSnapshotProp until then, so a receive that stopped half way is
// finished by the next restore, receive or snapshot of that name.
func (p *Provider) RecvSnapshot(ctx context.Context, target storage.Volume, source storage.Snapshot, src io.Reader) error {
	tgtDS := p.volumeDataset(target)

	// Strict probe: `zfs recv -F` over a dataset that exists, and the
	// cleanup below, would both destroy it, so a probe that failed must
	// not read as "absent".
	present, err := p.datasetPresent(ctx, tgtDS)
	if err != nil {
		return err
	}

	if present {
		err = p.finishReceive(ctx, tgtDS)
		if err != nil {
			return err
		}

		// Bug 254 fix: do NOT short-circuit before ensuring refreservation.
		// A prior crash may have left the recv'd dataset sparse-thick.
		return p.ensureRefreservation(ctx, tgtDS)
	}

	// The zvol is created already carrying the name of the snapshot to
	// remove, so a receive that stops after `zfs recv` still says what it
	// brought, and a retry finishes the cleanup instead of keeping it.
	out, err := p.exec.RunWithStdin(ctx, src, "zfs", "recv", "-F",
		"-o", receivedSnapshotProp+"="+source.SnapshotName, tgtDS)
	if err != nil {
		// On failure leave nothing behind — `zfs recv -F` may have
		// partially created the dataset; destroy it so the next
		// reconcile re-streams cleanly. It did not exist before this
		// call, so everything under it came from this stream.
		_, _ = p.exec.Run(ctx, "zfs", "destroy", "-r", tgtDS)

		return errors.Wrapf(err, "zfs recv %s: %s", tgtDS, strings.TrimSpace(string(out)))
	}

	err = p.finishReceive(ctx, tgtDS)
	if err != nil {
		return err
	}

	// THICK mode: restore the space guarantee even if the peer sender
	// shipped the stream without `-p` (Bug 251 defence-in-depth). Same
	// pattern as Bug 246's post-clone refreservation re-set. Thin mode
	// no-ops inside the helper.
	err = p.ensureRefreservation(ctx, tgtDS)
	if err != nil {
		return errors.Wrapf(err, "ensure refreservation on recv'd %s", tgtDS)
	}

	return nil
}

// datasetVolsize reads the zvol's `volsize` property in bytes. Used
// by RestoreVolumeFromSnapshot to size the post-clone refreservation
// at exactly the clone's own volsize — which `zfs clone` inherits
// from the origin snapshot, so we don't need to trust the caller's
// requested SizeKib.
func (p *Provider) datasetVolsize(ctx context.Context, dataset string) (int64, error) {
	out, err := p.exec.Run(ctx, "zfs", "get", "-Hp", "-o", "value", "volsize", dataset)
	if err != nil {
		return 0, errors.Wrapf(err, "zfs get volsize %s", dataset)
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return 0, errors.Errorf("zfs get volsize %s: empty output", dataset)
	}

	bytes, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse volsize %q", line)
	}

	return bytes, nil
}

// datasetRefreservation reads the zvol's `refreservation` property in
// bytes. ZFS reports an unset refreservation as the literal string
// "none" (and as "0" on some versions or after `zfs set
// refreservation=0`); both are normalised to int64(0) so callers can
// compare numerically against volsize without string-juggling.
func (p *Provider) datasetRefreservation(ctx context.Context, dataset string) (int64, error) {
	out, err := p.exec.Run(ctx, "zfs", "get", "-Hp", "-o", "value", "refreservation", dataset)
	if err != nil {
		return 0, errors.Wrapf(err, "zfs get refreservation %s", dataset)
	}

	line := strings.TrimSpace(string(out))
	if line == "" || line == "none" || line == "-" {
		return 0, nil
	}

	bytes, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse refreservation %q", line)
	}

	return bytes, nil
}

// ensureRefreservation is the unified refreservation policy point for
// the ZFS provider (Bug 252+253+254). On thin it's a no-op (sparse
// oversubscription is the thin contract). On thick it verifies the
// dataset's refreservation matches its volsize and re-sets it if not,
// so every path that mutates volsize OR short-circuits on dataset
// existence still lands on a thick-correct end state.
//
// Idempotent on purpose: callable on every reconcile pass and on the
// idempotent-skip branches of RestoreVolumeFromSnapshot / RecvSnapshot
// without spamming `zfs set` when the dataset is already in the right
// shape. The pre-check costs two `zfs get` calls; the set is only
// issued when refreservation < volsize (covers "none", "0", and the
// partial-write case where refreservation tracks an older volsize).
//
// Failure modes propagate as errors:
//   - volsize lookup failure on the target dataset.
//   - refreservation lookup failure.
//   - `zfs set refreservation=` failure (notably ENOSPC, which IS the
//     thick contract working as intended — the pool can't honour the
//     reservation, so the operation must visibly fail rather than
//     silently downgrade the volume to sparse).
func (p *Provider) ensureRefreservation(ctx context.Context, dataset string) error {
	if p.cfg.Thin {
		return nil
	}

	volsizeBytes, err := p.datasetVolsize(ctx, dataset)
	if err != nil {
		return errors.Wrapf(err, "lookup volsize for refreservation policy on %s", dataset)
	}

	currentRefres, err := p.datasetRefreservation(ctx, dataset)
	if err != nil {
		return errors.Wrapf(err, "lookup refreservation on %s", dataset)
	}

	if currentRefres >= volsizeBytes {
		return nil
	}

	_, err = p.exec.Run(ctx, "zfs", "set",
		"refreservation="+strconv.FormatInt(volsizeBytes, 10),
		dataset)
	if err != nil {
		return errors.Wrapf(err, "zfs set refreservation on %s", dataset)
	}

	return nil
}

// datasetExists is the idempotency primitive — analogous to lvExists.
// Bug 272: bounded ctx; same rationale as PoolStatus/VolumeStatus, but
// the hot-path impact is larger — datasetExists is called from every
// Create/Delete/Restore on the satellite reconcile worker.
func (p *Provider) datasetExists(ctx context.Context, ds string) bool {
	ctx, cancel := withProbeTimeout(ctx)
	defer cancel()

	out, err := p.exec.Run(ctx, "zfs", "list", "-H", "-o", "name", ds)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(out)) != ""
}

// probeTimeout bounds every read-only ZFS probe (zfs list / zpool
// list / zfs get). 30 s mirrors the LVM Bug 270 helper — comfortably
// above any healthy zfs response time but below the controller-
// runtime per-worker reconcile budget. Mutating calls (zfs create /
// destroy / send / recv / set) keep the caller's ctx so legitimate
// long-running operations are not truncated.
const probeTimeout = 30 * time.Second

// withProbeTimeout derives a bounded child context. If the caller
// already has a Deadline (unit test override, parent op already
// bounded), it is preserved — never shortened.
func withProbeTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, probeTimeout)
}

// volumeDataset is `<pool>/<resource>_<vol5digits>`.
func (p *Provider) volumeDataset(vol storage.Volume) string {
	return fmt.Sprintf("%s/%s_%05d", p.cfg.Pool, vol.ResourceName, vol.VolumeNumber)
}

// snapshotDataset is `<pool>/<resource>_00000@<snap>`. ZFS snapshots are
// per-dataset, so they're addressed via the parent zvol; we always
// snapshot volume 0 (multi-volume support lands in Phase 4).
func (p *Provider) snapshotDataset(snap storage.Snapshot) string {
	return fmt.Sprintf("%s/%s_00000@%s", p.cfg.Pool, snap.ResourceName, snap.SnapshotName)
}

const (
	stateNotProvisioned = storage.StateNotProvisioned
	mibPerKib           = 1024
	bytesPerKib         = 1024
	zfsListCols         = 3
	zpoolListCols       = 2
)
