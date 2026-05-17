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
}

// NewProvider constructs a Provider. The Exec is injected so unit tests
// can drive it without ZFS installed.
func NewProvider(cfg Config, ex storage.Exec) *Provider {
	return &Provider{cfg: cfg, exec: ex}
}

// Kind returns "ZFS" or "ZFS_THIN" depending on Config.Thin.
func (p *Provider) Kind() string {
	if p.cfg.Thin {
		return "ZFS_THIN"
	}

	return "ZFS"
}

// CreateVolume creates a zvol. Idempotent: existing dataset → no-op.
func (p *Provider) CreateVolume(ctx context.Context, vol storage.Volume) error {
	if p.datasetExists(ctx, p.volumeDataset(vol)) {
		return nil
	}

	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	args := []string{"create"}
	if p.cfg.Thin {
		args = append(args, "-s")
	}

	args = append(args, "-V", strconv.FormatInt(sizeMiB, 10)+"M", p.volumeDataset(vol))

	_, err := p.exec.Run(ctx, "zfs", args...)
	if err != nil {
		return errors.Wrapf(err, "zfs create %s", p.volumeDataset(vol))
	}

	return nil
}

// ResizeVolume sets the zvol's volsize to vol.SizeKib (rounded up
// to MiB). zfs is happy to set the same size twice, so the call is
// idempotent. Shrinks are rejected by ZFS itself when the existing
// volume already holds more data — the CSI grow-only contract makes
// the no-shrink invariant a non-issue here.
func (p *Provider) ResizeVolume(ctx context.Context, vol storage.Volume) error {
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := p.exec.Run(ctx, "zfs", "set",
		"volsize="+strconv.FormatInt(sizeMiB, 10)+"M",
		p.volumeDataset(vol))
	if err != nil {
		return errors.Wrapf(err, "zfs set volsize %s", p.volumeDataset(vol))
	}

	return nil
}

// DeleteVolume `zfs destroy -r`s the zvol (recursive to clean up any
// dependent snapshots automatically). Missing → no-op.
//
// Before the destroy we read the volume's `origin` property — if it
// points at a deferred-delete marker (DeleteSnapshot's
// `__DELETED__<ts>` rename), drop the marker after the volume is
// gone. That's the GC half of upstream's deferred-delete pattern:
// snapshots blocked by clones get renamed; when the last clone goes
// away the marker becomes destroyable. Best-effort — sweep errors
// don't bubble because the snapshot is now an internal artefact.
func (p *Provider) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	tgtDS := p.volumeDataset(vol)
	if !p.datasetExists(ctx, tgtDS) {
		return nil
	}

	origin := p.volumeOrigin(ctx, tgtDS)

	_, err := p.exec.Run(ctx, "zfs", "destroy", "-r", tgtDS)
	if err != nil {
		return errors.Wrapf(err, "zfs destroy %s", tgtDS)
	}

	if origin != "" && strings.Contains(origin, "__DELETED__") {
		_, _ = p.exec.Run(ctx, "zfs", "destroy", origin)
	}

	return nil
}

// VolumeStatus parses `zfs list -p` output (bytes, no suffixes).
func (p *Provider) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	out, err := p.exec.Run(ctx, "zfs",
		"list", "-H", "-p",
		"-o", "name,volsize,used",
		p.volumeDataset(vol))
	if err != nil {
		return storage.VolumeStatus{State: stateNotProvisioned}, nil //nolint:nilerr // missing == not provisioned
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
	out, err := p.exec.Run(ctx, "zpool", "list", "-H", "-p", "-o", "size,free", p.cfg.Pool)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrapf(err, "zpool list %s", p.cfg.Pool)
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.PoolStatus{}, errors.Errorf("zpool %s not found", p.cfg.Pool)
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
		return nil
	}

	_, err := p.exec.Run(ctx, "zfs", "snapshot", dataset)
	if err != nil {
		return errors.Wrapf(err, "zfs snapshot %s", dataset)
	}

	return nil
}

// DeleteSnapshot removes the snapshot, with upstream LINSTOR's
// "has dependent clones → rename to deferred-delete marker" fallback.
//
// `zfs destroy` fails when a clone still references the snapshot
// (cross-node clone, snapshot-restore-resource); upstream renames
// the snapshot to `<dataset>@<orig>__DELETED__<unix-ts>` so it
// disappears from the LINSTOR view while staying on disk to keep
// the dependent clone alive. When the last clone is destroyed, the
// marker is swept by DeleteVolume's origin check.
//
// Missing snapshot → no-op so the satellite reconciler can strip
// the Snapshot CRD's finalizer instead of looping on a "dataset
// does not exist" surface error.
func (p *Provider) DeleteSnapshot(ctx context.Context, snap storage.Snapshot) error {
	src := p.snapshotDataset(snap)

	if !p.datasetExists(ctx, src) {
		return nil
	}

	out, err := p.exec.Run(ctx, "zfs", "destroy", src)
	if err == nil {
		return nil
	}

	if !hasDependentClonesError(string(out), err.Error()) {
		return errors.Wrapf(err, "zfs destroy %s", src)
	}

	marker := fmt.Sprintf("%s__DELETED__%d", src, time.Now().Unix())

	_, rErr := p.exec.Run(ctx, "zfs", "rename", src, marker)
	if rErr != nil {
		return errors.Wrapf(rErr, "zfs rename %s → %s (deferred delete)", src, marker)
	}

	return nil
}

// hasDependentClonesError matches zfs destroy's "has dependent
// clones" error text. ZFS prints it in stdout (combined-output) or
// stderr depending on the version; both substrings are checked.
func hasDependentClonesError(stdout, stderr string) bool {
	return strings.Contains(stdout, "has dependent clones") ||
		strings.Contains(stderr, "has dependent clones")
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
// Idempotent: target dataset present → treat as resumed reconcile.
func (p *Provider) RestoreVolumeFromSnapshot(ctx context.Context, target storage.Volume, src storage.Snapshot) error {
	tgtDS := p.volumeDataset(target)
	if p.datasetExists(ctx, tgtDS) {
		return nil
	}

	srcDS := p.snapshotDataset(src)
	if !p.datasetExists(ctx, srcDS) {
		return errors.Wrapf(storage.ErrNotFound, "snapshot %s for clone", srcDS)
	}

	_, err := p.exec.Run(ctx, "zfs", "clone", srcDS, tgtDS)
	if err != nil {
		return errors.Wrapf(err, "zfs clone %s → %s", srcDS, tgtDS)
	}

	// THICK mode: restore the space guarantee that `zfs clone` always
	// drops. Thin mode skips this — sparse-everywhere is the thin
	// contract by design.
	if !p.cfg.Thin {
		volsizeBytes, err := p.datasetVolsize(ctx, tgtDS)
		if err != nil {
			return errors.Wrapf(err, "lookup volsize on cloned %s", tgtDS)
		}

		_, err = p.exec.Run(ctx, "zfs", "set",
			"refreservation="+strconv.FormatInt(volsizeBytes, 10),
			tgtDS)
		if err != nil {
			return errors.Wrapf(err, "zfs set refreservation on cloned %s", tgtDS)
		}
	}

	return nil
}

// SendSnapshot streams `zfs send <pool>/<rd>_00000@<snap>` so a peer
// satellite can pipe it into its own `zfs recv` and reproduce the
// dataset byte-for-byte (including the DRBD metadata block embedded
// in the zvol). The returned ReadCloser wraps the running zfs
// process's stdout — Close kills the process via the wrapped
// context cancel, so callers can abort mid-stream.
//
// Bypasses storage.Exec because Exec.Run buffers the full output in
// memory; multi-GB snapshot streams need a pipe. FakeExec tests
// don't cover this path — the integration test on the stand drives
// it through a real peer Fetch.
func (p *Provider) SendSnapshot(ctx context.Context, snap storage.Snapshot) (io.ReadCloser, error) {
	srcDS := p.snapshotDataset(snap)
	if !p.datasetExists(ctx, srcDS) {
		return nil, errors.Wrapf(storage.ErrNotFound, "snapshot %s for send", srcDS)
	}

	cmd := exec.CommandContext(ctx, "zfs", "send", srcDS)

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
func (p *Provider) RecvSnapshot(ctx context.Context, target storage.Volume, src io.Reader) error {
	tgtDS := p.volumeDataset(target)
	if p.datasetExists(ctx, tgtDS) {
		return nil
	}

	cmd := exec.CommandContext(ctx, "zfs", "recv", "-F", tgtDS)
	cmd.Stdin = src

	out, err := cmd.CombinedOutput()
	if err != nil {
		// On failure leave nothing behind — `zfs recv -F` may have
		// partially created the dataset; nuke it so the next
		// reconcile re-streams cleanly.
		_, _ = p.exec.Run(ctx, "zfs", "destroy", "-r", tgtDS)

		return errors.Wrapf(err, "zfs recv %s: %s", tgtDS, strings.TrimSpace(string(out)))
	}

	return nil
}

// ListVolumeNames enumerates every zvol in the configured pool that
// matches the blockstor naming convention `<pool>/<resource>_<vol5digits>`.
// Used by the orphan-storage sweeper (Bug 43) to find zvols whose
// owning Resource CRD has been force-deleted without the satellite's
// DeleteVolume ever running.
//
// Skips:
//   - Datasets outside the configured pool (defensive — `zfs list -r`
//     with `-d 1` should only return direct children, but the parse
//     guards anyway).
//   - Deferred-delete markers (names containing `__DELETED__`) — those
//     are upstream LINSTOR's "clone still references this snapshot"
//     gravestones, NOT user volumes; the sweep MUST NOT GC them.
//   - Datasets whose suffix isn't a 5-digit volume number — we don't
//     manage hand-created datasets in the pool and silently leaving
//     them alone is the right call (operator may have placed them
//     there intentionally).
func (p *Provider) ListVolumeNames(ctx context.Context) ([]storage.VolumeRef, error) {
	out, err := p.exec.Run(ctx, "zfs",
		"list", "-H", "-r", "-d", "1",
		"-o", "name", "-t", "volume",
		p.cfg.Pool)
	if err != nil {
		return nil, errors.Wrapf(err, "zfs list pool %s", p.cfg.Pool)
	}

	refs := make([]storage.VolumeRef, 0)

	prefix := p.cfg.Pool + "/"
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if !strings.HasPrefix(line, prefix) {
			continue
		}

		// Skip deferred-delete markers — they look like
		// `<pool>/<rsc>_00000@<snap>__DELETED__<ts>` and aren't
		// even zvols (they're tagged volumes by `-t volume` only
		// because the origin dataset is one). Defensive belt-and-
		// braces — the filter substring catches the rename pattern
		// even if upstream's naming shifts a character.
		if strings.Contains(line, "__DELETED__") {
			continue
		}

		base := strings.TrimPrefix(line, prefix)

		resource, vol, ok := parseVolumeName(base)
		if !ok {
			continue
		}

		refs = append(refs, storage.VolumeRef{
			ResourceName: resource,
			VolumeNumber: vol,
		})
	}

	return refs, nil
}

// parseVolumeName splits "<resource>_<vol5digits>" into its parts.
// Returns ok=false when the suffix isn't a 5-digit decimal number;
// the orphan sweeper relies on that to skip operator-created
// datasets that happen to share the pool.
func parseVolumeName(name string) (string, int32, bool) {
	idx := strings.LastIndex(name, "_")
	if idx == -1 {
		return "", 0, false
	}

	suffix := name[idx+1:]
	if len(suffix) != volNumberDigits {
		return "", 0, false
	}

	n, err := strconv.Atoi(suffix)
	if err != nil {
		return "", 0, false
	}

	return name[:idx], int32(n), true
}

// volNumberDigits is the fixed-width digit count blockstor uses for
// the per-RD volume index in dataset / LV names. Keep in sync with
// volumeDataset / volumeLVName.
const volNumberDigits = 5

// volumeOrigin reads the zvol's `origin` property — the snapshot
// dataset name when this volume was created via `zfs clone`, or
// `-` for a standalone dataset. Returns "" on either case + on any
// error (best-effort lookup for the deferred-delete sweep).
func (p *Provider) volumeOrigin(ctx context.Context, ds string) string {
	out, err := p.exec.Run(ctx, "zfs", "get", "-H", "-o", "value", "origin", ds)
	if err != nil {
		return ""
	}

	value := strings.TrimSpace(string(out))
	if value == "" || value == "-" {
		return ""
	}

	return value
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

// datasetExists is the idempotency primitive — analogous to lvExists.
func (p *Provider) datasetExists(ctx context.Context, ds string) bool {
	out, err := p.exec.Run(ctx, "zfs", "list", "-H", "-o", "name", ds)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(out)) != ""
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
	stateNotProvisioned = "NOT_PROVISIONED"
	mibPerKib           = 1024
	bytesPerKib         = 1024
	zfsListCols         = 3
	zpoolListCols       = 2
)
