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

// Package lvm provides LVM and LVM-thin storage backends. The LINSTOR
// `LVM_THIN` provider is the default for cozystack-style clusters because
// it gives proper snapshots without the storage-overhead of LVM-classic.
package lvm

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// ThinConfig parametrises a Thin provider with the LVM volume group and
// thin pool that back it. Both must already exist on the host; the
// satellite never auto-creates VGs (that's a host-prep concern).
type ThinConfig struct {
	VolumeGroup string
	ThinPool    string
}

// Thin implements storage.Provider for LINSTOR's `LVM_THIN` kind.
type Thin struct {
	cfg  ThinConfig
	exec storage.Exec
}

// NewThin constructs a Thin provider. The Exec is injected so unit tests
// can drive it without a real LVM stack.
func NewThin(cfg ThinConfig, ex storage.Exec) *Thin {
	return &Thin{cfg: cfg, exec: ex}
}

// Kind returns the upstream LINSTOR provider kind string.
func (*Thin) Kind() string { return "LVM_THIN" }

// volumeLVName mirrors upstream LINSTOR's naming: `<resource>_<vol5digits>`.
// We zero-pad the volume number to keep lexical sort meaningful.
func volumeLVName(vol storage.Volume) string {
	return fmt.Sprintf("%s_%05d", vol.ResourceName, vol.VolumeNumber)
}

// snapshotLVName for `lvcreate -s` of a Volume's first volume (#0). LINSTOR
// snapshots are per-resource, so the LV name is `<rd>_<snap>_00000`.
func snapshotLVName(snap storage.Snapshot) string {
	return fmt.Sprintf("%s_%s_00000", snap.ResourceName, snap.SnapshotName)
}

// CreateVolume idempotently creates the thin LV. If the LV already exists
// (regardless of size) we return nil — resize lives on a separate code
// path which Phase 4 introduces.
func (t *Thin) CreateVolume(ctx context.Context, vol storage.Volume) error {
	if t.lvExists(ctx, volumeLVName(vol)) {
		return nil
	}

	// LINSTOR rounds to MiB internally; we follow.
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--thin",
			"--virtualsize", strconv.FormatInt(sizeMiB, 10)+"MiB",
			"--name", volumeLVName(vol),
			t.cfg.VolumeGroup+"/"+t.cfg.ThinPool)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate %s", volumeLVName(vol))
	}

	return nil
}

// ResizeVolume grows the LV to vol.SizeKib (rounded up to MiB to
// match LINSTOR's reporting). Shrinks are rejected — DRBD doesn't
// support online shrink and CSI ControllerExpandVolume is grow-only.
// `lvextend --size` is a no-op when the requested size matches, so
// the call stays idempotent.
func (t *Thin) ResizeVolume(ctx context.Context, vol storage.Volume) error {
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := t.exec.Run(ctx, "lvextend",
		Args("--size", strconv.FormatInt(sizeMiB, 10)+"MiB",
			t.cfg.VolumeGroup+"/"+volumeLVName(vol))...)
	if err != nil {
		return errors.Wrapf(err, "lvextend %s", volumeLVName(vol))
	}

	return nil
}

// DeleteVolume idempotently removes the LV. Missing → no-op (reconcile
// loops re-call this).
func (t *Thin) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	if !t.lvExists(ctx, volumeLVName(vol)) {
		return nil
	}

	_, err := t.exec.Run(ctx, "lvremove",
		Args("--force",
			t.cfg.VolumeGroup+"/"+volumeLVName(vol))...)
	if err != nil {
		return errors.Wrapf(err, "lvremove %s", volumeLVName(vol))
	}

	return nil
}

// VolumeStatus reports observed disk state via lvs.
func (t *Thin) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return volumeStatusViaLVS(ctx, t.exec, t.cfg.VolumeGroup+"/"+volumeLVName(vol))
}

// PoolStatus reports the thin pool's free/total capacity.
func (t *Thin) PoolStatus(ctx context.Context) (storage.PoolStatus, error) {
	out, err := t.exec.Run(ctx, "lvs",
		Args("--noheadings",
			"--separator", "|",
			"-o", "lv_size,data_percent",
			"--units", "k",
			"--nosuffix",
			t.cfg.VolumeGroup+"/"+t.cfg.ThinPool)...)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "lvs (pool)")
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.PoolStatus{}, errors.Errorf("thin pool %s/%s not found",
			t.cfg.VolumeGroup, t.cfg.ThinPool)
	}

	parts := strings.SplitN(line, "|", lvsCols)
	if len(parts) != lvsCols {
		return storage.PoolStatus{}, errors.Errorf("lvs (pool): unexpected line %q", line)
	}

	totalKib, err := parseFloatToInt64(parts[0])
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse lv_size")
	}

	usedPct, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse data_percent")
	}

	usedKib := int64(float64(totalKib) * usedPct / pctMax)

	return storage.PoolStatus{
		FreeCapacityKib:   totalKib - usedKib,
		TotalCapacityKib:  totalKib,
		SupportsSnapshots: true,
	}, nil
}

// CreateSnapshot is `lvcreate -s` of the resource's volume 0. Multi-volume
// resources land in Phase 4.
func (t *Thin) CreateSnapshot(ctx context.Context, snap storage.Snapshot) error {
	source := fmt.Sprintf("%s_%05d", snap.ResourceName, 0)

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--snapshot",
			"--name", snapshotLVName(snap),
			t.cfg.VolumeGroup+"/"+source)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate -s %s", snapshotLVName(snap))
	}

	return nil
}

// DeleteSnapshot mirrors DeleteVolume on the snapshot LV. Missing
// snapshot LV → no-op so the satellite's reconcile loop can strip
// the Snapshot CRD's finalizer instead of looping forever on a
// "not found" surface error.
func (t *Thin) DeleteSnapshot(ctx context.Context, snap storage.Snapshot) error {
	if !t.lvExists(ctx, snapshotLVName(snap)) {
		return nil
	}

	_, err := t.exec.Run(ctx, "lvremove",
		Args("--force",
			t.cfg.VolumeGroup+"/"+snapshotLVName(snap))...)
	if err != nil {
		return errors.Wrapf(err, "lvremove -f %s", snapshotLVName(snap))
	}

	return nil
}

// RestoreVolumeFromSnapshot materialises target as a thin-pool LV
// clone of the snapshot. Thin snapshots in LVM are CoW by default
// (no `--addtag pvmove` flag toggling required) — instant create,
// lazy allocation on diverging writes.
//
// Upstream LINSTOR equivalent:
//
//	lvcreate -s --kernel --activate y --name <tgt> <vg>/<src-snap>
//
// Idempotent: target LV present → resumed reconcile, no-op.
func (t *Thin) RestoreVolumeFromSnapshot(ctx context.Context, target storage.Volume, src storage.Snapshot) error {
	tgtName := volumeLVName(target)
	if t.lvExists(ctx, tgtName) {
		return nil
	}

	srcName := snapshotLVName(src)
	if !t.lvExists(ctx, srcName) {
		return errors.Wrapf(storage.ErrNotFound, "snapshot LV %s/%s for clone", t.cfg.VolumeGroup, srcName)
	}

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--snapshot",
			"--kernel",
			"--setactivationskip", "n",
			"--activate", "y",
			"--name", tgtName,
			t.cfg.VolumeGroup+"/"+srcName)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate -s %s → %s", srcName, tgtName)
	}

	return nil
}

// SendSnapshot streams the snapshot LV's raw bytes for a peer
// satellite to pipe into its own RecvSnapshot. We use `dd
// if=/dev/<vg>/<snap-lv>` rather than upstream's thin_send/thin_recv
// pair because the latter needs metadata-aware tooling on both
// sides (thin-provisioning-tools >= 0.9, root + thin-pool exclusive
// access). The dd path wastes space relative to the delta format
// but works across stock kernels and is what the FILE backend
// already uses; matches semantics, sacrifices throughput on
// sparsely-allocated thin volumes.
//
// Caller follows up the recv with drbdmeta drop-md + create-md to
// stamp the local DRBD node-id over the source's embedded metadata.
func (t *Thin) SendSnapshot(ctx context.Context, snap storage.Snapshot) (io.ReadCloser, error) {
	srcName := snapshotLVName(snap)
	if !t.lvExists(ctx, srcName) {
		return nil, errors.Wrapf(storage.ErrNotFound, "snapshot LV %s/%s for send", t.cfg.VolumeGroup, srcName)
	}

	devPath := "/dev/" + t.cfg.VolumeGroup + "/" + srcName

	cmd := exec.CommandContext(ctx, "dd", "if="+devPath, "bs=1M", "status=none") //nolint:gosec // VG / LV names come from operator-owned StoragePool CRDs

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "stdout pipe %s", devPath)
	}

	err = cmd.Start()
	if err != nil {
		return nil, errors.Wrapf(err, "start dd %s", devPath)
	}

	return &lvmDDReader{cmd: cmd, stdout: stdout}, nil
}

// lvmDDReader bundles the dd-send process with its stdout pipe so
// Close terminates both. Mirrors the zfs send-reader pattern.
type lvmDDReader struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
}

// Read forwards to the underlying stdout pipe.
func (r *lvmDDReader) Read(p []byte) (int, error) {
	return r.stdout.Read(p) //nolint:wrapcheck // io contract preserves err shape
}

// Close kills the running dd process (no-op if it already exited)
// and reaps it.
func (r *lvmDDReader) Close() error {
	_ = r.stdout.Close()

	if r.cmd.Process != nil {
		_ = r.cmd.Process.Kill()
	}

	_ = r.cmd.Wait()

	return nil
}

// RecvSnapshot allocates a fresh thin LV the size of the target and
// `dd of=/dev/<vg>/<tgt>`-writes the stream into it. Pre-existing
// target LV → no-op; matches FILE/ZFS resumed-reconcile semantic.
//
// The post-recv dance is the same as ZFS: drbdmeta drop-md +
// drbdadm create-md to stamp the local node-id over the embedded
// metadata, then drbdadm adjust.
func (t *Thin) RecvSnapshot(ctx context.Context, target storage.Volume, src io.Reader) error {
	tgtName := volumeLVName(target)
	if t.lvExists(ctx, tgtName) {
		return nil
	}

	// Allocate the target LV first so dd has somewhere to write.
	// CreateVolume's idempotency already handles the "exists"
	// branch; pass through so the lvcreate flags match what a
	// regular CreateVolume would have done.
	err := t.CreateVolume(ctx, target)
	if err != nil {
		return errors.Wrapf(err, "pre-create LV %s for recv", tgtName)
	}

	devPath := "/dev/" + t.cfg.VolumeGroup + "/" + tgtName

	cmd := exec.CommandContext(ctx, "dd", "of="+devPath, "bs=1M", "status=none", "conv=fsync") //nolint:gosec // VG / LV names come from operator-owned StoragePool CRDs
	cmd.Stdin = src

	out, err := cmd.CombinedOutput()
	if err != nil {
		// Roll back the pre-created LV so the next reconcile re-
		// streams cleanly. Best-effort lvremove — if it also fails
		// the operator will see both errors in the surrounding log.
		_ = t.DeleteVolume(ctx, target)

		return errors.Wrapf(err, "dd recv %s: %s", devPath, strings.TrimSpace(string(out)))
	}

	return nil
}

// ListVolumeNames enumerates every LV in the configured VG that
// matches blockstor's `<resource>_<vol5digits>` naming. Used by the
// orphan-storage sweeper (Bug 43) to GC LVs whose owning Resource
// CRD has been force-deleted without the satellite's DeleteVolume
// running.
//
// Snapshot LVs (named `<rd>_<snap>_00000` — see snapshotLVName) live
// in the same VG; we filter them out via the lv_attr column. Volume
// LVs have type '-' (thick) or 'V' (thin virtual); snapshot LVs use
// 's' (regular) or carry origin metadata differentiable through
// lv_attr.
func (t *Thin) ListVolumeNames(ctx context.Context) ([]storage.VolumeRef, error) {
	return listLVMVolumes(ctx, t.exec, t.cfg.VolumeGroup)
}

// listLVMVolumes is the shared LV-enumeration helper used by both
// Thin and Thick providers. Both backends use the same per-VG `lvs`
// invocation; only the VG name differs.
func listLVMVolumes(ctx context.Context, ex storage.Exec, vg string) ([]storage.VolumeRef, error) {
	out, err := ex.Run(ctx, "lvs",
		Args("--noheadings",
			"-o", "lv_name,lv_attr",
			"--separator", "\t",
			vg)...)
	if err != nil {
		return nil, errors.Wrapf(err, "lvs %s", vg)
	}

	refs := make([]storage.VolumeRef, 0)

	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}

		name := strings.TrimSpace(parts[0])
		attr := strings.TrimSpace(parts[1])

		// First lv_attr char is the volume type. Snapshot LVs
		// (origin 's' or thin-snapshot variants) live in the
		// same VG; we don't manage them through DeleteVolume
		// so the sweeper skips them. Volume LVs have type '-'
		// (thick) or 'V' (thin virtual).
		if len(attr) == 0 {
			continue
		}

		volType := attr[0]
		if volType != '-' && volType != 'V' {
			continue
		}

		resource, vol, ok := parseLVName(name)
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

// parseLVName splits `<resource>_<vol5digits>` into (resource, vol).
// Mirrors zfs.parseVolumeName; kept package-local so each backend
// owns its naming convention.
func parseLVName(name string) (string, int32, bool) {
	idx := strings.LastIndex(name, "_")
	if idx == -1 {
		return "", 0, false
	}

	suffix := name[idx+1:]
	if len(suffix) != lvNumberDigits {
		return "", 0, false
	}

	n, err := strconv.Atoi(suffix)
	if err != nil {
		return "", 0, false
	}

	return name[:idx], int32(n), true
}

// lvNumberDigits is the fixed-width volume-number suffix length —
// matches the zero-padding in volumeLVName.
const lvNumberDigits = 5

// lvExists is the idempotency primitive used by Create/Delete. Errors
// from `lvs` are folded into "missing": we can't distinguish "lv not
// found" from "vg locked" via stdout, and the caller's subsequent op
// surfaces the real cause anyway.
func (t *Thin) lvExists(ctx context.Context, lvName string) bool {
	out, err := t.exec.Run(ctx, "lvs",
		Args("--noheadings",
			"-o", "lv_name",
			t.cfg.VolumeGroup+"/"+lvName)...)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(out)) != ""
}

const (
	mibPerKib = 1024
	pctMax    = 100.0
	lvsCols   = 2
)

// parseFloatToInt64 parses a numeric string LVM emits with `--units k
// --nosuffix`, which is e.g. "104857600.00".
func parseFloatToInt64(raw string) (int64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, errors.Wrap(err, "parse number")
	}

	return int64(v), nil
}
