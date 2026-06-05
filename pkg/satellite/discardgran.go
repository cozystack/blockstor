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
	"context"
	"strconv"
	"strings"

	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// DRBD disk-option keys for thin-aware resync. Rendered into the
// resource-scope `disk { }` block (the existing Resource.Disk map) so
// a resync to/from a thin/zfs-backed replica discards (skips) the
// zero/unallocated ranges instead of transferring zeros over the wire.
//
//   - rs-discard-granularity: the byte size of one discard unit DRBD
//     uses during resync. When set, DRBD issues an UNMAP/TRIM for an
//     all-zero, granularity-aligned region on the SyncTarget instead
//     of writing zeros — so a mostly-empty thin volume resyncs ~only
//     the written bytes. MUST be aligned to (a multiple of) the
//     backing device's discard granularity and the backing device
//     MUST support discard, or drbdsetup rejects it.
//   - discard-zeroes-if-aligned: tells DRBD that an aligned discard on
//     the backing device deterministically yields zeros (true for
//     zvols / thin-LV). Lets DRBD treat an aligned all-zero range as a
//     discard during resync rather than a zero-write.
//
// Both keys are rendered WITHOUT the `DrbdOptions/Disk/` prefix —
// that's the bare form drbdadm expects inside `disk { }`. They mirror
// upstream LINSTOR's auto-managed props (CtrlRscDfnApiCallHelper +
// CtrlRscCrtApiHelper).
const (
	diskOptRsDiscardGranularity = "rs-discard-granularity"
	diskOptDiscardZeroesAligned = "discard-zeroes-if-aligned"

	// drbdYes / drbdNo are DRBD's boolean disk-option values.
	drbdYes = "yes"
	drbdNo  = "no"
)

// DRBD's accepted bounds for rs-discard-granularity. Mirrors upstream
// LINSTOR's DRBD_DISC_GRAN_MIN / DRBD_DISC_GRAN_MAX
// (CtrlRscDfnApiCallHelper.java): the backing device's reported
// discard granularity is clamped into [4 KiB, 1 MiB]. Below 4 KiB the
// discard is finer than DRBD's resync extent and pointless; above
// 1 MiB DRBD refuses the value.
const (
	drbdDiscGranMinBytes = 4 * 1024
	drbdDiscGranMaxBytes = 1 * 1024 * 1024
)

// autoDiskOptionsForResource resolves the thin-aware-resync `disk { }`
// options for the LOCAL replica of dr, to be folded into the rendered
// resource-scope `disk { }` block (which DRBD applies to every
// volume).
//
// It probes each local diskful volume's backing device. Each volume
// contributes a discard-zeroes-if-aligned flag (yes/no per provider
// kind) and, when its device reports a non-zero discard granularity, an
// rs-discard-granularity value — INDEPENDENT gates (see autoDiskOptions).
//
// The resource-scope collapse is CONSERVATIVE for the (today
// hypothetical) multi-volume case (mergeResourceDiscardOptions):
// discard-zeroes-if-aligned is "yes" only when EVERY volume is
// discard-zero-safe (one "no" pins the resource to "no"); the
// rs-discard-granularity is the SMALLEST across the volumes so the
// single resource-scope value stays a valid multiple of every device's
// discard granularity — never an UNDER-aligned UNMAP. A volume whose
// device can't be probed simply contributes no granularity (DRBD falls
// back to a full copy where it can't discard).
//
// A diskless local replica (no diskful volumes) returns nil — there is
// no backing device to discard against.
func (r *Reconciler) autoDiskOptionsForResource(
	ctx context.Context,
	dr *intent.DesiredResource,
	devices map[int32]string,
) map[string]string {
	exec := r.cfg.Exec
	if exec == nil {
		// No exec wired (some unit harnesses) — can't probe DISC-GRAN.
		// Returning nil omits the auto disk block: safe full resync.
		return nil
	}

	if isDiskless(dr.GetFlags()) {
		return nil
	}

	out := map[string]string{}
	sawVolume := false

	for _, vol := range dr.GetVolumes() {
		provider, ok := r.cfg.Providers[vol.GetStoragePool()]
		if !ok || provider == nil {
			return nil
		}

		device := devices[vol.GetVolumeNumber()]

		volOpts := autoDiskOptions(ctx, exec, provider.Kind(), device)

		mergeResourceDiscardOptions(out, volOpts)

		sawVolume = true
	}

	if !sawVolume {
		return nil
	}

	return out
}

// mergeResourceDiscardOptions folds one volume's auto disk options into
// the resource-scope accumulator (a single `disk { }` block DRBD applies
// to every volume of the resource):
//
//   - discard-zeroes-if-aligned collapses CONSERVATIVELY to "yes" only
//     when EVERY volume is discard-zero-safe; a single "no" volume pins
//     the whole resource to "no". Otherwise a thick/file volume sharing
//     the resource could be told an aligned discard yields zero when it
//     does not — a data-safety violation. (Today resources are
//     single-volume, but the merge must stay safe for the multi-volume
//     future.)
//   - rs-discard-granularity collapses to the SMALLEST seen value so the
//     single resource-scope number stays a valid multiple of every
//     volume's backing-device granularity (never an UNDER-aligned UNMAP).
func mergeResourceDiscardOptions(acc, volOpts map[string]string) {
	for key, val := range volOpts {
		switch key {
		case diskOptRsDiscardGranularity:
			prev, have := acc[key]
			if !have || smallerNumeric(val, prev) {
				acc[key] = val
			}
		case diskOptDiscardZeroesAligned:
			// "no" wins: once any volume is unsafe the resource flag
			// must stay "no". Only set "yes" if not yet pinned to "no".
			if acc[key] != drbdNo {
				acc[key] = val
			}
		default:
			acc[key] = val
		}
	}
}

// smallerNumeric reports whether decimal string left parses to a
// smaller integer than right. Non-parseable inputs sort as "not
// smaller" so a bad value never displaces a good one.
func smallerNumeric(left, right string) bool {
	leftVal, errLeft := strconv.ParseInt(left, 10, 64)
	rightVal, errRight := strconv.ParseInt(right, 10, 64)

	if errLeft != nil || errRight != nil {
		return false
	}

	return leftVal < rightVal
}

// autoDiskOptions derives the thin-aware-resync `disk { }` options for
// a single local diskful volume. It returns a bare key→value map
// (sans the `DrbdOptions/Disk/` prefix) ready to merge into the
// rendered `disk { }` block.
//
// The two options are gated INDEPENDENTLY — this mirrors upstream
// LINSTOR, where `rs-discard-granularity` follows the backing device's
// reported discard granularity (CtrlRscDfnApiCallHelper) and
// `discard-zeroes-if-aligned` follows a provider-kind switch
// (CtrlRscCrtApiHelper). Coupling them (omitting the granularity just
// because a provider isn't discard-zero-safe) makes a partially-written
// FILE_THIN volume resync the WHOLE device instead of only the written
// bytes — the Q3 corner-case bug.
//
// Data-safety contract — the optimisation may ONLY skip provably-zero
// ranges (thin discard), NEVER any written data:
//
//   - discard-zeroes-if-aligned is `yes` ONLY for provider kinds whose
//     backing device deterministically reads/discards as zero in an
//     ALIGNED region: LVM_THIN, ZFS, ZFS_THIN. For every other kind
//     (FILE_THIN loop-backed sparse file, thick LVM, plain FILE,
//     unknown) it is `no` — matching upstream's CtrlRscCrtApiHelper
//     switch, which renders an explicit `no`. Note we deliberately do
//     NOT reuse IsThinOrZFS here: that helper includes FILE_THIN
//     (correct for the seed-GI skip-sync gate) but FILE_THIN must NOT
//     get discard-zeroes-if-aligned=yes.
//
//   - rs-discard-granularity is set whenever the backing block device
//     reports a non-zero discard granularity (lsblk DISC-GRAN > 0),
//     INDEPENDENT of provider kind. This is the safe & correct gate:
//     a non-zero DISC-GRAN means the device supports discard, so DRBD
//     can issue an aligned UNMAP for an all-zero region during resync
//     instead of writing zeros. (`discard-zeroes-if-aligned` separately
//     governs whether DRBD may TREAT an aligned all-zero range as a
//     discard on the SyncTarget; with it `no`, DRBD still benefits from
//     the granularity on the SyncSource side.) A device reporting 0
//     does not support discard — emitting a non-zero granularity there
//     would make drbdsetup reject the option, so we OMIT the key and
//     DRBD falls back to a full byte-copy resync.
//
// When in doubt the map omits the granularity key (full transfer),
// never the reverse. The discard-zeroes flag is ALWAYS present (yes or
// no), mirroring upstream which renders it explicitly. An empty return
// would mean "render no disk block"; this function returns at least the
// discard-zeroes flag for any diskful volume, so the block renders
// whenever a backing device is present.
func autoDiskOptions(
	ctx context.Context,
	exec storage.Exec,
	providerKind string,
	devicePath string,
) map[string]string {
	zeroesVal := drbdNo
	if discardZeroesIfAligned(providerKind) {
		zeroesVal = drbdYes
	}

	out := map[string]string{
		diskOptDiscardZeroesAligned: zeroesVal,
	}

	// Only attempt the granularity when we have a real device path to
	// probe. A diskless local replica (devicePath == "") has no
	// backing device — skip the granularity probe.
	if devicePath == "" {
		return out
	}

	gran, ok := discardGranularityBytes(ctx, exec, devicePath)
	if !ok {
		// Backing device reports no discard support (DISC-GRAN == 0)
		// or we could not determine it. Omit rs-discard-granularity;
		// DRBD does a full resync, which is always correct.
		return out
	}

	out[diskOptRsDiscardGranularity] = strconv.FormatInt(gran, 10)

	return out
}

// discardZeroesIfAligned reports whether the provider kind's backing
// device deterministically yields zeros on an ALIGNED discard, so DRBD
// can safely treat an aligned all-zero range as a discard during
// resync. Mirrors upstream LINSTOR's CtrlRscCrtApiHelper switch:
//
//   - LVM_THIN: dm-thin honours aligned discard → unprovisioned reads
//     zero.
//   - ZFS / ZFS_THIN: zvols are copy-on-write; an aligned discard frees
//     the block and subsequent reads return zero.
//   - FILE_THIN: loop-backed sparse file — aligned discard is NOT
//     guaranteed to punch a hole / read back zero. Upstream renders
//     `no` for FILE_THIN; we omit the option (equivalent: DRBD's
//     default is off).
//   - LVM (thick) / FILE / DISKLESS / unknown: not discard-zero safe.
func discardZeroesIfAligned(kind string) bool {
	switch kind {
	case ProviderKindLVMThin,
		ProviderKindZFS,
		ProviderKindZFSThin:
		return true
	}

	return false
}

// discardGranularityBytes probes the backing block device's discard
// granularity (in bytes) via `lsblk -Pbno DISC-GRAN <device>` and
// clamps it into DRBD's accepted [4 KiB, 1 MiB] window.
//
// Returns (value, true) only when the device reports a non-zero
// discard granularity. A reported 0 (device does not support discard)
// or any probe failure returns (0, false) — the caller then omits
// rs-discard-granularity so DRBD does a safe full resync.
//
// We clamp UP to 4 KiB (sub-4-KiB discards buy nothing against DRBD's
// resync extent) and DOWN to 1 MiB (drbdsetup's documented maximum).
// The clamped value stays a multiple of the device's granularity
// because both bounds are powers of two ≥ the practical zvol/thin
// granularities (volblocksize defaults 8–16 KiB, dm-thin chunks
// 64 KiB–1 MiB), and DRBD itself re-aligns the configured value to the
// backing queue limit at runtime — so a clamp can never produce an
// UNDER-aligned UNMAP that would corrupt data.
func discardGranularityBytes(ctx context.Context, exec storage.Exec, devicePath string) (int64, bool) {
	out, err := exec.Run(ctx, "lsblk", "-Pbndo", "DISC-GRAN", devicePath)
	if err != nil {
		return 0, false
	}

	gran, ok := parseDiscGran(string(out))
	if !ok || gran <= 0 {
		return 0, false
	}

	return clampDiscardGranularity(gran), true
}

// parseDiscGran extracts the DISC-GRAN byte value from `lsblk -P`
// output. With `-d` (no descendants) and a single device argument the
// output is a single line `DISC-GRAN="<bytes>"`. We tolerate extra
// columns / lines and take the first parseable DISC-GRAN. Exposed for
// unit tests so they don't need a real lsblk binary.
func parseDiscGran(raw string) (int64, bool) {
	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := parseLsblkLine(line)

		valStr, present := fields["DISC-GRAN"]
		if !present {
			continue
		}

		valStr = strings.TrimSpace(valStr)
		if valStr == "" {
			continue
		}

		gran, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			continue
		}

		return gran, true
	}

	return 0, false
}

// clampDiscardGranularity clamps a backing-device discard granularity
// into DRBD's accepted [4 KiB, 1 MiB] range. Mirrors upstream
// LINSTOR's chooseDrbdDiscGran clamp. Callers must have already
// rejected a 0 input (no-discard-support) — this only bounds a
// positive value.
func clampDiscardGranularity(gran int64) int64 {
	if gran < drbdDiscGranMinBytes {
		return drbdDiscGranMinBytes
	}

	if gran > drbdDiscGranMaxBytes {
		return drbdDiscGranMaxBytes
	}

	return gran
}
