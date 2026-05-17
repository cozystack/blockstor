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

package lvm

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// State strings shared between Thin / Thick. Match the upstream LINSTOR
// values that REST clients display via `linstor v list`.
const (
	stateProvisioned    = "PROVISIONED"
	stateNotProvisioned = "NOT_PROVISIONED"
)

// ConfigFilter is the inline `--config` argument passed to every
// LVM CLI invocation (lvs, pvs, vgs, lvcreate, lvextend, lvremove,
// vgcreate, vgremove). Mirrors upstream LINSTOR's
// `LinstorVlmLayer.java` defensive filter:
//
//	devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] }
//
// Rejects `/dev/drbdN` and ZFS zvol paths so LVM doesn't:
//   - scan its own LVs through their exposed DRBD device, which
//     would let it see the same VG twice and surface duplicate-VG
//     warnings at best, corrupt metadata at worst;
//   - try to inspect ZFS-managed block devices (zvols).
//
// Phase 10.5+ : every shell-out via `Exec.Run("lvX", …)` MUST
// include `--config ConfigFilter` as its first arg. The
// `Args(...)` helper enforces this.
const ConfigFilter = `devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] }`

// Args prepends the inline config filter onto the caller's
// argument list. Use this as the variadic args sent to
// `Exec.Run("lvs", lvm.Args(extra...)...)`. Centralising the
// filter in one place keeps every LVM invocation in lock-step
// with upstream's defensive scan-rejection rules.
func Args(extra ...string) []string {
	out := make([]string, 0, 2+len(extra))
	out = append(out, "--config", ConfigFilter)
	out = append(out, extra...)

	return out
}

// RestoreIncompleteTag is the LV tag used as a completion sentinel for
// the two-step restore/recv operations (Bug 257). It is set inline on
// the lvcreate that allocates the target LV BEFORE the dd byte-copy and
// removed via `lvchange --deltag` after the dd succeeds. The
// idempotent-skip on subsequent reconciles inspects the tag:
//
//   - tag absent → previous run completed cleanly, short-circuit.
//   - tag present → previous run crashed mid-dd, re-run the whole
//     sequence (the dd overwrites the garbage left behind).
//
// Without this sentinel a crash between lvcreate and dd would leave the
// LV existing but holding garbage; the next reconcile's bare
// "lvExists?" check would mis-trust it as the restored volume — silent
// data loss. The `@` prefix is the standard LVM tag-namespace marker so
// `lvs -o lv_tags` reports the exact string this constant carries.
const RestoreIncompleteTag = "@blockstor-restore-incomplete"

// lvHasRestoreIncompleteTag reports whether the named LV carries the
// completion sentinel (Bug 257). Used by the idempotent-skip path of
// Thick.RestoreVolumeFromSnapshot and Thin.RecvSnapshot to decide
// between "previous run completed, skip" and "previous run crashed,
// re-run the dd step". Errors from `lvs` are folded into false — the
// caller's subsequent op surfaces the real cause if the LV is gone.
func lvHasRestoreIncompleteTag(ctx context.Context, ex storage.Exec, vg, lvName string) bool {
	out, err := ex.Run(ctx, "lvs",
		Args("--noheadings",
			"-o", "lv_tags",
			vg+"/"+lvName)...)
	if err != nil {
		return false
	}

	return strings.Contains(string(out), RestoreIncompleteTag)
}

// volumeStatusViaLVS runs `lvs <vg>/<lv>` and parses the resulting
// (path, size) pair into a storage.VolumeStatus. Empty output → the
// LV doesn't exist; surface NOT_PROVISIONED so the satellite reconciler's
// next pass triggers a CreateVolume.
func volumeStatusViaLVS(ctx context.Context, exec storage.Exec, vgLV string) (storage.VolumeStatus, error) {
	out, err := exec.Run(ctx, "lvs",
		Args("--noheadings",
			"--separator", "|",
			"-o", "lv_path,lv_size",
			"--units", "k",
			"--nosuffix",
			vgLV)...)
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "lvs")
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.VolumeStatus{State: stateNotProvisioned}, nil
	}

	parts := strings.SplitN(line, "|", lvsCols)
	if len(parts) != lvsCols {
		return storage.VolumeStatus{}, errors.Errorf("lvs: unexpected line %q", line)
	}

	sizeKib, err := parseFloatToInt64(parts[1])
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "parse lv_size")
	}

	return storage.VolumeStatus{
		DevicePath:   strings.TrimSpace(parts[0]),
		AllocatedKib: sizeKib,
		UsableKib:    sizeKib,
		State:        stateProvisioned,
	}, nil
}
