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
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// PickStableID returns the most stable identifier the satellite
// has for this device. Preference order matches upstream LINSTOR
// (CmdPhysicalStorage.java + LsBlkUtils.java):
//
//  1. WWN (`/dev/disk/by-id/wwn-*`) — board-controller-stamped
//     World-Wide Name; survives drive cloning, partition
//     re-init, and most replacement scenarios.
//  2. scsi-SATA serial (`scsi-SATA_<vendor>_<model>_<serial>`) —
//     reliable on most SAS / SATA drives.
//  3. NVMe identifier (`nvme-<model>_<serial>`) — for NVMe
//     drives that don't expose a WWN.
//  4. by-path fallback — the last-resort path for virtio /
//     unidentified hardware. Less stable across reconfigs but
//     better than the volatile `/dev/sdN` form.
//
// Returns "" when the lsblk row had no stable signal at all
// (some virtio setups). Caller falls back to KName + a host-
// fingerprint UUID; that path is OK for fresh clusters but
// breaks on cross-host migration.
//
// Phase 10.7.
func PickStableID(dev *LsblkDevice) string {
	if dev.WWN != "" {
		return "wwn-" + dev.WWN
	}

	if strings.HasPrefix(dev.Transport, "sata") || strings.HasPrefix(dev.Transport, "sas") {
		if id := scsiSATASerial(dev); id != "" {
			return id
		}
	}

	if dev.Transport == "nvme" && dev.Model != "" && dev.Serial != "" {
		return "nvme-" + slugifyForLSStableID(dev.Model) + "_" + slugifyForLSStableID(dev.Serial)
	}

	if dev.KName != "" {
		// by-path style fallback — not derived from /dev/disk/by-path
		// directly because that requires a host filesystem read,
		// which is the caller's responsibility. We emit a marker
		// derived from KName as a deterministic placeholder.
		return "by-path-" + dev.KName
	}

	return ""
}

// scsiSATASerial composes an scsi-SATA-style identifier from a
// device's vendor/model/serial fields. Returns "" when any
// component is missing — half-formed identifiers would be
// indistinguishable across drives and break re-discovery on
// reboot.
func scsiSATASerial(dev *LsblkDevice) string {
	if dev.Model == "" || dev.Serial == "" {
		return ""
	}

	return "scsi-SATA_" + slugifyForLSStableID(dev.Model) + "_" + slugifyForLSStableID(dev.Serial)
}

// slugifyForLSStableID normalises a model / serial string into the
// shape the kernel uses in `/dev/disk/by-id/` symlinks: spaces
// become underscores, hyphens stay, everything else outside
// `[A-Za-z0-9_-]` gets dropped. Mirrors udev's
// `60-persistent-storage.rules` substitution.
func slugifyForLSStableID(in string) string {
	var b strings.Builder

	for _, r := range strings.TrimSpace(in) {
		switch {
		case r >= 'A' && r <= 'Z',
			r >= 'a' && r <= 'z',
			r >= '0' && r <= '9',
			r == '_', r == '-':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('_')
		}
	}

	return b.String()
}

// DiscoveryAction is one entry in the diff plan the discovery
// loop computes. Pure data — the actual store mutations happen
// in the loop's caller using a `store.PhysicalDeviceStore`.
type DiscoveryAction struct {
	Op     DiscoveryOp
	Device apiv1.PhysicalDevice
}

// DiscoveryOp enumerates the four diff outcomes. Phase 10.7.
type DiscoveryOp int

// DiscoveryOp values.
const (
	OpCreate DiscoveryOp = iota
	OpUpdate
	OpDelete
)

// DiscoveryDiff produces the (create / update / delete) action
// list from a fresh host scan + the existing PhysicalDevice CRDs
// for this node. Convergence rule: anything in `discovered` that
// isn't already a CRD becomes a Create; anything in `existing`
// that isn't in `discovered` becomes a Delete (the device was
// consumed by a pool or physically removed); a name match
// between the two lists triggers an Update only when an
// observable Status field changed (size, currentDevPath, model,
// rotational), so a no-op tick doesn't churn the apiserver.
//
// Both inputs MUST be sorted by Name for the diff to short-cut
// efficiently — pkg/store callers already return sorted slices,
// so this is the natural state.
//
// Phase 10.7.
func DiscoveryDiff(discovered, existing []apiv1.PhysicalDevice) []DiscoveryAction {
	out := []DiscoveryAction{}

	byName := map[string]*apiv1.PhysicalDevice{}
	for i := range discovered {
		byName[discovered[i].Name] = &discovered[i]
	}

	existingByName := map[string]*apiv1.PhysicalDevice{}
	for i := range existing {
		existingByName[existing[i].Name] = &existing[i]
	}

	// Creates + updates.
	for i := range discovered {
		dev := discovered[i]
		prev := existingByName[dev.Name]

		switch {
		case prev == nil:
			out = append(out, DiscoveryAction{Op: OpCreate, Device: dev})
		case discoveryStatusChanged(prev, &dev):
			out = append(out, DiscoveryAction{Op: OpUpdate, Device: dev})
		}
	}

	// Deletes — anything in existing but missing from
	// discovered. Skip already-attaching ones (their CRD has
	// AttachTo set, so they're in-flight; the satellite
	// reconciler will delete them on success).
	for i := range existing {
		dev := existing[i]
		if _, stillThere := byName[dev.Name]; stillThere {
			continue
		}

		if dev.AttachTo != nil {
			continue
		}

		out = append(out, DiscoveryAction{Op: OpDelete, Device: dev})
	}

	return out
}

// discoveryStatusChanged reports whether any observable Status
// field differs between a prior and current PhysicalDevice. We
// only care about fields that can legitimately change at runtime
// — size (drive replacement), currentDevPath (sdN re-letter),
// model / serial / rotational (drive swap behind a stable WWN),
// transport (rare but possible).
//
// Phase / AttachTo are NOT compared: those are write-only-from-
// elsewhere fields the discovery loop must never touch.
func discoveryStatusChanged(prev, cur *apiv1.PhysicalDevice) bool {
	if prev.SizeBytes != cur.SizeBytes {
		return true
	}

	if prev.CurrentDevPath != cur.CurrentDevPath {
		return true
	}

	if prev.Model != cur.Model {
		return true
	}

	if prev.Serial != cur.Serial {
		return true
	}

	if prev.Transport != cur.Transport {
		return true
	}

	return !ptrBoolEq(prev.Rotational, cur.Rotational)
}

// ptrBoolEq reports whether two `*bool` carry the same value.
// Used by the diff to detect rotational-flag flips after a drive
// swap (an HDD slot now houses an SSD).
func ptrBoolEq(prev, cur *bool) bool {
	switch {
	case prev == nil && cur == nil:
		return true
	case prev == nil || cur == nil:
		return false
	}

	return *prev == *cur
}
