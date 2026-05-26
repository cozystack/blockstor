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
	"github.com/cozystack/blockstor/pkg/drbd"
)

// IsThinOrZFS reports whether a provider kind is guaranteed to hand
// back a zero-initialised block device on CreateVolume — i.e. fresh
// reads return zeros without the satellite having to wipe the volume
// first. DRBD-9's full initial-sync exists to copy a primary's bytes
// onto a fresh secondary; when BOTH sides are guaranteed-zero by
// construction the sync is moving zeros over the wire and the result
// is identical to skipping the sync entirely. Mirrors upstream
// LINSTOR's `DrbdLayerUtils.skipInitSync` short-circuit:
// server/src/main/java/com/linbit/linstor/utils/layer/DrbdLayerUtils.java
// (skipInitSync returns true when the backing is thinly-provisioned
// OR when every storage device is ZFS / ZFS_THIN).
//
// Mapping:
//
//   - LVM_THIN: thin LVM allocates blocks on first write; unprovisioned
//     ranges read as zero. The kernel dm-thin layer guarantees this
//     (`thin_pool_status` "no_space_reservation"; reads on
//     unprovisioned regions short-circuit to zero in the dm code).
//   - ZFS_THIN: sparse zvols (`zfs create -s -V <size>`) — same property
//     as thin LVM: unwritten blocks read as zero.
//   - ZFS: even thick (`zfs create -V <size>` without -s) zvols are
//     copy-on-write on a fresh dataset; the COW tree has no allocated
//     blocks for the new zvol so reads return zero until first write.
//     Upstream LINSTOR groups both ZFS variants into skipInitSync
//     for the same reason.
//   - FILE_THIN: sparse file (`truncate -s <size>`); a hole returns
//     zeros via the filesystem.
//
// Returns false for:
//
//   - LVM: thick LVM hands back whatever bytes were on the PV's
//     extents previously. NOT safe to skip initial-sync.
//   - FILE: same — a fully-allocated file may carry stale data.
//   - DISKLESS / unknown: no backing storage to assert anything about.
//
// Keep this aligned with `factory.go`'s ProviderKind* constants.
func IsThinOrZFS(kind string) bool {
	switch kind {
	case ProviderKindLVMThin,
		ProviderKindZFSThin,
		ProviderKindZFS,
		ProviderKindFileThin:
		return true
	}

	return false
}

// day0GiFor is the satellite-local alias for the single-sourced
// drbd.Day0GIFor derivation. Kept so existing satellite call sites
// read naturally; the derivation itself (and its rationale) lives in
// pkg/drbd/gi.go so the controller and dispatcher seed-safety gates
// compare against the exact same value the satellite stamps.
func day0GiFor(resourceName string, volumeNumber int32) string {
	return drbd.Day0GIFor(resourceName, volumeNumber)
}
