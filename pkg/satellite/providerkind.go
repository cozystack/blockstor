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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
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

// day0GiFor derives a deterministic 64-bit DRBD GI for a per-RD,
// per-volume "day 0" stamp. Same RD name + volume number always
// yields the same value, so every replica on every node converges
// on identical current_uuid / bitmap_uuid pairs without needing a
// shared random seed — DRBD's GI handshake then matches and skips
// the full initial-sync (upstream LINSTOR's path goes through
// `getCurrentGiFromVlmDfnProp` which is itself derived from the
// RD's VolumeDefinition; we follow the same "same RD ⇒ same GI"
// rule but without the controller-side prop because no peer
// has stamped CurrentGi on a brand-new RD).
//
// Format: 16 hex characters, upper-case — DRBD's drbdmeta accepts
// hex GI tokens. Trailing `0` enforces the DRBD-9 convention that
// a real GI is even (low bit is the "primary indicator" — odd =
// primary writes happened, even = clean). A synthetic day0 is
// deliberately even so it represents "consistent / no primary
// writes yet" — DRBD's handshake then treats matching day0 +
// zero bitmap as "already in sync".
func day0GiFor(resourceName string, volumeNumber int32) string {
	h := sha256.Sum256(fmt.Appendf(nil, "blockstor-day0:%s/%d", resourceName, volumeNumber))
	// Take the first 8 bytes, force low bit to 0 (even).
	h[7] &^= 0x01

	return strings.ToUpper(hex.EncodeToString(h[:8]))
}
