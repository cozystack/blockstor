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

package placer

import (
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// IsProviderKindMixingAllowed reports whether two ProviderKinds may
// host diskful replicas of the same ResourceDefinition.
//
// The table mirrors upstream LINSTOR's
// `DeviceProviderKind.isMixingAllowed` (linstor-server's
// server/src/main/java/com/linbit/linstor/storage/kinds/
// DeviceProviderKind.java, lines 298-364). Behaviour:
//
//   - DISKLESS combined with anything is allowed — DISKLESS replicas
//     don't claim a backing pool, so there is nothing to "mix".
//   - Same-kind pairs are always allowed (LVM↔LVM, ZFS↔ZFS, …).
//   - Cross-family pairs are forbidden by default. The notable
//     conditional case in upstream (LVM_THIN ↔ ZFS_THIN, LVM ↔ ZFS,
//     LVM ↔ LVM_THIN, …) is gated on an explicit cluster property
//     `allowStorPoolMixing` plus DRBD version ≥ 9.1.18 / 9.2.7. We
//     don't carry that cluster prop yet, so those cells return false
//     here. TODO(bug-76-followup): when the cluster prop lands, mirror
//     `allowedWithRecentEnoughDrbdVersion` for the gated cells.
//   - FILE/FILE_THIN are interchangeable with each other and DISKLESS.
//   - SPDK and REMOTE_SPDK are family-internal (no cross-family
//     allowance even with the cluster prop).
//   - EBS_INIT/EBS_TARGET are interchangeable with each other and
//     DISKLESS.
//   - STORAGE_SPACES/STORAGE_SPACES_THIN allow LVM/LVM_THIN/ZFS/
//     ZFS_THIN unconditionally (this is upstream's `allowed`, not
//     `allowedWithRecentEnoughDrbdVersion`).
//
// We define the names case-sensitively against the strings exposed
// via `pkg/api/v1.StoragePoolKind*` (which match LINSTOR's REST
// enum names). Unknown kinds are conservatively rejected unless
// they're literally equal.
//
// Order doesn't matter: f(left,right) == f(right,left) per upstream
// contract.
func IsProviderKindMixingAllowed(left, right string) bool {
	// DISKLESS as either side: no backing pool, nothing to gate.
	if left == apiv1.StoragePoolKindDiskless || right == apiv1.StoragePoolKindDiskless {
		return true
	}

	// Same kind is always allowed. This also covers any
	// non-enumerated kinds (e.g. future providers) safely.
	if left == right {
		return true
	}

	return isMixingAllowedDirected(left, right) || isMixingAllowedDirected(right, left)
}

// isMixingAllowedDirected mirrors the upstream switch in directed
// form. The public helper symmetrises by trying both orderings.
//
// We omit the `allowedWithRecentEnoughDrbdVersion` branches of the
// upstream table because we don't yet plumb the `allowStorPoolMixing`
// cluster prop or per-node DRBD versions into the placer; doing so
// is the explicit follow-up to Bug 76.
func isMixingAllowedDirected(kind1, kind2 string) bool {
	switch kind1 {
	case apiv1.StoragePoolKindFile, apiv1.StoragePoolKindFileThin:
		return kind2 == apiv1.StoragePoolKindFile ||
			kind2 == apiv1.StoragePoolKindFileThin ||
			kind2 == apiv1.StoragePoolKindDiskless

	case apiv1.StoragePoolKindLVM:
		// Upstream's `allowed` table: LVM ↔ LVM only (plus
		// STORAGE_SPACES family which we don't model yet).
		// LVM ↔ LVM_THIN/ZFS/ZFS_THIN sits behind
		// allowStorPoolMixing — see TODO above.
		return false

	case apiv1.StoragePoolKindLVMThin:
		// Symmetric to LVM: same-family already handled in caller;
		// every cross-cell here is gated.
		return false

	case apiv1.StoragePoolKindZFS, apiv1.StoragePoolKindZFSThin:
		// ZFS ↔ ZFS_THIN is "allowed" in upstream (no flag needed),
		// since they share the same on-disk format and `zfs send`
		// payload. Same-kind is already handled by the caller; the
		// remaining cell is ZFS ↔ ZFS_THIN.
		return kind2 == apiv1.StoragePoolKindZFS ||
			kind2 == apiv1.StoragePoolKindZFSThin

	case apiv1.StoragePoolKindRemoteSPDK:
		// REMOTE_SPDK is family-internal; no cross-family mixing.
		return false
	}

	return false
}
