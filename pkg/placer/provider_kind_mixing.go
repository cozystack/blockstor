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
//     `AllowMixingStoragePoolDriver` plus DRBD version ≥ 9.1.18 / 9.2.7.
//     Scenario 6.W07 plumbs the prop through `allowMixing`: when true
//     ONLY the LVM_THIN ↔ ZFS_THIN cell opens (the cell explicitly
//     covered by wave2-06 e2e). The wider upstream matrix (LVM ↔ ZFS,
//     LVM ↔ LVM_THIN, …) stays closed even with the flag set — opening
//     them would silently widen the placer beyond what's tested.
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
// `allowMixing` is the resolved controller-scope
// `AllowMixingStoragePoolDriver` prop (apiv1.PropAllowMixingStoragePoolDriver).
// Operators must independently satisfy the upstream prerequisites —
// DRBD ≥ 9.2.7 and LINSTOR ≥ 1.27.0 (see prop doc on
// `PropAllowMixingStoragePoolDriver`) — before flipping the flag;
// the placer takes the prop's value at face value, mirroring upstream
// LINSTOR's "operator opts in, operator owns the consequences"
// contract.
//
// Order doesn't matter: f(a,b,flag) == f(b,a,flag) per upstream
// contract.
func IsProviderKindMixingAllowed(left, right string, allowMixing bool) bool {
	// DISKLESS as either side: no backing pool, nothing to gate.
	if left == apiv1.StoragePoolKindDiskless || right == apiv1.StoragePoolKindDiskless {
		return true
	}

	// Same kind is always allowed. This also covers any
	// non-enumerated kinds (e.g. future providers) safely.
	if left == right {
		return true
	}

	if isMixingAllowedDirected(left, right) || isMixingAllowedDirected(right, left) {
		return true
	}

	// 6.W07 override: the operator has opted in via cluster prop
	// AllowMixingStoragePoolDriver=true, so open the explicitly-tested
	// LVM_THIN ↔ ZFS_THIN cell. The widening is deliberately narrow —
	// only the single cell wave2-06 §6.W07 calls out — so opening the
	// flag never silently exceeds the matrix the e2e suite exercises.
	if allowMixing && isThinCrossKindPair(left, right) {
		return true
	}

	return false
}

// isThinCrossKindPair reports whether {left, right} is exactly the
// LVM_THIN ↔ ZFS_THIN pair (in either order). Pulled out of the public
// helper so the 6.W07 override is one named call instead of an inline
// conjunction — the symmetric "either order" check makes the inline
// form noisy.
func isThinCrossKindPair(left, right string) bool {
	if left == apiv1.StoragePoolKindLVMThin && right == apiv1.StoragePoolKindZFSThin {
		return true
	}

	if left == apiv1.StoragePoolKindZFSThin && right == apiv1.StoragePoolKindLVMThin {
		return true
	}

	return false
}

// isMixingAllowedDirected mirrors the upstream switch in directed
// form. The public helper symmetrises by trying both orderings.
//
// We omit the `allowedWithRecentEnoughDrbdVersion` branches of the
// upstream table because we only model the single LVM_THIN ↔ ZFS_THIN
// cell behind 6.W07's `AllowMixingStoragePoolDriver` flag; broader
// upstream cells (LVM ↔ ZFS, LVM ↔ LVM_THIN, …) stay closed until a
// follow-up scenario plumbs them through e2e too.
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
		// allowStorPoolMixing in upstream; 6.W07 only widens the
		// LVM_THIN ↔ ZFS_THIN cell, not these.
		return false

	case apiv1.StoragePoolKindLVMThin:
		// Symmetric to LVM: same-family already handled in caller;
		// the LVM_THIN ↔ ZFS_THIN override is applied by the public
		// helper after this directed lookup returns false.
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
