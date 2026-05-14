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

package placer_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
)

// TestIsProviderKindMixingAllowed pins the table that mirrors
// upstream LINSTOR's `DeviceProviderKind.isMixingAllowed`. We test
// the relation in symmetric form (both orderings) — the public
// helper is documented as order-independent.
//
// The cases bucket along the three semantic axes:
//
//   - same-kind pairs always allowed (LVM/LVM, ZFS/ZFS, …)
//   - DISKLESS combined with anything always allowed (witnesses)
//   - cross-kind pairs are denied by default. Notably
//     LVM_THIN↔ZFS_THIN is denied here even though upstream would
//     allow it conditionally on the cluster-wide `allowStorPoolMixing`
//     property and DRBD ≥ 9.1.18. We don't model that prop yet (see
//     follow-up note in provider_kind_mixing.go), so the cell stays false.
func TestIsProviderKindMixingAllowed(t *testing.T) {
	t.Parallel()

	type row struct {
		name string
		a, b string
		want bool
	}

	cases := []row{
		// Same-kind: every kind pairs with itself.
		{"lvm_lvm", apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindLVM, true},
		{"lvm_thin_lvm_thin", apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindLVMThin, true},
		{"zfs_zfs", apiv1.StoragePoolKindZFS, apiv1.StoragePoolKindZFS, true},
		{"zfs_thin_zfs_thin", apiv1.StoragePoolKindZFSThin, apiv1.StoragePoolKindZFSThin, true},
		{"file_file", apiv1.StoragePoolKindFile, apiv1.StoragePoolKindFile, true},
		{"file_thin_file_thin", apiv1.StoragePoolKindFileThin, apiv1.StoragePoolKindFileThin, true},

		// DISKLESS witnesses don't claim a backing pool — always OK.
		{"diskless_lvm", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindLVM, true},
		{"diskless_lvm_thin", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindLVMThin, true},
		{"diskless_zfs", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindZFS, true},
		{"diskless_zfs_thin", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindZFSThin, true},
		{"diskless_file", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindFile, true},
		{"diskless_file_thin", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindFileThin, true},
		{"diskless_diskless", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindDiskless, true},

		// FILE / FILE_THIN are interchangeable per upstream.
		{"file_file_thin", apiv1.StoragePoolKindFile, apiv1.StoragePoolKindFileThin, true},

		// ZFS ↔ ZFS_THIN: upstream `allowed` (no flag needed) since
		// the on-disk format and `zfs send` payload are compatible.
		{"zfs_zfs_thin", apiv1.StoragePoolKindZFS, apiv1.StoragePoolKindZFSThin, true},

		// Cross-family pairs: denied. Upstream gates several of these
		// behind allowStorPoolMixing + DRBD-version; we don't carry
		// the cluster prop yet, so they stay denied (Bug 76 scope).
		{"lvm_zfs", apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindZFS, false},
		{"lvm_zfs_thin", apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindZFSThin, false},
		{"lvm_lvm_thin", apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindLVMThin, false},
		{"lvm_thin_zfs", apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindZFS, false},
		{"lvm_thin_zfs_thin", apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindZFSThin, false},
		{"file_lvm", apiv1.StoragePoolKindFile, apiv1.StoragePoolKindLVM, false},
		{"file_zfs", apiv1.StoragePoolKindFile, apiv1.StoragePoolKindZFS, false},
		{"file_thin_lvm", apiv1.StoragePoolKindFileThin, apiv1.StoragePoolKindLVM, false},
		{"file_thin_zfs_thin", apiv1.StoragePoolKindFileThin, apiv1.StoragePoolKindZFSThin, false},

		// Unknown kinds: equal → ok (fallback), distinct → rejected.
		{"unknown_unknown_eq", "BANANA", "BANANA", true},
		{"unknown_unknown_diff", "BANANA", "PEAR", false},
		{"unknown_lvm", "BANANA", apiv1.StoragePoolKindLVM, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := placer.IsProviderKindMixingAllowed(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("IsProviderKindMixingAllowed(%q, %q) = %v; want %v",
					tc.a, tc.b, got, tc.want)
			}

			// Symmetry contract: swapping the arguments must yield
			// the same answer.
			swapped := placer.IsProviderKindMixingAllowed(tc.b, tc.a)
			if swapped != tc.want {
				t.Errorf("IsProviderKindMixingAllowed(%q, %q) [swapped] = %v; want %v",
					tc.b, tc.a, swapped, tc.want)
			}
		})
	}
}
