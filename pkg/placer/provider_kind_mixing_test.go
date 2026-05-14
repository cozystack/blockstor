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
//     LVM_THIN↔ZFS_THIN is denied unless the operator opts in via the
//     6.W07 cluster prop `AllowMixingStoragePoolDriver=true`
//     (apiv1.PropAllowMixingStoragePoolDriver). That override is
//     exercised in TestIsProviderKindMixingAllowedWith6W07Override.
//
// This case body runs with allowMixing=false (the default) so it
// pins the upstream-default table.
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

		// Cross-family pairs: denied by default. The 6.W07 override
		// opens LVM_THIN ↔ ZFS_THIN only — the others stay denied even
		// with the flag set; see TestIsProviderKindMixingAllowedWith6W07Override.
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

			got := placer.IsProviderKindMixingAllowed(tc.a, tc.b, false)
			if got != tc.want {
				t.Errorf("IsProviderKindMixingAllowed(%q, %q, false) = %v; want %v",
					tc.a, tc.b, got, tc.want)
			}

			// Symmetry contract: swapping the arguments must yield
			// the same answer.
			swapped := placer.IsProviderKindMixingAllowed(tc.b, tc.a, false)
			if swapped != tc.want {
				t.Errorf("IsProviderKindMixingAllowed(%q, %q, false) [swapped] = %v; want %v",
					tc.b, tc.a, swapped, tc.want)
			}
		})
	}
}

// TestIsProviderKindMixingAllowedWith6W07Override pins the 6.W07
// override semantic: when the operator sets
// `AllowMixingStoragePoolDriver=true` on the controller singleton,
// exactly ONE additional cell of the mixing matrix opens —
// LVM_THIN ↔ ZFS_THIN. Every other cross-family cell stays closed,
// matching the deliberately narrow widening documented in
// provider_kind_mixing.go.
//
// We re-assert the unchanged cells (same-kind, DISKLESS, FILE family,
// ZFS↔ZFS_THIN) so a regression that flips the flag's polarity (e.g.
// opens too many cells) is caught by the same table.
//
// Cross-listed: wave1 6.8 / Bug 76 / wave2-06 §6.W07.
func TestIsProviderKindMixingAllowedWith6W07Override(t *testing.T) {
	t.Parallel()

	type row struct {
		name string
		a, b string
		want bool
	}

	cases := []row{
		// The 6.W07 cell: opens once the operator opts in.
		{"lvm_thin_zfs_thin_opens", apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindZFSThin, true},

		// Same-kind still allowed (unchanged).
		{"lvm_thin_lvm_thin_still_ok", apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindLVMThin, true},
		{"zfs_thin_zfs_thin_still_ok", apiv1.StoragePoolKindZFSThin, apiv1.StoragePoolKindZFSThin, true},

		// DISKLESS witnesses still OK (unchanged).
		{"diskless_lvm_thin_still_ok", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindLVMThin, true},
		{"diskless_zfs_thin_still_ok", apiv1.StoragePoolKindDiskless, apiv1.StoragePoolKindZFSThin, true},

		// FILE family unchanged.
		{"file_file_thin_still_ok", apiv1.StoragePoolKindFile, apiv1.StoragePoolKindFileThin, true},

		// ZFS ↔ ZFS_THIN unchanged (upstream `allowed` without flag).
		{"zfs_zfs_thin_still_ok", apiv1.StoragePoolKindZFS, apiv1.StoragePoolKindZFSThin, true},

		// Cells that stay closed despite the flag — the 6.W07
		// override is intentionally narrow to match the e2e matrix:
		//   LVM ↔ LVM_THIN, LVM ↔ ZFS, LVM ↔ ZFS_THIN,
		//   LVM_THIN ↔ ZFS, FILE ↔ LVM/ZFS_THIN.
		{"lvm_lvm_thin_still_denied", apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindLVMThin, false},
		{"lvm_zfs_still_denied", apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindZFS, false},
		{"lvm_zfs_thin_still_denied", apiv1.StoragePoolKindLVM, apiv1.StoragePoolKindZFSThin, false},
		{"lvm_thin_zfs_still_denied", apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindZFS, false},
		{"file_lvm_still_denied", apiv1.StoragePoolKindFile, apiv1.StoragePoolKindLVM, false},
		{"file_thin_zfs_thin_still_denied", apiv1.StoragePoolKindFileThin, apiv1.StoragePoolKindZFSThin, false},

		// Unknown kinds: flag must not widen them; equal → ok, distinct → rejected.
		{"unknown_unknown_eq_still_ok", "BANANA", "BANANA", true},
		{"unknown_unknown_diff_still_denied", "BANANA", "PEAR", false},
		{"unknown_lvm_thin_still_denied", "BANANA", apiv1.StoragePoolKindLVMThin, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := placer.IsProviderKindMixingAllowed(tc.a, tc.b, true)
			if got != tc.want {
				t.Errorf("IsProviderKindMixingAllowed(%q, %q, true) = %v; want %v",
					tc.a, tc.b, got, tc.want)
			}

			swapped := placer.IsProviderKindMixingAllowed(tc.b, tc.a, true)
			if swapped != tc.want {
				t.Errorf("IsProviderKindMixingAllowed(%q, %q, true) [swapped] = %v; want %v",
					tc.b, tc.a, swapped, tc.want)
			}
		})
	}
}
