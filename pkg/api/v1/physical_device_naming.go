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

package v1

import "strings"

// SplitLvmThinPoolName mirrors upstream LINSTOR's LvmThinDriverKind.VGName /
// LVName: a `vg/thin`-shaped pool name splits on the slash, and a bare name
// gets the `linstor_` volume-group prefix while the bare name becomes the
// thin LV.
//
// The prefix is not decoration. The volume group and the thin LV inside it
// are different objects with different namespaces, and giving them one name
// is what upstream avoids by prefixing. Writing the bare name into both
// fields produces a pool whose two halves claim the same identity, and a
// `vg/thin` value written whole puts a slash inside a volume-group name,
// which is not a legal VG name at all.
func SplitLvmThinPoolName(poolName string) (string, string) {
	if before, after, ok := strings.Cut(poolName, "/"); ok {
		return before, after
	}

	return "linstor_" + poolName, poolName
}

// FillAttachToFromPoolName derives the provider-specific backing names from
// the pool name an operator passed, leaving any field already set alone.
//
// Shared because both write doors reach it: the REST handler for
// `physical-storage create-device-pool` and the CLI verb of the same name
// write the same object, and a naming rule implemented twice is a rule the
// two doors eventually disagree about.
func FillAttachToFromPoolName(out *PhysicalDeviceAttachTo, kind, poolName string) {
	if out == nil || poolName == "" {
		return
	}

	switch kind {
	case StoragePoolKindLVM:
		if out.VGName == "" {
			out.VGName = poolName
		}
	case StoragePoolKindLVMThin:
		vgName, thinLV := SplitLvmThinPoolName(poolName)
		if out.VGName == "" {
			out.VGName = vgName
		}

		if out.ThinPoolName == "" {
			out.ThinPoolName = thinLV
		}
	case StoragePoolKindZFS, StoragePoolKindZFSThin:
		if out.ZPoolName == "" {
			out.ZPoolName = poolName
		}
	case StoragePoolKindFile, StoragePoolKindFileThin:
		if out.Directory == "" {
			out.Directory = poolName
		}
	}
}
