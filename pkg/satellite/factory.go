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
	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Upstream-LINSTOR provider kind string constants. Match the wire
// format `DesiredStoragePool.provider_kind` carries — keep them in
// sync with `pkg/api/v1.StoragePool.ProviderKind` strings.
const (
	ProviderKindLVM      = "LVM"
	ProviderKindLVMThin  = "LVM_THIN"
	ProviderKindZFS      = "ZFS"
	ProviderKindZFSThin  = "ZFS_THIN"
	ProviderKindFile     = "FILE"
	ProviderKindFileThin = "FILE_THIN"
	ProviderKindDiskless = "DISKLESS"
)

// Upstream LINSTOR property keys used to configure each provider
// kind. Mirrored verbatim so existing operators / piraeus-operator
// manifests round-trip.
const (
	propLvmVG    = "StorDriver/LvmVg"
	propThinPool = "StorDriver/ThinPool"
	propZPool    = "StorDriver/ZPool"
	propFileDir  = "StorDriver/FileDir"
)

// NewProviderFromKind instantiates the matching `storage.Provider`
// for the upstream-LINSTOR provider kind, reading kind-specific
// configuration from the props bag. Phase 10.5: replaces the
// satellite's startup-CLI-flag-only Provider registry with dynamic
// pool registration via `ApplyStoragePools`.
//
// Returns (nil, nil) for `DISKLESS` — that kind has no underlying
// storage and the satellite's reconciler short-circuits its volume
// path. Unknown kinds and missing-config-key cases return a wrapped
// error so the per-pool ApplyStoragePools result surfaces a
// readable message rather than a silent skip.
func NewProviderFromKind(kind string, props map[string]string, exec storage.Exec) (storage.Provider, error) {
	switch kind {
	case ProviderKindLVM:
		return newLVMThick(props, exec)
	case ProviderKindLVMThin:
		return newLVMThin(props, exec)
	case ProviderKindZFS:
		return newZFS(props, exec, false)
	case ProviderKindZFSThin:
		return newZFS(props, exec, true)
	case ProviderKindFile:
		return newFile(props, exec, false)
	case ProviderKindFileThin:
		return newFile(props, exec, true)
	case ProviderKindDiskless:
		// DISKLESS pools never own local storage — they exist as
		// allocator targets only. Returning nil tells the caller
		// "valid kind, nothing to register".
		return nil, nil //nolint:nilnil // intentional: see comment
	}

	return nil, errors.Errorf("unknown provider kind %q", kind)
}

func newLVMThick(props map[string]string, exec storage.Exec) (storage.Provider, error) {
	vg := props[propLvmVG]
	if vg == "" {
		return nil, errors.Errorf("LVM provider requires %q in props", propLvmVG)
	}

	return lvm.NewThick(lvm.ThickConfig{VolumeGroup: vg}, exec), nil
}

func newLVMThin(props map[string]string, exec storage.Exec) (storage.Provider, error) {
	vg := props[propLvmVG]
	if vg == "" {
		return nil, errors.Errorf("LVM_THIN provider requires %q in props", propLvmVG)
	}

	thinPool := props[propThinPool]
	if thinPool == "" {
		return nil, errors.Errorf("LVM_THIN provider requires %q in props", propThinPool)
	}

	return lvm.NewThin(lvm.ThinConfig{VolumeGroup: vg, ThinPool: thinPool}, exec), nil
}

func newZFS(props map[string]string, exec storage.Exec, thin bool) (storage.Provider, error) {
	pool := props[propZPool]
	if pool == "" {
		kind := ProviderKindZFS
		if thin {
			kind = ProviderKindZFSThin
		}

		return nil, errors.Errorf("%s provider requires %q in props", kind, propZPool)
	}

	return zfs.NewProvider(zfs.Config{Pool: pool, Thin: thin}, exec), nil
}

func newFile(props map[string]string, exec storage.Exec, thin bool) (storage.Provider, error) {
	dir := props[propFileDir]
	if dir == "" {
		kind := ProviderKindFile
		if thin {
			kind = ProviderKindFileThin
		}

		return nil, errors.Errorf("%s provider requires %q in props", kind, propFileDir)
	}

	return file.NewProvider(file.Config{Dir: dir, Thin: thin}, exec), nil
}
