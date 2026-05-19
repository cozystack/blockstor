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
	"context"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Day0GiForTest re-exports the package-private day0 GI derivation for
// the external test package (`satellite_test`). Kept test-only so the
// derivation stays an implementation detail — the controller / REST
// layers have no business calling it.
func Day0GiForTest(resourceName string, volumeNumber int32) string {
	return day0GiFor(resourceName, volumeNumber)
}

// WipeDeviceForTest re-exports the package-private `wipeDevice` so
// the external test package can pin the Bug 336 v2 guaranteed-clean
// wipe contract (wipefs + dd zero both ends + rereadpt + partprobe)
// without exposing the helper to production callers.
func WipeDeviceForTest(ctx context.Context, exec storage.Exec, devicePath string) error {
	return wipeDevice(ctx, exec, devicePath)
}
