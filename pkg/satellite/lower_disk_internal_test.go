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

package satellite

import (
	"testing"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// TestLowerDiskKibExternalMetadata: metadata on its own pool leaves the
// data volume at the definition's size, as LINSTOR sizes it.
func TestLowerDiskKibExternalMetadata(t *testing.T) {
	t.Parallel()

	vol := &intent.DesiredVolume{SizeKib: 20971520, DeviceSizeKib: 20971528, MetaPool: "meta"}

	got := lowerDiskKib(&intent.DesiredResource{LayerStack: []string{"DRBD", "STORAGE"}}, vol)
	if got != 20971520 {
		t.Fatalf("external metadata: got %d KiB, want 20971520", got)
	}
}
