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

package controller

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// A replica's confirmation is a status-only write, which the reconciler
// otherwise ignores; it must still wake the reconciler, or the delete waits
// for the next resync.
func TestDeletedVolumesChangedWakesTheReconciler(t *testing.T) {
	t.Parallel()

	before := &blockstoriov1alpha1.Resource{}
	after := before.DeepCopy()
	after.Status.DeletedVolumes = []blockstoriov1alpha1.DeletedVolumeStatus{{VolumeNumber: 1, DeleteID: "d"}}

	if !deletedVolumesChanged(before, after) {
		t.Error("a new confirmation did not count as a change")
	}

	other := before.DeepCopy()
	other.Status.InUse = true

	if deletedVolumesChanged(before, other) {
		t.Error("an unrelated status write counted as a confirmation")
	}
}
