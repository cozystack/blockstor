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

package controllers

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// A stack stored in another case still names DRBD; reading it as "no DRBD"
// skipped the DRBD allocation while the apply path brought DRBD up.
func TestRDNeedsDRBDIgnoresCase(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{}
	rd.Spec.LayerStack = []string{"drbd", "storage"}

	if !rdNeedsDRBD(rd) {
		t.Fatal("a lowercase drbd,storage stack read as one without DRBD")
	}

	rd.Spec.LayerStack = []string{"storage"}
	if rdNeedsDRBD(rd) {
		t.Fatal("a storage-only stack read as one with DRBD")
	}
}
