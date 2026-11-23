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

package store

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// A diskless replica already being deleted is going whatever the restore does:
// it does not refuse the name.
func TestRefuseReplicasWithoutDefinitionPassesOverOneBeingDeleted(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "dst-going", NodeName: "node-a",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagDelete},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := RefuseReplicasWithoutDefinition(t.Context(), st, "dst-going"); err != nil {
		t.Fatalf("refused over a replica being deleted: %v", err)
	}
}
