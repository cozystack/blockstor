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

package store_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The in-memory store keys a definition's children the way the Kubernetes
// one names them, by naming.Key: a listing of My_Res never shows the
// replicas or snapshots of my_res, a definition of its own. (The Kubernetes
// store cannot hold a replica of a definition outside RFC 1123 at all: the
// Resource CRD wants its name to be the definition's and node's names joined,
// and a hashed name never is.)
func TestInMemoryListingsKeepCaseVariantsOutsideRFC1123Apart(t *testing.T) {
	t.Parallel()

	s := store.NewInMemory()
	ctx := t.Context()

	for _, rd := range []string{"My_Res", "my_res"} {
		if err := s.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
			t.Fatalf("Create definition %q: %v", rd, err)
		}
	}

	if err := s.Resources().Create(ctx, &apiv1.Resource{Name: "my_res", NodeName: "n1"}); err != nil {
		t.Fatalf("Create replica: %v", err)
	}

	if err := s.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap-1", ResourceName: "my_res", Nodes: []string{"n1"}}); err != nil {
		t.Fatalf("Create snapshot: %v", err)
	}

	if got, err := s.Resources().ListByDefinition(ctx, "My_Res"); err != nil || len(got) != 0 {
		t.Errorf("My_Res lists %d replica(s) of my_res, %v", len(got), err)
	}

	if got, err := s.Snapshots().ListByDefinition(ctx, "My_Res"); err != nil || len(got) != 0 {
		t.Errorf("My_Res lists %d snapshot(s) of my_res, %v", len(got), err)
	}

	if got, err := s.Resources().ListByDefinition(ctx, "my_res"); err != nil || len(got) != 1 {
		t.Errorf("my_res lists %d replica(s), %v; want its own one", len(got), err)
	}
}
