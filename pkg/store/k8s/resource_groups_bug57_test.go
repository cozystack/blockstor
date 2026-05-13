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

package k8s_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// TestResourceGroupCanonicalCamelCaseRoundTrip pins the k8s-store
// half of Bug 57: when the REST layer creates `DfltRscGrp`, the
// CRD's metadata.name is the lowercased rfc1123-clean slug
// `dfltrscgrp` (so `kubectl get resourcegroup dfltrscgrp` works),
// but a subsequent Get/List must surface the canonical CamelCase
// spelling on the wire — via the `blockstor.io/linstor-name`
// annotation set by SetOriginalName.
func TestResourceGroupCanonicalCamelCaseRoundTrip(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	s := k8s.New(cli)

	in := &apiv1.ResourceGroup{Name: "DfltRscGrp"}
	if err := s.ResourceGroups().Create(t.Context(), in); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Get by canonical spelling must succeed (Name() lowercases on
	// lookup so this is the case-insensitive contract LINSTOR
	// callers rely on).
	got, err := s.ResourceGroups().Get(t.Context(), "DfltRscGrp")
	if err != nil {
		t.Fatalf("Get by CamelCase: %v", err)
	}

	if got.Name != "DfltRscGrp" {
		t.Errorf("Get name: got %q, want %q (canonical CamelCase)",
			got.Name, "DfltRscGrp")
	}

	// Get by lowercased spelling must also succeed (kubectl-style
	// callers) and surface the same canonical name on the wire.
	gotLower, err := s.ResourceGroups().Get(t.Context(), "dfltrscgrp")
	if err != nil {
		t.Fatalf("Get by lowercase: %v", err)
	}

	if gotLower.Name != "DfltRscGrp" {
		t.Errorf("Get(lowercase) name: got %q, want %q (canonical even when "+
			"the caller passed lowercase)", gotLower.Name, "DfltRscGrp")
	}

	// List must surface the canonical spelling too — this is what
	// `linstor rg l` reads when it prints the table.
	list, err := s.ResourceGroups().List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(list) != 1 || list[0].Name != "DfltRscGrp" {
		t.Errorf("List: got %+v, want exactly one entry named %q",
			list, "DfltRscGrp")
	}
}
