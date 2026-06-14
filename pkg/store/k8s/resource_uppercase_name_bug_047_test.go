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

package k8s_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// BUG-047 (deeper layer): relaxing the REST name validator to accept
// uppercase RD names surfaced a case-mismatch in the Resource / Snapshot
// CRD admission. The k8s store's Name() helper lowercases metadata.name,
// but spec.resourceDefinitionName preserves the caller's original case.
// The composite-key CEL rule compared the two case-SENSITIVELY, so a
// Resource create for an uppercase RD (csi-sanity's `…BA880D4F…`) was
// rejected at the apiserver with:
//
//	metadata.name must equal <spec.resourceDefinitionName>.<spec.nodeName>
//
// even though both names address the same object. The fix mirrors the
// StoragePool rule: compare with .lowerAscii() on both sides. These
// tests exercise the REAL apiserver (envtest), so the CEL rule actually
// fires — a unit-level fake store would not catch the regression.

// TestBug047ResourceCreateWithUppercaseRDName creates a Resource whose
// RD name carries uppercase hex (the csi-sanity shape). Before the fix
// the envtest apiserver rejected the create; after, it must succeed and
// round-trip by the original name.
func TestBug047ResourceCreateWithUppercaseRDName(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	s := k8s.New(fixture.client).Resources()
	ctx := t.Context()

	const (
		rdName = "sanity-controller-source-vol-BA880D4F-EF2FC6EB"
		node   = "big-worker-2"
	)

	if err := s.Create(ctx, &apiv1.Resource{Name: rdName, NodeName: node}); err != nil {
		t.Fatalf("Create Resource with uppercase RD name: %v (BUG-047: the "+
			"composite-key CEL rule must compare case-insensitively)", err)
	}

	got, err := s.Get(ctx, rdName, node)
	if err != nil {
		t.Fatalf("Get %s/%s after create: %v", rdName, node, err)
	}

	if got.Name != rdName || got.NodeName != node {
		t.Errorf("round-trip mismatch: got %s/%s, want %s/%s",
			got.Name, got.NodeName, rdName, node)
	}
}

// TestBug047SnapshotCreateWithUppercaseRDName is the Snapshot sibling:
// the Snapshot CRD carries the same composite-key CEL rule
// (<rd>.<snap>) and was fixed the same way. csi-sanity's CreateSnapshot
// specs hit this path with uppercase RD names.
func TestBug047SnapshotCreateWithUppercaseRDName(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	st := k8s.New(fixture.client)
	ctx := t.Context()

	const (
		rdName   = "CreateSnapshot-volume-1-BA880D4F-EF2FC6EB"
		snapName = "snapshot-1"
		node     = "big-worker-2"
	)

	// Seed the parent RD so the snapshot has something to hang off of.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD %q: %v", rdName, err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         snapName,
		ResourceName: rdName,
		Nodes:        []string{node},
	}); err != nil {
		t.Fatalf("Create Snapshot with uppercase RD name: %v (BUG-047: the "+
			"composite-key CEL rule must compare case-insensitively)", err)
	}

	got, err := st.Snapshots().Get(ctx, rdName, snapName)
	if err != nil {
		t.Fatalf("Get snapshot %s/%s after create: %v", rdName, snapName, err)
	}

	if got.Name != snapName || got.ResourceName != rdName {
		t.Errorf("round-trip mismatch: got %s/%s, want %s/%s",
			got.ResourceName, got.Name, rdName, snapName)
	}
}
