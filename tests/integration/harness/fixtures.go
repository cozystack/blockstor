//go:build integration

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

package harness

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Canonical fixture node names. Kept as named constants (not a
// slice) so other harness helpers can reference them by symbol.
const (
	NodeWorker1 = "worker-1"
	NodeWorker2 = "worker-2"
	NodeWorker3 = "worker-3"

	// FixtureDefaultRG is the canonical resource-group every wave-1
	// SC targets. Spawn-from-RG tests assume this exists with
	// placeCount=2.
	FixtureDefaultRG = "default"

	// providerLVMThin / providerZFSThin / providerFile mirror the
	// kebab-case pool names a real LINSTOR/blockstor deployment uses
	// for these provider kinds.
	providerLVMThin = "lvm-thin"
	providerZFSThin = "zfs-thin"
	providerFile    = "file"

	// satellitePortDefault is the upstream-LINSTOR satellite plain-
	// text port. Same value the production manifests use.
	satellitePortDefault int32 = 3366
)

// FixtureNodes returns the canonical 3-node cluster names. A
// function (not a `var`) so the linter is happy and so tests
// always get a fresh slice they can mutate without surprise.
func FixtureNodes() []string {
	return []string{NodeWorker1, NodeWorker2, NodeWorker3}
}

// FixtureProvider is the (provider-kind, pool-name) pair the
// fixture seeds on each node.
type FixtureProvider struct {
	Kind     string
	PoolName string
}

// FixtureProviders returns the canonical provider list per node:
// one LVM_THIN pool, one ZFS_THIN, one FILE. Total: 9 SPs across
// 3 nodes.
func FixtureProviders() []FixtureProvider {
	return []FixtureProvider{
		{Kind: providerLVMThinUpper, PoolName: providerLVMThin},
		{Kind: providerZFSThinUpper, PoolName: providerZFSThin},
		{Kind: "FILE", PoolName: providerFile},
	}
}

// SeedThreeNodeCluster creates the canonical fixture: 3 Nodes, 9
// StoragePools (3 providers × 3 nodes), and one default RG. Safe
// to call multiple times — AlreadyExists is treated as success so
// per-group tests that re-seed don't have to special-case
// "previous test left this here".
func SeedThreeNodeCluster(t *testing.T, stack *Stack) {
	t.Helper()

	ctx := context.Background()
	cli := stack.Env.Client

	for _, name := range FixtureNodes() {
		seedNode(ctx, t, cli, name)
	}

	for _, name := range FixtureNodes() {
		for _, prov := range FixtureProviders() {
			seedStoragePool(ctx, t, cli, name, prov.PoolName, prov.Kind)
		}
	}

	seedDefaultResourceGroup(ctx, t, cli)
}

func seedNode(ctx context.Context, t *testing.T, cli client.Client, name string) {
	t.Helper()

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: "SATELLITE",
			NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{
				{
					Name:                    "default",
					Address:                 "10.0.0." + nodeOctet(name),
					SatellitePort:           satellitePortDefault,
					SatelliteEncryptionType: "PLAIN",
				},
			},
		},
	}

	err := cli.Create(ctx, node)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create Node %q: %v", name, err)
	}
}

func seedStoragePool(ctx context.Context, t *testing.T, cli client.Client, node, pool, kind string) {
	t.Helper()

	pool2 := &blockstoriov1alpha1.StoragePool{
		// Composite name pinned by the CEL XValidation rule on
		// StoragePool — keep `<pool>.<node>` exactly.
		ObjectMeta: metav1.ObjectMeta{Name: pool + "." + node},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     node,
			PoolName:     pool,
			ProviderKind: kind,
		},
	}

	err := cli.Create(ctx, pool2)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create StoragePool %q: %v", pool2.Name, err)
	}
}

func seedDefaultResourceGroup(ctx context.Context, t *testing.T, cli client.Client) {
	t.Helper()

	resourceGroup := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: FixtureDefaultRG},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			Description: "harness default RG (placeCount=2)",
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				PlaceCount: 2,
			},
		},
	}

	err := cli.Create(ctx, resourceGroup)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create ResourceGroup %q: %v", resourceGroup.Name, err)
	}
}

// nodeOctet returns the last octet (`1`, `2`, `3`) for the
// canonical 10.0.0.<x> address derived from the node name.
// Falls back to "1" for any unrecognised name so the helper is
// not load-bearing on the naming scheme.
func nodeOctet(name string) string {
	switch name {
	case NodeWorker1:
		return "1"
	case NodeWorker2:
		return "2"
	case NodeWorker3:
		return "3"
	default:
		return "1"
	}
}
