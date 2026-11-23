// SPDX-License-Identifier: Apache-2.0

//go:build integration

package harness

import (
	"context"
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// A test that places right after seeding finds every pool with free space
// in the cache the placer reads; before the satellite's first capacity
// stamp it finds none, and its placement is deferred out of the test's
// reach.
func TestSeededPoolsHaveCapacityWhereThePlacerReads(t *testing.T) {
	stack := StartStack(t)
	SeedThreeNodeCluster(t, stack)

	var pools blockstoriov1alpha1.StoragePoolList
	if err := stack.Manager.GetClient().List(context.Background(), &pools); err != nil {
		t.Fatalf("list pools: %v", err)
	}

	if want := len(FixtureNodes()) * len(FixtureProviders()); len(pools.Items) < want {
		t.Fatalf("pools in the cache: %d, want %d", len(pools.Items), want)
	}

	for i := range pools.Items {
		if pools.Items[i].Status.FreeCapacity == 0 {
			t.Errorf("pool %s has no free capacity when seeding returns", pools.Items[i].Name)
		}
	}
}
