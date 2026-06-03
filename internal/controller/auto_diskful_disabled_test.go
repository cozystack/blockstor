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

package controller_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedAutoDiskful390 builds a 3-node store (n1/n2/n3, each with a usable
// pool) plus an RG (placeCount) and an RD parented to it carrying a
// positive auto-diskful prop. Per-node flags let a caller mark a node
// EVICTED/LOST. Replicas are created verbatim from the supplied specs so
// the test controls the DISKLESS / INACTIVE / TIE_BREAKER mix exactly.
func seedAutoDiskful390(
	t *testing.T,
	ctx context.Context,
	st store.Store,
	placeCount int32,
	nodeFlags map[string][]string,
	replicas []apiv1.Resource,
) *apiv1.ResourceDefinition {
	t.Helper()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  n,
			Type:  apiv1.NodeTypeSatellite,
			Flags: nodeFlags[n],
		}); err != nil {
			t.Fatalf("seed node %q: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %q: %v", n, err)
		}
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  apiv1.LaxInt32(placeCount),
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	rd := &apiv1.ResourceDefinition{
		Name:              "pvc-390",
		ResourceGroupName: "rg",
		Props:             map[string]string{apiv1.AutoDiskfulPropKey: "5"},
	}
	if err := st.ResourceDefinitions().Create(ctx, rd); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for i := range replicas {
		rep := replicas[i]
		rep.Name = rd.Name

		if err := st.Resources().Create(ctx, &rep); err != nil {
			t.Fatalf("seed replica on %q: %v", rep.NodeName, err)
		}
	}

	return rd
}

// TestAutoDiskfulDeadlineArmsOnEvictedNode (Bug 390 #2): a replica on an
// EVICTED node must NOT be counted as a live diskful. With placeCount=2,
// one healthy diskful on n1 and one diskful on the EVICTED n2, the real
// diskful count is 1 (< 2), so the deficit timer must arm rather than be
// cleared. n3 carries a promotable diskless candidate.
func TestAutoDiskfulDeadlineArmsOnEvictedNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskful390(t, ctx, st,
		2,
		map[string][]string{"n2": {apiv1.NodeFlagEvicted}},
		[]apiv1.Resource{
			{NodeName: "n1"},
			{NodeName: "n2"}, // diskful, but on an EVICTED node
			{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless}},
		},
	)

	t0 := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if res.RequeueAfter != 5*time.Minute {
		t.Errorf("RequeueAfter: got %v, want 5m (timer must arm — EVICTED diskful does not count)", res.RequeueAfter)
	}

	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := t0.Add(5 * time.Minute).Format(time.RFC3339)
	if got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation] != want {
		t.Errorf("deadline annotation: got %q, want %q (EVICTED-node diskful masked the deficit)",
			got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation], want)
	}
}

// TestAutoDiskfulDeadlineArmsOnInactiveReplica (Bug 390 #3): an INACTIVE
// replica (`drbdadm down`) carries neither DISKLESS nor TIE_BREAKER but
// serves no I/O. With placeCount=2, one healthy diskful on n1 and one
// INACTIVE replica on n2, the live diskful count is 1 (< 2) and the
// timer must arm.
func TestAutoDiskfulDeadlineArmsOnInactiveReplica(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskful390(t, ctx, st,
		2,
		nil,
		[]apiv1.Resource{
			{NodeName: "n1"},
			{NodeName: "n2", Flags: []string{apiv1.ResourceFlagInactive}},
			{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless}},
		},
	)

	t0 := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if res.RequeueAfter != 5*time.Minute {
		t.Errorf("RequeueAfter: got %v, want 5m (timer must arm — INACTIVE replica does not count)", res.RequeueAfter)
	}

	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := t0.Add(5 * time.Minute).Format(time.RFC3339)
	if got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation] != want {
		t.Errorf("deadline annotation: got %q, want %q (INACTIVE replica masked the deficit)",
			got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation], want)
	}
}

// TestAutoDiskfulHealthyClustersStripDeadline guards against a
// false-positive: when placeCount=2 is genuinely satisfied by two live,
// active, non-disabled diskful replicas, the timer must NOT arm (and any
// stale deadline is stripped). This pins that the Bug 390 gating does not
// over-count absence.
func TestAutoDiskfulHealthyClustersStripDeadline(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskful390(t, ctx, st,
		2,
		nil,
		[]apiv1.Resource{
			{NodeName: "n1"},
			{NodeName: "n2"},
			{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless}},
		},
	)

	t0 := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter: got %v, want 0 (cluster healthy — no deficit)", res.RequeueAfter)
	}

	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if _, ok := got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation]; ok {
		t.Errorf("deadline stamped on a healthy cluster: %v", got.Annotations)
	}
}

// TestAutoDiskfulPromotesOnHealthyNotDisabled (Bug 390 #4): when the
// deadline fires, promoteOne must never select a candidate on a disabled
// (EVICTED/LOST) node. Here n1 is the surviving diskful, n2 is a diskless
// replica on a LOST node (must be skipped), and n3 is a diskless replica
// on a healthy node (the only legal promotion target). The fix must
// promote n3 and leave the LOST n2 untouched.
func TestAutoDiskfulPromotesOnHealthyNotDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskful390(t, ctx, st,
		2,
		map[string][]string{"n2": {apiv1.NodeFlagLost}},
		[]apiv1.Resource{
			{NodeName: "n1"},
			{NodeName: "n2", Flags: []string{apiv1.ResourceFlagDiskless}}, // on a LOST node
			{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless}}, // healthy candidate
		},
	)

	t0 := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	// Arm.
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}}); err != nil {
		t.Fatalf("Reconcile (arm): %v", err)
	}

	// Fire.
	rec.Now = func() time.Time { return t0.Add(6 * time.Minute) }
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}}); err != nil {
		t.Fatalf("Reconcile (fire): %v", err)
	}

	// The LOST-node replica must stay diskless — never promoted onto a
	// draining/gone node.
	lost, err := st.Resources().Get(ctx, rd.Name, "n2")
	if err != nil {
		t.Fatalf("get n2: %v", err)
	}

	if !slices.Contains(lost.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("LOST-node replica promoted to diskful: flags=%v", lost.Flags)
	}

	if lost.Props["StorPoolName"] != "" {
		t.Errorf("LOST-node replica got a StorPoolName stamp: %v", lost.Props)
	}

	// The healthy n3 replica is the legal target — it must be promoted.
	healthy, err := st.Resources().Get(ctx, rd.Name, "n3")
	if err != nil {
		t.Fatalf("get n3: %v", err)
	}

	if slices.Contains(healthy.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("healthy candidate not promoted: flags=%v", healthy.Flags)
	}

	if healthy.Props["StorPoolName"] != "pool" {
		t.Errorf("healthy candidate StorPoolName: got %q, want pool", healthy.Props["StorPoolName"])
	}
}
