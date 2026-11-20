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

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// seedOrphanWitnessRD seeds an RD with the given replicas.
func seedOrphanWitnessRD(t *testing.T, st store.Store, rd string, replicas []apiv1.Resource) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for i := range replicas {
		replicas[i].Name = rd
		if err := st.Resources().Create(ctx, &replicas[i]); err != nil {
			t.Fatalf("seed replica %s: %v", replicas[i].NodeName, err)
		}
	}
}

func deleteReplica(t *testing.T, base, rd, node string) {
	t.Helper()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/"+node)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete %s/%s: got %d, want 200", rd, node, resp.StatusCode)
	}
}

// undeletedFromView returns the nodes of rd's replicas that the
// resource view lists without the DELETE flag: the exact predicate
// linstor-csi applies before it deletes the resource definition.
func undeletedFromView(t *testing.T, base, rd string) []string {
	t.Helper()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	var view []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode view: %v", err)
	}

	var out []string

	for i := range view {
		if view[i].Name == rd && !slices.Contains(view[i].Flags, apiv1.ResourceFlagDelete) {
			out = append(out, view[i].NodeName)
		}
	}

	return out
}

// TestResourceDeleteReapsUnneededWitness replays the shapes upstream's
// auto-tiebreaker helper decides inside the `r d` transaction. Where
// fewer than two voting diskful replicas remain the witness must be
// gone (or flagged DELETE) when the DELETE returns, as on LINSTOR
// v1.33.1; linstor-csi lists right after its last replica delete and
// skips the resource-definition delete for good if it sees a live
// TIE_BREAKER. The reconciler's carve-outs hold the witness at one
// diskful: the operator's --keep-tiebreaker and an INACTIVE replica
// waiting for a backfill onto the witness (Bug 393).
func TestResourceDeleteReapsUnneededWitness(t *testing.T) {
	witness := []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}

	cases := []struct {
		name     string
		replicas []apiv1.Resource
		deletes  []string
		want     [][]string // undeleted replicas after each delete
	}{
		{
			name:     "csi deletes both diskful",
			replicas: []apiv1.Resource{{NodeName: "n1"}, {NodeName: "n2"}, {NodeName: "n3", Flags: witness}},
			deletes:  []string{"n1", "n2"},
			want:     [][]string{{"n2"}, nil},
		},
		{
			name:     "keep-tiebreaker holds the witness",
			replicas: []apiv1.Resource{{NodeName: "n1"}, {NodeName: "n2"}, {NodeName: "n3", Flags: witness}},
			deletes:  []string{"n1?keep_tiebreaker=true"},
			want:     [][]string{{"n2", "n3"}},
		},
		{
			name: "inactive replica holds the witness",
			replicas: []apiv1.Resource{
				{NodeName: "n1"},
				{NodeName: "n2"},
				{NodeName: "n3", Flags: witness},
				{NodeName: "n4", Flags: []string{apiv1.ResourceFlagInactive}},
			},
			deletes: []string{"n1"},
			want:    [][]string{{"n2", "n3", "n4"}},
		},
		{
			name: "two diskful left stay with the reconciler",
			replicas: []apiv1.Resource{
				{NodeName: "n1"}, {NodeName: "n2"}, {NodeName: "n3", Flags: witness}, {NodeName: "n4"},
			},
			deletes: []string{"n1"},
			want:    [][]string{{"n2", "n3", "n4"}},
		},
		{
			name:     "a plain diskless replica is not a witness",
			replicas: []apiv1.Resource{{NodeName: "n1"}, {NodeName: "n2", Flags: []string{apiv1.ResourceFlagDiskless}}},
			deletes:  []string{"n1"},
			want:     [][]string{{"n2"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemory()
			seedOrphanWitnessRD(t, st, "pvc-reap", tc.replicas)

			base, stop := startServerWithStore(t, st)
			defer stop()

			for i, node := range tc.deletes {
				deleteReplica(t, base, "pvc-reap", node)

				if got := undeletedFromView(t, base, "pvc-reap"); !slices.Equal(got, tc.want[i]) {
					t.Fatalf("after deleting %s: undeleted %v, want %v", node, got, tc.want[i])
				}
			}
		})
	}
}

// TestResourceDeleteKeepsWitnessTheCacheHidesAPeerOf: another apiserver
// replica has just created a diskful this replica's informer cache
// does not show yet. The definition really has two diskful replicas
// left after the delete, so its witness is the third quorum voter and
// must stay; read from the cache it looked like one diskful and the
// witness went.
func TestResourceDeleteKeepsWitnessTheCacheHidesAPeerOf(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	live := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&crdv1alpha1.Resource{}).Build()

	// The cached view trails: it answers every List without the
	// replica on n4.
	cached := interceptor.NewClient(live, interceptor.Funcs{
		List: func(ctx context.Context, inner ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
			if err := inner.List(ctx, list, opts...); err != nil {
				return err //nolint:wrapcheck // pass-through test double
			}

			if rl, ok := list.(*crdv1alpha1.ResourceList); ok {
				rl.Items = slices.DeleteFunc(rl.Items, func(r crdv1alpha1.Resource) bool { return r.Spec.NodeName == "n4" })
			}

			return nil
		},
	})

	st := k8s.NewWithAPIReader(cached, live)
	seedOrphanWitnessRD(t, st, "pvc-stale", []apiv1.Resource{
		{NodeName: "n1"},
		{NodeName: "n2"},
		{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}},
		{NodeName: "n4"},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	deleteReplica(t, base, "pvc-stale", "n1")

	if _, err := st.Resources().Get(t.Context(), "pvc-stale", "n3"); err != nil {
		t.Fatalf("witness of a two-diskful definition was reaped: %v", err)
	}
}

// TestResourceDeleteLeavesNoLiveReplicaWhileTeardownIsSlow replays
// linstor-csi's DeleteVolume against the CRD store with the satellite
// finalizers held, the production path: the deleted diskful replicas
// stay listed with DELETE synthesised from their DeletionTimestamp for
// longer than the handler waits. The RD reconciler runs after each
// delete, as it would concurrently. Once both diskful replicas are
// deleted, csi's final listing must find nothing without DELETE; a
// witness the reconciler grew back for the two terminating replicas
// is the leak.
func TestResourceDeleteLeavesNoLiveReplicaWhileTeardownIsSlow(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&crdv1alpha1.Resource{}).Build()
	st := k8s.NewWithAPIReader(cli, cli)
	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	seedOrphanWitnessRD(t, st, "pvc-slow", []apiv1.Resource{
		{NodeName: "n1"},
		{NodeName: "n2"},
		{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}},
	})

	// The satellites hold the diskful replicas through a slow DRBD or
	// ZFS teardown; a diskless witness has nothing to tear down and
	// goes at once.
	for _, n := range []string{"n1", "n2"} {
		holdReplica(t, cli, "pvc-slow", n)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	rec := &controller.ResourceDefinitionReconciler{Client: cli, APIReader: cli, Scheme: scheme, Store: st}
	reconcile := func() {
		t.Helper()

		if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-slow"}}); err != nil {
			t.Fatalf("reconcile definition: %v", err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		deleteReplica(t, base, "pvc-slow", n)
		reconcile()
	}

	if got := undeletedFromView(t, base, "pvc-slow"); len(got) != 0 {
		t.Fatalf("undeleted %v after both diskful deletes, want none (linstor-csi would keep the RD)", got)
	}
}

// holdReplica puts a finalizer on the replica's CRD, standing in for the
// satellite's, so a delete leaves it terminating.
func holdReplica(t *testing.T, cli ctrlclient.Client, rd, node string) {
	t.Helper()

	var crd crdv1alpha1.Resource
	if err := cli.Get(t.Context(), types.NamespacedName{Name: naming.Key(rd + "." + node)}, &crd); err != nil {
		t.Fatalf("get replica %s: %v", node, err)
	}

	crd.Finalizers = append(crd.Finalizers, "test.blockstor.org/teardown")
	if err := cli.Update(t.Context(), &crd); err != nil {
		t.Fatalf("hold replica %s: %v", node, err)
	}
}
