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
	"fmt"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

// apiServerReads counts what a store's direct reader asks the API
// server for: every Get and List past the informer cache is one
// request to kube-apiserver.
type apiServerReads struct {
	client.Reader

	mu    sync.Mutex
	gets  []string
	lists []string
}

func (r *apiServerReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.mu.Lock()
	r.gets = append(r.gets, fmt.Sprintf("%T %s", obj, key.Name))
	r.mu.Unlock()

	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *apiServerReads) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var lo client.ListOptions
	lo.ApplyOptions(opts)

	selector := ""
	if lo.FieldSelector != nil {
		selector = lo.FieldSelector.String()
	}

	r.mu.Lock()
	r.lists = append(r.lists, fmt.Sprintf("%T %s", list, selector))
	r.mu.Unlock()

	return r.Reader.List(ctx, list, opts...)
}

func (r *apiServerReads) reset() {
	r.mu.Lock()
	r.gets, r.lists = nil, nil
	r.mu.Unlock()
}

func (r *apiServerReads) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.gets), len(r.lists)
}

// settledGroupStore is a Kubernetes store over a cluster of three online
// nodes and one group of place count 3, holding definitions settled
// definitions with three UpToDate replicas each and, when short is
// set, one more definition named pvc-short with two. Its cache and the
// API server agree; reads is the API server side.
func settledGroupStore(tb testing.TB, definitions int, short bool) (store.Store, *apiServerReads) {
	tb.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		tb.Fatalf("corev1: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		tb.Fatalf("blockstor: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		WithIndex(&blockstoriov1alpha1.Resource{}, storek8s.FieldResourceDefinitionName, func(obj client.Object) []string {
			res, ok := obj.(*blockstoriov1alpha1.Resource)
			if !ok {
				return nil
			}

			return []string{res.Spec.ResourceDefinitionName}
		}).
		Build()

	ctx := context.Background()
	seed := storek8s.New(cli)
	nodes := []string{"n1", "n2", "n3"}

	for _, n := range nodes {
		if err := seed.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			tb.Fatalf("seed node %q: %v", n, err)
		}

		if err := seed.Nodes().SetConnectionStatus(ctx, n, apiv1.NodeTypeOnline); err != nil {
			tb.Fatalf("set node %q online: %v", n, err)
		}
	}

	if err := seed.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "pool"},
	}); err != nil {
		tb.Fatalf("seed rg: %v", err)
	}

	seedDefinition := func(name string, replicas []string) {
		if err := seed.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name, ResourceGroupName: "rg"}); err != nil {
			tb.Fatalf("seed rd %q: %v", name, err)
		}

		for _, n := range replicas {
			if err := seed.Resources().Create(ctx, &apiv1.Resource{Name: name, NodeName: n}); err != nil {
				tb.Fatalf("seed replica %q on %q: %v", name, n, err)
			}
		}
	}

	for i := range definitions {
		seedDefinition(fmt.Sprintf("pvc-settled-%03d", i), nodes)
	}

	if short {
		seedDefinition("pvc-short", nodes[:2])
	}

	// The satellites' report: every volume UpToDate.
	var all blockstoriov1alpha1.ResourceList
	if err := cli.List(ctx, &all); err != nil {
		tb.Fatalf("list seeded replicas: %v", err)
	}

	for i := range all.Items {
		res := &all.Items[i]
		res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DiskState: "UpToDate"}}

		if err := cli.Update(ctx, res); err != nil {
			tb.Fatalf("report %q UpToDate: %v", res.Name, err)
		}
	}

	reads := &apiServerReads{Reader: cli}

	return storek8s.NewWithAPIReader(cli, reads), reads
}

func scheduledPass(tb testing.TB, st store.Store) {
	tb.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		tb.Fatalf("blockstor: %v", err)
	}

	rec := &controllerpkg.RGRebalanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
		Store:  st,
	}

	if _, err := rec.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		tb.Fatalf("Reconcile: %v", err)
	}
}

// TestScheduledRebalanceOfASettledGroupReadsOnlyTheCache is the idle
// load seen on a test cluster with 180 definitions: every five minutes
// the scheduled pass read each DRBD definition from the API server
// three times and listed its replicas there twice, in a burst of a few
// hundred requests, although every definition already had its replicas
// and nothing was placed. LINSTOR's balance task reads its own memory
// and only adjusts a definition with fewer diskful replicas than the
// place count; a definition the cache shows at the count needs no read
// past it.
func TestScheduledRebalanceOfASettledGroupReadsOnlyTheCache(t *testing.T) {
	t.Parallel()

	st, reads := settledGroupStore(t, 5, false)
	reads.reset()

	scheduledPass(t, st)

	if gets, lists := reads.counts(); gets != 0 || lists != 0 {
		t.Fatalf("a pass over settled definitions sent %d GETs %v and %d LISTs %v to the API server; want none",
			gets, reads.gets, lists, reads.lists)
	}
}

// TestScheduledRebalanceStillConfirmsADefinitionItPlacesOn: a definition
// the cache shows short of its place count is still read past the cache
// before anything is placed on it, and it alone.
func TestScheduledRebalanceStillConfirmsADefinitionItPlacesOn(t *testing.T) {
	t.Parallel()

	st, reads := settledGroupStore(t, 5, true)
	reads.reset()

	scheduledPass(t, st)

	gets, _ := reads.counts()
	if gets == 0 {
		t.Fatal("the short definition was not read past the cache before the placer ran")
	}

	for _, get := range reads.gets {
		if get != "*v1alpha1.ResourceDefinition pvc-short" {
			t.Errorf("uncached read of something other than the short definition: %s", get)
		}
	}

	for _, list := range reads.lists {
		if list != "*v1alpha1.ResourceList spec.resourceDefinitionName=pvc-short" {
			t.Errorf("uncached list of something other than the short definition's replicas: %s", list)
		}
	}
}

// BenchmarkScheduledRebalanceAPIRequests reports the requests one
// scheduled pass sends to the API server for a group of settled
// definitions the size of the test cluster's DRBD ones.
func BenchmarkScheduledRebalanceAPIRequests(b *testing.B) {
	const definitions = 115

	st, reads := settledGroupStore(b, definitions, false)

	var gets, lists int

	b.ResetTimer()

	for range b.N {
		reads.reset()
		scheduledPass(b, st)

		g, l := reads.counts()
		gets += g
		lists += l
	}

	b.ReportMetric(float64(gets)/float64(b.N), "GET/pass")
	b.ReportMetric(float64(lists)/float64(b.N), "LIST/pass")
}
