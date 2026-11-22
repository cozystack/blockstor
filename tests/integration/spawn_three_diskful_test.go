// SPDX-License-Identifier: Apache-2.0

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

package integration

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// slowCreateStore makes each replica create cost delay, the API round
// trips a create takes on a real cluster. The placer writes a
// placement's replicas one at a time, so this is the gap in which the
// controller sees the replicas written so far.
type slowCreateStore struct {
	store.Store

	delay time.Duration
}

func (s slowCreateStore) Resources() store.ResourceStore {
	return slowCreateResources{ResourceStore: s.Store.Resources(), delay: s.delay}
}

type slowCreateResources struct {
	store.ResourceStore

	delay time.Duration
}

func (r slowCreateResources) Create(ctx context.Context, res *apiv1.Resource) error {
	time.Sleep(r.delay)

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // test double
}

// slowWholeClusterReader makes a Resource list without a field
// selector cost delay, a read of every Resource on a cluster with a few
// hundred replicas. A read scoped to one definition, and a list of the
// handful of nodes, stay at envtest speed.
type slowWholeClusterReader struct {
	client.Reader

	delay time.Duration
}

func (s slowWholeClusterReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := &client.ListOptions{}
	listOpts.ApplyOptions(opts)

	_, resources := list.(*blockstoriov1alpha1.ResourceList)
	if resources && (listOpts.FieldSelector == nil || listOpts.FieldSelector.Empty()) {
		time.Sleep(s.delay)
	}

	return s.Reader.List(ctx, list, opts...) //nolint:wrapcheck // test double
}

// A three-replica spawn comes up with three diskful replicas. The
// placer writes them one at a time; the controller's tiebreaker
// reconciler, reading only the definition's replicas, sees the first
// two and puts a TIE_BREAKER witness on the node the third is for
// before the placer creates it. The placer has to take that witness
// over rather than count it as its replica: on the test cluster such a
// spawn came up with a witness and was ready only minutes later. The
// latencies are those of that cluster's size: 100 ms a create, 400 ms
// a read of every Resource (SPAWN3_CREATE_MS, SPAWN3_WHOLE_READ_MS).
//
// Taking the witness over settles the count but not the peers: they
// set the witness up as a diskless peer, and when it came up diskful
// one of them refused the handshake and left the connection StandAlone.
// So no witness may appear during the placement at all, the way none
// does in LINSTOR, which decides on the witness once the placement is
// written.
func TestSpawnThreeDiskfulComesUpWithoutAWitness(t *testing.T) {
	s := startBurstStackWith(t, []string{"n1", "n2", "n3"}, burstLatency{
		create:           time.Duration(envInt("SPAWN3_CREATE_MS", 100)) * time.Millisecond,
		wholeClusterRead: time.Duration(envInt("SPAWN3_WHOLE_READ_MS", 400)) * time.Millisecond,
	})
	ctx := t.Context()

	for i := range envInt("SPAWN3_ROUNDS", 4) {
		name := fmt.Sprintf("spawn3-%02d", i)

		_, _, err := s.post(ctx, "/v1/resource-definitions", apiv1.ResourceDefinitionCreate{
			ResourceDefinition: apiv1.ResourceDefinition{Name: name, ResourceGroupName: burstRG},
		})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}

		_, _, err = s.post(ctx, "/v1/resource-definitions/"+name+"/volume-definitions", apiv1.VolumeDefinitionCreate{
			VolumeDefinition: apiv1.VolumeDefinition{SizeKib: burstVolKib},
		})
		if err != nil {
			t.Fatalf("volume of %s: %v", name, err)
		}

		witnesses := s.watchWitnesses(ctx, t, name)

		_, _, err = s.post(ctx, "/v1/resource-definitions/"+name+"/autoplace", apiv1.AutoPlaceRequest{
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: burstPool},
		})
		if err != nil {
			t.Fatalf("autoplace %s: %v", name, err)
		}

		var list blockstoriov1alpha1.ResourceList

		err = s.cli.List(ctx, &list)
		if err != nil {
			t.Fatalf("list: %v", err)
		}

		for j := range list.Items {
			res := &list.Items[j]
			if res.Spec.ResourceDefinitionName != name {
				continue
			}

			if slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) ||
				slices.Contains(res.Spec.Flags, apiv1.ResourceFlagTieBreaker) {
				t.Errorf("%s on %s: flags %v once the autoplace answered, want a diskful replica on every node",
					name, res.Spec.NodeName, res.Spec.Flags)
			}
		}

		err = s.waitReplicas(ctx, name, 3)
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}

		if nodes := witnesses(); len(nodes) > 0 {
			t.Errorf("%s: a TIE_BREAKER witness appeared on %v during the placement; the peers set it up "+
				"as a diskless peer and drop the connection when the placement turns it diskful", name, nodes)
		}
	}
}

// watchWitnesses records every node a TIE_BREAKER replica of rdName
// shows up on from now until the returned function is called, which
// stops the watch and returns them. A witness the placement took over
// is gone by the time the autoplace answers, so only a watch sees it.
func (s *burstStack) watchWitnesses(ctx context.Context, t *testing.T, rdName string) func() []string {
	t.Helper()

	w, err := s.watch.Watch(ctx, &blockstoriov1alpha1.ResourceList{})
	if err != nil {
		t.Fatalf("watch resources: %v", err)
	}

	var (
		mu    sync.Mutex
		nodes []string
		done  = make(chan struct{})
	)

	go func() {
		defer close(done)

		for ev := range w.ResultChan() {
			res, ok := ev.Object.(*blockstoriov1alpha1.Resource)
			if !ok || res.Spec.ResourceDefinitionName != rdName ||
				!slices.Contains(res.Spec.Flags, apiv1.ResourceFlagTieBreaker) {
				continue
			}

			mu.Lock()
			if !slices.Contains(nodes, res.Spec.NodeName) {
				nodes = append(nodes, res.Spec.NodeName)
			}
			mu.Unlock()
		}
	}()

	return func() []string {
		w.Stop()
		<-done

		mu.Lock()
		defer mu.Unlock()

		return nodes
	}
}
