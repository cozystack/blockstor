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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug 204a node-connection witness. The same wholesale `Update(...)`
// stale-snapshot class as the ExtraProps witness, but on the
// `Spec.NodeConnections` map-of-maps that backs upstream
// `/v1/node-connections/{src}/{dst}` (Bug 101).
//
// 100 goroutines each append a unique pair-id with one key/value;
// the helper must funnel them all through optimistic-concurrency
// Patch with the Bug 201 backoff so every pair lands. Without the
// helper, the wholesale-Update wire-replace path drops some entries
// because every goroutine reads the same baseline `NodeConnections`
// map and overwrites whatever a sibling already inserted.

// TestBug204aNodeConnectionsConcurrentWritesAllPersist bursts 100
// disjoint pair inserts against the singleton ControllerConfig CRD.
// Each goroutine adds one pair-id `pair-i` with `{prop: v-i}`. All
// 100 pair-ids must be present after the burst.
func TestBug204aNodeConnectionsConcurrentWritesAllPersist(t *testing.T) {
	t.Parallel()

	const burst = 100

	cli := bug204aNewFakeClient(t)
	seedControllerConfig(t, cli)

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			pair := fmt.Sprintf("pair-%d", idx)
			val := fmt.Sprintf("v-%d", idx)

			err := k8s.PatchControllerNodeConnections(t.Context(), cli,
				func(nc map[string]map[string]string) error {
					if _, ok := nc[pair]; !ok {
						nc[pair] = map[string]string{}
					}

					nc[pair]["prop"] = val

					return nil
				},
			)
			if err != nil {
				errCount.Add(1)

				t.Errorf("patch %d: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("unexpected errors: %d", errCount.Load())
	}

	var got crdv1alpha1.ControllerConfig
	if err := cli.Get(t.Context(),
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&got,
	); err != nil {
		t.Fatalf("final Get: %v", err)
	}

	for i := range burst {
		pair := fmt.Sprintf("pair-%d", i)
		want := fmt.Sprintf("v-%d", i)

		props, ok := got.Spec.NodeConnections[pair]
		if !ok {
			t.Errorf("lost-update: pair %q missing entirely", pair)

			continue
		}

		if props["prop"] != want {
			t.Errorf("lost-update: NodeConnections[%q][prop] got %q want %q", pair, props["prop"], want)
		}
	}
}

// TestBug204aNodeConnectionsAutoCreatesSingleton exercises the
// "ControllerConfig CRD doesn't exist yet" branch — matching the
// upstream `applyNodeConnectionProps` auto-create branch — so a
// fresh cluster doesn't need `kubectl apply` of ControllerConfig
// before `linstor node-connection set-property` works.
func TestBug204aNodeConnectionsAutoCreatesSingleton(t *testing.T) {
	t.Parallel()

	cli := bug204aNewFakeClient(t)

	err := k8s.PatchControllerNodeConnections(t.Context(), cli,
		func(nc map[string]map[string]string) error {
			nc["a::b"] = map[string]string{"foo": "bar"}

			return nil
		},
	)
	if err != nil {
		t.Fatalf("PatchControllerNodeConnections: %v", err)
	}

	var got crdv1alpha1.ControllerConfig
	if err := cli.Get(t.Context(),
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&got,
	); err != nil {
		t.Fatalf("Get after auto-create: %v", err)
	}

	props, ok := got.Spec.NodeConnections["a::b"]
	if !ok {
		t.Fatalf("auto-create lost pair a::b")
	}

	if props["foo"] != "bar" {
		t.Errorf("auto-create lost value: got %q want %q", props["foo"], "bar")
	}
}

// TestBug204aMixedExtraPropsAndNodeConnectionsConverge bursts the
// two helpers against the same singleton — proving that an
// ExtraProps mutation and a NodeConnections mutation racing each
// other do NOT clobber one another, which is the cross-field
// lost-update class Bug 204a explicitly targets (the two REST call
// paths racing).
func TestBug204aMixedExtraPropsAndNodeConnectionsConverge(t *testing.T) {
	t.Parallel()

	const burst = 50

	cli := bug204aNewFakeClient(t)
	seedControllerConfig(t, cli)

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(2)

		// ExtraProps writer.
		go func(idx int) {
			defer wg.Done()

			err := k8s.PatchControllerExtraProps(t.Context(), cli, func(props map[string]string) error {
				props[fmt.Sprintf("ek-%d", idx)] = fmt.Sprintf("ev-%d", idx)

				return nil
			})
			if err != nil {
				errCount.Add(1)

				t.Errorf("ep set %d: %v", idx, err)
			}
		}(i)

		// NodeConnections writer.
		go func(idx int) {
			defer wg.Done()

			pair := fmt.Sprintf("nc-pair-%d", idx)

			err := k8s.PatchControllerNodeConnections(t.Context(), cli,
				func(nc map[string]map[string]string) error {
					nc[pair] = map[string]string{"prop": fmt.Sprintf("nv-%d", idx)}

					return nil
				},
			)
			if err != nil {
				errCount.Add(1)

				t.Errorf("nc set %d: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("unexpected errors: %d", errCount.Load())
	}

	var got crdv1alpha1.ControllerConfig
	if err := cli.Get(t.Context(),
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&got,
	); err != nil {
		t.Fatalf("final Get: %v", err)
	}

	for i := range burst {
		ek := fmt.Sprintf("ek-%d", i)
		ev := fmt.Sprintf("ev-%d", i)

		if got.Spec.ExtraProps[ek] != ev {
			t.Errorf("lost-update: ExtraProps[%q] got %q want %q", ek, got.Spec.ExtraProps[ek], ev)
		}

		pair := fmt.Sprintf("nc-pair-%d", i)
		nv := fmt.Sprintf("nv-%d", i)

		props, ok := got.Spec.NodeConnections[pair]
		if !ok {
			t.Errorf("lost-update: NodeConnections pair %q missing", pair)

			continue
		}

		if props["prop"] != nv {
			t.Errorf("lost-update: NodeConnections[%q][prop] got %q want %q", pair, props["prop"], nv)
		}
	}
}
