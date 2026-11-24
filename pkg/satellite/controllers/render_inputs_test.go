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

package controllers

import (
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

func renderInputsFixture(t *testing.T) *ResourceReconciler {
	t.Helper()

	rd := func(name, group string) *blockstoriov1alpha1.ResourceDefinition {
		return &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: group},
		}
	}
	res := func(rd, node string) *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: node},
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		// The group is spelled as the operator typed it on the RD.
		rd("pvc-a", "SC-Fast"), rd("pvc-b", "sc-slow"), rd("pvc-c", ""),
		res("pvc-a", "n1"), res("pvc-a", "n2"),
		res("pvc-b", "n1"), res("pvc-b", "n3"),
		res("pvc-c", "n1"),
		res("pvc-d", "n2"), res("pvc-d", "n3"),
	).Build()

	return &ResourceReconciler{Client: cli, Config: Config{NodeName: "n1"}}
}

func requestNames(reqs []reconcile.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, req.Name)
	}

	slices.Sort(out)

	return out
}

// TestRenderInputsReachTheReplicasThatRenderThem: a change of a group,
// the controller config, a node or a pool reaches the local replicas
// whose .res it goes into, and no other.
func TestRenderInputsReachTheReplicasThatRenderThem(t *testing.T) {
	r := renderInputsFixture(t)
	ctx := t.Context()

	cases := []struct {
		name string
		got  []reconcile.Request
		want []string
	}{
		{
			name: "group",
			got:  r.enqueueLocalForGroup(ctx, &blockstoriov1alpha1.ResourceGroup{ObjectMeta: metav1.ObjectMeta{Name: "sc-fast"}}),
			want: []string{"pvc-a.n1"},
		},
		{
			name: "group with no definition",
			got:  r.enqueueLocalForGroup(ctx, &blockstoriov1alpha1.ResourceGroup{ObjectMeta: metav1.ObjectMeta{Name: "unused"}}),
		},
		{
			name: "controller config",
			got:  r.enqueueAllLocal(ctx, &blockstoriov1alpha1.ControllerConfig{}),
			want: []string{"pvc-a.n1", "pvc-b.n1", "pvc-c.n1"},
		},
		{
			name: "peer node",
			got:  r.enqueueLocalForNodeObject(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n3"}}),
			want: []string{"pvc-b.n1"},
		},
		{
			name: "local node",
			got:  r.enqueueLocalForNodeObject(ctx, &blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}),
			want: []string{"pvc-a.n1", "pvc-b.n1", "pvc-c.n1"},
		},
		{
			name: "pool on a peer node",
			got: r.enqueueLocalForPool(ctx, &blockstoriov1alpha1.StoragePool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool.n2"},
				Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: "n2"},
			}),
			want: []string{"pvc-a.n1"},
		},
	}

	for _, tc := range cases {
		if got := requestNames(tc.got); !slices.Equal(got, tc.want) {
			t.Errorf("%s: enqueued %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestCoreNodeWatchTakesOnlyInternalIPChanges: the kubelet rewrites a
// node's status on its own; only an InternalIP change goes into a .res.
func TestCoreNodeWatchTakesOnlyInternalIPChanges(t *testing.T) {
	node := func(ip string, ready corev1.ConditionStatus) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "n2"},
			Status: corev1.NodeStatus{
				Addresses:  []corev1.NodeAddress{{Type: corev1.NodeHostName, Address: "n2"}, {Type: corev1.NodeInternalIP, Address: ip}},
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}},
			},
		}
	}

	pred := internalIPChanged()

	if pred.Update(event.UpdateEvent{ObjectOld: node("10.0.0.2", corev1.ConditionTrue), ObjectNew: node("10.0.0.2", corev1.ConditionFalse)}) {
		t.Error("a condition change passed")
	}

	if !pred.Update(event.UpdateEvent{ObjectOld: node("10.0.0.2", corev1.ConditionTrue), ObjectNew: node("10.0.0.9", corev1.ConditionTrue)}) {
		t.Error("an InternalIP change did not pass")
	}

	if pred.Create(event.CreateEvent{Object: node("10.0.0.2", corev1.ConditionTrue)}) {
		t.Error("a create passed")
	}
}

// TestGroupAndControllerOptionsReachConvergedReplicas: with no periodic
// pass, `linstor rg drbd-options` and `linstor controller drbd-options`
// must still reach the replicas already up, as LINSTOR pushes them to
// its satellites. A status write of the group starts nothing.
func TestGroupAndControllerOptionsReachConvergedReplicas(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local = "n1"
		peer  = "n2"
		pool  = "lvm-thin"
		group = "sc-fast"
		rd    = "pvc-grouped"
	)

	cli, mgr := startBringupEnvtest(t)

	for _, node := range []string{local, peer} {
		bringupCreate(t, cli, &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: node},
			Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		})
	}

	bringupCreate(t, cli, &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: pool + "." + local},
		Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: local, PoolName: pool, ProviderKind: "LVM_THIN"},
	})
	bringupCreate(t, cli, &blockstoriov1alpha1.ResourceGroup{ObjectMeta: metav1.ObjectMeta{Name: group}})
	bringupCreate(t, cli, &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rd},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: group,
			LayerStack:        []string{"DRBD", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1 << 20}},
		},
	})
	bringupReplica(t, cli, rd, local, pool, 0, 0)
	bringupReplica(t, cli, rd, peer, pool, 0, 1)

	reader := &concurrencyReader{Reader: mgr.GetAPIReader()}

	rec := &ResourceReconciler{
		Client: mgr.GetClient(),
		Config: Config{
			NodeName: local,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{pool: &noopProvider{}},
				NodeName:  local,
			}),
			Exec:      storage.NewFakeExec(),
			APIReader: reader,
		},
	}

	err := rec.SetupWithManager(mgr)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup

	wg.Go(func() {
		mgrErr := mgr.Start(ctx)
		if mgrErr != nil {
			t.Errorf("manager: %v", mgrErr)
		}
	})

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	bringupWaitFor(t, 30*time.Second, func() bool { return reader.passes.Load() >= 1 })
	time.Sleep(2 * time.Second)
	rec.passes.report()

	var rg blockstoriov1alpha1.ResourceGroup

	getGroup := func() {
		t.Helper()

		err := cli.Get(t.Context(), client.ObjectKey{Name: group}, &rg)
		if err != nil {
			t.Fatalf("get group: %v", err)
		}
	}

	getGroup()

	rg.Status.Conditions = []metav1.Condition{{
		Type: "Test", Status: metav1.ConditionTrue, Reason: "Test", LastTransitionTime: metav1.Now(),
	}}

	err = cli.Status().Update(t.Context(), &rg)
	if err != nil {
		t.Fatalf("write group status: %v", err)
	}

	time.Sleep(2 * time.Second)

	if n := rec.passes.passCount(passConfig); n != 0 {
		t.Fatalf("%d passes for a group status write", n)
	}

	getGroup()

	rg.Spec.DRBDOptions = &blockstoriov1alpha1.DRBDOptions{Net: &blockstoriov1alpha1.DRBDNetOptions{MaxBuffers: int32Ptr(8192)}}

	err = cli.Update(t.Context(), &rg)
	if err != nil {
		t.Fatalf("set group drbd options: %v", err)
	}

	bringupWaitFor(t, 10*time.Second, func() bool { return rec.passes.passCount(passConfig) > 0 })
	rec.passes.report()

	bringupCreate(t, cli, &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			DRBDOptions: &blockstoriov1alpha1.DRBDOptions{Net: &blockstoriov1alpha1.DRBDNetOptions{MaxBuffers: int32Ptr(4096)}},
		},
	})

	bringupWaitFor(t, 10*time.Second, func() bool { return rec.passes.passCount(passConfig) > 0 })
}
