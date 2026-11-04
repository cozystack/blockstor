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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The kernel knows a resource by its definition's CR name: a hash for
// My_Res, pvc-mix for an old row that keeps Pvc-Mix in spec, the name
// itself for a plain pvc-<uuid>. The observer writes onto the Resource CR
// the store created, named from the registered spellings.
func TestTheObserverWritesOntoTheReplicaTheStoreNamed(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		registered string
		specRD     string
		node       string
	}{
		{"My_Res", k8s.Name("My_Res"), "Node-1"},
		{"Pvc-Mix", "Pvc-Mix", "Node-1"},
		{"pvc-0b5d6c1e-4f3a-4e2b-9a1d-7c8e9f0a1b2c", "pvc-0b5d6c1e-4f3a-4e2b-9a1d-7c8e9f0a1b2c", "Node-1"},
		{"pvc-0b5d6c1e-4f3a-4e2b-9a1d-7c8e9f0a1b2c", "pvc-0b5d6c1e-4f3a-4e2b-9a1d-7c8e9f0a1b2c", "My_Node"},
	} {
		t.Run(tc.registered+"/"+tc.node, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("scheme: %v", err)
			}

			rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name(tc.registered)}}
			k8s.SetOriginalName(&rd.ObjectMeta, tc.registered)

			node := &blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name(tc.node)}}
			k8s.SetOriginalName(&node.ObjectMeta, tc.node)

			crName := k8s.ResourceCRName(tc.registered, tc.node)
			replica := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: crName},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: tc.specRD,
					NodeName:               node.Name,
					Props:                  map[string]string{"StorPoolName": "thin1"},
				},
			}

			cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd, node, replica).
				WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

			o := &ObserverRunnable{Client: cli, NodeName: node.Name}
			ctx := context.Background()
			kernel := k8s.Name(tc.specRD)

			if err := o.writeStatus(ctx, &observation{ResourceName: kernel, DrbdState: "UpToDate", Role: "Secondary"}); err != nil {
				t.Fatalf("writeStatus: %v", err)
			}

			if err := o.stampKernelLoaded(ctx, kernel, true); err != nil {
				t.Fatalf("stampKernelLoaded: %v", err)
			}

			if err := o.writeSkipDiskProp(ctx, kernel); err != nil {
				t.Fatalf("writeSkipDiskProp: %v", err)
			}

			var got blockstoriov1alpha1.Resource
			if err := cli.Get(ctx, client.ObjectKey{Name: crName}, &got); err != nil {
				t.Fatalf("get %s: %v", crName, err)
			}

			if got.Status.Role != "Secondary" || got.Status.DrbdState != "UpToDate" {
				t.Errorf("status on %s = role %q state %q; want the observation", crName, got.Status.Role, got.Status.DrbdState)
			}

			if !meta.IsStatusConditionTrue(got.Status.Conditions, blockstoriov1alpha1.ConditionKernelLoaded) {
				t.Errorf("KernelLoaded on %s = %v; want True", crName, got.Status.Conditions)
			}

			if got.Spec.Props[skipDiskPropKey] != skipDiskPropValue {
				t.Errorf("SkipDisk on %s = %q; want %q", crName, got.Spec.Props[skipDiskPropKey], skipDiskPropValue)
			}

			var all blockstoriov1alpha1.ResourceList
			if err := cli.List(ctx, &all); err != nil || len(all.Items) != 1 {
				t.Errorf("resources %d, %v; want only the store's one", len(all.Items), err)
			}
		})
	}
}

func crNameScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	return scheme
}

// The SkipDisk clearer reaches the replica of a hashed definition the way
// the observer that stamped it does.
func TestTheSkipDiskClearerReachesAHashedDefinitionsReplica(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("My_Res")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "My_Res")

	crName := k8s.ResourceCRName("My_Res", "n1")
	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: crName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rd.Name,
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(crNameScheme(t)).WithObjects(rd, replica).Build()
	o := &ObserverRunnable{Client: cli, NodeName: "n1"}

	if err := o.writeSkipDiskProp(context.Background(), rd.Name); err != nil {
		t.Fatalf("stamp: %v", err)
	}

	var stamped blockstoriov1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: crName}, &stamped); err != nil || stamped.Spec.Props[skipDiskPropKey] != skipDiskPropValue {
		t.Fatalf("stamp did not land on %s: %v, %v", crName, stamped.Spec.Props, err)
	}

	if err := (&SkipDiskClearer{Client: cli, NodeName: "n1"}).ClearSkipDisk(context.Background(), rd.Name); err != nil {
		t.Fatalf("clear: %v", err)
	}

	var got blockstoriov1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: crName}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if _, ok := got.Spec.Props[skipDiskPropKey]; ok {
		t.Errorf("SkipDisk still on %s after the clear: %v", crName, got.Spec.Props)
	}
}

// The cross-node fetcher finds a hashed definition's snapshot by the name
// the store gave it, from the definition's CR name the restore carries.
func TestTheSnapshotFetcherFindsAHashedDefinitionsSnapshot(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("My_Res")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "My_Res")

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.SnapshotCRName("My_Res", "snap-1")},
		Spec:       blockstoriov1alpha1.SnapshotSpec{ResourceDefinitionName: rd.Name, SnapshotName: "snap-1"},
	}

	cli := fake.NewClientBuilder().WithScheme(crNameScheme(t)).WithObjects(rd, snap).Build()

	_, _, err := (&SnapshotFetcher{Client: cli, NodeName: "n1"}).Fetch(context.Background(), rd.Name, "snap-1", 0)
	if err == nil || !strings.Contains(err.Error(), "empty Spec.Nodes") {
		t.Errorf("fetch = %v; want it to reach the snapshot, which names no peers", err)
	}
}

// A lifecycle change on a hashed definition wakes the reconcile of the
// replica the store named.
func TestTheObserverWakesAHashedDefinitionsReplica(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("My_Res")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "My_Res")

	trigger := make(chan event.GenericEvent, 1)
	o := &ObserverRunnable{
		Client:           fake.NewClientBuilder().WithScheme(crNameScheme(t)).WithObjects(rd).Build(),
		NodeName:         "n1",
		ReconcileTrigger: trigger,
	}

	o.emitReconcileTrigger(context.Background(), &observation{ResourceName: rd.Name})

	select {
	case ev := <-trigger:
		if want := k8s.ResourceCRName("My_Res", "n1"); ev.Object.GetName() != want {
			t.Errorf("woke %q, want %q", ev.Object.GetName(), want)
		}
	default:
		t.Fatal("no reconcile trigger")
	}
}

// The MetadataCreated and FilesystemFormatted stampers land on the replica
// the store named, from the CR names the apply chain knows it by.
func TestTheConditionStampersReachAHashedDefinitionsReplica(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("My_Res")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "My_Res")

	node := &blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("My_Node")}}
	k8s.SetOriginalName(&node.ObjectMeta, "My_Node")

	crName := k8s.ResourceCRName("My_Res", "My_Node")
	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: crName},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd.Name, NodeName: node.Name},
	}

	for cond, stamp := range map[string]func(client.Client) error{
		blockstoriov1alpha1.ConditionMetadataCreated: func(c client.Client) error {
			return (&MetadataCreatedStamper{Client: c}).StampMetadataCreated(context.Background(), rd.Name, node.Name)
		},
		blockstoriov1alpha1.ConditionFilesystemFormatted: func(c client.Client) error {
			return (&FilesystemFormattedStamper{Client: c}).StampFilesystemFormatted(context.Background(), rd.Name, node.Name)
		},
	} {
		cli := fake.NewClientBuilder().WithScheme(crNameScheme(t)).WithObjects(rd.DeepCopy(), node.DeepCopy(), replica.DeepCopy()).
			WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

		if err := stamp(cli); err != nil {
			t.Fatalf("%s: %v", cond, err)
		}

		var got blockstoriov1alpha1.Resource
		if err := cli.Get(context.Background(), client.ObjectKey{Name: crName}, &got); err != nil {
			t.Fatalf("get: %v", err)
		}

		if !meta.IsStatusConditionTrue(got.Status.Conditions, cond) {
			t.Errorf("%s on %s = %v; want True", cond, crName, got.Status.Conditions)
		}
	}
}
