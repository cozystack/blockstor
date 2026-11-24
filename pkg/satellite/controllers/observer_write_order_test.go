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
	"fmt"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Reads hide the replicas of a satellite that just started until it
// has written their status again, and the pass after the dump writes
// every one of them. A KernelLoaded Condition written ahead of
// each status held every replica back by a write: on a node with ~140
// replicas they read Unknown for about ten seconds. The statuses of a
// pass go out first, the Conditions after.
func TestObserverWritesAPassStatusesBeforeItsConditions(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	const replicas = 24

	builder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&blockstoriov1alpha1.Resource{})

	for i := range replicas {
		rd := fmt.Sprintf("pvc-order-%02d", i)
		builder = builder.WithObjects(&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName(rd, "n1")},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: "n1"},
		})
	}

	var (
		mu     sync.Mutex
		writes []string
	)

	cli := interceptor.NewClient(builder.Build(), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			kind := "condition"
			if isObserverStatusApply(opts) {
				kind = "status"
			}

			mu.Lock()
			writes = append(writes, kind)
			mu.Unlock()

			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})

	fx := storage.NewFakeExec()
	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: "n1"}

	for i := range replicas {
		rd := fmt.Sprintf("pvc-order-%02d", i)

		for _, line := range []string{
			"exists resource name:" + rd + " role:Secondary suspended:no",
			"exists device name:" + rd + " volume:0 minor:1000 disk:Diskless client:no quorum:yes",
		} {
			ev, err := drbd.ParseEvent(line)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}

			o.ingestEvent(t.Context(), ev)
		}
	}

	o.flush(t.Context(), drbd.NewAdm(fx))

	mu.Lock()
	defer mu.Unlock()

	if len(writes) != 2*replicas {
		t.Fatalf("%d writes, want a status and a Condition for each of %d replicas: %v", len(writes), replicas, writes)
	}

	for i, kind := range writes[:replicas] {
		if kind != "status" {
			t.Fatalf("write %d of the pass is a %s, before every status was out: %v", i, kind, writes)
		}
	}

	for i := range replicas {
		var res blockstoriov1alpha1.Resource

		err := cli.Get(t.Context(), client.ObjectKey{Name: k8s.ResourceCRName(fmt.Sprintf("pvc-order-%02d", i), "n1")}, &res)
		if err != nil {
			t.Fatalf("get replica %d: %v", i, err)
		}

		if len(res.Status.Volumes) != 1 || res.Status.Volumes[0].DiskState != "Diskless" ||
			!meta.IsStatusConditionTrue(res.Status.Conditions, blockstoriov1alpha1.ConditionKernelLoaded) {
			t.Fatalf("replica %d not written in full: %+v", i, res.Status)
		}
	}
}
