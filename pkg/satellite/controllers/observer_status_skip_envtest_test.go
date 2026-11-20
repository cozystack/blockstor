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
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// countStatusPatches counts the observer's status applies.
func countStatusPatches(cli client.WithWatch, applies *atomic.Int64) client.Client {
	return interceptor.NewClient(cli, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			applies.Add(1)

			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// The resync re-applies what the apiserver already holds only when
// something else wrote the replica since. Re-sending every replica
// every 5 s took more than the client's 20 requests a second on a node
// with about a hundred replicas, so the frames that carry a running
// resync's progress queued behind it and `SyncTarget(x%)` trailed the
// kernel by several seconds.
func TestObserverResyncSkipsAReplicaNothingChanged(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	var applies atomic.Int64

	cli := countStatusPatches(base, &applies)
	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())
	o.handleObservation(ctx, adm, peerConnected("n2"))

	o.resyncOnce(ctx)

	before := applies.Load()

	for range 3 {
		o.resyncOnce(ctx)
	}

	if got := applies.Load() - before; got != 0 {
		t.Fatalf("resync re-applied an unchanged replica %d times", got)
	}

	// Someone else's write puts the replica back on the next pass, so
	// a clobber is still repaired within one resync.
	stamp := &blockstoriov1alpha1.Resource{
		TypeMeta:   metav1.TypeMeta{Kind: resourceKind, APIVersion: blockstoriov1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: blockstoriov1alpha1.ResourceStatus{Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{{
			VolumeNumber: 0, StoragePool: "thin1", DevicePath: "/dev/drbd1001",
		}}},
	}

	err := base.Status().Patch(ctx, stamp,
		client.Apply, //nolint:staticcheck // SA1019: same call the reconciler makes
		client.FieldOwner(volumeStatusFieldOwner))
	if err != nil {
		t.Fatalf("restamp: %v", err)
	}

	before = applies.Load()
	o.resyncOnce(ctx)

	if got := applies.Load() - before; got != 1 {
		t.Fatalf("resync after another writer applied %d times, want 1", got)
	}

	status := getStatusReplica(t, base, name)
	if status.Volumes[0].DiskState != "UpToDate" || len(status.Connections) != 1 {
		t.Fatalf("state lost: %+v", status)
	}
}
