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

package k8s_test

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A snapshot the replica's cache has not seen yet is still deleted: the
// delete finds it on the API server, and the patch that writes its mark
// reads past the cache too, so a cache miss is never reported as "already
// absent" with the snapshot left behind.
func TestDeletingASnapshotTheCacheHasNotSeenDeletesIt(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	apiServer := fake.NewClientBuilder().WithScheme(scheme).WithIndex(&crdv1alpha1.Snapshot{}, k8s.FieldSnapshotDefinitionName,
		func(o client.Object) []string {
			return []string{o.(*crdv1alpha1.Snapshot).Spec.ResourceDefinitionName}
		}).Build()

	if err := apiServer.Create(context.Background(), &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "src.snap1"},
		Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "src", SnapshotName: "snap1"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The cache: it has not seen the snapshot, so a Get answers NotFound.
	cached := interceptor.NewClient(apiServer, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*crdv1alpha1.Snapshot); ok {
				return apierrors.NewNotFound(schema.GroupResource{Resource: "snapshots"}, key.Name)
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})

	st := k8s.NewWithAPIReader(cached, apiServer)

	if err := store.DeleteSnapshot(context.Background(), st, "src", "snap1"); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	var left crdv1alpha1.SnapshotList
	if err := apiServer.List(context.Background(), &left); err != nil {
		t.Fatalf("list: %v", err)
	}

	if n := len(left.Items); n != 0 {
		t.Errorf("%d snapshot(s) remain after the delete", n)
	}
}
