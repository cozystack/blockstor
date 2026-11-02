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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A write that lands between the patch's read and its update makes the
// update conflict, and the patch decides again on the fresh props instead
// of writing over them.
func TestPatchSnapshotPropsSeesAWriteInBetween(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "src.clone-dst"},
		Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "src", SnapshotName: "clone-dst"},
	}

	raced := false

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()
	cli := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if !raced {
				raced = true

				var live crdv1alpha1.Snapshot
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &live); err != nil {
					return err //nolint:wrapcheck // pass-through test double
				}

				live.Spec.Props = map[string]string{"Blockstor/CloneSnapshotReaping": "other-reap"}
				if err := c.Update(ctx, &live); err != nil {
					return err //nolint:wrapcheck // pass-through test double
				}
			}

			return c.Update(ctx, obj, opts...)
		},
	})

	var seen []string

	err := k8s.New(cli).Snapshots().PatchSnapshotProps(context.Background(), "src", "clone-dst",
		func(props map[string]string) error {
			seen = append(seen, props["Blockstor/CloneSnapshotReaping"])
			props["Blockstor/CloneSnapshotReaping"] = "mine"

			return nil
		})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	if len(seen) != 2 || seen[1] != "other-reap" {
		t.Errorf("mutate saw %q, want a second run that sees the other reap's write", seen)
	}
}
