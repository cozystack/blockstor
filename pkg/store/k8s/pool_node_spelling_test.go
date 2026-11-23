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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A pool keeps the node spelling it was created with, node-1, and a
// replica's pool lookup may ask under Node-1, or the other way round. The
// store finds the pool under either.
func TestStoragePoolIsFoundUnderAnotherSpellingOfItsNode(t *testing.T) {
	t.Parallel()

	for created, asked := range map[string]string{"node-1": "Node-1", "Node-1": "node-1"} {
		t.Run(created, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			if err := crdv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("AddToScheme: %v", err)
			}

			pool := &crdv1alpha1.StoragePool{
				ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("data." + created)},
				Spec:       crdv1alpha1.StoragePoolSpec{NodeName: created, PoolName: "data", ProviderKind: "LVM"},
			}

			st := k8s.New(fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build())

			got, err := st.StoragePools().Get(t.Context(), asked, "data")
			if err != nil {
				t.Fatalf("Get(%s, data) for a pool created on %s: %v", asked, created, err)
			}

			if got.StoragePoolName != "data" {
				t.Errorf("Get(%s, data) = %+v", asked, got)
			}
		})
	}
}
