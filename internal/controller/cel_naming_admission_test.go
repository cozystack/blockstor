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

package controller

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// These tests pin the CRD-level CEL validation that enforces the
// cluster-wide `<resource>.<node>` naming convention on every node-
// bound CRD. The rule lives on the type (see
// `api/v1alpha1/{storagepool_types,resource_types}.go`) and is
// rendered into the CRD's `x-kubernetes-validations` block by
// controller-gen. A regression that drops the marker (or flips the
// rule's order) would let mis-named CRDs land — operators could no
// longer grep for `<node>.` to find every resource bound to one
// satellite, and the store's `crdName` helper would silently
// disagree with the wire-side composite key.
var _ = Describe("CEL naming-convention validation", func() {
	ctx := context.Background()

	cleanupSP := func(name string) {
		sp := &blockstoriov1alpha1.StoragePool{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		}
		_ = k8sClient.Delete(ctx, sp)
	}

	cleanupResource := func(name string) {
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		}
		_ = k8sClient.Delete(ctx, res)
	}

	Context("StoragePool.metadata.name", func() {
		It("accepts a canonical <pool>.<node> name", func() {
			name := "zfs-thin.w1"

			defer cleanupSP(name)

			sp := &blockstoriov1alpha1.StoragePool{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.StoragePoolSpec{
					NodeName:     "w1",
					PoolName:     "zfs-thin",
					ProviderKind: "ZFS_THIN",
				},
			}

			Expect(k8sClient.Create(ctx, sp)).To(Succeed())
		})

		It("rejects a name that doesn't match <pool>.<node>", func() {
			name := "wrong-name"

			defer cleanupSP(name)

			sp := &blockstoriov1alpha1.StoragePool{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.StoragePoolSpec{
					NodeName:     "w1",
					PoolName:     "zfs-thin",
					ProviderKind: "ZFS_THIN",
				},
			}

			err := k8sClient.Create(ctx, sp)
			Expect(err).To(HaveOccurred())
			Expect(strings.Contains(err.Error(), "metadata.name must equal")).To(BeTrue(),
				"expected CEL message, got: %s", err.Error())
		})

		It("rejects a flipped <node>.<pool> name", func() {
			name := "w1.zfs-thin"

			defer cleanupSP(name)

			sp := &blockstoriov1alpha1.StoragePool{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.StoragePoolSpec{
					NodeName:     "w1",
					PoolName:     "zfs-thin",
					ProviderKind: "ZFS_THIN",
				},
			}

			Expect(k8sClient.Create(ctx, sp)).To(HaveOccurred())
		})
	})

	Context("Resource.metadata.name", func() {
		It("accepts a canonical <rd>.<node> name", func() {
			name := "pvc-cel-ok.w1"

			defer cleanupResource(name)

			res := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "pvc-cel-ok",
					NodeName:               "w1",
				},
			}

			Expect(k8sClient.Create(ctx, res)).To(Succeed())
		})

		It("rejects a name that doesn't match <rd>.<node>", func() {
			name := "wrong-resource-name"

			defer cleanupResource(name)

			res := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "pvc-cel-bad",
					NodeName:               "w1",
				},
			}

			err := k8sClient.Create(ctx, res)
			Expect(err).To(HaveOccurred())
			Expect(strings.Contains(err.Error(), "metadata.name must equal")).To(BeTrue(),
				"expected CEL message, got: %s", err.Error())
		})
	})
})
