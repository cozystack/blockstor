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
	"sigs.k8s.io/controller-runtime/pkg/client"

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

	// Bug 71: when a CRD ends up in a state where `metadata.name` no
	// longer matches the composite naming rule (e.g. created before
	// the CEL marker was added, or by an older satellite that picked
	// the wrong key), the satellite finalizer reconciler must still be
	// able to drive the deletion forward. Without an escape on the
	// rule the apiserver rejects every UPDATE on the stale object —
	// including the satellite's finalizer-strip — so the CRD becomes
	// immortal and blocks the entire cluster cleanup.
	//
	// The chosen escape is `optionalOldSelf: true` paired with
	// `oldSelf.hasValue() || ...`, which turns the rule into a
	// create-only check (`oldSelf` is unset only on Create). On Update
	// the rule short-circuits, so finalizer-strip on a name-violating
	// object goes through regardless of how the object got into that
	// state. metadata.name is k8s-immutable, so the invariant can only
	// be broken by a spec mutation — which is not a real-world
	// operation a controller would ever issue on its own resource.
	Context("CEL allows finalizer strip during deletion", func() {
		It("StoragePool: rule does not fire on update path (allows finalizer-strip on stale name)", func() {
			name := "bug71spdel.w1"
			defer cleanupSP(name)

			sp := &blockstoriov1alpha1.StoragePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:       name,
					Finalizers: []string{"blockstor.io.blockstor.io/satellite-storagepool"},
				},
				Spec: blockstoriov1alpha1.StoragePoolSpec{
					NodeName:     "w1",
					PoolName:     "bug71spdel",
					ProviderKind: "ZFS_THIN",
				},
			}
			Expect(k8sClient.Create(ctx, sp)).To(Succeed())

			Expect(k8sClient.Delete(ctx, sp)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, sp)).To(Succeed())
			Expect(sp.DeletionTimestamp).NotTo(BeNil(),
				"deletionTimestamp must be set after Delete with finalizer present")

			// Mutate the spec into a name-violating state AND strip the
			// finalizer in one update — simulating the move the satellite
			// reconciler issues on a stale-named pool. Without the fix
			// the apiserver rejects this with "metadata.name must equal".
			sp.Spec.NodeName = "w2"
			sp.Finalizers = nil
			Expect(k8sClient.Update(ctx, sp)).To(Succeed(),
				"create-only rule must let finalizer-strip through even on a name-violating SP")
		})

		It("Resource: rule does not fire on update path (allows finalizer-strip on stale name)", func() {
			name := "bug71resdel.w1"
			defer cleanupResource(name)

			res := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{
					Name:       name,
					Finalizers: []string{"blockstor.io.blockstor.io/satellite-resource"},
				},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "bug71resdel",
					NodeName:               "w1",
				},
			}
			Expect(k8sClient.Create(ctx, res)).To(Succeed())

			Expect(k8sClient.Delete(ctx, res)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, res)).To(Succeed())
			Expect(res.DeletionTimestamp).NotTo(BeNil())

			res.Spec.NodeName = "w2"
			res.Finalizers = nil
			Expect(k8sClient.Update(ctx, res)).To(Succeed(),
				"create-only rule must let finalizer-strip through even on a name-violating Resource")
		})
	})
})
