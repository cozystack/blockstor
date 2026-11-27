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

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

var _ = Describe("volume definition delete", func() {
	const rdName = "pvc-vdd-envtest"

	var (
		st  store.Store
		rec *ResourceDefinitionReconciler
	)

	replica := func(node string) *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rdName, NodeName: node, StoragePool: "thin"},
		}
	}

	confirm := func(node, deleteID string) {
		var res blockstoriov1alpha1.Resource
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName + "." + node}, &res)).To(Succeed())

		res.Status.DeletedVolumes = []blockstoriov1alpha1.DeletedVolumeStatus{{VolumeNumber: 1, DeleteID: deleteID}}
		Expect(k8sClient.Status().Update(ctx, &res)).To(Succeed())
	}

	volumeNumbers := func() []int32 {
		var rd blockstoriov1alpha1.ResourceDefinition
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName}, &rd)).To(Succeed())

		out := make([]int32, 0, len(rd.Spec.VolumeDefinitions))
		for _, vd := range rd.Spec.VolumeDefinitions {
			out = append(out, vd.VolumeNumber)
		}

		return out
	}

	reconcile := func() {
		_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rdName}})
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		st = k8s.New(k8sClient)
		rec = &ResourceDefinitionReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Store: st, APIReader: k8sClient}

		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: rdName},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				LayerStack: []string{"STORAGE"},
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{VolumeNumber: 0, SizeKib: 65536},
					{VolumeNumber: 1, SizeKib: 65536},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rd)).To(Succeed())
		Expect(k8sClient.Create(ctx, replica("node-a"))).To(Succeed())
		Expect(k8sClient.Create(ctx, replica("node-b"))).To(Succeed())
	})

	AfterEach(func() {
		for _, node := range []string{"node-a", "node-b"} {
			res := replica(node)
			if k8sClient.Get(ctx, client.ObjectKeyFromObject(res), res) == nil {
				res.Finalizers = nil
				_ = k8sClient.Update(ctx, res)
				_ = k8sClient.Delete(ctx, res)
			}
		}

		_ = k8sClient.Delete(ctx, &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: rdName}})
		Eventually(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{Name: rdName}, &blockstoriov1alpha1.ResourceDefinition{})

			return apierrors.IsNotFound(err)
		}).Should(BeTrue())
	})

	It("removes the definition only once every replica confirmed under its deleteID", func() {
		flagged, marked, err := store.MarkVolumeDefinitionDeleted(ctx, st, rdName, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(marked).To(BeTrue())
		Expect(flagged.DeleteID).NotTo(BeEmpty())

		reconcile()
		Expect(volumeNumbers()).To(Equal([]int32{0, 1}), "no replica confirmed yet")

		confirm("node-a", flagged.DeleteID)
		confirm("node-b", "an-earlier-delete")
		reconcile()
		Expect(volumeNumbers()).To(Equal([]int32{0, 1}), "node-b's confirmation is for another delete")

		confirm("node-b", flagged.DeleteID)
		reconcile()
		Expect(volumeNumbers()).To(Equal([]int32{0}), "every replica confirmed")
	})

	It("waits for a replica being deleted until it is gone", func() {
		flagged, _, err := store.MarkVolumeDefinitionDeleted(ctx, st, rdName, 1)
		Expect(err).NotTo(HaveOccurred())

		confirm("node-a", flagged.DeleteID)

		held := replica("node-b")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(held), held)).To(Succeed())
		held.Finalizers = []string{"blockstor.org/satellite-resource"}
		Expect(k8sClient.Update(ctx, held)).To(Succeed())
		Expect(k8sClient.Delete(ctx, held)).To(Succeed())

		reconcile()
		Expect(volumeNumbers()).To(Equal([]int32{0, 1}), "node-b's own delete has not taken its volume yet")

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(held), held)).To(Succeed())
		held.Finalizers = nil
		Expect(k8sClient.Update(ctx, held)).To(Succeed())

		Eventually(func() []int32 {
			reconcile()

			return volumeNumbers()
		}).Should(Equal([]int32{0}))
	})

	It("never clears the DELETE flag", func() {
		_, _, err := store.MarkVolumeDefinitionDeleted(ctx, st, rdName, 1)
		Expect(err).NotTo(HaveOccurred())

		var rd blockstoriov1alpha1.ResourceDefinition
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName}, &rd)).To(Succeed())

		rd.Spec.VolumeDefinitions[1].Flags = nil
		err = k8sClient.Update(ctx, &rd)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got %v", err)
		Expect(err.Error()).To(ContainSubstring("never cleared"))
	})

	It("does not remove a definition re-created under the number", func() {
		flagged, _, err := store.MarkVolumeDefinitionDeleted(ctx, st, rdName, 1)
		Expect(err).NotTo(HaveOccurred())

		err = st.VolumeDefinitions().RemoveDeleted(ctx, rdName, 1, "another-delete")
		Expect(err).To(MatchError(store.ErrVolumeDefinitionNotDeleting))
		Expect(volumeNumbers()).To(Equal([]int32{0, 1}))

		Expect(st.VolumeDefinitions().RemoveDeleted(ctx, rdName, 1, flagged.DeleteID)).To(Succeed())
		Expect(volumeNumbers()).To(Equal([]int32{0}))
	})
})
