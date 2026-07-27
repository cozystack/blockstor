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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Adopted snapshots (imported from LINSTOR by a migration tool) carry
// AnnotationSnapshotAdopted and an EMPTY Status. Before the adoption
// gate, Reconcile fed them into nextPhase, whose false/false branch —
// with no per-node acks recorded — decided the orchestration had not
// started yet and flipped Spec.SuspendIO=true: every diskful peer of
// the parent RD would freeze production I/O (drbdsetup suspend-io)
// just to re-take a snapshot that already exists on disk. The gate
// must instead backfill the terminal Ready state and leave the Spec
// flags alone. This spec FAILS on the pre-gate controller (SuspendIO
// observably flips true) and passes with the gate.
var _ = Describe("Snapshot Controller adopted-snapshot gate", func() {
	Context("When reconciling an adopted Snapshot with empty status", func() {
		const resourceName = "rd7.adopted1"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the adopted Snapshot")

			existing := &blockstoriov1alpha1.Snapshot{}

			err := k8sClient.Get(ctx, typeNamespacedName, existing)
			if err != nil && errors.IsNotFound(err) {
				resource := &blockstoriov1alpha1.Snapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
						Annotations: map[string]string{
							blockstoriov1alpha1.AnnotationSnapshotAdopted:          "true",
							blockstoriov1alpha1.AnnotationSnapshotAdoptedCreatedAt: "1770000000123",
						},
					},
					Spec: blockstoriov1alpha1.SnapshotSpec{
						ResourceDefinitionName: "rd7",
						SnapshotName:           "adopted1",
						Nodes:                  []string{"node-x", "node-y"},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &blockstoriov1alpha1.Snapshot{}

			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("must not start the suspend orchestration and must backfill Ready", func() {
			controllerReconciler := &SnapshotReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var snap blockstoriov1alpha1.Snapshot

			Expect(k8sClient.Get(ctx, typeNamespacedName, &snap)).To(Succeed())

			By("leaving the orchestration flags alone (no production I/O freeze)")
			Expect(snap.Spec.SuspendIO).To(BeFalse(),
				"adopted snapshot must never enter Phase 1 (suspend-io)")
			Expect(snap.Spec.TakeSnapshot).To(BeFalse(),
				"adopted snapshot must never be re-taken")

			By("backfilling the terminal per-node Ready state")
			Expect(snap.Status.NodeStatus).To(HaveLen(2))

			for _, entry := range snap.Status.NodeStatus {
				Expect(entry.Ready).To(BeTrue())
				Expect(entry.CreateTimestamp).To(Equal(int64(1770000000123)))
				Expect(entry.SuspendIOAcked).To(BeFalse())
			}

			By("staying terminal on a second reconcile (idempotent)")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &snap)).To(Succeed())
			Expect(snap.Spec.SuspendIO).To(BeFalse())
			Expect(snap.Status.NodeStatus).To(HaveLen(2))
		})
	})
})
