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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug-021 (release-gate, P1) — the `blockstor.io/rebalance-pending`
// RG annotation could never be removed in production. The strip
// helpers nil-ed the wire annotation map once it became empty, but
// the k8s store's mergeUserAnnotationsInto treats a NIL wire map as
// "annotations untouched" (deliberately, to protect reconciler-
// stamped keys), so the deletion never reached the CRD and the
// rebalance pass re-fired on every event — observed as the
// annotation persisting >90s with constant placer churn. The
// InMemory store replaced rows wholesale, which is why every unit
// test stayed green.
//
// These specs run the REAL strip paths against the REAL k8s store
// over envtest (the exact production wiring: reconciler → wire
// strip → Store.Update → mergeUserAnnotationsInto → apiserver) and
// assert the marker is gone FROM THE CRD. Pre-fix, both specs fail
// with the marker still present.
var _ = Describe("RGRebalance annotation strip through the k8s store (Bug-021)", func() {
	ctx := context.Background()

	newRebalanceReconciler := func() *RGRebalanceReconciler {
		return &RGRebalanceReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Store:  storek8s.New(k8sClient),
		}
	}

	It("removes a lone rebalance-pending annotation from the RG CRD via Reconcile", func() {
		const rgName = "rg-bug021-strip-lone"

		rg := &blockstoriov1alpha1.ResourceGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: rgName,
				// The bug shape: the marker is the ONLY annotation, so
				// the strip leaves an EMPTY map behind. Pre-fix the
				// helper nil-ed it and the Update became a silent no-op.
				Annotations: map[string]string{
					apiv1.AnnotationRGRebalancePending: "2026-06-12T00:00:00Z",
				},
			},
		}
		Expect(k8sClient.Create(ctx, rg)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, rg)
		})

		_, err := newRebalanceReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: rgName},
		})
		Expect(err).NotTo(HaveOccurred())

		var got blockstoriov1alpha1.ResourceGroup
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rgName}, &got)).To(Succeed())
		Expect(got.Annotations).NotTo(HaveKey(apiv1.AnnotationRGRebalancePending),
			"rebalance-pending must be stripped from the CRD after the pass (Bug-021)")
	})

	It("strips only the rebalance-pending key and keeps sibling annotations", func() {
		const rgName = "rg-bug021-strip-sibling"

		rg := &blockstoriov1alpha1.ResourceGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: rgName,
				Annotations: map[string]string{
					apiv1.AnnotationRGRebalancePending: "2026-06-12T00:00:00Z",
					"aux/operator-note":                "keep-me",
				},
			},
		}
		Expect(k8sClient.Create(ctx, rg)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, rg)
		})

		_, err := newRebalanceReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: rgName},
		})
		Expect(err).NotTo(HaveOccurred())

		var got blockstoriov1alpha1.ResourceGroup
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rgName}, &got)).To(Succeed())
		Expect(got.Annotations).NotTo(HaveKey(apiv1.AnnotationRGRebalancePending))
		Expect(got.Annotations).To(HaveKeyWithValue("aux/operator-note", "keep-me"),
			"the strip must not clobber unrelated user annotations")
	})

	It("removes a lone spawn-shortfall annotation from the RD CRD", func() {
		const rdName = "rd-bug021-strip-shortfall"

		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name: rdName,
				Annotations: map[string]string{
					apiv1.RDSpawnShortfallAnnotation: "2026-06-12T00:00:00Z",
				},
			},
		}
		Expect(k8sClient.Create(ctx, rd)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, rd)
		})

		rec := newRebalanceReconciler()

		// Same strip-then-Update shape replaySpawnShortfalls drives
		// after a successful placer top-up: fetch the wire RD, strip
		// the marker, persist through the store.
		wire, err := rec.Store.ResourceDefinitions().Get(ctx, rdName)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.stripShortfallAnnotation(ctx, &wire)).To(Succeed())

		var got blockstoriov1alpha1.ResourceDefinition
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName}, &got)).To(Succeed())
		Expect(got.Annotations).NotTo(HaveKey(apiv1.RDSpawnShortfallAnnotation),
			"spawn-shortfall must be stripped from the CRD (Bug-021, same class)")
	})
})
