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

// These tests pin the apiserver-side admission validation we get for
// free from the kubebuilder `+kubebuilder:validation:Enum=...` markers
// on api/v1alpha1/drbd_options.go. The tests submit garbage values
// against a live envtest apiserver; the apiserver REJECTS them at
// CREATE time, before the controller ever sees the object. Phase
// 10.3 step 6.
//
// A regression that drops the markers (or a typo that breaks the
// enum string format) would silently let invalid configuration land
// in the CRD store, where the satellite would parse it at runtime
// and either render a broken .res or surface a confusing
// drbdadm error.
var _ = Describe("DRBDOptions admission validation", func() {
	ctx := context.Background()

	rdName := func(suffix string) string {
		return "rd-admission-" + suffix
	}

	cleanup := func(name string) {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		}
		_ = k8sClient.Delete(ctx, rd)
	}

	Context("Net.Protocol enum (A;B;C)", func() {
		It("accepts a valid value", func() {
			name := rdName("net-proto-valid")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
						Net: &blockstoriov1alpha1.DRBDNetOptions{Protocol: "C"},
					},
				},
			}

			Expect(k8sClient.Create(ctx, rd)).To(Succeed())
		})

		It("rejects an out-of-enum value", func() {
			name := rdName("net-proto-bad")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
						Net: &blockstoriov1alpha1.DRBDNetOptions{Protocol: "Z"},
					},
				},
			}

			err := k8sClient.Create(ctx, rd)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("protocol"))
		})
	})

	Context("Disk.OnIOError enum (detach;pass-on;call-local-io-error)", func() {
		It("accepts the production-safe value", func() {
			name := rdName("disk-io-valid")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
						Disk: &blockstoriov1alpha1.DRBDDiskOptions{OnIOError: "detach"},
					},
				},
			}

			Expect(k8sClient.Create(ctx, rd)).To(Succeed())
		})

		It("rejects an out-of-enum value", func() {
			name := rdName("disk-io-bad")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
						Disk: &blockstoriov1alpha1.DRBDDiskOptions{OnIOError: "panic"},
					},
				},
			}

			err := k8sClient.Create(ctx, rd)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("onioerror"))
		})
	})

	Context("Resource.Quorum enum (off;majority;all)", func() {
		It("accepts majority", func() {
			name := rdName("res-quorum-valid")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
						Resource: &blockstoriov1alpha1.DRBDResourceOptions{Quorum: "majority"},
					},
				},
			}

			Expect(k8sClient.Create(ctx, rd)).To(Succeed())
		})

		It("rejects an out-of-enum value", func() {
			name := rdName("res-quorum-bad")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
						Resource: &blockstoriov1alpha1.DRBDResourceOptions{Quorum: "yes-plz"},
					},
				},
			}

			err := k8sClient.Create(ctx, rd)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("quorum"))
		})
	})

	Context("Net.AfterSb0Pri enum", func() {
		It("rejects an out-of-enum value", func() {
			name := rdName("sb0-bad")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
						Net: &blockstoriov1alpha1.DRBDNetOptions{AfterSb0Pri: "make-stuff-up"},
					},
				},
			}

			err := k8sClient.Create(ctx, rd)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("aftersb0pri"))
		})
	})

	Context("nil DRBDOptions is allowed", func() {
		It("accepts an RD with no DRBDOptions", func() {
			name := rdName("nil-drbd")

			defer cleanup(name)

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{},
			}

			Expect(k8sClient.Create(ctx, rd)).To(Succeed())
		})
	})
})
