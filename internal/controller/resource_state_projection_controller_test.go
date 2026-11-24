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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A satellite that comes back after a node reset starts writing its
// replicas' state seconds before the heartbeat watchdog flips the node
// ONLINE. The satellite applies Status.DrbdState without ForceOwnership,
// so while the controller held that field for an Unknown of its own,
// every one of those writes conflicted and the replicas kept reading
// Unknown until the node turned ONLINE and the claim was released.
var _ = Describe("Resource state projection release", func() {
	const (
		nodeName = "proj-node"
		rdName   = "proj-rd"
		resName  = rdName + "." + nodeName
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resName}

	applyState := func(owner, state string, opts ...client.SubResourcePatchOption) error {
		apply := &blockstoriov1alpha1.Resource{
			TypeMeta:   metav1.TypeMeta{Kind: "Resource", APIVersion: blockstoriov1alpha1.GroupVersion.String()},
			ObjectMeta: metav1.ObjectMeta{Name: resName},
			Status:     blockstoriov1alpha1.ResourceStatus{DrbdState: state},
		}

		opts = append(opts, client.FieldOwner(owner))

		return k8sClient.Status().Patch(ctx, apply,
			client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
			opts...)
	}

	reconcileReplica := func() {
		r := &ResourceStateProjectionReleaseReconciler{Client: k8sClient}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		node := &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName},
			Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())

		node.Status.ConnectionStatus = blockstoriov1alpha1.NodeConnectionStatusOffline
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: resName},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rdName, NodeName: nodeName},
		}
		Expect(k8sClient.Create(ctx, res)).To(Succeed())
	})

	AfterEach(func() {
		for _, obj := range []client.Object{
			&blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: resName}},
			&blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}},
		} {
			err := k8sClient.Delete(ctx, obj)
			if err != nil && !apierrors.IsNotFound(err) {
				Fail(err.Error())
			}
		}
	})

	It("leaves the satellite's state alone on a replica of an OFFLINE node", func() {
		Expect(applyState(k8s.SatelliteFieldOwner, "UpToDate")).To(Succeed())

		var before blockstoriov1alpha1.Resource
		Expect(k8sClient.Get(ctx, key, &before)).To(Succeed())

		reconcileReplica()

		var after blockstoriov1alpha1.Resource
		Expect(k8sClient.Get(ctx, key, &after)).To(Succeed())
		Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))
		Expect(after.Status.DrbdState).To(Equal("UpToDate"))

		By("the returning satellite writing before the node reads ONLINE")
		Expect(applyState(k8s.SatelliteFieldOwner, "Outdated")).To(Succeed())
	})

	It("releases an earlier controller's Unknown while the node is still OFFLINE", func() {
		Expect(applyState(k8s.SatelliteFieldOwner, "UpToDate")).To(Succeed())
		Expect(applyState(ResourceStateProjectionFieldOwner, "Unknown", client.ForceOwnership)).To(Succeed())

		By("the claim taken before the upgrade conflicting with the satellite")
		err := applyState(k8s.SatelliteFieldOwner, "UpToDate")
		Expect(apierrors.IsConflict(err)).To(BeTrue(), "want a conflict, got %v", err)

		reconcileReplica()

		var after blockstoriov1alpha1.Resource
		Expect(k8sClient.Get(ctx, key, &after)).To(Succeed())
		Expect(holdsProjectionClaim(&after)).To(BeFalse(), "managedFields: %+v", after.ManagedFields)
		Expect(after.Status.DrbdState).To(BeEmpty())

		Expect(applyState(k8s.SatelliteFieldOwner, "UpToDate")).To(Succeed())

		By("a second pass finding nothing left to release")
		Expect(k8sClient.Get(ctx, key, &after)).To(Succeed())
		reconcileReplica()

		var again blockstoriov1alpha1.Resource
		Expect(k8sClient.Get(ctx, key, &again)).To(Succeed())
		Expect(again.ResourceVersion).To(Equal(after.ResourceVersion))
	})
})
