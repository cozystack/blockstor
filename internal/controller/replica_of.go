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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// isReplicaOf reports whether res belongs to the definition stored as
// rdName, the CR name. The store writes a replica with the definition's CR
// name in spec, but a replica written or adopted before the store and
// linstor-migrate did that keeps the spelling it was given, Pvc-Mix say, so
// the two are compared the way the store addresses the definition.
func isReplicaOf(res *blockstoriov1alpha1.Resource, rdName string) bool {
	return k8s.Name(res.Spec.ResourceDefinitionName) == rdName
}

// sameDefinition reports whether two spellings of a definition's name, as
// two replicas or a replica and a snapshot carry them, name the same
// definition: the one the store keeps under their common folded name.
func sameDefinition(a, b string) bool {
	return k8s.Name(a) == k8s.Name(b)
}

// sameNode reports whether two spellings of a node's name refer to the same
// node. A replica stores its node's CR name, while a snapshot's node list,
// a storage pool and the wire carry the registered spelling, and a replica
// written or adopted before the store stored CR names may carry that
// spelling too. Both sides map to the node's CR name, which is how the
// store addresses it.
func sameNode(a, b string) bool {
	return k8s.SameName(a, b)
}

// getReplica reads the replica of definition rd on node into out, whichever
// spellings the caller and the stored row use. The store names the CR from
// the registered spellings, so the folded composite name finds it for every
// name inside RFC 1123; a name outside it hashes case-sensitively, and a
// replica written in another spelling is then found by its spec instead.
// It answers NotFound when there is no such replica.
func getReplica(ctx context.Context, c client.Reader, rd, node string, out *blockstoriov1alpha1.Resource) error {
	err := c.Get(ctx, client.ObjectKey{Name: k8s.Name(rd + "." + node)}, out)
	if err == nil || !apierrors.IsNotFound(err) {
		return err
	}

	var list blockstoriov1alpha1.ResourceList

	err = c.List(ctx, &list)
	if err != nil {
		return err
	}

	for i := range list.Items {
		item := &list.Items[i]
		if sameDefinition(item.Spec.ResourceDefinitionName, rd) && sameNode(item.Spec.NodeName, node) {
			*out = *item

			return nil
		}
	}

	return apierrors.NewNotFound(schema.GroupResource{Group: blockstoriov1alpha1.SchemeGroupVersion.Group, Resource: "resources"}, rd+"."+node)
}
