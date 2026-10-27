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
	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// isReplicaOf reports whether res belongs to the definition stored as
// rdName, the CR name. A replica keeps its definition's name as it was
// spelled when it was written, Pvc-Mix say, while the CR is stored under
// the folded name, so the two are compared the way the store addresses
// the definition.
func isReplicaOf(res *blockstoriov1alpha1.Resource, rdName string) bool {
	return k8s.Name(res.Spec.ResourceDefinitionName) == rdName
}

// sameDefinition reports whether two spellings of a definition's name, as
// two replicas or a replica and a snapshot carry them, name the same
// definition: the one the store keeps under their common folded name.
func sameDefinition(a, b string) bool {
	return k8s.Name(a) == k8s.Name(b)
}
