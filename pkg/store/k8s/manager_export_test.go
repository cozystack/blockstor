// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"time"

	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// NewIndexedManagerWithin is NewManager's manager half, with the registration
// budget the test picks.
func NewIndexedManagerWithin(cfg *rest.Config, opts ctrl.Options, budget time.Duration) (ctrl.Manager, error) {
	return newIndexedManager(cfg, opts, budget)
}

// NodeScopedReadsBypassTheCache reports whether the store answers node-scoped
// reads from a direct reader rather than from its client.
func NodeScopedReadsBypassTheCache(s *Store) bool {
	return s.resources.apiReader != nil && s.storagePools.apiReader != nil
}

// ReplicaKeyIndexer is the extractor of the FieldResourceDefinitionKey
// index NewManager and RegisterReplicaKeyIndex register.
func ReplicaKeyIndexer(obj ctrlclient.Object) []string {
	return replicaKeyIndex().extract(obj)
}

// ForgetSelectorRefusal clears what ReplicasOf remembered of a server that
// refused the definition selector.
func ForgetSelectorRefusal() { serverRefusesSelector.Store(false) }
