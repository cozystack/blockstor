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

// Package store defines the persistence interface used by REST handlers.
//
// In Phase 1 we use an in-memory implementation under InMemory{} so endpoints
// can be wired and tested without bringing up Kubernetes. In Phase 2 a
// CRD-backed implementation lives next to it and the controller switches to
// it via flag. The interface is the seam.
package store

import (
	"context"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Sentinel errors. REST handlers map these to HTTP statuses (404, 409, …).
var (
	// ErrNotFound is returned when the requested object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrAlreadyExists is returned when creating an object that already exists.
	ErrAlreadyExists = errors.New("object already exists")
)

// NodeStore persists Node objects. Create/Update take pointers so callers
// don't pay for a copy of the (~100-byte) Node value through the interface
// boundary; the implementation must defensively copy if it stores a long
// reference.
type NodeStore interface {
	List(ctx context.Context) ([]apiv1.Node, error)
	Get(ctx context.Context, name string) (apiv1.Node, error)
	Create(ctx context.Context, n *apiv1.Node) error
	Update(ctx context.Context, n *apiv1.Node) error
	Delete(ctx context.Context, name string) error
}

// StoragePoolStore persists StoragePool objects. The composite key is
// (node name, pool name); the same pool name can co-exist on different nodes.
type StoragePoolStore interface {
	List(ctx context.Context) ([]apiv1.StoragePool, error)
	ListByNode(ctx context.Context, node string) ([]apiv1.StoragePool, error)
	Get(ctx context.Context, node, pool string) (apiv1.StoragePool, error)
	Create(ctx context.Context, sp *apiv1.StoragePool) error
	Update(ctx context.Context, sp *apiv1.StoragePool) error
	Delete(ctx context.Context, node, pool string) error
}

// ResourceGroupStore persists ResourceGroup objects. Keyed by name.
type ResourceGroupStore interface {
	List(ctx context.Context) ([]apiv1.ResourceGroup, error)
	Get(ctx context.Context, name string) (apiv1.ResourceGroup, error)
	Create(ctx context.Context, rg *apiv1.ResourceGroup) error
	Update(ctx context.Context, rg *apiv1.ResourceGroup) error
	Delete(ctx context.Context, name string) error
}

// Store aggregates per-resource stores. Phase 2 grows this interface as more
// CRDs land (ResourceDefinition, Snapshot, ...).
type Store interface {
	Nodes() NodeStore
	StoragePools() StoragePoolStore
	ResourceGroups() ResourceGroupStore
}
