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

package storetest

import (
	"context"
	"slices"
	"sync"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// ReplicaDelete is one replica delete a DeleteRecorder saw.
type ReplicaDelete struct {
	Node string
	// DefinitionFlagged reports whether the parent definition already
	// carried DELETE when the replica was deleted.
	DefinitionFlagged bool
}

// DeleteRecorder wraps a store and notes every replica delete in order,
// with whether its definition had been flagged DELETE by then. The `rd d`
// doors are tested with it: the flag has to be on the definition before
// the first replica goes.
type DeleteRecorder struct {
	store.Store

	mu      sync.Mutex
	deletes []ReplicaDelete
}

// NewDeleteRecorder wraps backend.
func NewDeleteRecorder(backend store.Store) *DeleteRecorder {
	return &DeleteRecorder{Store: backend}
}

// Resources returns the recording replica store.
func (r *DeleteRecorder) Resources() store.ResourceStore {
	return recordingResources{ResourceStore: r.Store.Resources(), rec: r}
}

// Deletes returns the replica deletes seen so far.
func (r *DeleteRecorder) Deletes() []ReplicaDelete {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.deletes)
}

type recordingResources struct {
	store.ResourceStore

	rec *DeleteRecorder
}

func (r recordingResources) Delete(ctx context.Context, rdName, node string) error {
	rd, err := r.rec.ResourceDefinitions().Get(ctx, rdName)

	r.rec.mu.Lock()
	r.rec.deletes = append(r.rec.deletes, ReplicaDelete{
		Node:              node,
		DefinitionFlagged: err == nil && slices.Contains(rd.Flags, apiv1.ResourceFlagDelete),
	})
	r.rec.mu.Unlock()

	return r.ResourceStore.Delete(ctx, rdName, node) //nolint:wrapcheck // pass-through test double
}
