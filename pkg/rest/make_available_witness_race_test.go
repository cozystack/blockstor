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

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// witnessRaceResources answers the first read of one replica with "not
// found" and puts a tiebreaker witness there in the same moment, the
// way the tiebreaker reconciler can between make-available's read and
// its create.
type witnessRaceResources struct {
	store.ResourceStore

	rdName, node string
	once         sync.Once
}

func (w *witnessRaceResources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	raced := false

	if rdName == w.rdName && node == w.node {
		w.once.Do(func() {
			raced = true
		})
	}

	if !raced {
		return w.ResourceStore.Get(ctx, rdName, node) //nolint:wrapcheck // test helper
	}

	err := w.ResourceStore.Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: node,
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	})
	if err != nil {
		return apiv1.Resource{}, err //nolint:wrapcheck // test helper
	}

	return apiv1.Resource{}, store.ErrNotFound
}

type witnessRaceStore struct {
	store.Store

	resources store.ResourceStore
}

func (s *witnessRaceStore) Resources() store.ResourceStore { return s.resources }

// TestMakeAvailableTakesOverAWitnessPlacedMidRequest: a witness the
// tiebreaker reconciler places on the target node after make-available
// read it as empty is taken over, as LINSTOR does under its lock, and
// the attach answers 200 rather than 409 "already exists".
func TestMakeAvailableTakesOverAWitnessPlacedMidRequest(t *testing.T) {
	t.Parallel()

	inner := store.NewInMemory()
	ctx := t.Context()

	if err := inner.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	st := &witnessRaceStore{
		Store:     inner,
		resources: &witnessRaceResources{ResourceStore: inner.Resources(), rdName: "pvc-1", node: "n2"},
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceMakeAvailable{Diskful: false})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n2/make-available", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := inner.Resources().Get(ctx, "pvc-1", "n2")
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("the attach left the witness a witness: %v", got.Flags)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped on a diskless attach: %v", got.Flags)
	}
}
