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
	"sync"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// trailingVDStore serves the volume definition as it was before a patch
// for lag after the patch, the way the informer cache trails a write to
// the API server.
type trailingVDStore struct {
	store.VolumeDefinitionStore

	lag time.Duration

	mu    sync.Mutex
	stale *apiv1.VolumeDefinition
	until time.Time
}

func (f *trailingVDStore) PatchVolumeDefinitionSpec(
	ctx context.Context, rdName string, vn int32, mutate func(*apiv1.VolumeDefinition) error,
) error {
	before, err := f.VolumeDefinitionStore.Get(ctx, rdName, vn)
	if err != nil {
		return err //nolint:wrapcheck // pass-through test double
	}

	err = f.VolumeDefinitionStore.PatchVolumeDefinitionSpec(ctx, rdName, vn, mutate)
	if err != nil {
		return err //nolint:wrapcheck // pass-through test double
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.stale, f.until = &before, time.Now().Add(f.lag)

	return nil
}

func (f *trailingVDStore) Get(ctx context.Context, rdName string, vn int32) (apiv1.VolumeDefinition, error) {
	f.mu.Lock()
	stale, until := f.stale, f.until
	f.mu.Unlock()

	if stale != nil && time.Now().Before(until) {
		return *stale, nil
	}

	return f.VolumeDefinitionStore.Get(ctx, rdName, vn) //nolint:wrapcheck // pass-through test double
}

type trailingVDsStore struct {
	store.Store

	vds *trailingVDStore
}

func (s trailingVDsStore) VolumeDefinitions() store.VolumeDefinitionStore { return s.vds }

// linstor-csi clears the inherited file system props of a cloned volume
// with a modify of delete_props, and the read after it has to see them
// gone, as on LINSTOR, whose next read answers from the state the modify
// changed. With reads served from a cache that trails the write, the GET
// returned the props the modify had just deleted (TestLinstorCSIv113,
// CreateVolumeFromVolume).
func TestVDModifyIsVisibleToTheNextRead(t *testing.T) {
	t.Parallel()

	mem := store.NewInMemory()
	ctx := t.Context()

	if err := mem.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-clone"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := mem.VolumeDefinitions().Create(ctx, "pvc-clone", &apiv1.VolumeDefinition{
		VolumeNumber: 0, SizeKib: 1 << 20,
		Props: map[string]string{"FileSystem/Type": "ext4", "FileSystem/MkfsParams": "-E nodiscard"},
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	st := trailingVDsStore{Store: mem, vds: &trailingVDStore{VolumeDefinitionStore: mem.VolumeDefinitions(), lag: time.Second}}

	base, stop := startServerWithStore(t, st)
	defer stop()

	url := base + "/v1/resource-definitions/pvc-clone/volume-definitions/0"

	resp := httpPut(t, url, []byte(`{"delete_props":["FileSystem/Type","FileSystem/MkfsParams"]}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("modify: %d", resp.StatusCode)
	}

	resp = httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	var vd apiv1.VolumeDefinition
	if err := json.NewDecoder(resp.Body).Decode(&vd); err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, key := range []string{"FileSystem/Type", "FileSystem/MkfsParams"} {
		if _, ok := vd.Props[key]; ok {
			t.Errorf("%s read back after the modify deleted it", key)
		}
	}
}
