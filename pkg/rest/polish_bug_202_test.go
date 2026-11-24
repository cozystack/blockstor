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
	"fmt"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 202 (P3) gave `handleVDDelete` a post-delete re-walk that restored
// the volume definition when a replica became in use between the in-use
// check and the delete. A volume definition delete is LINSTOR's now: the
// definition is flagged DELETE and each satellite deletes its own volume,
// and a flag a satellite may already have acted on cannot be taken back
// without bringing that volume back empty. A replica that is in use when
// its satellite gets to it refuses to drop the device, which keeps the
// definition until the device is released; the check before the flag is
// what turns an operator away, as in LINSTOR.

// TestBug202VDDeleteHappyPath pins the end-to-end no-race case via the
// HTTP wire: a VD with no referencing Resources deletes cleanly with a
// 200 + MASK_INFO envelope. Guards against the post-walk accidentally
// refusing on an empty-reference happy path.
func TestBug202VDDeleteHappyPath(t *testing.T) {
	t.Parallel()

	const (
		rdName  = "rd202-happy"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib})
	if err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", resp.StatusCode)
	}

	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); err == nil {
		t.Errorf("VD %s/%d still present after successful DELETE", rdName, volNum)
	}
}
