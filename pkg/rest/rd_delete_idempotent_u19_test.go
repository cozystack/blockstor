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
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// U19/U90/U112/U41/U101/U186/U242 — "Resources stuck on DELETING; can't
// complete or retry the delete; can't reuse a name stuck in DELETING."
//
// BS realises the two-phase RD delete with CRD finalizers: a repeat
// `rd d` re-stamps the (already-set) DeletionTimestamp via an idempotent
// client.Delete, so the CLI never errors and the operator can safely
// retry. The finalizer-blocked DELETING latch itself needs a real CRD
// store + satellite finalizer, so it is pinned at L6
// (rd-d-deleting-surface.sh). This L1 test pins the REST-level
// idempotency contract on the in-memory store: a SECOND `rd d` after the
// first succeeded returns 200 + WARN "already absent" (exit 0), and the
// freed name is immediately reusable — the two operator-visible
// guarantees the user reports demanded.

// TestRDDeleteIdempotentRetryAfterSuccess pins that a second `rd d` on
// an already-removed RD is a clean idempotent no-op (200 + WARN), not an
// error. This is the retry path the user reports ("can't retry the
// delete") and the CSI DeleteVolume replay both depend on.
func TestRDDeleteIdempotentRetryAfterSuccess(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u19-idem"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// First delete: real drop, MASK_INFO success.
	first := httpDelete(t, base+"/v1/resource-definitions/"+rd)
	defer func() { _ = first.Body.Close() }()

	if first.StatusCode != http.StatusOK {
		t.Fatalf("first rd d status: got %d, want 200", first.StatusCode)
	}

	// Second delete (the retry): must be an idempotent no-op, NOT an
	// error. python-linstor maps a 200 envelope to exit 0.
	second := httpDelete(t, base+"/v1/resource-definitions/"+rd)
	defer func() { _ = second.Body.Close() }()

	if second.StatusCode != http.StatusOK {
		t.Fatalf("retried rd d status: got %d, want 200 (idempotent no-op)", second.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(second.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].RetCode&maskWarn == 0 {
		t.Fatalf("expected WARN envelope on retried rd d, got %+v", rc)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("retried rd d message %q missing 'already absent' marker", rc[0].Message)
	}
}

// TestRDNameReusableAfterDelete pins U186/U242: once an RD's delete
// completes, the SAME name must be reusable — a fresh create must
// succeed (the name was freed, no stale row blocks re-creation).
func TestRDNameReusableAfterDelete(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u242-reuse"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	del := httpDelete(t, base+"/v1/resource-definitions/"+rd)
	_ = del.Body.Close()

	if del.StatusCode != http.StatusOK {
		t.Fatalf("rd d status: got %d, want 200", del.StatusCode)
	}

	// Re-create the same name through the store (the CLI path the L6
	// cell exercises end-to-end). It must succeed: the name was freed.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("name %q not reusable after delete: %v", rd, err)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, rd); err != nil {
		t.Errorf("re-created RD %q not retrievable: %v", rd, err)
	}
}
