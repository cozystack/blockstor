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

// Cluster-passphrase rotation guard — PUT /v1/encryption/passphrase.
//
// blockstor uses the cluster passphrase VERBATIM as the cryptsetup key
// (Secret → injectLUKSMasterPassphrase → dispatcher `LuksPassphrase` →
// `cryptsetup luksOpen --key-file -`). Rotating it therefore leaves
// every LUKS header on disk locked with the old value. Pre-guard the
// handler answered `200 Master passphrase modified` and the cluster
// stranded its encrypted volumes at the next attach, reboot or resize.
//
// These tests pin the guard AND its boundaries. The boundary cases
// matter as much as the refusal: a guard that also blocked rotation on
// an unencrypted cluster, or that broke the documented same-value
// idempotent no-op, would be a worse regression than the bug.
//
//   - LUKS RD present, value changes           → 409, Secret intact
//   - LUKS RD present, force=true (body)       → 200, Secret rotated
//   - LUKS RD present, ?force=true (query)     → 200, Secret rotated
//   - LUKS RD present, same value (no-op)      → 200, not refused
//   - no LUKS RD (DRBD,STORAGE only)           → 200, Secret rotated
//   - lower-case `luks` in the layer stack     → 409 (case-insensitive)
//   - wrong old passphrase + LUKS RD           → 401, guard not reached
//   - RD inherits LUKS from its ResourceGroup   → 409 (set too narrow)
//   - LUKS RD pins its OWN passphrase           → 200 (set too wide)
//   - RD list unreadable                        → 500, fail closed
//
// The positive controls are the point of the last two: the case-fold
// case proves the guard does not miss the spelling `--layer-list
// luks,storage` produces, and the "no LUKS RD" case proves a green
// refusal test is not just a handler that rejects everything.

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedRotationGuardRD puts a resource definition with the given layer
// stack into the server's store.
func seedRotationGuardRD(t *testing.T, srv *Server, name string, layers []string) {
	t.Helper()

	err := srv.Store.ResourceDefinitions().Create(context.Background(), &apiv1.ResourceDefinition{
		Name:       name,
		LayerStack: layers,
	})
	if err != nil {
		t.Fatalf("seed RD %s: %v", name, err)
	}
}

// readGuardSecret returns the current stored cluster passphrase.
func readGuardSecret(t *testing.T, cli client.Client) string {
	t.Helper()

	var sec corev1.Secret

	err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace,
		Name:      defaultPassphraseSecretName,
	}, &sec)
	if err != nil {
		t.Fatalf("passphrase Secret missing: %v", err)
	}

	return string(sec.Data[passphraseSecretKey])
}

// newRotationGuardServer builds a Secret-backed server, seeds the
// cluster passphrase, and returns the base URL plus a stop func.
func newRotationGuardServer(t *testing.T, seed string) (*Server, string, func()) {
	t.Helper()

	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)

	body, _ := json.Marshal(map[string]string{"new_passphrase": seed})

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		stop()
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	return srv, base, stop
}

// TestRotationRefusedWhileLUKSResourceDefinitionsExist is the
// canonical red. Without the guard this PUT returns 200 and silently
// strands `pvc-encrypted`.
func TestRotationRefusedWhileLUKSResourceDefinitionsExist(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	seedRotationGuardRD(t, srv, "pvc-encrypted", []string{"DRBD", "LUKS", "STORAGE"})

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-master",
		"new_passphrase": "new-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (rotation would strand LUKS volumes)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Fatalf("envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
	}

	// The operator has to be able to act on this: the message must
	// name the blocking RD, not just say "refused".
	if !strings.Contains(rcs[0].Message, "pvc-encrypted") {
		t.Errorf("message %q does not name the blocking resource definition", rcs[0].Message)
	}

	if got := readGuardSecret(t, srv.Client); got != "old-master" {
		t.Errorf("Secret rotated despite refusal: got %q, want %q", got, "old-master")
	}
}

// TestRotationRefusalIsCaseInsensitive pins that the guard sees the
// lower-cased spelling `linstor rd create --layer-list luks,storage`
// produces. A case-sensitive check would let exactly the RDs created
// through the everyday CLI form slip past.
func TestRotationRefusalIsCaseInsensitive(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	seedRotationGuardRD(t, srv, "pvc-lower", []string{"drbd", "luks", "storage"})

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-master",
		"new_passphrase": "new-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 for lower-cased `luks` layer", resp.StatusCode)
	}

	if got := readGuardSecret(t, srv.Client); got != "old-master" {
		t.Errorf("Secret rotated despite refusal: got %q", got)
	}
}

// TestRotationAllowedWithoutLUKSResourceDefinitions is the positive
// control for every refusal above: on a cluster with no encrypted
// volumes the rotation is harmless and MUST still succeed. Without
// this case a guard that rejected unconditionally would look correct.
func TestRotationAllowedWithoutLUKSResourceDefinitions(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	seedRotationGuardRD(t, srv, "pvc-plain", []string{"DRBD", "STORAGE"})

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-master",
		"new_passphrase": "new-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (no encrypted volumes to strand)", resp.StatusCode)
	}

	if got := readGuardSecret(t, srv.Client); got != "new-master" {
		t.Errorf("Secret not rotated: got %q, want %q", got, "new-master")
	}
}

// TestRotationForcedOverridesGuard pins the escape hatch in both
// spellings — a body field and a query parameter — mirroring the `vd
// s` shrink override (known-deltas row 60) so operators meet one
// idiom across blockstor's destructive verbs.
func TestRotationForcedOverridesGuard(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
		path string
	}{
		{
			name: "body field",
			body: map[string]any{
				"old_passphrase": "old-master",
				"new_passphrase": "new-master",
				"force":          true,
			},
			path: "/v1/encryption/passphrase",
		},
		{
			name: "query parameter",
			body: map[string]any{
				"old_passphrase": "old-master",
				"new_passphrase": "new-master",
			},
			path: "/v1/encryption/passphrase?force=true",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, base, stop := newRotationGuardServer(t, "old-master")
			defer stop()

			seedRotationGuardRD(t, srv, "pvc-encrypted", []string{"DRBD", "LUKS", "STORAGE"})

			body, _ := json.Marshal(tc.body)

			resp := httpPut(t, base+tc.path, body)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200 (force opts out of the guard)", resp.StatusCode)
			}

			if got := readGuardSecret(t, srv.Client); got != "new-master" {
				t.Errorf("Secret not rotated under force: got %q, want %q", got, "new-master")
			}
		})
	}
}

// TestRotationSameValueStillNoOpsWithLUKSPresent pins that the guard
// sits AFTER the idempotent same-value branch. Re-stamping the current
// passphrase changes no key and must stay a clean 200 even on a
// cluster full of encrypted volumes — a retried CLI call must not
// start failing with 409.
func TestRotationSameValueStillNoOpsWithLUKSPresent(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	seedRotationGuardRD(t, srv, "pvc-encrypted", []string{"DRBD", "LUKS", "STORAGE"})

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-master",
		"new_passphrase": "old-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (same-value no-op must not be refused)", resp.StatusCode)
	}

	if got := readGuardSecret(t, srv.Client); got != "old-master" {
		t.Errorf("Secret changed on a no-op: got %q", got)
	}
}

// TestRotationWrongOldPassphraseStillUnauthorized pins that the guard
// did not reorder the auth check. A 409 here instead of 401 would leak
// "this cluster has encrypted volumes" to a caller who failed to prove
// knowledge of the current passphrase.
func TestRotationWrongOldPassphraseStillUnauthorized(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	seedRotationGuardRD(t, srv, "pvc-encrypted", []string{"DRBD", "LUKS", "STORAGE"})

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "WRONG",
		"new_passphrase": "new-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401 (auth precedes the rotation guard)", resp.StatusCode)
	}

	if got := readGuardSecret(t, srv.Client); got != "old-master" {
		t.Errorf("Secret changed on a failed auth: got %q", got)
	}
}

// --- guard scope + fail-closed ------------------------------------
//
// Review found the first version's blocking set both too wide and too
// narrow, and the fail-closed branch — the one its comment argues
// hardest for — untested. These three cover that.

// errSimulatedOutage is the static sentinel listErrRDStore returns.
// Static rather than inline so err113 stays happy and a caller could
// errors.Is-match it if the guard ever grows error classification.
var errSimulatedOutage = errors.New("simulated apiserver outage")

// listErrRDStore fails every ResourceDefinitions().List so the guard's
// "cannot determine whether encrypted volumes exist" branch runs.
type listErrRDStore struct {
	store.ResourceDefinitionStore
}

func (listErrRDStore) List(context.Context) ([]apiv1.ResourceDefinition, error) {
	return nil, errSimulatedOutage
}

// rdListErrStore swaps just the RD store on an otherwise real Store.
type rdListErrStore struct {
	store.Store
}

func (s rdListErrStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return listErrRDStore{ResourceDefinitionStore: s.Store.ResourceDefinitions()}
}

// TestRotationFailsClosedWhenResourceDefinitionsUnreadable pins the
// branch the guard's comment argues for: if we cannot tell whether
// encrypted volumes exist, we must NOT rotate. A rotation is rare and
// high-blast-radius; a 500 the operator retries is the cheap outcome,
// silently stranding every encrypted volume is not.
func TestRotationFailsClosedWhenResourceDefinitionsUnreadable(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	srv.Store = rdListErrStore{Store: srv.Store}

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-master",
		"new_passphrase": "new-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500 (must not rotate on an unreadable RD list)", resp.StatusCode)
	}

	if got := readGuardSecret(t, srv.Client); got != "old-master" {
		t.Errorf("Secret rotated despite an unreadable RD list: got %q", got)
	}
}

// TestRotationRefusedForRGInheritedLUKS pins the "too narrow" half. An
// RD created through the Kubernetes door can leave spec.layerStack
// empty and inherit LUKS from its ResourceGroup; reading rd.LayerStack
// alone missed exactly those, and they are just as stranded.
func TestRotationRefusedForRGInheritedLUKS(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	err := srv.Store.ResourceGroups().Create(context.Background(), &apiv1.ResourceGroup{
		Name:         "encrypted-rg",
		SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}},
	})
	if err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	err = srv.Store.ResourceDefinitions().Create(context.Background(), &apiv1.ResourceDefinition{
		Name:              "pvc-inherits-luks",
		ResourceGroupName: "encrypted-rg",
		// LayerStack deliberately empty: the stack comes from the RG.
	})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-master",
		"new_passphrase": "new-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 for an RD inheriting LUKS from its RG", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || !strings.Contains(rcs[0].Message, "pvc-inherits-luks") {
		t.Errorf("envelope does not name the inheriting RD: %+v", rcs)
	}

	if got := readGuardSecret(t, srv.Client); got != "old-master" {
		t.Errorf("Secret rotated despite refusal: got %q", got)
	}
}

// TestRotationAllowedWhenLUKSRDsCarryTheirOwnKey pins the "too wide"
// half. An RD that pins its own passphrase does not use the cluster
// value, so rotating the cluster passphrase cannot strand it and must
// not be blocked on its account.
func TestRotationAllowedWhenLUKSRDsCarryTheirOwnKey(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	err := srv.Store.ResourceDefinitions().Create(context.Background(), &apiv1.ResourceDefinition{
		Name:       "pvc-own-key",
		LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
		Props:      map[string]string{"DrbdOptions/Encryption/passphrase": "rd-owned"},
	})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-master",
		"new_passphrase": "new-master",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (an RD on its own key is not stranded)", resp.StatusCode)
	}

	if got := readGuardSecret(t, srv.Client); got != "new-master" {
		t.Errorf("Secret not rotated: got %q, want %q", got, "new-master")
	}
}

// TestForceIsRejectedOnCreateAndEnter pins the strict-decode fix. The
// first version put `Force` on the shared passphraseRequest, so POST
// and PATCH — which decode with DisallowUnknownFields — silently began
// accepting a `force` key that means nothing to them, where before it
// was a 400. Moving it to a PUT-only body restored that. Without this
// test the regression is invisible: nothing else asserts what the
// create/enter verbs do with an unknown field.
func TestForceIsRejectedOnCreateAndEnter(t *testing.T) {
	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	body := []byte(`{"new_passphrase":"x","force":true}`)

	for _, tc := range []struct {
		name string
		do   func() *http.Response
	}{
		{"POST create-passphrase", func() *http.Response { return httpPost(t, base+"/v1/encryption/passphrase", body) }},
		{"PATCH enter-passphrase", func() *http.Response { return httpPatch(t, base+"/v1/encryption/passphrase", body) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := tc.do()
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status: got %d, want 400 — `force` is a PUT-only field and must stay unknown here",
					resp.StatusCode)
			}
		})
	}
}

// TestForceIsAcceptedInThePUTBody is the positive control for the test
// above: the same key must still be honoured where it belongs.
func TestForceIsAcceptedInThePUTBody(t *testing.T) {
	srv, base, stop := newRotationGuardServer(t, "old-master")
	defer stop()

	seedRotationGuardRD(t, srv, "pvc-encrypted", []string{"DRBD", "LUKS", "STORAGE"})

	body := []byte(`{"old_passphrase":"old-master","new_passphrase":"new-master","force":true}`)

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (`force` is valid on PUT)", resp.StatusCode)
	}

	if got := readGuardSecret(t, srv.Client); got != "new-master" {
		t.Errorf("Secret not rotated under force: got %q", got)
	}
}
