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

// `linstor vd set-passphrase` must refuse, and must not keep the key.
//
// The handler used to stamp the operator's key into
// `VolumeDefinition.Props["DrbdOptions/Encrypt/Passphrase"]` and answer
// 200. Nothing read that key back — the dispatcher lifts only
// `DrbdOptions/EncryptPassphrase` and `DrbdOptions/Encryption/passphrase`
// onto the `LuksPassphrase` wire prop — so the volume kept the cluster
// key while the CLI reported success. Worse, volume definitions are
// inline in `ResourceDefinition.spec.volumeDefinitions`, so the key sat
// in cleartext in etcd behind `get resourcedefinitions`, a materially
// wider RBAC surface than Secret access.
//
// The existing Bug 233 tests accept either 200 or 501 on the happy
// path, so they cannot tell the two apart. These pin the behaviour that
// actually matters:
//
//   - well-formed request → 501 (not 200)
//   - the supplied key is NOT persisted anywhere on the RD
//   - 404 (unknown RD) and 400 (empty value) still precede the 501
//
// The persistence assertion is the load-bearing one. A handler that
// returned 501 while still writing the key would satisfy a status-code
// check and leave the disclosure exactly where it was.

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// seedVDPassphraseRD creates an RD with one volume definition so the
// handler's 404 probes pass and it reaches the refusal.
func seedVDPassphraseRD(t *testing.T, srv *Server, rdName string) {
	t.Helper()

	ctx := context.Background()

	err := srv.Store.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       rdName,
		LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
	})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err = srv.Store.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1048576,
	})
	if err != nil {
		t.Fatalf("seed VD: %v", err)
	}
}

// assertNoPassphraseLeftOnRD walks every props bag reachable from the
// RD — the RD's own and each volume definition's — and fails if the
// secret appears in any value. Deliberately searches for the VALUE
// rather than the known prop key: a future refactor that persists it
// under a different name would still be a leak, and keying the
// assertion to one constant would miss it.
func assertNoPassphraseLeftOnRD(t *testing.T, srv *Server, rdName, secret string) {
	t.Helper()

	ctx := context.Background()

	rd, err := srv.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	for k, v := range rd.Props {
		if strings.Contains(v, secret) {
			t.Errorf("supplied passphrase persisted on RD prop %q", k)
		}
	}

	vds, err := srv.Store.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		t.Fatalf("list VDs: %v", err)
	}

	for i := range vds {
		for k, v := range vds[i].Props {
			if strings.Contains(v, secret) {
				t.Errorf("supplied passphrase persisted on VD %d prop %q", vds[i].VolumeNumber, k)
			}
		}
	}
}

// TestVDSetPassphraseRefusesAndDoesNotPersist is the canonical red.
func TestVDSetPassphraseRefusesAndDoesNotPersist(t *testing.T) {
	const (
		rdName = "pvc-byok"
		secret = "operator-supplied-key-do-not-store"
	)

	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	seedVDPassphraseRD(t, srv, rdName)

	body, _ := json.Marshal(map[string]string{"new_passphrase": secret})

	resp := httpPut(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions/0/encryption-passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want 501 (per-volume keys are not implemented)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Fatalf("envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
	}

	// The operator must be told their key was discarded — otherwise
	// they may reasonably assume a stored-but-inactive value.
	if !strings.Contains(rcs[0].Message, "NOT stored") {
		t.Errorf("message %q does not tell the operator the key was discarded", rcs[0].Message)
	}

	assertNoPassphraseLeftOnRD(t, srv, rdName, secret)
}

// TestVDSetPassphraseBareStringAlsoRefusesAndDoesNotPersist covers the
// upstream `PassPhraseEnter` bare-JSON-string body shape. It reaches
// the same handler through a different decoder branch, so the
// no-persistence property has to be pinned on both.
func TestVDSetPassphraseBareStringAlsoRefusesAndDoesNotPersist(t *testing.T) {
	const (
		rdName = "pvc-byok-bare"
		secret = "bare-string-key-do-not-store"
	)

	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	seedVDPassphraseRD(t, srv, rdName)

	body, _ := json.Marshal(secret)

	resp := httpPut(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions/0/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want 501", resp.StatusCode)
	}

	assertNoPassphraseLeftOnRD(t, srv, rdName, secret)
}

// TestVDSetPassphraseErrorPrecedenceUnchanged is the positive control
// for the two refusals above: the 501 must not have swallowed the
// pre-existing 404/400 branches. A handler that answered 501 to
// everything would pass the tests above and silently break callers
// that distinguish "no such volume" from "unimplemented".
func TestVDSetPassphraseErrorPrecedenceUnchanged(t *testing.T) {
	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	seedVDPassphraseRD(t, srv, "pvc-known")

	cases := []struct {
		name string
		path string
		body []byte
		want int
	}{
		{
			name: "unknown resource definition",
			path: "/v1/resource-definitions/pvc-absent/volume-definitions/0/encryption-passphrase",
			body: []byte(`{"new_passphrase":"x"}`),
			want: http.StatusNotFound,
		},
		{
			name: "unknown volume number",
			path: "/v1/resource-definitions/pvc-known/volume-definitions/7/encryption-passphrase",
			body: []byte(`{"new_passphrase":"x"}`),
			want: http.StatusNotFound,
		},
		{
			name: "empty new passphrase",
			path: "/v1/resource-definitions/pvc-known/volume-definitions/0/encryption-passphrase",
			body: []byte(`{"new_passphrase":""}`),
			want: http.StatusBadRequest,
		},
		{
			name: "malformed body",
			path: "/v1/resource-definitions/pvc-known/volume-definitions/0/encryption-passphrase",
			body: []byte(`{`),
			want: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := httpPut(t, base+tc.path, tc.body)
			_ = resp.Body.Close()

			if resp.StatusCode != tc.want {
				t.Errorf("status: got %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}
