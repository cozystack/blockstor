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

// Scenario 6.W13 pins: `linstor encryption enter-passphrase` unblocks
// LUKS resources after a controller restart. The Secret survives
// (apiserver-persisted), but the in-memory unlock flag is per-process,
// so a fresh Server boots in the locked state and MUST surface
// `state.suspended=true` on every LUKS-stack resource until PATCH
// /v1/encryption/passphrase succeeds.

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// luksResourceSeed returns a Resource with a DRBD-over-LUKS-over-STORAGE
// layer stack, suitable for the W13 view tests. Layer chain mirrors
// what `linstor rd c --layer-list drbd,luks,storage` produces and is
// what the satellite's events2 observer would persist on a real
// LUKS-encrypted replica.
func luksResourceSeed(name, node string) apiv1.Resource {
	return apiv1.Resource{
		Name:     name,
		NodeName: node,
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Children: []apiv1.ResourceLayer{{
				Type: apiv1.LayerKindLUKS,
				Children: []apiv1.ResourceLayer{{
					Type: apiv1.LayerKindStorage,
					Storage: &apiv1.StorageResourceLayer{
						ProviderKind: apiv1.StoragePoolKindLVMThin,
					},
				}},
			}},
		},
	}
}

// fetchResourcesView GETs /v1/view/resources and decodes the wire
// shape. Caller asserts on the Suspended field of state.
func fetchResourcesView(t *testing.T, base string) []apiv1.ResourceWithVolumes {
	t.Helper()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("view: got %d, want 200", resp.StatusCode)
	}

	var out []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode view: %v", err)
	}

	return out
}

// findResource returns the ResourceWithVolumes matching (name, node)
// from a view slice, or fatals.
func findResource(t *testing.T, view []apiv1.ResourceWithVolumes, name, node string) apiv1.ResourceWithVolumes {
	t.Helper()

	for _, r := range view {
		if r.Name == name && r.NodeName == node {
			return r
		}
	}

	t.Fatalf("resource %s@%s not in view (%d entries)", name, node, len(view))

	return apiv1.ResourceWithVolumes{}
}

// seedPassphraseSecret creates the cluster passphrase Secret directly
// on the fake apiserver — analogous to a previous controller having
// POSTed create-passphrase before the restart.
func seedPassphraseSecret(t *testing.T, srv *Server, value string) {
	t.Helper()

	err := srv.Client.Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultPassphraseSecretName,
			Namespace: srv.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{passphraseSecretKey: []byte(value)},
	})
	if err != nil {
		t.Fatalf("seed passphrase Secret: %v", err)
	}
}

// newW13Server is the shared setup: Server with in-memory Store, fake
// apiserver Client, the standard namespace. Caller seeds the resource
// + Secret on the returned Server.
func newW13Server(t *testing.T) *Server {
	t.Helper()

	return &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    newFakeRESTClient(t),
		Namespace: passphraseSecretTestNamespace,
	}
}

// TestPassphraseEnterAcceptsBareKey: scenario 6.W13 documents the
// body shape `{"passphrase":"..."}` — `linstor encryption
// enter-passphrase` posts the bare field, no `new_` prefix.
// Backward-compat with `new_passphrase` is exercised by every
// pre-existing TestPassphraseCreateThenEnter etc.; this test pins
// the new bare-key surface so a regression that drops the
// passphraseRequest.proofOfKnowledge dual-key parser surfaces
// immediately.
func TestPassphraseEnterAcceptsBareKey(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})
	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", createResp.StatusCode)
	}

	bareBody, _ := json.Marshal(map[string]string{"passphrase": "secret"})

	resp := httpPatch(t, base+"/v1/encryption/passphrase", bareBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("bare-key PATCH: got %d, want 200", resp.StatusCode)
	}
}

// TestPassphraseEnterBareKeyMismatch401: bare-key body must yield
// the same 401 surface as the new_passphrase path on mismatch —
// the W13 wire contract is "wrong → 401" regardless of which JSON
// field name the client sent.
func TestPassphraseEnterBareKeyMismatch401(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "right"})
	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", createResp.StatusCode)
	}

	wrongBody, _ := json.Marshal(map[string]string{"passphrase": "WRONG"})

	resp := httpPatch(t, base+"/v1/encryption/passphrase", wrongBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: got %d, want 401", resp.StatusCode)
	}
}

// TestResourceViewSurfacesSuspendedOnLUKSWhileLocked pins that a
// LUKS-stack resource surfaces state.suspended=true on
// /v1/view/resources while the controller process is locked — even
// though a passphrase Secret already exists on the apiserver (so
// reads of the Secret succeed, but the in-memory unlock flag is
// still false). Simulates a controller that has been restarted
// **after** create-passphrase: the Secret survives, the unlock
// does not. LUKS provisioning must stay blocked until PATCH lands.
func TestResourceViewSurfacesSuspendedOnLUKSWhileLocked(t *testing.T) {
	srv := newW13Server(t)
	seedPassphraseSecret(t, srv, "op-secret")

	seed := luksResourceSeed("pvc-luks", "n1")
	if err := srv.Store.Resources().Create(context.Background(), &seed); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	got := findResource(t, fetchResourcesView(t, base), "pvc-luks", "n1")

	if got.State.Suspended == nil {
		t.Fatalf("state.suspended unset on LUKS-stack resource while locked; want true (Suspended)")
	}

	if !*got.State.Suspended {
		t.Errorf("state.suspended: got false, want true (locked controller must block LUKS provisioning)")
	}
}

// TestResourceViewLUKSResumesAfterEnterPassphrase pins the second
// half of the W13 contract: after the operator PATCHes the master
// passphrase against the same Server, the LUKS-stack resource flips
// from Suspended=true to Suspended=false on the next GET (rendered
// Available by the CLI), with no controller restart needed in
// between. This is the "LUKS provisioning resumes within next
// reconcile tick" pin.
func TestResourceViewLUKSResumesAfterEnterPassphrase(t *testing.T) {
	srv := newW13Server(t)
	seedPassphraseSecret(t, srv, "op-secret")

	seed := luksResourceSeed("pvc-luks", "n1")
	if err := srv.Store.Resources().Create(context.Background(), &seed); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	// Pre-condition: Suspended on initial GET (locked).
	pre := findResource(t, fetchResourcesView(t, base), "pvc-luks", "n1")
	if pre.State.Suspended == nil || !*pre.State.Suspended {
		t.Fatalf("pre-PATCH state.suspended: got %v, want true", pre.State.Suspended)
	}

	// PATCH the right passphrase.
	enterBody, _ := json.Marshal(map[string]string{"passphrase": "op-secret"})
	enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		t.Fatalf("enter-passphrase: got %d, want 200", enterResp.StatusCode)
	}

	// Post-condition: Suspended=false (Available) on next GET.
	post := findResource(t, fetchResourcesView(t, base), "pvc-luks", "n1")
	if post.State.Suspended == nil {
		t.Fatalf("post-PATCH state.suspended is nil; want false (Available)")
	}

	if *post.State.Suspended {
		t.Errorf("post-PATCH state.suspended: got true, want false (LUKS provisioning must resume)")
	}
}

// TestResourceViewWrongPassphraseKeepsSuspended pins that a 401 from
// a failed enter-passphrase does NOT inadvertently flip the in-memory
// unlock flag — a regression here would silently grant LUKS
// provisioning to any caller after a single wrong-passphrase guess.
func TestResourceViewWrongPassphraseKeepsSuspended(t *testing.T) {
	srv := newW13Server(t)
	seedPassphraseSecret(t, srv, "op-secret")

	seed := luksResourceSeed("pvc-luks", "n1")
	if err := srv.Store.Resources().Create(context.Background(), &seed); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	wrongBody, _ := json.Marshal(map[string]string{"passphrase": "WRONG"})
	wrongResp := httpPatch(t, base+"/v1/encryption/passphrase", wrongBody)
	_ = wrongResp.Body.Close()

	if wrongResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-PATCH status: got %d, want 401", wrongResp.StatusCode)
	}

	got := findResource(t, fetchResourcesView(t, base), "pvc-luks", "n1")
	if got.State.Suspended == nil || !*got.State.Suspended {
		t.Errorf("post-wrong-PATCH state.suspended: got %v, want true (still locked)",
			got.State.Suspended)
	}
}

// TestControllerRestartReSuspendsLUKS is the headline scenario-6.W13
// pin: it simulates a controller restart by booting a second Server
// instance against the SAME fake-apiserver client + the SAME Store.
// The Secret + resource survive (apiserver-persisted); the in-memory
// unlock flag is per-process and resets on the fresh Server. The
// LUKS-stack resource MUST surface Suspended=true again until a fresh
// PATCH lands, then flip back to Suspended=false.
func TestControllerRestartReSuspendsLUKS(t *testing.T) {
	st := store.NewInMemory()
	cli := newFakeRESTClient(t)

	// Round 1: original controller, unlock it, observe Available.
	srv1 := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: passphraseSecretTestNamespace,
	}
	seedPassphraseSecret(t, srv1, "op-secret")

	seed := luksResourceSeed("pvc-luks", "n1")
	if err := st.Resources().Create(context.Background(), &seed); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base1, stop1 := startServerCustom(t, srv1)

	enterBody, _ := json.Marshal(map[string]string{"passphrase": "op-secret"})
	enterResp := httpPatch(t, base1+"/v1/encryption/passphrase", enterBody)
	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		stop1()
		t.Fatalf("round1 enter: got %d, want 200", enterResp.StatusCode)
	}

	preRestart := findResource(t, fetchResourcesView(t, base1), "pvc-luks", "n1")
	stop1()

	if preRestart.State.Suspended == nil || *preRestart.State.Suspended {
		t.Fatalf("round1 post-PATCH: state.suspended=%v, want false (Available)",
			preRestart.State.Suspended)
	}

	// Round 2: fresh Server against the SAME apiserver client + the
	// SAME in-memory Store. Secret + resource survive; the unlock
	// flag does not.
	srv2 := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: passphraseSecretTestNamespace,
	}

	base2, stop2 := startServerCustom(t, srv2)
	defer stop2()

	postRestart := findResource(t, fetchResourcesView(t, base2), "pvc-luks", "n1")
	if postRestart.State.Suspended == nil || !*postRestart.State.Suspended {
		t.Errorf("post-restart: state.suspended=%v, want true (Suspended — fresh process, locked)",
			postRestart.State.Suspended)
	}

	// PATCH the right passphrase against the fresh Server and
	// confirm the view flips back to Available.
	enter2Body, _ := json.Marshal(map[string]string{"passphrase": "op-secret"})
	enter2Resp := httpPatch(t, base2+"/v1/encryption/passphrase", enter2Body)
	_ = enter2Resp.Body.Close()

	if enter2Resp.StatusCode != http.StatusOK {
		t.Fatalf("round2 enter: got %d, want 200", enter2Resp.StatusCode)
	}

	resumed := findResource(t, fetchResourcesView(t, base2), "pvc-luks", "n1")
	if resumed.State.Suspended == nil || *resumed.State.Suspended {
		t.Errorf("post-restart resumed: state.suspended=%v, want false (Available)",
			resumed.State.Suspended)
	}
}

// TestResourceViewNoLUKSNoSuspendedField pins the negative space: a
// non-LUKS (DRBD+STORAGE) resource MUST NOT carry the suspended field
// on the wire, even while the controller is locked. The CLI uses
// field absence as the discriminator for "not an encrypted volume"
// vs. "encrypted, unlocked"; a regression that always stamped
// suspended=false would force every CLI column to render `Available`
// on plain DRBD replicas, which is misleading.
func TestResourceViewNoLUKSNoSuspendedField(t *testing.T) {
	srv := newW13Server(t)

	seed := apiv1.Resource{
		Name:     "pvc-plain",
		NodeName: "n1",
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Children: []apiv1.ResourceLayer{{
				Type: apiv1.LayerKindStorage,
				Storage: &apiv1.StorageResourceLayer{
					ProviderKind: apiv1.StoragePoolKindLVMThin,
				},
			}},
		},
	}
	if err := srv.Store.Resources().Create(context.Background(), &seed); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	got := findResource(t, fetchResourcesView(t, base), "pvc-plain", "n1")
	if got.State.Suspended != nil {
		t.Errorf("non-LUKS resource state.suspended: got %v, want nil (field must be omitted)",
			got.State.Suspended)
	}
}

// TestCreatePassphraseImplicitlyUnlocks pins that a successful POST
// (create-passphrase) on an empty cluster also flips the in-memory
// unlock flag, so the immediately-following /v1/view/resources sees
// LUKS resources as Available without requiring a separate PATCH.
// The operator demonstrably knew the value they just stamped — a
// second hop would surprise the CLI's create-passphrase flow.
func TestCreatePassphraseImplicitlyUnlocks(t *testing.T) {
	srv := newW13Server(t)

	seed := luksResourceSeed("pvc-luks", "n1")
	if err := srv.Store.Resources().Create(context.Background(), &seed); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "first-time"})
	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create: got %d, want 201", createResp.StatusCode)
	}

	got := findResource(t, fetchResourcesView(t, base), "pvc-luks", "n1")
	if got.State.Suspended == nil {
		t.Fatalf("state.suspended nil after fresh create-passphrase; want false (Available)")
	}

	if *got.State.Suspended {
		t.Errorf("state.suspended after create-passphrase: got true, want false (operator just stamped it)")
	}
}
