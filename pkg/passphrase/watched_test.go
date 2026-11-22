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

package passphrase_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/keyring"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// watchBudget is how long a watch gets to list its object in envtest.
const watchBudget = 30 * time.Second

// countingTransport counts the requests that are not watches: what the
// kube-apiserver audit log shows as the idle load.
type countingTransport struct {
	next     http.RoundTripper
	requests *atomic.Int64
}

func (c countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Query().Get("watch") != "true" {
		c.requests.Add(1)
	}

	return c.next.RoundTrip(req) //nolint:wrapcheck // a transport passes through
}

type watchEnv struct {
	env   *envtest.Environment
	admin client.Client
}

func startWatchEnv(t *testing.T) *watchEnv {
	t.Helper()

	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}

	t.Cleanup(func() { _ = env.Stop() })

	adminScheme := scheme(t)

	err = rbacv1.AddToScheme(adminScheme)
	if err != nil {
		t.Fatalf("scheme: %v", err)
	}

	admin, err := client.New(cfg, client.Options{Scheme: adminScheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	ctx := t.Context()

	seed := []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		refConfig("linstor-passphrase", ""),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "linstor-passphrase", Namespace: ns},
			Data:       map[string][]byte{passphrase.DefaultPassphraseKey: []byte("piraeus")},
		},
		// Not one of blockstor's Secrets: no watch may see it.
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "piraeus-tls", Namespace: ns},
			Data:       map[string][]byte{"tls.key": []byte("private")},
		},
	}

	for _, obj := range seed {
		err = admin.Create(ctx, obj)
		if err != nil {
			t.Fatalf("seed %s: %v", obj.GetName(), err)
		}
	}

	ring, key, err := keyring.New([]byte("piraeus"), passphrase.SlotNameSecret, keyring.OriginSecret, time.Now())
	if err != nil {
		t.Fatalf("keyring: %v", err)
	}

	key.Destroy()

	_, err = passphrase.CreateKeyring(ctx, admin, ns, ring)
	if err != nil {
		t.Fatalf("create keyring: %v", err)
	}

	return &watchEnv{env: env, admin: admin}
}

// user is a client identity bound to the Secret verbs given, limited to
// blockstor's Secrets by name, and to get, list and watch on the
// ControllerConfig.
func (e *watchEnv) user(t *testing.T, name string, secretVerbs ...string) *rest.Config {
	t.Helper()

	ctx := t.Context()

	user, err := e.env.AddUser(envtest.User{Name: name, Groups: nil}, nil)
	if err != nil {
		t.Fatalf("add user: %v", err)
	}

	rbac := []client.Object{
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Rules: []rbacv1.PolicyRule{{
				APIGroups:     []string{""},
				Resources:     []string{"secrets"},
				ResourceNames: []string{passphrase.KeyringSecretName, passphrase.UnlockSecretName, "linstor-passphrase"},
				Verbs:         secretVerbs,
			}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: name}},
		},
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{blockstoriov1alpha1.GroupVersion.Group},
				Resources: []string{"controllerconfigs"},
				Verbs:     []string{"get", "list", "watch"},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: name}},
		},
	}

	for _, obj := range rbac {
		err = e.admin.Create(ctx, obj)
		if err != nil {
			t.Fatalf("rbac %s: %v", obj.GetName(), err)
		}
	}

	return user.Config()
}

// watched is a WatchedReader for cfg, started for the test, whose
// requests other than watches add to requests.
func watched(t *testing.T, cfg *rest.Config, syncTimeout time.Duration) (*passphrase.WatchedReader, *atomic.Int64) {
	t.Helper()

	requests := &atomic.Int64{}

	counted := rest.CopyConfig(cfg)
	counted.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return countingTransport{next: next, requests: requests}
	})

	httpClient, err := rest.HTTPClientFor(counted)
	if err != nil {
		t.Fatalf("http client: %v", err)
	}

	mapper, err := apiutil.NewDynamicRESTMapper(counted, httpClient)
	if err != nil {
		t.Fatalf("mapper: %v", err)
	}

	direct, err := client.New(counted, client.Options{HTTPClient: httpClient, Scheme: scheme(t), Mapper: mapper})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	reader := &passphrase.WatchedReader{
		Config:      counted,
		Options:     cache.Options{HTTPClient: httpClient, Scheme: scheme(t), Mapper: mapper},
		Direct:      direct,
		Namespace:   ns,
		SyncTimeout: syncTimeout,
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = reader.Start(ctx) }()

	// Start has stored its context once a read starts a watch; a read
	// before that goes to Direct, which a test would count as idle load.
	deadline := time.Now().Add(watchBudget)
	for !passphrase.Started(reader) {
		if time.Now().After(deadline) {
			t.Fatal("the reader did not start")
		}

		time.Sleep(time.Millisecond)
	}

	return reader, requests
}

func unlock(t *testing.T, unlocker *passphrase.Unlocker) passphrase.State {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	status, err := unlocker.Unlock(ctx)
	if err != nil {
		t.Fatalf("unlock: %v", err)
	}

	return status.State
}

// Under a Role that grants get, list and watch on blockstor's Secrets by
// name, the encryption state comes from the watches once they have listed:
// deriving it again and again sends no request, and a change to the
// passphrase Secret still reaches it.
func TestWatchedReaderDerivesTheStateWithoutRequests(t *testing.T) {
	t.Parallel()

	env := startWatchEnv(t)
	reader, requests := watched(t, env.user(t, "watcher", "get", "list", "watch"), watchBudget)
	unlocker := &passphrase.Unlocker{Secrets: passphrase.Secrets{Reader: reader, Namespace: ns, Direct: nil}}

	deadline := time.Now().Add(watchBudget)

	for {
		before := requests.Load()

		if state := unlock(t, unlocker); state != passphrase.StateUnlocked {
			t.Fatalf("state = %s, want unlocked", state)
		}

		if requests.Load() == before {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("deriving the state still sends requests after %v", watchBudget)
		}

		time.Sleep(50 * time.Millisecond)
	}

	before := requests.Load()

	for range 20 {
		if state := unlock(t, unlocker); state != passphrase.StateUnlocked {
			t.Fatalf("state = %s, want unlocked", state)
		}
	}

	if sent := requests.Load() - before; sent != 0 {
		t.Errorf("20 derivations of an unchanged state sent %d requests, want 0", sent)
	}

	var sec corev1.Secret

	err := env.admin.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "linstor-passphrase"}, &sec)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	sec.Data[passphrase.DefaultPassphraseKey] = []byte("not the passphrase")

	err = env.admin.Update(t.Context(), &sec)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	for unlock(t, unlocker) != passphrase.StateLocked {
		if time.Now().After(deadline) {
			t.Fatal("the changed passphrase Secret never reached the state")
		}

		time.Sleep(50 * time.Millisecond)
	}

	var other corev1.Secret

	err = reader.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "piraeus-tls"}, &other)
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("read of a Secret the Role does not name = %v, want forbidden", err)
	}
}

// Under a Role that grants only get, as the one shipped before the watches,
// no watch can list its object: the state is read through the API, as it
// was before, both while the watches try and once they have given up.
func TestWatchedReaderFallsBackWithoutListAndWatch(t *testing.T) {
	t.Parallel()

	env := startWatchEnv(t)
	syncTimeout := 2 * time.Second
	reader, requests := watched(t, env.user(t, "getter", "get"), syncTimeout)
	unlocker := &passphrase.Unlocker{Secrets: passphrase.Secrets{Reader: reader, Namespace: ns, Direct: nil}}

	if state := unlock(t, unlocker); state != passphrase.StateUnlocked {
		t.Fatalf("state while the watches try = %s, want unlocked", state)
	}

	time.Sleep(2 * syncTimeout)

	before := requests.Load()

	if state := unlock(t, unlocker); state != passphrase.StateUnlocked {
		t.Fatalf("state once the watches gave up = %s, want unlocked", state)
	}

	if requests.Load() == before {
		t.Error("the state was not read through the API")
	}
}
