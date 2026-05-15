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

// Bug 110 — `PATCH /v1/encryption/passphrase` hangs 6+ seconds then
// drops the connection. Root cause: the apiserver ServiceAccount has
// no `secrets` RBAC verb, so the controller-runtime cached Get against
// the passphrase Secret stalls on a never-syncing informer (the
// reflector keeps retrying a Forbidden list/watch). The handler MUST
// fail fast with a 500 + envelope instead of letting the connection
// hang past the operator's deadline.
//
// These tests pin both ends of the contract:
//   - POST / PATCH / PUT must complete in well under 2 seconds even
//     when the backing K8s client returns Forbidden on Get.
//   - The wire shape on failure is the standard `[]APICallRc` envelope
//     so python-linstor renders a clean error instead of a traceback.
//   - The RBAC YAML grants the apiserver ClusterRole `secrets` so the
//     in-cluster code path never reaches the Forbidden branch.

package rest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// secretsForbiddenClient wraps a fake controller-runtime client and
// returns 403 on every Get/Create/Update against corev1.Secret. This
// mimics the in-cluster Bug 110 reproducer: the apiserver SA lacks
// `secrets` RBAC, so every Secret access fails with Forbidden.
func secretsForbiddenClient(t *testing.T) client.Client {
	t.Helper()

	scheme := k8sruntime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 to scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor to scheme: %v", err)
	}

	base := fake.NewClientBuilder().WithScheme(scheme).Build()

	forbidden := func(verb, resource, name string) error {
		gr := schema.GroupResource{Group: "", Resource: resource}

		return apierrors.NewForbidden(gr, name,
			errors.Newf("user is forbidden to %s %s at the cluster scope", verb, resource))
	}

	return interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption,
		) error {
			if _, isSecret := obj.(*corev1.Secret); isSecret {
				return forbidden("get", "secrets", key.Name)
			}

			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.CreateOption,
		) error {
			if _, isSecret := obj.(*corev1.Secret); isSecret {
				return forbidden("create", "secrets", obj.GetName())
			}

			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.UpdateOption,
		) error {
			if _, isSecret := obj.(*corev1.Secret); isSecret {
				return forbidden("update", "secrets", obj.GetName())
			}

			return c.Update(ctx, obj, opts...)
		},
	})
}

// newForbiddenSecretsServer constructs a Server whose Client returns
// Forbidden on every Secret access. The Store is in-memory so non-
// Secret endpoints stay reachable.
func newForbiddenSecretsServer(t *testing.T) *Server {
	t.Helper()

	return &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    secretsForbiddenClient(t),
		Namespace: passphraseSecretTestNamespace,
	}
}

// assertFastEnvelope reads the response body, asserts the elapsed
// time is below 2 seconds, and confirms the body decodes into a
// non-empty `[]APICallRc` envelope. Used by every Bug 110 test so
// the "fail fast" contract is checked uniformly.
func assertFastEnvelope(t *testing.T, resp *http.Response, elapsed time.Duration) {
	t.Helper()

	if elapsed > 2*time.Second {
		t.Errorf("handler took %s; want < 2s (Bug 110 hang regression)", elapsed)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	_ = resp.Body.Close()

	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(body, &rcs); err != nil {
		t.Fatalf("decode envelope: %v (body=%q)", err, string(body))
	}

	if len(rcs) == 0 {
		t.Errorf("envelope empty; got body=%q", string(body))
	}
}

// TestBug110_CreateFailsFastOnForbiddenSecret pins that POST
// /v1/encryption/passphrase returns a quick 500 + envelope when the
// backing Secret access fails with Forbidden, instead of hanging on
// a stuck informer.
func TestBug110_CreateFailsFastOnForbiddenSecret(t *testing.T) {
	srv := newForbiddenSecretsServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	start := time.Now()
	resp := httpPost(t, base+"/v1/encryption/passphrase", body)
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status: got %d, want 500", resp.StatusCode)
	}

	assertFastEnvelope(t, resp, elapsed)
}

// TestBug110_EnterFailsFastOnForbiddenSecret pins the PATCH variant
// — the exact endpoint the bug report repro hits.
func TestBug110_EnterFailsFastOnForbiddenSecret(t *testing.T) {
	srv := newForbiddenSecretsServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	start := time.Now()
	resp := httpPatch(t, base+"/v1/encryption/passphrase", body)
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status: got %d, want 500", resp.StatusCode)
	}

	assertFastEnvelope(t, resp, elapsed)
}

// TestBug110_ModifyFailsFastOnForbiddenSecret pins the PUT variant.
func TestBug110_ModifyFailsFastOnForbiddenSecret(t *testing.T) {
	srv := newForbiddenSecretsServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "old",
		"new_passphrase": "new",
	})

	start := time.Now()
	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status: got %d, want 500", resp.StatusCode)
	}

	assertFastEnvelope(t, resp, elapsed)
}

// TestBug110_RBACGrantsSecretsToApiserver pins the deployment RBAC:
// the apiserver ClusterRole MUST include `secrets` so the in-cluster
// passphrase Get/Create/Update path never falls into the Bug 110
// Forbidden branch in production. Parsed from
// stand/blockstor-apiserver-deploy.yaml because that's the file the
// `make iter` deploy loop applies.
func TestBug110_RBACGrantsSecretsToApiserver(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}

	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	deployPath := filepath.Join(repoRoot, "stand", "blockstor-apiserver-deploy.yaml")

	raw, err := os.ReadFile(deployPath)
	if err != nil {
		t.Fatalf("read %s: %v", deployPath, err)
	}

	// Walk every YAML document in the multi-doc file; find the
	// ClusterRole named blockstor-apiserver; assert the rule set
	// includes the empty-apiGroup `secrets` resource with at least
	// the verbs needed by the passphrase handler.
	docs := strings.Split(string(raw), "\n---")

	var (
		foundRole         bool
		foundSecretsRule  bool
		foundRequiredVerb = map[string]bool{
			"get": false, "create": false, "update": false,
		}
	)

	for _, doc := range docs {
		var obj struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Rules []struct {
				APIGroups []string `json:"apiGroups"`
				Resources []string `json:"resources"`
				Verbs     []string `json:"verbs"`
			} `json:"rules"`
		}

		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			continue
		}

		if obj.Kind != "ClusterRole" || obj.Metadata.Name != "blockstor-apiserver" {
			continue
		}

		foundRole = true

		for _, rule := range obj.Rules {
			hasEmptyGroup := false

			for _, g := range rule.APIGroups {
				if g == "" {
					hasEmptyGroup = true

					break
				}
			}

			if !hasEmptyGroup {
				continue
			}

			hasSecrets := false

			for _, r := range rule.Resources {
				if r == "secrets" {
					hasSecrets = true

					break
				}
			}

			if !hasSecrets {
				continue
			}

			foundSecretsRule = true

			for _, v := range rule.Verbs {
				if _, want := foundRequiredVerb[v]; want {
					foundRequiredVerb[v] = true
				}
			}
		}
	}

	if !foundRole {
		t.Fatalf("ClusterRole blockstor-apiserver not found in %s", deployPath)
	}

	if !foundSecretsRule {
		t.Fatalf("ClusterRole blockstor-apiserver: no rule grants `secrets` (Bug 110)")
	}

	for verb, ok := range foundRequiredVerb {
		if !ok {
			t.Errorf("ClusterRole blockstor-apiserver: missing verb %q on `secrets` (Bug 110)", verb)
		}
	}
}
