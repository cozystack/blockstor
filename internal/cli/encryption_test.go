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

package cli_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

const testNamespace = "blockstor-system"

// newKubeApp wires the CLI with both an in-memory store and a fake
// cluster client, so the commands that reach objects outside the CRD
// surface can be exercised.
func newKubeApp(t *testing.T) (*cli.App, ctrlclient.Client, *bytes.Buffer) {
	t.Helper()

	scheme := runtime.NewScheme()

	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core scheme: %v", err)
	}

	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register crd scheme: %v", err)
	}

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	backend := store.NewInMemory()

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return backend, nil
		},
		KubeFor: func(context.Context) (ctrlclient.Client, string, error) {
			return client, testNamespace, nil
		},
	}

	return app, client, &errBuf
}

func readPassphrase(t *testing.T, client ctrlclient.Client) string {
	t.Helper()

	var secret corev1.Secret

	key := types.NamespacedName{Namespace: testNamespace, Name: "blockstor-cluster-passphrase"}
	if err := client.Get(t.Context(), key, &secret); err != nil {
		t.Fatalf("get passphrase secret: %v", err)
	}

	return string(secret.Data["passphrase"])
}

// The cluster master key lives in a Secret, so create-passphrase
// writes it where the controller and the satellites read it from.
func TestEncryptionCreatePassphrase(t *testing.T) {
	t.Parallel()

	for _, flag := range [][]string{
		{"--passphrase", "s3cret"},
		{"-p", "s3cret"},
	} {
		app, client, errBuf := newKubeApp(t)

		argv := append([]string{"encryption", "create-passphrase"}, flag...)
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v: exit = %d (stderr: %s)", flag, got, errBuf.String())
		}

		if got := readPassphrase(t, client); got != "s3cret" {
			t.Errorf("%v: stored passphrase = %q, want s3cret", flag, got)
		}
	}
}

// Re-running with the same passphrase is a success, so a pre-flight
// step in a script stays idempotent — but a DIFFERENT one is refused
// rather than silently rotating the master key, which would leave
// every existing LUKS volume undecryptable.
func TestEncryptionCreatePassphraseDoesNotRotate(t *testing.T) {
	t.Parallel()

	app, client, errBuf := newKubeApp(t)

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase", "-p", "first"}); got != 0 {
		t.Fatalf("first create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase", "-p", "first"}); got != 0 {
		t.Errorf("re-running with the same passphrase exit = %d, want 0", got)
	}

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase", "-p", "second"}); got == 0 {
		t.Error("a different passphrase silently replaced the cluster master key")
	}

	if got := readPassphrase(t, client); got != "first" {
		t.Errorf("stored passphrase = %q, want the original", got)
	}
}

// Unlocking is state inside the controller process, not an object in
// Kubernetes, so the CLI cannot perform it. It verifies what it can
// and then says where the unlock has to go — exiting 0 would leave the
// operator believing the cluster was unlocked when it was not.
func TestEncryptionEnterPassphraseIsRefusedNotFaked(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newKubeApp(t)

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase", "-p", "s3cret"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"encryption", "enter-passphrase", "-p", "s3cret"}); got == 0 {
		t.Fatal("enter-passphrase reported success it cannot deliver")
	}

	if !strings.Contains(errBuf.String(), "/v1/encryption/passphrase") {
		t.Errorf("the refusal does not say where the unlock has to go:\n%s", errBuf.String())
	}

	errBuf.Reset()

	if got := app.Run(t.Context(), []string{"encryption", "enter-passphrase", "-p", "wrong"}); got == 0 {
		t.Error("a wrong passphrase was accepted")
	}

	if !strings.Contains(errBuf.String(), "does not match") {
		t.Errorf("a wrong passphrase was not reported as such:\n%s", errBuf.String())
	}
}

// Without cluster access the command says so rather than failing
// obscurely deeper down.
func TestEncryptionWithoutKubeAccess(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase", "-p", "s3cret"}); got == 0 {
		t.Fatal("create-passphrase succeeded with no cluster access")
	}

	if !strings.Contains(errBuf.String(), "cluster access") {
		t.Errorf("the failure does not explain itself:\n%s", errBuf.String())
	}
}
