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
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/passphrase"
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

// keyringOpens reports whether the cluster's keyring opens with pass.
func keyringOpens(t *testing.T, client ctrlclient.Client, pass string) bool {
	t.Helper()

	ring, _, err := passphrase.Secrets{Reader: client, Namespace: testNamespace}.Keyring(t.Context())
	if err != nil || ring == nil {
		t.Fatalf("keyring %v, %v", ring, err)
	}

	_, err = ring.Opens([]byte(pass))

	return err == nil
}

// create-passphrase enrols the passphrase in the keyring the controller
// and the satellites unlock from.
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

		if !keyringOpens(t, client, "s3cret") {
			t.Errorf("%v: the keyring does not open with s3cret", flag)
		}
	}
}

// TestEncryptionPassphraseFromStdin: the master key must not have to
// be typed on the command line, where it lands in shell history and
// stays readable in /proc/<pid>/cmdline to every local user for the
// length of the call. Omitting the value reads it off stdin instead.
func TestEncryptionPassphraseFromStdin(t *testing.T) {
	t.Parallel()

	app, client, errBuf := newKubeApp(t)
	app.In = strings.NewReader("piped-s3cret\n")

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !keyringOpens(t, client, "piped-s3cret") {
		t.Errorf("the keyring does not open with piped-s3cret")
	}
}

// An empty stdin is still a refusal: `passphrase.Read` cannot tell an
// empty Secret from a missing one, so storing one wedges the cluster
// between two contradictory diagnoses.
func TestEncryptionPassphraseFromEmptyStdin(t *testing.T) {
	t.Parallel()

	app, _, _ := newKubeApp(t)
	app.In = strings.NewReader("\n")

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase"}); got == 0 {
		t.Fatal("an empty passphrase on stdin exited 0; want a refusal")
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

	if !keyringOpens(t, client, "first") {
		t.Errorf("the keyring does not open with first")
	}
}

// enter-passphrase proves the operator knows the master key, and that
// is what it checks. A wrong passphrase fails; the right one succeeds
// and says that the controller's own unlocked flag — which only drives
// the Suspended/Available column in its REST view, and gates nothing —
// was not touched.
func TestEncryptionEnterPassphrase(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newKubeApp(t)

	// Before a passphrase exists there is nothing to prove knowledge
	// of, so this is a failure rather than a vacuous success.
	if got := app.Run(t.Context(), []string{"encryption", "enter-passphrase", "-p", "s3cret"}); got == 0 {
		t.Fatal("enter-passphrase succeeded on a cluster with no passphrase")
	}

	errBuf.Reset()

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase", "-p", "s3cret"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"encryption", "enter-passphrase", "-p", "wrong"}); got == 0 {
		t.Error("a wrong passphrase was accepted")
	}

	if !strings.Contains(errBuf.String(), "does not match") {
		t.Errorf("a wrong passphrase was not reported as such:\n%s", errBuf.String())
	}

	errBuf.Reset()

	if got := app.Run(t.Context(), []string{"encryption", "enter-passphrase", "-p", "s3cret"}); got != 0 {
		t.Fatalf("the right passphrase exit = %d, want 0 (stderr: %s)", got, errBuf.String())
	}

	if !strings.Contains(errBuf.String(), "verified") {
		t.Errorf("a successful verification said nothing about what it did:\n%s", errBuf.String())
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

// The slot verbs read their passphrases from stdin or a file, never argv:
// add a recovery passphrase, list the slots, change and remove one; each
// passphrase opens exactly while its slot is there.
func TestEncryptionSlotVerbs(t *testing.T) {
	t.Parallel()

	app, client, errBuf := newKubeApp(t)

	run := func(want bool, stdin string, argv ...string) {
		t.Helper()

		app.In = strings.NewReader(stdin)

		if got := app.Run(t.Context(), argv) == 0; got != want {
			t.Fatalf("%v: success %v, want %v (stderr: %s)", argv, got, want, errBuf.String())
		}
	}

	run(true, "", "encryption", "create-passphrase", "-p", "first")
	run(false, "wrong\nsecond\n", "encryption", "add-passphrase", "--name", "recovery")
	run(false, "first\nsecond\n", "encryption", "add-passphrase")
	run(false, "first\nsecond\n", "encryption", "add-passphrase", "--name", "recovery", "first", "second")
	run(false, "first\nsecond\n", "encryption", "add-passphrase", "--name", "recovery", "--passphrase", "first")
	run(true, "first\nsecond\n", "encryption", "add-passphrase", "--name", "recovery")

	if !keyringOpens(t, client, "second") {
		t.Fatal("the added passphrase does not open")
	}

	var out bytes.Buffer

	app.Out = &out

	run(true, "", "encryption", "list-passphrases")

	listed := out.String()
	if !strings.Contains(listed, "encryption: unlocked") || !strings.Contains(listed, "recovery") || !strings.Contains(listed, " unlocks") {
		t.Fatalf("list:\n%s", listed)
	}

	ring, _, err := passphrase.Secrets{Reader: client, Namespace: testNamespace}.Keyring(t.Context())
	if err != nil || ring == nil || !strings.Contains(listed, "key ID: "+ring.KeyID+"\n") {
		t.Fatalf("list does not name the keyring's key ID (%v):\n%s", err, listed)
	}

	dir := t.TempDir()
	oldFile, newFile := dir+"/old", dir+"/new"

	if err := os.WriteFile(oldFile, []byte("second"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := os.WriteFile(newFile, []byte("third"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	run(false, "second\nthird\n", "encryption", "modify-passphrase", "second", "third")
	run(false, "first\nthird\n", "encryption", "add-passphrase", "--name", "r2", "--new-passphrase", "third")
	run(true, "", "encryption", "modify-passphrase", "--old-passphrase-file", oldFile, "--new-passphrase-file", newFile)

	if keyringOpens(t, client, "second") || !keyringOpens(t, client, "third") {
		t.Fatal("modify did not replace the slot")
	}

	run(false, "first\n", "encryption", "remove-passphrase", "generated")
	run(false, "third\n", "encryption", "remove-passphrase", "recovery")
	run(true, "first\n", "encryption", "remove-passphrase", "recovery")

	if keyringOpens(t, client, "third") {
		t.Fatal("the removed passphrase still opens")
	}
}

// A passphrase file is read byte for byte, as the passphrase Secret is: a
// trailing newline is part of the passphrase it enrols, so a file written
// from the Secret enrols what the Secret opens.
func TestEncryptionPassphraseFileIsVerbatim(t *testing.T) {
	t.Parallel()

	app, client, errBuf := newKubeApp(t)

	dir := t.TempDir()
	authFile, newFile := dir+"/auth", dir+"/new"

	if err := os.WriteFile(authFile, []byte("first"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := os.WriteFile(newFile, []byte("from-secret\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	for _, argv := range [][]string{
		{"encryption", "create-passphrase", "-p", "first"},
		{"encryption", "add-passphrase", "--name", "recovery", "--passphrase-file", authFile, "--new-passphrase-file", newFile},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v: exit %d (stderr: %s)", argv, got, errBuf.String())
		}
	}

	if !keyringOpens(t, client, "from-secret\n") || keyringOpens(t, client, "from-secret") {
		t.Fatal("the file's trailing newline was not kept in the enrolled passphrase")
	}
}

// enter-passphrase that leaves the cluster locked exits non-zero: the
// passphrase is right, but the named Secret does not open the keyring.
func TestEncryptionEnterOnALockedClusterFails(t *testing.T) {
	t.Parallel()

	app, client, errBuf := newKubeApp(t)
	ctx := t.Context()

	err := client.Create(ctx, &crdv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: crdv1alpha1.ControllerConfigName},
		Spec:       crdv1alpha1.ControllerConfigSpec{PassphraseSecretRef: &crdv1alpha1.PassphraseSecretRef{Name: "p"}},
	})
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: testNamespace},
		Data:       map[string][]byte{"MASTER_PASSPHRASE": []byte("piraeus")},
	}

	err = client.Create(ctx, secret)
	if err != nil {
		t.Fatalf("secret: %v", err)
	}

	_, err = passphrase.Bootstrap(ctx, client, passphrase.Secrets{Reader: client, Namespace: testNamespace}, time.Now())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if got := app.Run(ctx, []string{"encryption", "enter-passphrase", "--passphrase", "piraeus"}); got != 0 {
		t.Fatalf("enter on an unlocked cluster: exit %d (%s)", got, errBuf.String())
	}

	err = client.Delete(ctx, secret)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	if got := app.Run(ctx, []string{"encryption", "enter-passphrase", "--passphrase", "piraeus"}); got == 0 {
		t.Fatal("enter on a locked cluster exited 0")
	}

	if !strings.Contains(errBuf.String(), "stays locked") {
		t.Errorf("stderr %q does not say the cluster stays locked", errBuf.String())
	}
}

// modify-passphrase is LINSTOR's verb, and linstor-client passes
// --old-passphrase and --new-passphrase on the command line: the bundled
// CLI takes them too, and warns that they show in the process list.
func TestEncryptionModifyTakesLinstorsFlags(t *testing.T) {
	t.Parallel()

	app, client, errBuf := newKubeApp(t)

	if got := app.Run(t.Context(), []string{"encryption", "create-passphrase", "-p", "first"}); got != 0 {
		t.Fatalf("create: exit %d (%s)", got, errBuf.String())
	}

	errBuf.Reset()

	argv := []string{"encryption", "modify-passphrase", "--old-passphrase", "first", "--new-passphrase", "-second"}
	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("modify: exit %d (%s)", got, errBuf.String())
	}

	if !keyringOpens(t, client, "-second") || keyringOpens(t, client, "first") {
		t.Fatal("modify did not replace the slot")
	}

	if !strings.Contains(errBuf.String(), "process list") {
		t.Errorf("no warning about the process list: %q", errBuf.String())
	}
}
