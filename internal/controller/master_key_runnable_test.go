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

package controller_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// bootstrapBudget is how long the runnable gets to create the keyring.
const bootstrapBudget = 30 * time.Second

// On a cluster that names a piraeus-shaped passphrase Secret and has no
// keyring, the first pass of the master-key runnable creates one that the
// Secret opens, and the loop keeps running until its context ends.
func TestMasterKeyRunnableCreatesTheKeyring(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec: blockstoriov1alpha1.ControllerConfigSpec{
				PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{Name: "linstor-passphrase"},
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "linstor-passphrase", Namespace: "blockstor-system"},
			Data:       map[string][]byte{"MASTER_PASSPHRASE": []byte("piraeus")},
		},
	).Build()

	r := &controllerpkg.MasterKeyRunnable{Client: cli, Reader: cli, Namespace: "blockstor-system", Period: time.Millisecond}

	if !r.NeedLeaderElection() {
		t.Fatal("two controllers would each bootstrap")
	}

	// The bootstrap runs Argon2id at 64 MiB; under a full `go test ./...`
	// every package competes for the CPU, so the budget is generous. The
	// loop ends as soon as the keyring appears.
	ctx, cancel := context.WithTimeout(t.Context(), bootstrapBudget)
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- r.Start(ctx) }()

	secrets := passphrase.Secrets{Reader: cli, Namespace: "blockstor-system"}

	for ctx.Err() == nil {
		ring, _, err := secrets.Keyring(ctx)
		if err != nil {
			t.Fatalf("keyring: %v", err)
		}

		if ring != nil {
			if _, err := ring.Opens([]byte("piraeus")); err != nil {
				t.Fatalf("the Secret does not open the keyring: %v", err)
			}

			cancel()

			if err := <-done; err != nil {
				t.Fatalf("Start: %v", err)
			}

			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	var sec corev1.Secret

	_ = cli.Get(context.Background(), client.ObjectKey{Namespace: "blockstor-system", Name: passphrase.KeyringSecretName}, &sec)
	t.Fatalf("no keyring after %v of passes", bootstrapBudget)
}

// After modify-passphrase of the Secret's passphrase, a pass keeps the
// superseded slot while the Secret still holds the old passphrase and
// removes it once the Secret holds the new one.
func TestMasterKeyRunnableRemovesSupersededSlots(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	const ns = "blockstor-system"

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec:       blockstoriov1alpha1.ControllerConfigSpec{PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{Name: "p"}},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns},
			Data:       map[string][]byte{"MASTER_PASSPHRASE": []byte("old")},
		},
	).Build()

	r := &controllerpkg.MasterKeyRunnable{Client: cli, Reader: cli, Namespace: ns}
	r.Pass(t.Context())

	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := ops.Modify(t.Context(), []byte("old"), []byte("new"))
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	slots := func() int {
		ring, _, err := ops.Secrets.Keyring(t.Context())
		if err != nil || ring == nil {
			t.Fatalf("keyring: %v", err)
		}

		return len(ring.Slots)
	}

	r.Pass(t.Context())

	if got := slots(); got != 2 {
		t.Fatalf("%d slots while the Secret holds the old passphrase, want 2", got)
	}

	var sec corev1.Secret

	_ = cli.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "p"}, &sec)
	sec.Data["MASTER_PASSPHRASE"] = []byte("new")

	if err := cli.Update(t.Context(), &sec); err != nil {
		t.Fatalf("update: %v", err)
	}

	r.Pass(t.Context())

	if got := slots(); got != 1 {
		t.Fatalf("%d slots after the Secret moved, want 1", got)
	}
}

// Once the keyring exists and no slot is superseded, a pass reads no
// passphrase, so it runs no Argon2id: the steady state costs two Secret
// reads from Reader every period and no derivation.
func TestMasterKeyRunnableSteadyPassDerivesNothing(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	const ns = "blockstor-system"

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec:       blockstoriov1alpha1.ControllerConfigSpec{PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{Name: "p"}},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns},
			Data:       map[string][]byte{"MASTER_PASSPHRASE": []byte("piraeus")},
		},
	).Build()

	passReads := 0

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == "p" {
				passReads++
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})

	r := &controllerpkg.MasterKeyRunnable{Client: cli, Reader: cli, Namespace: ns}

	// The first pass creates the keyring, which reads the passphrase.
	r.Pass(t.Context())

	ring, _, err := passphrase.Secrets{Reader: base, Namespace: ns}.Keyring(t.Context())
	if err != nil || ring == nil {
		t.Fatalf("no keyring after the first pass: %v", err)
	}

	passReads = 0

	for range 3 {
		r.Pass(t.Context())
	}

	if passReads != 0 {
		t.Fatalf("steady passes read the passphrase %d times, want none", passReads)
	}
}

// A pass with nothing to do reads only through Reader, which production
// serves from watches, so an idle controller sends no request; Direct, the
// API reader, is left for the reads before a write.
func TestMasterKeyRunnableIdlePassReadsNothingDirect(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	const ns = "blockstor-system"

	watched := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec:       blockstoriov1alpha1.ControllerConfigSpec{PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{Name: "p"}},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns},
			Data:       map[string][]byte{"MASTER_PASSPHRASE": []byte("piraeus")},
		},
	).Build()

	directReads := 0

	direct := interceptor.NewClient(watched, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			directReads++

			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			directReads++

			return c.List(ctx, list, opts...)
		},
	})

	r := &controllerpkg.MasterKeyRunnable{Client: watched, Reader: watched, Direct: direct, Namespace: ns}

	// The first pass creates the keyring.
	r.Pass(t.Context())

	ring, _, err := passphrase.Secrets{Reader: watched, Namespace: ns}.Keyring(t.Context())
	if err != nil || ring == nil {
		t.Fatalf("no keyring after the first pass: %v", err)
	}

	directReads = 0

	for range 3 {
		r.Pass(t.Context())
	}

	if directReads != 0 {
		t.Errorf("idle passes read %d times past the watches, want none", directReads)
	}
}

// Reader trails the API server, so the read-modify-write that removes a
// superseded slot reads through Direct: from a keyring Reader holds at an
// old resourceVersion, every update would conflict.
func TestMasterKeyRunnableRemovesSupersededSlotsPastAStaleReader(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	const ns = "blockstor-system"

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec:       blockstoriov1alpha1.ControllerConfigSpec{PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{Name: "p"}},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns},
			Data:       map[string][]byte{"MASTER_PASSPHRASE": []byte("old")},
		},
	).Build()

	(&controllerpkg.MasterKeyRunnable{Client: cli, Reader: cli, Namespace: ns}).Pass(t.Context())

	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := ops.Modify(t.Context(), []byte("old"), []byte("new"))
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	var sec corev1.Secret

	_ = cli.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "p"}, &sec)
	sec.Data["MASTER_PASSPHRASE"] = []byte("new")

	if err := cli.Update(t.Context(), &sec); err != nil {
		t.Fatalf("update: %v", err)
	}

	// The keyring as a watch that has not caught up holds it: the same
	// content under an older resourceVersion.
	stale := interceptor.NewClient(cli, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			err := c.Get(ctx, key, obj, opts...)
			if err == nil && key.Name == passphrase.KeyringSecretName {
				obj.SetResourceVersion("1")
			}

			return err
		},
	})

	r := &controllerpkg.MasterKeyRunnable{Client: cli, Reader: stale, Direct: cli, Namespace: ns}
	r.Pass(t.Context())

	ring, _, err := ops.Secrets.Keyring(t.Context())
	if err != nil || ring == nil {
		t.Fatalf("keyring: %v", err)
	}

	if got := len(ring.Slots); got != 1 {
		t.Fatalf("%d slots after the Secret moved, want 1: the write read the stale keyring", got)
	}
}
