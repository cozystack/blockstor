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

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

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

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
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
	t.Fatal("no keyring after a second of passes")
}
