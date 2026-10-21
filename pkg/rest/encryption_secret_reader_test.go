// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cozystack/blockstor/pkg/passphrase"
)

var errCachedSecretRead = errors.New("a Secret was read through the cached client")

// The passphrase Secret is read past the cache: through the cached client the
// first read starts an informer over every Secret in the cluster, which needs
// cluster-wide list and watch on Secrets for the one this server reads. Both
// the read and the read-before-write of a passphrase change go through the
// reader, and the write itself still goes through the client.
func TestThePassphraseSecretIsReadPastTheCache(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	base := newFakeRESTClient(t)

	if err := base.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: passphrase.DefaultSecretName, Namespace: testRESTNamespace},
		Data:       map[string][]byte{passphraseSecretKey: []byte("first")},
	}); err != nil {
		t.Fatalf("seed the Secret: %v", err)
	}

	cached := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption,
		) error {
			if _, isSecret := obj.(*corev1.Secret); isSecret {
				return errCachedSecretRead
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})

	srv := &Server{Client: cached, SecretReader: base, Namespace: testRESTNamespace}

	got, err := srv.readPassphraseSecret(ctx)
	if err != nil || got != "first" {
		t.Fatalf("read = %q, %v; want the seeded passphrase read through the reader", got, err)
	}

	if err := srv.writePassphraseSecret(ctx, "second"); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got, err := srv.readPassphraseSecret(ctx); err != nil || got != "second" {
		t.Errorf("read after the write = %q, %v; want the new passphrase", got, err)
	}
}
