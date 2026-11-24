// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cozystack/blockstor/pkg/passphrase"
)

var errCachedSecretRead = errors.New("a Secret was read through the cached client")

// The passphrase and keyring Secrets are read past the cache: through the
// cached client the first read starts an informer over every Secret in the
// cluster, which needs cluster-wide list and watch on Secrets for the two this
// server reads. Status, verification and the read-before-write of a slot
// change all go through the reader; the write still goes through the client.
func TestThePassphraseSecretIsReadPastTheCache(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	base := newFakeRESTClient(t)
	seedEncryption(t, base, testPassphrase)

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

	status, err := srv.encryptionStatus(ctx)
	if err != nil || status.State != passphrase.StateUnlocked {
		t.Fatalf("status = %s, %v; want unlocked, read through the reader", status.State, err)
	}

	ops, err := srv.passphraseOps()
	if err != nil {
		t.Fatalf("ops: %v", err)
	}

	_, err = ops.Add(ctx, []byte(testPassphrase), []byte("second"), "recovery")
	if err != nil {
		t.Fatalf("add a slot: %v", err)
	}

	_, err = ops.Enter(ctx, []byte("second"))
	if err != nil {
		t.Errorf("enter the added passphrase: %v", err)
	}
}

// The resource view is polled, so the state it stamps `suspended` from is
// read through StateReader, which production serves from watches. The
// passphrase status endpoint keeps reading through SecretReader: right
// after an enter, the operator is told what the API server holds.
func TestTheViewReadsTheEncryptionStateThroughStateReader(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	base := newFakeRESTClient(t)
	seedEncryption(t, base, testPassphrase)

	counting := func(reads *int) client.Reader {
		return interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption,
			) error {
				*reads++

				return c.Get(ctx, key, obj, opts...)
			},
		})
	}

	var stateReads, secretReads int

	srv := &Server{
		Client:       base,
		SecretReader: counting(&secretReads),
		StateReader:  counting(&stateReads),
		Namespace:    testRESTNamespace,
	}

	if state := srv.encryptionState(ctx); state != passphrase.StateUnlocked {
		t.Fatalf("view state = %s, want unlocked", state)
	}

	if stateReads == 0 || secretReads != 0 {
		t.Errorf("the view read %d times through StateReader and %d through SecretReader, want only StateReader",
			stateReads, secretReads)
	}

	stateReads = 0

	status, err := srv.encryptionStatus(ctx)
	if err != nil || status.State != passphrase.StateUnlocked {
		t.Fatalf("status = %s, %v; want unlocked", status.State, err)
	}

	if secretReads == 0 || stateReads != 0 {
		t.Errorf("the status endpoint read %d times through SecretReader and %d through StateReader, want only SecretReader",
			secretReads, stateReads)
	}
}
