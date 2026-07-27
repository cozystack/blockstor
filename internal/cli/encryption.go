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

package cli

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cozystack/blockstor/pkg/passphrase"

	"github.com/cozystack/blockstor/internal/cli/command"
)

var (
	errNoKubeAccess = errors.New(
		"this command needs cluster access beyond the CRD surface; run it against a real cluster")
	errPassphraseMismatch   = errors.New("the supplied passphrase does not match the cluster's")
	errUnlockIsProcessState = errors.New(
		"unlocking is per-controller-process state, not a Kubernetes object; " +
			"send it to the controller's /v1/encryption/passphrase endpoint")
)

// encryptionPassphrase reads the passphrase an operator supplied.
//
// `-p` means --passphrase on this verb and --pastable everywhere else,
// so a bare positional is accepted too: the shared parser records the
// value as a positional when it sees the short form here.
func encryptionPassphrase(run *runContext) (string, error) {
	if value := run.Flags.Values["passphrase"]; value != "" {
		return value, nil
	}

	if len(run.Flags.Positionals) > 0 {
		return run.Flags.Positionals[0], nil
	}

	return "", fmt.Errorf("%w: no passphrase given", command.ErrUsage)
}

// encryptionCreatePassphrase implements `encryption create-passphrase`.
//
// The cluster master key lives in a Secret, so this writes it there —
// the same place the controller and the satellites read it from.
func encryptionCreatePassphrase(ctx context.Context, run *runContext) error {
	value, err := encryptionPassphrase(run)
	if err != nil {
		return err
	}

	client, namespace, err := run.kube(ctx)
	if err != nil {
		return err
	}

	name, err := passphrase.SecretName(ctx, client)
	if err != nil {
		return fmt.Errorf("resolve passphrase secret name: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       map[string][]byte{passphrase.SecretKey: []byte(value)},
	}

	err = client.Create(ctx, secret)
	if err == nil {
		return nil
	}

	if !isAlreadyExists(err) {
		return fmt.Errorf("create passphrase secret %s/%s: %w", namespace, name, err)
	}

	// An existing passphrase is not overwritten here: rotating the
	// master key re-encrypts every LUKS volume key, and silently
	// replacing it would leave every existing volume undecryptable.
	// Re-running with the SAME value stays a success so a pre-flight
	// step in a script is idempotent.
	current, err := passphrase.Read(ctx, client, namespace)
	if err != nil {
		return fmt.Errorf("read passphrase secret: %w", err)
	}

	if current != value {
		return fmt.Errorf("passphrase already set: %w", errPassphraseMismatch)
	}

	return nil
}

// encryptionEnterPassphrase implements `encryption enter-passphrase`.
//
// Entering the passphrase unlocks the CONTROLLER PROCESS — it is
// in-memory state there, not an object in Kubernetes — so a CLI that
// speaks to the API server cannot perform it. It verifies what it can
// (that the passphrase matches) and then says where the unlock has to
// go, rather than exiting 0 and leaving the operator believing the
// cluster is unlocked.
func encryptionEnterPassphrase(ctx context.Context, run *runContext) error {
	value, err := encryptionPassphrase(run)
	if err != nil {
		return err
	}

	client, namespace, err := run.kube(ctx)
	if err != nil {
		return err
	}

	current, err := passphrase.Read(ctx, client, namespace)
	if err != nil {
		return fmt.Errorf("read passphrase secret: %w", err)
	}

	if current != value {
		return fmt.Errorf("cannot unlock: %w", errPassphraseMismatch)
	}

	return fmt.Errorf("passphrase verified but not applied: %w", errUnlockIsProcessState)
}

// kube opens the raw cluster client, or explains that this invocation
// has none.
func (run *runContext) kube(ctx context.Context) (ctrlclient.Client, string, error) {
	if run.Kube == nil {
		return nil, "", errNoKubeAccess
	}

	client, namespace, err := run.Kube(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("open cluster client: %w", err)
	}

	return client, namespace, nil
}
