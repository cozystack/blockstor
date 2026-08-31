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
	"bufio"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cozystack/blockstor/pkg/passphrase"

	"github.com/cozystack/blockstor/internal/cli/command"
)

var (
	errNoKubeAccess = errors.New(
		"this command needs cluster access beyond the CRD surface; run it against a real cluster")
	errPassphraseMismatch = errors.New("the supplied passphrase does not match the cluster's")
	errNoPassphraseSet    = errors.New("this cluster has no passphrase; create one first")
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

	if len(run.Flags.Positionals) > 0 && run.Flags.Positionals[0] != "" {
		return run.Flags.Positionals[0], nil
	}

	value, err := readPassphrase(run)
	if err != nil {
		return "", err
	}

	// An empty value is REJECTED, not stored. `passphrase.Read` cannot
	// tell an empty Secret from a missing one, so an empty master key
	// wedges the cluster into a state where create reports "already
	// set" and enter reports "none set" — two contradictory diagnoses
	// with no way out through this CLI — while encrypting every volume
	// with nothing.
	if value == "" {
		return "", fmt.Errorf("%w: the passphrase may not be empty", command.ErrUsage)
	}

	return value, nil
}

// readPassphrase takes the cluster master key off stdin, so it does not
// have to be typed on the command line.
//
// On argv the key lands in shell history and stays readable in
// /proc/<pid>/cmdline to every local user for as long as the call runs.
// Both spellings still work — scripts depend on them — but this is the
// one to reach for, and it is what an operator gets by default now that
// omitting the value prompts instead of failing.
//
// An interactive terminal is read without echo; anything else is read
// as a plain line, so `echo secret | blockstor …` and a redirect from a
// file both work.
func readPassphrase(run *runContext) (string, error) {
	in := run.In
	if in == nil {
		in = os.Stdin
	}

	if file, ok := in.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		fmt.Fprint(run.Err, "Passphrase: ")

		typed, err := term.ReadPassword(int(file.Fd()))

		fmt.Fprintln(run.Err)

		if err != nil {
			return "", fmt.Errorf("read passphrase: %w", err)
		}

		return string(typed), nil
	}

	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read passphrase: %w", err)
	}

	return strings.TrimRight(line, "\r\n"), nil
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

	if subtle.ConstantTimeCompare([]byte(current), []byte(value)) != 1 {
		return fmt.Errorf("passphrase already set: %w", errPassphraseMismatch)
	}

	return nil
}

// encryptionEnterPassphrase implements `encryption enter-passphrase`.
//
// The verb proves the operator knows the cluster master key, and that
// is exactly what happens here: the supplied value is compared against
// the Secret, and a wrong one fails.
//
// The controller additionally flips an in-memory unlocked flag when it
// serves this over REST. That flag has one consumer — it sets
// `state.suspended` on LUKS resources in the REST view — and gates
// nothing: the LUKS create check reads the Secret, and satellites
// decrypt from the Secret too. It is also per-process, so with several
// apiserver replicas it already disagrees with itself. This CLI
// therefore verifies and succeeds, and says on stderr that the
// server-side display flag was not touched, rather than refusing a
// command whose real effect it can deliver in full.
//
// The comparison is constant-time out of habit rather than necessity:
// this runs client-side, after the caller's own RBAC already let them
// read the Secret, so there is no remote attacker to time. (It also
// still leaks the length, which ConstantTimeCompare does not hide.)
// The controller's REST path is where the property actually matters.
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

	if current == "" {
		return fmt.Errorf("cannot unlock: %w", errNoPassphraseSet)
	}

	if subtle.ConstantTimeCompare([]byte(current), []byte(value)) != 1 {
		return fmt.Errorf("cannot unlock: %w", errPassphraseMismatch)
	}

	_, err = fmt.Fprintln(run.Err,
		"passphrase verified; the controller's own unlocked flag (which only drives the "+
			"Suspended/Available column in its REST view) is untouched")
	if err != nil {
		return fmt.Errorf("write notice: %w", err)
	}

	return nil
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
