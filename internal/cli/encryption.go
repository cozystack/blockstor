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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"golang.org/x/term"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/keyring"
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

	if fd, ok := terminalFd(in); ok {
		return readPassphraseFromTerminal(run, fd)
	}

	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read passphrase: %w", err)
	}

	return strings.TrimRight(line, "\r\n"), nil
}

// terminalFd reports the descriptor to prompt on, when the input is an
// interactive terminal.
//
// os.File spells a descriptor as a uintptr while the terminal package
// wants an int. The range check is not ceremony for the linter: a value
// that does not fit is not a descriptor any syscall handed us, and
// truncating it would ask the terminal package about some unrelated
// open file. Falling through to the plain-line path is the right answer
// for input this cannot identify.
func terminalFd(in io.Reader) (int, bool) {
	file, ok := in.(*os.File)
	if !ok {
		return 0, false
	}

	raw := file.Fd()
	if raw > math.MaxInt {
		return 0, false
	}

	fd := int(raw)

	return fd, term.IsTerminal(fd)
}

// readPassphraseFromTerminal prompts without echoing, so the key does
// not end up on screen or in a scrollback buffer.
func readPassphraseFromTerminal(run *runContext, fd int) (string, error) {
	fmt.Fprint(run.Err, "Passphrase: ")

	typed, err := term.ReadPassword(fd)

	fmt.Fprintln(run.Err)

	if err != nil {
		return "", fmt.Errorf("read passphrase: %w", err)
	}

	return string(typed), nil
}

// encryptionOps opens the passphrase operations against the cluster: the
// CLI talks to the Kubernetes API directly, and reads Secrets by name.
func encryptionOps(ctx context.Context, run *runContext) (*passphrase.Ops, error) {
	client, namespace, err := run.kube(ctx)
	if err != nil {
		return nil, err
	}

	return &passphrase.Ops{Client: client, Secrets: passphrase.Secrets{Reader: client, Namespace: namespace}, Now: nil}, nil
}

// encryptionCreatePassphrase implements `encryption create-passphrase`:
// the cluster's first master passphrase, enrolled in a keyring slot; the
// cluster unlocks from a generated passphrase in a Secret blockstor owns.
// A repeat with the same passphrase is a no-op.
func encryptionCreatePassphrase(ctx context.Context, run *runContext) error {
	value, err := encryptionPassphrase(run)
	if err != nil {
		return err
	}

	ops, err := encryptionOps(ctx, run)
	if err != nil {
		return err
	}

	_, err = ops.Create(ctx, []byte(value))
	if errors.Is(err, passphrase.ErrKeyringExists) {
		return fmt.Errorf("passphrase already set: %w", errPassphraseMismatch)
	}

	if err != nil {
		return fmt.Errorf("create the master passphrase: %w", err)
	}

	return nil
}

// encryptionEnterPassphrase implements `encryption enter-passphrase`: it
// verifies the passphrase against every slot, and on a cluster locked out
// of its generated unlock Secret it re-creates that Secret.
func encryptionEnterPassphrase(ctx context.Context, run *runContext) error {
	value, err := encryptionPassphrase(run)
	if err != nil {
		return err
	}

	ops, err := encryptionOps(ctx, run)
	if err != nil {
		return err
	}

	result, err := ops.Enter(ctx, []byte(value))
	if err != nil {
		return passphraseVerbError("cannot unlock", err)
	}

	if result.Status.State == passphrase.StateLocked {
		return fmt.Errorf("%w: the passphrase Secret %s does not open the keyring", errStaysLocked, result.Status.Ref.Name)
	}

	notice := "passphrase verified"
	if result.Repaired {
		notice = "passphrase verified; re-created the unlock Secret " + passphrase.UnlockSecretName
	}

	return writeNotice(run, notice)
}

// encryptionModifyPassphrase implements `encryption modify-passphrase`:
// linstor-client's --old-passphrase and --new-passphrase, or the file,
// terminal and stdin forms of the blockstor slot verbs.
func encryptionModifyPassphrase(ctx context.Context, run *runContext) error {
	input, err := newSlotPassphrases(run, 0, true)
	if err != nil {
		return err
	}

	oldValue, err := input.read("old-passphrase", "Old passphrase: ")
	if err != nil {
		return err
	}

	newValue, err := input.read("new-passphrase", "New passphrase: ")
	if err != nil {
		return err
	}

	ops, err := encryptionOps(ctx, run)
	if err != nil {
		return err
	}

	result, err := ops.Modify(ctx, []byte(oldValue), []byte(newValue))
	if err != nil {
		return passphraseVerbError("cannot modify the passphrase", err)
	}

	if result.SecretToUpdate == "" {
		return nil
	}

	return writeNotice(run, "the cluster still unlocks with the old passphrase from Secret "+result.SecretToUpdate+
		"; put the new one there, and the old one's slot is removed once the Secret opens the new one")
}

// encryptionAddPassphrase implements `encryption add-passphrase --name`.
func encryptionAddPassphrase(ctx context.Context, run *runContext) error {
	name := run.Flags.Values["name"]
	if name == "" {
		return fmt.Errorf("%w: --name is required", command.ErrUsage)
	}

	input, err := newSlotPassphrases(run, 0, false)
	if err != nil {
		return err
	}

	auth, err := input.read("passphrase", "Passphrase: ")
	if err != nil {
		return err
	}

	newValue, err := input.read("new-passphrase", "New passphrase: ")
	if err != nil {
		return err
	}

	ops, err := encryptionOps(ctx, run)
	if err != nil {
		return err
	}

	slotID, err := ops.Add(ctx, []byte(auth), []byte(newValue), name)
	if err != nil {
		return passphraseVerbError("cannot add the passphrase", err)
	}

	return writeNotice(run, "added passphrase "+name+" as slot "+slotID)
}

// encryptionRemovePassphrase implements `encryption remove-passphrase`.
func encryptionRemovePassphrase(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) != 1 || run.Flags.Positionals[0] == "" {
		return fmt.Errorf("%w: name the slot to remove, and nothing else", command.ErrUsage)
	}

	input, err := newSlotPassphrases(run, 1, false)
	if err != nil {
		return err
	}

	auth, err := input.read("passphrase", "Passphrase of a slot that stays: ")
	if err != nil {
		return err
	}

	ops, err := encryptionOps(ctx, run)
	if err != nil {
		return err
	}

	err = ops.Remove(ctx, []byte(auth), run.Flags.Positionals[0])
	if err != nil {
		return passphraseVerbError("cannot remove the passphrase", err)
	}

	return nil
}

// encryptionListPassphrases implements `encryption list-passphrases`: one
// line per slot, no key material, the unlock slot marked.
func encryptionListPassphrases(ctx context.Context, run *runContext) error {
	ops, err := encryptionOps(ctx, run)
	if err != nil {
		return err
	}

	slots, status, err := ops.List(ctx)
	if err != nil {
		return fmt.Errorf("list the passphrases: %w", err)
	}

	_, err = fmt.Fprintf(run.Out, "encryption: %s\n", status.State)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	for _, slot := range slots {
		marks := ""
		if slot.Unlocks {
			marks += " unlocks"
		}

		if slot.Superseded {
			marks += " superseded"
		}

		_, err = fmt.Fprintf(run.Out, "%s\t%s\t%s\t%s\t%s%s\n",
			slot.ID, slot.Name, slot.Origin, slot.CreatedAt.Format("2006-01-02T15:04:05Z07:00"), slot.KDF, marks)
		if err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}

	return nil
}

// slotPassphrases reads the passphrases of the blockstor-only slot verbs:
// from `--<name>-file`, or else from the terminal, or else one per line
// from stdin. Never from argv, where they would land in shell history and
// in /proc/<pid>/cmdline.
type slotPassphrases struct {
	run   *runContext
	lines *bufio.Reader

	// argvFlags is set for modify-passphrase, a LINSTOR verb whose
	// --old-passphrase and --new-passphrase linstor-client passes on
	// the command line; they are honoured with a warning.
	argvFlags bool
}

var errStaysLocked = errors.New("the passphrase is right, but the cluster stays locked")

var errPassphraseOnArgv = errors.New(
	"passphrases of this command are read from a file, the terminal or stdin, never from the command line")

func newSlotPassphrases(run *runContext, positionals int, argvFlags bool) (*slotPassphrases, error) {
	onArgv := run.Flags.Values["passphrase"] != "" || run.Flags.Values["p-value"] != "" || len(run.Flags.Positionals) > positionals
	if !argvFlags {
		onArgv = onArgv || run.Flags.Values["old-passphrase"] != "" || run.Flags.Values["new-passphrase"] != ""
	}

	if onArgv {
		return nil, fmt.Errorf("%w: %w", command.ErrUsage, errPassphraseOnArgv)
	}

	in := run.In
	if in == nil {
		in = os.Stdin
	}

	return &slotPassphrases{run: run, lines: bufio.NewReader(in), argvFlags: argvFlags}, nil
}

func (p *slotPassphrases) read(name, prompt string) (string, error) {
	var (
		value string
		err   error
	)

	switch path := p.run.Flags.Values[name+"-file"]; {
	case p.argvFlags && p.run.Flags.Values[name] != "":
		value = p.run.Flags.Values[name]

		fmt.Fprintf(p.run.Err, "warning: --%s is visible in the process list and shell history; "+
			"--%s-file or stdin keeps it out\n", name, name)
	case path != "":
		var data []byte

		data, err = os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read --%s-file: %w", name, err)
		}

		// Byte for byte, as the passphrase Secret and linstor-migrate
		// take it: a newline the file ends with is part of the
		// passphrase, so a file written from the Secret enrols the
		// passphrase the Secret opens.
		value = string(data)
	default:
		value, err = p.prompt(prompt)
		if err != nil {
			return "", err
		}
	}

	if value == "" {
		return "", fmt.Errorf("%w: the %s may not be empty", command.ErrUsage, strings.ReplaceAll(name, "-", " "))
	}

	return value, nil
}

func (p *slotPassphrases) prompt(prompt string) (string, error) {
	in := p.run.In
	if in == nil {
		in = os.Stdin
	}

	if fd, ok := terminalFd(in); ok {
		fmt.Fprint(p.run.Err, prompt)

		typed, err := term.ReadPassword(fd)

		fmt.Fprintln(p.run.Err)

		if err != nil {
			return "", fmt.Errorf("read passphrase: %w", err)
		}

		return string(typed), nil
	}

	line, err := p.lines.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read passphrase: %w", err)
	}

	return strings.TrimRight(line, "\r\n"), nil
}

func passphraseVerbError(what string, err error) error {
	switch {
	case errors.Is(err, keyring.ErrWrongPassphrase):
		return fmt.Errorf("%s: %w", what, errPassphraseMismatch)
	case errors.Is(err, passphrase.ErrNoKeyring):
		return fmt.Errorf("%s: %w", what, errNoPassphraseSet)
	default:
		return fmt.Errorf("%s: %w", what, err)
	}
}

func writeNotice(run *runContext, notice string) error {
	_, err := fmt.Fprintln(run.Err, notice)
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

// errLUKSWithoutPassphrase refuses a LUKS layer on a cluster that has no
// master key yet.
var errLUKSWithoutPassphrase = errors.New(
	"the cluster has no encryption passphrase; run `encryption create-passphrase` first")

// checkLUKSPrerequisite refuses a definition that asks for LUKS before the
// cluster can encrypt anything.
//
// Without a master key the satellite has nothing to derive a volume key from:
// the resource is created, the volume never opens, and the operator is left
// reading satellite logs to discover a missing prerequisite the command could
// have named. REST refuses the same shape up front.
//
// Reachability, not correctness, is the reason for the nil checks. The CLI
// reaches the Secret through the same Kubernetes client the passphrase verbs
// use, and that client is absent in unit tests and on hosts wired only for
// the CRD surface. Where the passphrase cannot be read at all, the check
// stands down rather than refusing a definition it cannot judge.
func checkLUKSPrerequisite(ctx context.Context, run *runContext, layers []string) error {
	if !apiv1.LayerInStack(layers, apiv1.LayerKindLUKS) {
		return nil
	}

	if run.Kube == nil {
		return nil
	}

	client, namespace, err := run.kube(ctx)
	if err != nil {
		return nil //nolint:nilerr // no cluster access: nothing to judge against
	}

	// A read FAILURE is not an absent passphrase. Returning nil here would
	// let an apiserver blip write a LUKS stack onto a cluster that may have
	// no key, which is the one direction this guard must not take — REST
	// answers 400 at the same point.
	ring, _, err := passphrase.Secrets{Reader: client, Namespace: namespace}.Keyring(ctx)
	if err != nil {
		return fmt.Errorf("read the keyring to check the %s prerequisite: %w", apiv1.LayerKindLUKS, err)
	}

	if ring != nil {
		return nil
	}

	return fmt.Errorf("layer list includes %s: %w", apiv1.LayerKindLUKS, errLUKSWithoutPassphrase)
}
