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

// Package cli wires the command surface to the store and turns the
// result into an exit code.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli/color"
	"github.com/cozystack/blockstor/internal/cli/command"
)

// Exit codes. These mirror the upstream client because this
// repository's replay workflows assert them: a client-side rejection
// and an API-level rejection are different failures and scripts branch
// on the difference.
const (
	exitOK     = 0
	exitUsage  = 2
	exitFailed = 10
)

// ErrNotImplemented marks a registered command with no handler yet.
var ErrNotImplemented = errors.New("not implemented")

// App is one CLI invocation.
type App struct {
	Out io.Writer
	Err io.Writer

	// In is where a secret is read from when the operator does not
	// want it on the command line. nil means os.Stdin.
	In io.Reader

	// StoreFor opens the store. Injected so tests run against the
	// in-memory store and the real binary against the cluster.
	StoreFor func(ctx context.Context) (store.Store, error)

	// IsTTY reports whether Out is an interactive terminal; nil means
	// "detect from Out".
	IsTTY func() bool

	// KubeFor opens a raw Kubernetes client and the namespace the
	// controller runs in. Only the handful of commands that touch
	// objects outside the CRD surface — the cluster passphrase lives
	// in a Secret — need it, so it may be nil and those commands then
	// say why they cannot run rather than failing obscurely.
	KubeFor func(ctx context.Context) (ctrlclient.Client, string, error)
}

// handler runs one command.
type handler func(ctx context.Context, rc *runContext) error

// runContext carries everything a handler needs.
type runContext struct {
	Store store.Store
	Out   io.Writer
	Err   io.Writer
	In    io.Reader
	Flags *flagSet
	Color color.Writer

	// Kube is nil unless the invocation was wired with cluster
	// access beyond the CRD surface.
	Kube func(ctx context.Context) (ctrlclient.Client, string, error)
}

// Run executes argv and returns the process exit code.
func (a *App) Run(ctx context.Context, argv []string) int {
	// An operator who types `blockstor` alone wants to know what it
	// can do, not a bare error — but an invocation that names no
	// command is still a malformed one, so the tree goes to stderr and
	// the exit code stays the client-side rejection the replay
	// workflows assert. Asking for help EXPLICITLY is a success, and
	// prints to stdout so it can be piped.
	if len(argv) == 0 {
		err := writeHelp(a.Err)
		if err != nil {
			return a.fail(err)
		}

		return exitUsage
	}

	if isHelpRequest(argv) {
		return a.help()
	}

	// `blockstor rd --help` names an object without a verb. Resolve
	// would reject "--help" as an unknown subcommand, so answer it
	// here, before the grammar gets a chance to be unhelpful.
	if len(argv) > 1 && isHelpRequest(argv[1:]) {
		noun, ok := command.Lookup(argv[0])
		if ok {
			return a.nounHelp(noun)
		}
	}

	cmd, rest, err := command.Resolve(argv)
	if err != nil {
		return a.fail(err)
	}

	// `blockstor r l --help` asks about THAT command, not for a flag
	// and not for the whole tree.
	if isHelpRequest(rest) {
		return a.commandHelp(cmd)
	}

	flags, err := parseFlags(rest)
	if err != nil {
		return a.fail(err)
	}

	run, ok := handlers[cmd.String()]
	if !ok {
		return a.fail(fmt.Errorf("%w: %s", ErrNotImplemented, cmd))
	}

	return a.dispatch(ctx, cmd, run, flags)
}

// UnimplementedCommands lists registered commands with no handler, so
// the gap between the advertised surface and the working one is
// visible rather than discovered by an operator mid-incident.
func (a *App) UnimplementedCommands() []string {
	var missing []string

	for _, noun := range command.Nouns() {
		for _, verb := range noun.Verbs {
			key := noun.Name + " " + verb.Name
			if _, ok := handlers[key]; !ok {
				missing = append(missing, key)
			}
		}
	}

	sort.Strings(missing)

	return missing
}

// dispatch resolves the client-side settings, opens the store and runs
// the command.
//
// Colour is parsed BEFORE the cluster is opened. Both are client-side
// mistakes, and the other order made `--color=bogus` exit 10 on an
// unreachable cluster and 2 on a reachable one — the same typo
// classified two different ways — besides opening a connection for an
// invocation already known to be invalid.
func (a *App) dispatch(ctx context.Context, cmd command.Command, run handler, flags *flagSet) int {
	mode, err := color.ParseMode(flags.Color)
	if err != nil {
		return a.fail(fmt.Errorf("%w: %w", command.ErrUsage, err))
	}

	var backend store.Store

	// A command that answers from the binary alone must not need a
	// cluster to do it. Opening the store unconditionally made
	// `controller version` exit 10 on a host with no kubeconfig —
	// which is exactly the sanity check a CI image runs on a freshly
	// built binary, before any cluster exists.
	if !localOnly[cmd.String()] {
		backend, err = a.StoreFor(ctx)
		if err != nil {
			return a.fail(err)
		}
	}

	a.noteIgnoredFlags(flags)

	err = run(ctx, &runContext{
		Store: backend,
		Out:   a.Out,
		Err:   a.Err,
		In:    a.In,
		Flags: flags,
		Color: color.New(color.EnabledFor(mode, a.tty())),
		Kube:  a.KubeFor,
	})
	if err != nil {
		return a.fail(err)
	}

	return exitOK
}

// localOnly names the commands that answer without a cluster, and so
// run with a nil store. Keep it to handlers that genuinely touch
// nothing: everything else wants the connection error up front rather
// than a nil dereference halfway through.
//
// cmdControllerVersion is the one command that answers from the binary
// alone, named once so the dispatch exemption and the handler table
// cannot drift apart.
const cmdControllerVersion = "controller version"

//nolint:gochecknoglobals // a fixed, tiny set matched in one place
var localOnly = map[string]bool{
	cmdControllerVersion: true,
}

// nounHelp writes one object's verbs to stdout.
func (a *App) nounHelp(noun command.Noun) int {
	err := writeNounHelp(a.Out, noun)
	if err != nil {
		return a.fail(err)
	}

	return exitOK
}

// commandHelp writes one command's synopsis to stdout.
func (a *App) commandHelp(cmd command.Command) int {
	verb, ok := command.Describe(cmd.Noun, cmd.Verb)
	if !ok {
		return a.help()
	}

	err := writeCommandHelp(a.Out, cmd, verb)
	if err != nil {
		return a.fail(err)
	}

	return exitOK
}

// help writes the command tree to stdout so it can be piped.
func (a *App) help() int {
	err := writeHelp(a.Out)
	if err != nil {
		return a.fail(err)
	}

	return exitOK
}

// noteIgnoredFlags warns about a flag this client accepts for
// compatibility but cannot act on. Staying silent would let
// `--controllers <other-cluster>` read the kubeconfig's cluster
// without a word.
func (a *App) noteIgnoredFlags(flags *flagSet) {
	if flags.IgnoredControllers {
		fmt.Fprintln(a.Err,
			"note: --controllers is ignored; this client reads the cluster named by your kubeconfig")
	}
}

// fail reports err and maps it to an exit code: a client-side
// rejection (unknown command, bad flag) is 2, everything else — an API
// or backend failure — is 10.
func (a *App) fail(err error) int {
	fmt.Fprintf(a.Err, "error: %v\n", err)

	if errors.Is(err, command.ErrUsage) || errors.Is(err, ErrNotImplemented) {
		return exitUsage
	}

	return exitFailed
}

// tty reports whether the output is an interactive terminal.
func (a *App) tty() bool {
	if a.IsTTY != nil {
		return a.IsTTY()
	}

	file, ok := a.Out.(*os.File)
	if !ok {
		return false
	}

	info, err := file.Stat()
	if err != nil {
		return false
	}

	return info.Mode()&os.ModeCharDevice != 0
}
