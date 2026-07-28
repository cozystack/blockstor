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
		err := writeHelp(a.Out)
		if err != nil {
			return a.fail(err)
		}

		return exitOK
	}

	cmd, rest, err := command.Resolve(argv)
	if err != nil {
		return a.fail(err)
	}

	flags, err := parseFlags(rest)
	if err != nil {
		return a.fail(err)
	}

	run, ok := handlers[cmd.String()]
	if !ok {
		return a.fail(fmt.Errorf("%w: %s", ErrNotImplemented, cmd))
	}

	backend, err := a.StoreFor(ctx)
	if err != nil {
		return a.fail(err)
	}

	a.noteIgnoredFlags(flags)

	mode, err := color.ParseMode(flags.Color)
	if err != nil {
		return a.fail(fmt.Errorf("%w: %w", command.ErrUsage, err))
	}

	err = run(ctx, &runContext{
		Store: backend,
		Out:   a.Out,
		Err:   a.Err,
		Flags: flags,
		Color: color.New(color.EnabledFor(mode, a.tty())),
		Kube:  a.KubeFor,
	})
	if err != nil {
		return a.fail(err)
	}

	return exitOK
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
