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
	"errors"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

var errBackend = errors.New("backend exploded")

// newApp wires the CLI against an in-memory store, so dispatch and
// rendering are exercised without a cluster.
func newApp(t *testing.T, seed func(context.Context, store.Store)) (*cli.App, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()

	backend := store.NewInMemory()

	if seed != nil {
		seed(t.Context(), backend)
	}

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return backend, nil
		},
	}

	return app, &out, &errBuf
}

// appStore reaches the backend an App was wired with, so a write test
// can assert on what actually landed rather than on what the CLI
// printed.
func appStore(t *testing.T, app *cli.App) store.Store {
	t.Helper()

	backend, err := app.StoreFor(t.Context())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	return backend
}

func seedResource(ctx context.Context, backend store.Store) {
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
}

// The exit code is a contract: this repository's replay workflows
// assert 0 for success and 2 for a client-side (argparse-class)
// rejection, with 10 reserved for an API-level one. A wrong code turns
// a failing command into a passing test somewhere else.
func TestExitCodes(t *testing.T) {
	t.Parallel()

	t.Run("success is 0", func(t *testing.T) {
		t.Parallel()

		app, _, _ := newApp(t, seedResource)
		if got := app.Run(t.Context(), []string{"r", "l"}); got != 0 {
			t.Errorf("exit = %d, want 0", got)
		}
	})

	t.Run("unknown command is 2", func(t *testing.T) {
		t.Parallel()

		app, _, errBuf := newApp(t, nil)

		got := app.Run(t.Context(), []string{"frobnicate", "list"})
		if got != 2 {
			t.Errorf("exit = %d, want 2 (client-side rejection)", got)
		}

		if errBuf.Len() == 0 {
			t.Error("a rejected command printed nothing to stderr")
		}
	})

	t.Run("no arguments is 2", func(t *testing.T) {
		t.Parallel()

		app, _, _ := newApp(t, nil)
		if got := app.Run(t.Context(), nil); got != 2 {
			t.Errorf("exit = %d, want 2", got)
		}
	})

	t.Run("backend failure is 10", func(t *testing.T) {
		t.Parallel()

		var out, errBuf bytes.Buffer

		app := &cli.App{
			Out: &out,
			Err: &errBuf,
			StoreFor: func(context.Context) (store.Store, error) {
				return nil, errBackend
			},
		}

		got := app.Run(t.Context(), []string{"r", "l"})
		if got != 10 {
			t.Errorf("exit = %d, want 10 (API-level failure)", got)
		}

		if !strings.Contains(errBuf.String(), "backend exploded") {
			t.Errorf("stderr = %q, want the underlying cause", errBuf.String())
		}
	})
}

// A listing renders the table an operator expects, on stdout, with the
// diagnostics kept on stderr so a pipeline reads clean data.
func TestResourceListRenders(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedResource)

	if got := app.Run(t.Context(), []string{"resource", "list"}); got != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %s)", got, errBuf.String())
	}

	for _, want := range []string{"ResourceName", "Node", "State", "pvc-x", "node-1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output is missing %q:\n%s", want, out.String())
		}
	}

	if errBuf.Len() != 0 {
		t.Errorf("stderr should stay clean on success, got: %s", errBuf.String())
	}
}

// The short aliases reach the same handler as the long form — that is
// how every script in this repo invokes the CLI.
func TestAliasReachesSameHandler(t *testing.T) {
	t.Parallel()

	appLong, longOut, _ := newApp(t, seedResource)
	appShort, shortOut, _ := newApp(t, seedResource)

	if got := appLong.Run(t.Context(), []string{"resource", "list"}); got != 0 {
		t.Fatalf("long form exit = %d", got)
	}

	if got := appShort.Run(t.Context(), []string{"r", "l"}); got != 0 {
		t.Fatalf("short form exit = %d", got)
	}

	if longOut.String() != shortOut.String() {
		t.Errorf("alias produced different output:\n--- long ---\n%s\n--- short ---\n%s",
			longOut.String(), shortOut.String())
	}
}

// `-m` switches to the machine-readable envelope, and the table
// renderer is bypassed entirely.
func TestMachineReadableFlag(t *testing.T) {
	t.Parallel()

	for _, flag := range []string{"-m", "--machine-readable"} {
		app, out, _ := newApp(t, seedResource)

		if got := app.Run(t.Context(), []string{"r", "l", flag}); got != 0 {
			t.Fatalf("%s: exit = %d", flag, got)
		}

		if !strings.HasPrefix(strings.TrimSpace(out.String()), "[[") {
			t.Errorf("%s did not produce the double-nested envelope:\n%s", flag, out.String())
		}

		if strings.Contains(out.String(), "ResourceName") {
			t.Errorf("%s still rendered a table:\n%s", flag, out.String())
		}
	}
}

// Writing to a buffer is not a terminal, so colour stays off by
// default — the harnesses grep this output. `--color=always` forces it
// on for a human piping into `less -R`.
func TestColourGating(t *testing.T) {
	t.Parallel()

	app, out, _ := newApp(t, seedResource)

	if got := app.Run(t.Context(), []string{"r", "l"}); got != 0 {
		t.Fatalf("exit = %d", got)
	}

	if strings.Contains(out.String(), "\x1b[") {
		t.Errorf("non-terminal output was coloured:\n%q", out.String())
	}

	forced, forcedOut, _ := newApp(t, seedResource)

	if got := forced.Run(t.Context(), []string{"r", "l", "--color=always"}); got != 0 {
		t.Fatalf("exit = %d", got)
	}

	if !strings.Contains(forcedOut.String(), "\x1b[") {
		t.Errorf("--color=always produced no colour:\n%q", forcedOut.String())
	}
}

// Every command in the registry is dispatchable. A command advertised
// in the grammar but missing a handler is worse than one that was
// never advertised: an operator finds it in the help, reaches for it
// mid-incident, and gets an error instead of an action.
func TestEveryRegisteredCommandIsReachable(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	for _, missing := range app.UnimplementedCommands() {
		t.Errorf("the grammar advertises %q but nothing implements it", missing)
	}
}

// The command tree is generated from the registry, so it cannot
// advertise something that does not dispatch. Asking for it
// explicitly is a success and prints to stdout, so `blockstor help |
// grep` works; naming no command at all is still a malformed
// invocation, so the tree goes to stderr and the exit code stays the
// client-side rejection scripts branch on.
func TestHelp(t *testing.T) {
	t.Parallel()

	for _, argv := range [][]string{{"help"}, {"--help"}, {"-h"}} {
		app, out, errBuf := newApp(t, nil)

		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v: exit = %d, want 0", argv, got)
		}

		for _, want := range []string{"resource-definition", "toggle-disk", "spawn-resources", "(rd)"} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("%v: help is missing %q:\n%s", argv, want, out.String())
			}
		}

		if errBuf.Len() != 0 {
			t.Errorf("%v: an explicit help wrote to stderr: %s", argv, errBuf.String())
		}
	}

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), nil); got != 2 {
		t.Errorf("no arguments exit = %d, want 2", got)
	}

	if !strings.Contains(errBuf.String(), "usage:") {
		t.Errorf("a bare invocation printed no usage:\n%s", errBuf.String())
	}

	if out.Len() != 0 {
		t.Errorf("a bare invocation wrote to stdout: %s", out.String())
	}
}

// TestNounHelpListsItsVerbs: `blockstor rd --help` used to die in the
// resolver as an unknown subcommand — the least useful answer possible
// to a request for help.
func TestNounHelpListsItsVerbs(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"rd", "--help"}); got != 0 {
		t.Fatalf("rd --help exit = %d (stderr: %s)", got, errBuf.String())
	}

	text := out.String()

	for _, want := range []string{
		"usage: blockstor resource-definition (rd) <action>",
		"modify (m) <rd>",
		"clone <src> <target>",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("noun help missing %q; got:\n%s", want, text)
		}
	}

	// It answers about THIS object, not every object.
	if strings.Contains(text, "storage-pool") {
		t.Errorf("noun help leaked the whole tree:\n%s", text)
	}
}

// TestCommandHelpIsAboutThatCommand: `blockstor rd modify --help` used
// to print the entire command tree, answering a question nobody asked.
func TestCommandHelpIsAboutThatCommand(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"rd", "modify", "--help"}); got != 0 {
		t.Fatalf("rd modify --help exit = %d (stderr: %s)", got, errBuf.String())
	}

	text := out.String()

	if !strings.HasPrefix(text, "usage: blockstor resource-definition modify <rd>") {
		t.Errorf("command help does not lead with its own usage; got:\n%s", text)
	}

	if !strings.Contains(text, "also spelled: m") {
		t.Errorf("command help drops the alias; got:\n%s", text)
	}

	if strings.Contains(text, "storage-pool") {
		t.Errorf("command help printed the whole tree:\n%s", text)
	}
}
