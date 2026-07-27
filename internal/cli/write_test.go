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
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Setting a property to an EMPTY value deletes the key. This is a real
// contract, not a nicety: replay workflows in this repo restore a
// cluster's automatic behaviour by setting a property to "" and then
// assert the key is gone from list-properties.
func TestSetPropertyEmptyValueDeletesKey(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:  "pvc-x",
			Props: map[string]string{"DrbdOptions/auto-quorum": "disabled"},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "sp", "pvc-x", "DrbdOptions/auto-quorum", ""}); got != 0 {
		t.Fatalf("set-property exit = %d (stderr: %s)", got, errBuf.String())
	}

	out.Reset()

	if got := app.Run(t.Context(), []string{"rd", "lp", "pvc-x"}); got != 0 {
		t.Fatalf("list-properties exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(out.String(), "auto-quorum") {
		t.Errorf("setting an empty value did not delete the key:\n%s", out.String())
	}
}

// Setting a non-empty value stores it, and list-properties shows it as
// a key/value row an operator (and a grep) can read.
func TestSetAndListProperty(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	})

	if got := app.Run(t.Context(), []string{"rd", "set-property", "pvc-x", "Aux/site", "dc1"}); got != 0 {
		t.Fatalf("set-property exit = %d (stderr: %s)", got, errBuf.String())
	}

	out.Reset()

	if got := app.Run(t.Context(), []string{"rd", "list-properties", "pvc-x"}); got != 0 {
		t.Fatalf("list-properties exit = %d", got)
	}

	for _, want := range []string{"Aux/site", "dc1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("list-properties is missing %q:\n%s", want, out.String())
		}
	}
}

// The controller's own property bag uses the same grammar, and the
// install scripts grep its output for the range keys they just set.
func TestControllerProperties(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"c", "sp", "TcpPortAutoRange", "20000-20999"}); got != 0 {
		t.Fatalf("controller set-property exit = %d (stderr: %s)", got, errBuf.String())
	}

	out.Reset()

	if got := app.Run(t.Context(), []string{"c", "lp"}); got != 0 {
		t.Fatalf("controller list-properties exit = %d", got)
	}

	if !strings.Contains(out.String(), "TcpPortAutoRange") {
		t.Errorf("controller list-properties lost the key:\n%s", out.String())
	}
}

// `controller version` reports without needing any object to exist.
func TestControllerVersion(t *testing.T) {
	t.Parallel()

	app, out, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"controller", "version"}); got != 0 {
		t.Fatalf("version exit = %d", got)
	}

	if strings.TrimSpace(out.String()) == "" {
		t.Error("version printed nothing")
	}
}

// Deleting an object that is already gone SUCCEEDS. The upstream
// behaviour this repo pins treats delete as idempotent, and several
// teardown paths rely on it — a non-zero exit there would fail cleanup
// runs that are otherwise fine.
func TestDeleteIsIdempotent(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"rd", "d", "never-existed"}); got != 0 {
		t.Errorf("deleting an absent definition exit = %d, want 0 (stderr: %s)", got, errBuf.String())
	}
}

// A definition can be created and then appears in the listing.
func TestResourceDefinitionCreate(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"rd", "c", "pvc-new"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	out.Reset()

	if got := app.Run(t.Context(), []string{"rd", "l"}); got != 0 {
		t.Fatalf("list exit = %d", got)
	}

	if !strings.Contains(out.String(), "pvc-new") {
		t.Errorf("created definition is missing from the listing:\n%s", out.String())
	}
}

// A command that needs a positional argument and does not get one is a
// client-side rejection, not a crash.
func TestMissingPositionalIsUsageError(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"rd", "c"}); got != 2 {
		t.Errorf("create with no name exit = %d, want 2", got)
	}

	if errBuf.Len() == 0 {
		t.Error("a usage error printed nothing to stderr")
	}
}
