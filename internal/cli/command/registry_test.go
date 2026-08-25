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

package command_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// Operators and this repository's own harnesses drive the CLI by the
// upstream client's grammar, in both long and short form. Every pair
// below is taken from a real invocation in tests/e2e/cli-matrix,
// tests/operator-harness, tests/e2e or stand/ — if one of these stops
// resolving, a script in this repo breaks.
func TestResolveAliases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		argv     []string
		wantNoun string
		wantVerb string
	}{
		// Nouns in short form with the list verb.
		{[]string{"n", "l"}, "node", "list"},
		{[]string{"sp", "l"}, "storage-pool", "list"},
		{[]string{"rd", "l"}, "resource-definition", "list"},
		{[]string{"vd", "l"}, "volume-definition", "list"},
		{[]string{"r", "l"}, "resource", "list"},
		{[]string{"v", "l"}, "volume", "list"},
		{[]string{"s", "l"}, "snapshot", "list"},
		{[]string{"rg", "l"}, "resource-group", "list"},
		{[]string{"ps", "l"}, "physical-storage", "list"},

		// Long form resolves to itself.
		{[]string{"node", "list"}, "node", "list"},
		{[]string{"resource-definition", "create"}, "resource-definition", "create"},

		// Verb aliases.
		{[]string{"rd", "c"}, "resource-definition", "create"},
		{[]string{"rd", "d"}, "resource-definition", "delete"},
		{[]string{"rd", "ap"}, "resource-definition", "auto-place"},
		{[]string{"rd", "m"}, "resource-definition", "modify"},
		{[]string{"rg", "m"}, "resource-group", "modify"},
		{[]string{"vd", "s"}, "volume-definition", "set-size"},
		{[]string{"vd", "c"}, "volume-definition", "create"},
		{[]string{"r", "td"}, "resource", "toggle-disk"},
		{[]string{"r", "lv"}, "resource", "list-volumes"},
		{[]string{"r", "deact"}, "resource", "deactivate"},
		{[]string{"n", "rst"}, "node", "restore"},
		{[]string{"n", "e"}, "node", "evict"},
		{[]string{"ps", "cdp"}, "physical-storage", "create-device-pool"},
		{[]string{"c", "lp"}, "controller", "list-properties"},
		{[]string{"c", "dp"}, "controller", "delete-property"},

		// `spawn` is accepted as a synonym of `spawn-resources`.
		{[]string{"rg", "spawn"}, "resource-group", "spawn-resources"},
		{[]string{"rg", "spawn-resources"}, "resource-group", "spawn-resources"},

		// The same token means different things by POSITION: `sp` is
		// the storage-pool noun in slot 1 and the set-property verb in
		// slot 2; likewise `c` (controller vs create) and `s`
		// (snapshot vs set-size).
		{[]string{"rd", "sp"}, "resource-definition", "set-property"},
		{[]string{"r", "sp"}, "resource", "set-property"},
		{[]string{"rg", "sp"}, "resource-group", "set-property"},
		{[]string{"c", "sp"}, "controller", "set-property"},
		{[]string{"sp", "c"}, "storage-pool", "create"},
		{[]string{"sp", "d"}, "storage-pool", "delete"},
	}

	for _, tc := range cases {
		cmd, _, err := command.Resolve(tc.argv)
		if err != nil {
			t.Errorf("Resolve(%v): %v", tc.argv, err)

			continue
		}

		if cmd.Noun != tc.wantNoun || cmd.Verb != tc.wantVerb {
			t.Errorf("Resolve(%v) = %s %s, want %s %s",
				tc.argv, cmd.Noun, cmd.Verb, tc.wantNoun, tc.wantVerb)
		}
	}
}

// The snapshot restore verbs are three tokens deep — `snapshot resource
// restore` (`s r rst`), plus the resource-definition and
// volume-definition variants. Resolution must consume all three and
// leave the flags/positionals untouched.
func TestResolveNestedSnapshotRestore(t *testing.T) {
	t.Parallel()

	cases := []struct {
		argv     []string
		wantVerb string
		wantRest []string
	}{
		{
			[]string{"s", "r", "rst", "--from-resource", "src"},
			"resource restore",
			[]string{"--from-resource", "src"},
		},
		{
			[]string{"snapshot", "resource", "restore", "--to-resource", "dst"},
			"resource restore",
			[]string{"--to-resource", "dst"},
		},
		{
			[]string{"snapshot", "resource-definition", "restore"},
			"resource-definition restore",
			nil,
		},
		{
			[]string{"snapshot", "volume-definition", "restore"},
			"volume-definition restore",
			nil,
		},
	}

	for _, tc := range cases {
		cmd, rest, err := command.Resolve(tc.argv)
		if err != nil {
			t.Errorf("Resolve(%v): %v", tc.argv, err)

			continue
		}

		if cmd.Noun != "snapshot" || cmd.Verb != tc.wantVerb {
			t.Errorf("Resolve(%v) = %s %s, want snapshot %s", tc.argv, cmd.Noun, cmd.Verb, tc.wantVerb)
		}

		if len(rest) != len(tc.wantRest) {
			t.Errorf("Resolve(%v) rest = %v, want %v", tc.argv, rest, tc.wantRest)

			continue
		}

		for i := range tc.wantRest {
			if rest[i] != tc.wantRest[i] {
				t.Errorf("Resolve(%v) rest[%d] = %q, want %q", tc.argv, i, rest[i], tc.wantRest[i])
			}
		}
	}
}

// Everything after the command path is handed back untouched — the
// per-command flag parser owns it. This matters because the upstream
// grammar allows a flag to sit before OR after the positionals
// (`r td --storage-pool=X node rd` and `r td node rd --storage-pool X`
// both appear in this repo).
func TestResolveReturnsRemainingArgsVerbatim(t *testing.T) {
	t.Parallel()

	argv := []string{"r", "td", "--storage-pool=data", "node-1", "pvc-x", "--diskless"}

	cmd, rest, err := command.Resolve(argv)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if cmd.Noun != "resource" || cmd.Verb != "toggle-disk" {
		t.Fatalf("resolved %s %s, want resource toggle-disk", cmd.Noun, cmd.Verb)
	}

	want := []string{"--storage-pool=data", "node-1", "pvc-x", "--diskless"}
	if len(rest) != len(want) {
		t.Fatalf("rest = %v, want %v", rest, want)
	}

	for i := range want {
		if rest[i] != want[i] {
			t.Errorf("rest[%d] = %q, want %q", i, rest[i], want[i])
		}
	}
}

// An unknown noun or verb is a CLIENT-side rejection. The harnesses
// pin the upstream exit codes: 2 for an argparse-level rejection, 10
// for an API-level one, so the error must carry the client class.
func TestResolveUnknownIsClientError(t *testing.T) {
	t.Parallel()

	for _, argv := range [][]string{
		{"nope", "list"},
		{"node", "frobnicate"},
		{"node"},
		{},
	} {
		_, _, err := command.Resolve(argv)
		if err == nil {
			t.Errorf("Resolve(%v): want error, got nil", argv)

			continue
		}

		if !errors.Is(err, command.ErrUsage) {
			t.Errorf("Resolve(%v) error = %v, want ErrUsage (exit 2 class)", argv, err)
		}
	}
}

// The registry is the single source of truth for the command surface:
// the command tree, the help output and these tests all read it. Every
// entry must therefore be well-formed, and every alias unique within
// its slot — a duplicate alias would silently shadow a command.
func TestRegistryIsWellFormed(t *testing.T) {
	t.Parallel()

	seenNounAlias := map[string]string{}

	for _, noun := range command.Nouns() {
		if noun.Name == "" {
			t.Error("noun with empty name")
		}

		for _, alias := range append([]string{noun.Name}, noun.Aliases...) {
			if prev, dup := seenNounAlias[alias]; dup {
				t.Errorf("noun alias %q claimed by both %q and %q", alias, prev, noun.Name)
			}

			seenNounAlias[alias] = noun.Name
		}

		if len(noun.Verbs) == 0 {
			t.Errorf("noun %q has no verbs", noun.Name)
		}

		seenVerbAlias := map[string]string{}

		for _, verb := range noun.Verbs {
			if verb.Name == "" {
				t.Errorf("noun %q has a verb with an empty name", noun.Name)
			}

			for _, alias := range append([]string{verb.Name}, verb.Aliases...) {
				if prev, dup := seenVerbAlias[alias]; dup {
					t.Errorf("noun %q: verb alias %q claimed by both %q and %q",
						noun.Name, alias, prev, verb.Name)
				}

				seenVerbAlias[alias] = verb.Name
			}
		}
	}
}

// The minimum viable surface: every command this repository's
// executable tests actually invoke must exist in the registry. This is
// the list that has to be complete before the upstream client can be
// dropped as a dependency, so it is asserted rather than described.
func TestRegistryCoversExercisedSurface(t *testing.T) {
	t.Parallel()

	required := map[string][]string{
		"node": {
			"list", "list-properties", "create", "delete",
			"lost", "evacuate", "evict", "restore", "set-property",
		},
		"storage-pool":        {"list", "create", "delete", "set-property"},
		"physical-storage":    {"list", "create-device-pool"},
		"resource-definition": {"list", "create", "delete", "clone", "modify", "set-property", "list-properties", "drbd-options", "auto-place"},
		"volume-definition":   {"list", "create", "delete", "set-size", "set-property"},
		"resource":            {"list", "list-volumes", "create", "delete", "toggle-disk", "activate", "deactivate", "set-property", "list-properties"},
		"volume":              {"list"},
		"snapshot": {
			"list", "create", "create-multiple", "delete", "rollback",
			"resource restore", "resource-definition restore", "volume-definition restore",
		},
		"resource-group": {
			"list", "create", "modify", "delete", "spawn-resources",
			"adjust", "query-size-info", "query-max-volume-size", "set-property", "list-properties",
		},
		"volume-group": {"create", "list"},
		"controller":   {"version", "list-properties", "set-property", "drbd-options"},
		"encryption":   {"create-passphrase", "enter-passphrase"},
	}

	for noun, verbs := range required {
		for _, verb := range verbs {
			if !command.Has(noun, verb) {
				t.Errorf("registry is missing %q %q (exercised by this repo's tests)", noun, verb)
			}
		}
	}
}

// A noun that accepts set-property must accept list-properties and
// delete-property too. Half a property surface is worse than none: a
// runbook sets a key, cannot read it back, and cannot undo it.
func TestPropertyVerbsComeAsASet(t *testing.T) {
	t.Parallel()

	for _, noun := range command.Nouns() {
		if !command.Has(noun.Name, "set-property") {
			continue
		}

		for _, verb := range []string{"list-properties", "delete-property"} {
			if !command.Has(noun.Name, verb) {
				t.Errorf("%s has set-property but not %s", noun.Name, verb)
			}
		}
	}
}

// noArgumentVerbs are the commands that legitimately take nothing after
// the verb. Everything else must carry a Usage synopsis, so `--help`
// cannot quietly go blank when a verb is added.
//
//nolint:gochecknoglobals // fixture for the drift guard below
var noArgumentVerbs = map[string]bool{
	"node list":                    true,
	"node info":                    true,
	"storage-pool list":            true,
	"physical-storage list":        true,
	"resource-definition list":     true,
	"volume-definition list":       true,
	"resource list":                true,
	"resource list-volumes":        true,
	"volume list":                  true,
	"snapshot list":                true,
	"snapshot rollback":            true,
	"resource-group list":          true,
	"volume-group list":            true,
	"controller version":           true,
	"controller list-properties":   true,
	"encryption create-passphrase": true,
	"encryption enter-passphrase":  true,
}

// TestEveryVerbDocumentsItsArguments: per-command help is only useful if
// it is complete. A verb that takes arguments and carries no synopsis
// prints a usage line that stops exactly where the operator needs it to
// continue, so the registry has to stay populated as verbs are added.
func TestEveryVerbDocumentsItsArguments(t *testing.T) {
	t.Parallel()

	for _, noun := range command.Nouns() {
		for _, verb := range noun.Verbs {
			full := noun.Name + " " + verb.Name

			if noArgumentVerbs[full] {
				if verb.Usage != "" {
					t.Errorf("%s is listed as taking no arguments but documents %q",
						full, verb.Usage)
				}

				continue
			}

			if verb.Usage == "" {
				t.Errorf("%s has no argument synopsis; add one to the registry "+
					"or list it in noArgumentVerbs", full)
			}
		}
	}
}
