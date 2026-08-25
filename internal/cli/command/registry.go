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

// Package command holds the CLI's command surface: the noun/verb
// grammar, the short aliases and the resolution of an argv prefix into
// a command.
//
// The registry below is the single source of truth. The command tree,
// the help output and the tests all read it, so a command added
// without its alias — or an alias that shadows another command — fails
// a test rather than surprising an operator.
//
// The grammar mirrors the upstream client's so existing runbooks and
// this repository's shell harnesses keep working verbatim. It was
// assembled from invocations in tests/e2e/cli-matrix,
// tests/operator-harness, tests/e2e and stand/ — never from the
// upstream client's (GPL) source.
package command

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrUsage marks a client-side rejection: an unknown noun or verb, or
// a malformed invocation. The upstream client exits 2 for this class
// (an API-level rejection is 10), and this repository's replay
// workflows assert those codes, so the class must survive to main().
var ErrUsage = errors.New("usage")

// Verb is one operation on a noun. Name and aliases may contain a
// space for the nested snapshot restore verbs (`snapshot resource
// restore`), which are three tokens deep.
type Verb struct {
	Name    string
	Aliases []string
	// Usage is the argument synopsis that follows the verb, e.g.
	// "<node> <pool>". Empty means the verb takes no arguments.
	// TestEveryVerbDocumentsItsArguments keeps this populated, so
	// `--help` can never go stale against what actually dispatches.
	Usage string
}

// Noun is one object kind plus the verbs it accepts.
type Noun struct {
	Name    string
	Aliases []string
	Verbs   []Verb
}

// Command is a resolved (noun, verb) pair.
type Command struct {
	Noun string
	Verb string
}

// String renders the canonical long form, e.g. "resource toggle-disk".
func (c Command) String() string {
	return c.Noun + " " + c.Verb
}

// Argument synopses that recur across nouns.
const (
	argName            = "<name>"
	argNode            = "<node>"
	argNodePool        = "<node> <pool>"
	argNodeRD          = "<node> <rd>"
	argRD              = "<rd>"
	argRDVolume        = "<rd> <volume-number>"
	argRG              = "<rg>"
	argSnapshotRestore = "--from-resource <rd> --from-snapshot <snap> --to-resource <new>"
)

// Canonical verb names that recur across nouns.
const (
	verbList        = "list"
	verbListProps   = "list-properties"
	verbCreate      = "create"
	verbDelete      = "delete"
	verbSetProp     = "set-property"
	verbDeleteProp  = "delete-property"
	verbModify      = "modify"
	verbDRBDOptions = "drbd-options"
)

// Short aliases that recur across nouns.
const (
	aliasList      = "l"
	aliasCreate    = "c"
	aliasDelete    = "d"
	aliasModify    = "m"
	aliasSetProp   = "sp"
	aliasListProps = "lp"
	aliasDelProp   = "dp"
)

// registry is the full command surface. Ordering is the help output's
// ordering, so it is grouped the way an operator thinks about the
// system rather than alphabetically.
//
//nolint:gochecknoglobals // the command surface is static data
var registry = []Noun{
	{
		Name: "node", Aliases: []string{"n"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbListProps, Aliases: []string{aliasListProps}, Usage: argNode},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: argName + " <address>"},
			{Name: verbDelete, Aliases: []string{aliasDelete}, Usage: argName},
			{Name: "lost", Usage: argNode},
			{Name: "evacuate", Usage: argNode},
			// `evict` is not a verb the upstream python client has; the
			// operator harness shims it to a REST call today. Providing
			// it natively lets that shim go away.
			{Name: "evict", Aliases: []string{"e"}, Usage: argNode},
			{Name: "restore", Aliases: []string{"rst"}, Usage: argNode},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: argNode + " <key> <value>"},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: argNode + " <key>"},
			{Name: "info"},
		},
	},
	{
		Name: "storage-pool", Aliases: []string{"sp"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: "<provider> <node> <pool> [<backing>]"},
			{Name: verbDelete, Aliases: []string{aliasDelete}, Usage: argNodePool},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: argNodePool + " <key> <value>"},
			{Name: verbListProps, Aliases: []string{aliasListProps}, Usage: argNodePool},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: argNodePool + " <key>"},
		},
	},
	{
		Name: "physical-storage", Aliases: []string{"ps"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: "create-device-pool", Aliases: []string{"cdp"}, Usage: "<provider> <node> <device>... --pool-name <name>"},
		},
	},
	{
		Name: "resource-definition", Aliases: []string{"rd"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: argName},
			{Name: verbDelete, Aliases: []string{aliasDelete}, Usage: argName},
			{Name: "clone", Usage: "<src> <target>"},
			{Name: verbModify, Aliases: []string{aliasModify}, Usage: argRD + " [--resource-group <rg>] [--layer-list <layers>]"},
			{Name: "auto-place", Aliases: []string{"ap"}, Usage: argRD},
			{Name: verbDRBDOptions, Usage: argRD + " --<knob> <value>..."},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: argRD + " <key> <value>"},
			{Name: verbListProps, Aliases: []string{aliasListProps}, Usage: argRD},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: argRD + " <key>"},
		},
	},
	{
		Name: "volume-definition", Aliases: []string{"vd"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: argRD + " <size>"},
			{Name: verbDelete, Aliases: []string{aliasDelete}, Usage: argRDVolume},
			{Name: "set-size", Aliases: []string{"s"}, Usage: argRDVolume + " <size>"},
			{Name: verbDRBDOptions, Usage: argRDVolume + " --<knob> <value>..."},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: argRDVolume + " <key> <value>"},
			{Name: verbListProps, Aliases: []string{aliasListProps}, Usage: argRDVolume},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: argRDVolume + " <key>"},
		},
	},
	{
		Name: "resource", Aliases: []string{"r"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: "list-volumes", Aliases: []string{"lv"}},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: "<node>... <rd>"},
			{Name: verbDelete, Aliases: []string{aliasDelete}, Usage: argNodeRD},
			{Name: "toggle-disk", Aliases: []string{"td"}, Usage: argNodeRD},
			{Name: "activate", Usage: argNodeRD},
			{Name: "deactivate", Aliases: []string{"deact"}, Usage: argNodeRD},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: argNodeRD + " <key> <value>"},
			{Name: verbListProps, Aliases: []string{aliasListProps}, Usage: argNodeRD},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: argNodeRD + " <key>"},
		},
	},
	{
		Name: "volume", Aliases: []string{"v"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
		},
	},
	{
		Name: "snapshot", Aliases: []string{"s"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: "[<node>...] <rd> <snap>"},
			{Name: "create-multiple", Usage: "<rd>:<snap>... | <snap> <rd>..."},
			{Name: verbDelete, Aliases: []string{aliasDelete}, Usage: argRD + " <snap>"},
			{Name: "rollback"},
			// Three-token verbs: `snapshot resource restore` (`s r rst`).
			{Name: "resource restore", Aliases: []string{"r rst", "r restore", "resource rst"}, Usage: argSnapshotRestore},
			{Name: "resource-definition restore", Aliases: []string{"rd rst", "rd restore", "resource-definition rst"}, Usage: argSnapshotRestore},
			{Name: "volume-definition restore", Aliases: []string{"vd rst", "vd restore", "volume-definition rst"}, Usage: argSnapshotRestore},
		},
	},
	{
		Name: "resource-group", Aliases: []string{"rg"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: argName},
			{Name: verbModify, Aliases: []string{aliasModify}, Usage: argName},
			{Name: verbDelete, Aliases: []string{aliasDelete}, Usage: argName},
			// `sp` is deliberately NOT an alias for spawn-resources: it
			// is set-property on every other noun, and silently meaning
			// two different things would be a footgun in a runbook.
			{Name: "spawn-resources", Aliases: []string{"spawn"}, Usage: argRG + " <rd> <size>..."},
			{Name: "adjust", Usage: "[<rg>]"},
			{Name: "query-size-info", Usage: argRG},
			{Name: "query-max-volume-size", Usage: argRG},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: argRG + " <key> <value>"},
			{Name: verbListProps, Aliases: []string{aliasListProps}, Usage: argRG},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: argRG + " <key>"},
		},
	},
	{
		Name: "volume-group", Aliases: []string{"vg"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}, Usage: "<resource-group>"},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: argRG + " <volume-number> <key> <value>"},
			{Name: verbListProps, Aliases: []string{aliasListProps}, Usage: argRG + " <volume-number>"},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: argRG + " <volume-number> <key>"},
		},
	},
	{
		Name: "controller", Aliases: []string{"c"},
		Verbs: []Verb{
			{Name: "version"},
			{Name: verbDRBDOptions, Usage: "--<knob> <value>..."},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}, Usage: "<key> <value>"},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}, Usage: "<key>"},
		},
	},
	{
		Name: "encryption",
		Verbs: []Verb{
			{Name: "create-passphrase"},
			{Name: "enter-passphrase"},
		},
	},
}

// maxVerbTokens is the longest verb path in the registry (`resource
// restore`). Resolution tries the longest match first so a nested verb
// wins over a single-token one.
const maxVerbTokens = 2

// Nouns returns the registry.
func Nouns() []Noun {
	return registry
}

// Lookup resolves an object token — canonical name or alias — to the
// noun it names. Help rendering needs the noun before a verb is known,
// which Resolve cannot give it.
func Lookup(token string) (Noun, bool) {
	noun := lookupNoun(token)
	if noun == nil {
		return Noun{}, false
	}

	return *noun, true
}

// Describe returns the registered verb for a canonical (noun, verb)
// pair, so help can print the argument synopsis the registry carries.
func Describe(noun, verb string) (Verb, bool) {
	for i := range registry {
		if registry[i].Name != noun {
			continue
		}

		for j := range registry[i].Verbs {
			if registry[i].Verbs[j].Name == verb {
				return registry[i].Verbs[j], true
			}
		}
	}

	return Verb{}, false
}

// Has reports whether the canonical (noun, verb) pair is registered.
func Has(noun, verb string) bool {
	for i := range registry {
		if registry[i].Name != noun {
			continue
		}

		for j := range registry[i].Verbs {
			if registry[i].Verbs[j].Name == verb {
				return true
			}
		}
	}

	return false
}

// Resolve consumes the command path at the head of argv and returns the
// canonical command plus the remaining arguments verbatim. Everything
// after the path belongs to the per-command flag parser: the upstream
// grammar lets a flag sit before or after the positionals, so this
// function deliberately does not interpret it.
func Resolve(argv []string) (Command, []string, error) {
	if len(argv) == 0 {
		return Command{}, nil, fmt.Errorf("%w: no command given", ErrUsage)
	}

	noun := lookupNoun(argv[0])
	if noun == nil {
		return Command{}, nil, fmt.Errorf("%w: unknown object %q", ErrUsage, argv[0])
	}

	rest := argv[1:]
	if len(rest) == 0 {
		return Command{}, nil, fmt.Errorf("%w: %s needs a subcommand", ErrUsage, noun.Name)
	}

	// Longest match first: `resource restore` must win over `resource`.
	for take := min(maxVerbTokens, len(rest)); take >= 1; take-- {
		candidate := strings.Join(rest[:take], " ")

		verb := lookupVerb(noun, candidate)
		if verb != nil {
			return Command{Noun: noun.Name, Verb: verb.Name}, rest[take:], nil
		}
	}

	return Command{}, nil, fmt.Errorf("%w: unknown %s subcommand %q", ErrUsage, noun.Name, rest[0])
}

func lookupNoun(token string) *Noun {
	for i := range registry {
		if registry[i].Name == token {
			return &registry[i]
		}

		if slices.Contains(registry[i].Aliases, token) {
			return &registry[i]
		}
	}

	return nil
}

func lookupVerb(noun *Noun, candidate string) *Verb {
	for i := range noun.Verbs {
		if noun.Verbs[i].Name == candidate {
			return &noun.Verbs[i]
		}

		if slices.Contains(noun.Verbs[i].Aliases, candidate) {
			return &noun.Verbs[i]
		}
	}

	return nil
}
