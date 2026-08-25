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
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: verbDelete, Aliases: []string{aliasDelete}},
			{Name: "lost"},
			{Name: "evacuate"},
			// `evict` is not a verb the upstream python client has; the
			// operator harness shims it to a REST call today. Providing
			// it natively lets that shim go away.
			{Name: "evict", Aliases: []string{"e"}},
			{Name: "restore", Aliases: []string{"rst"}},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
			{Name: "info"},
		},
	},
	{
		Name: "storage-pool", Aliases: []string{"sp"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: verbDelete, Aliases: []string{aliasDelete}},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
		},
	},
	{
		Name: "physical-storage", Aliases: []string{"ps"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: "create-device-pool", Aliases: []string{"cdp"}},
		},
	},
	{
		Name: "resource-definition", Aliases: []string{"rd"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: verbDelete, Aliases: []string{aliasDelete}},
			{Name: "clone"},
			{Name: verbModify, Aliases: []string{aliasModify}},
			{Name: "auto-place", Aliases: []string{"ap"}},
			{Name: verbDRBDOptions},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
		},
	},
	{
		Name: "volume-definition", Aliases: []string{"vd"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: verbDelete, Aliases: []string{aliasDelete}},
			{Name: "set-size", Aliases: []string{"s"}},
			{Name: verbDRBDOptions},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
		},
	},
	{
		Name: "resource", Aliases: []string{"r"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: "list-volumes", Aliases: []string{"lv"}},
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: verbDelete, Aliases: []string{aliasDelete}},
			{Name: "toggle-disk", Aliases: []string{"td"}},
			{Name: "activate"},
			{Name: "deactivate", Aliases: []string{"deact"}},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
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
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: "create-multiple"},
			{Name: verbDelete, Aliases: []string{aliasDelete}},
			{Name: "rollback"},
			// Three-token verbs: `snapshot resource restore` (`s r rst`).
			{Name: "resource restore", Aliases: []string{"r rst", "r restore", "resource rst"}},
			{Name: "resource-definition restore", Aliases: []string{"rd rst", "rd restore", "resource-definition rst"}},
			{Name: "volume-definition restore", Aliases: []string{"vd rst", "vd restore", "volume-definition rst"}},
		},
	},
	{
		Name: "resource-group", Aliases: []string{"rg"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: verbModify, Aliases: []string{aliasModify}},
			{Name: verbDelete, Aliases: []string{aliasDelete}},
			// `sp` is deliberately NOT an alias for spawn-resources: it
			// is set-property on every other noun, and silently meaning
			// two different things would be a footgun in a runbook.
			{Name: "spawn-resources", Aliases: []string{"spawn"}},
			{Name: "adjust"},
			{Name: "query-size-info"},
			{Name: "query-max-volume-size"},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
		},
	},
	{
		Name: "volume-group", Aliases: []string{"vg"},
		Verbs: []Verb{
			{Name: verbList, Aliases: []string{aliasList}},
			{Name: verbCreate, Aliases: []string{aliasCreate}},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
		},
	},
	{
		Name: "controller", Aliases: []string{"c"},
		Verbs: []Verb{
			{Name: "version"},
			{Name: verbDRBDOptions},
			{Name: verbSetProp, Aliases: []string{aliasSetProp}},
			{Name: verbListProps, Aliases: []string{aliasListProps}},
			{Name: verbDeleteProp, Aliases: []string{aliasDelProp}},
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
