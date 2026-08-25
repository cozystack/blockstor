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
	"fmt"
	"io"
	"strings"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// isHelpRequest reports whether argv asks for the command tree rather
// than naming a command.
func isHelpRequest(argv []string) bool {
	if len(argv) == 0 {
		return false
	}

	switch argv[0] {
	case "help", "-h", "--help":
		return true
	default:
		return false
	}
}

// writeHelp prints the command tree, generated from the registry so it
// cannot drift from what actually dispatches.
func writeHelp(out io.Writer) error {
	_, err := fmt.Fprint(out, "usage: blockstor <object> <action> [arguments]\n\n")
	if err != nil {
		return fmt.Errorf("write usage: %w", err)
	}

	for _, noun := range command.Nouns() {
		_, err = fmt.Fprintf(out, "%s\n", withAliases(noun.Name, noun.Aliases))
		if err != nil {
			return fmt.Errorf("write help: %w", err)
		}

		for _, verb := range noun.Verbs {
			_, err = fmt.Fprintf(out, "    %s\n", withAliases(verb.Name, verb.Aliases))
			if err != nil {
				return fmt.Errorf("write help: %w", err)
			}
		}
	}

	return nil
}

// writeNounHelp prints one object's verbs. `blockstor rd --help` used
// to die in the resolver as an unknown subcommand, which is the least
// useful answer to a request for help.
func writeNounHelp(out io.Writer, noun command.Noun) error {
	_, err := fmt.Fprintf(out, "usage: blockstor %s <action> [arguments]\n\n",
		withAliases(noun.Name, noun.Aliases))
	if err != nil {
		return fmt.Errorf("write usage: %w", err)
	}

	for _, verb := range noun.Verbs {
		line := withAliases(verb.Name, verb.Aliases)
		if verb.Usage != "" {
			line += " " + verb.Usage
		}

		_, err = fmt.Fprintf(out, "    %s\n", line)
		if err != nil {
			return fmt.Errorf("write help: %w", err)
		}
	}

	return nil
}

// writeCommandHelp prints the synopsis of one command. `blockstor rd
// modify --help` used to print the whole tree, which answers a question
// nobody asked.
func writeCommandHelp(out io.Writer, cmd command.Command, verb command.Verb) error {
	line := "usage: blockstor " + cmd.Noun + " " + cmd.Verb
	if verb.Usage != "" {
		line += " " + verb.Usage
	}

	_, err := fmt.Fprintf(out, "%s\n", line)
	if err != nil {
		return fmt.Errorf("write usage: %w", err)
	}

	if len(verb.Aliases) > 0 {
		_, err = fmt.Fprintf(out, "\nalso spelled: %s\n", strings.Join(verb.Aliases, ", "))
		if err != nil {
			return fmt.Errorf("write aliases: %w", err)
		}
	}

	return nil
}

// withAliases renders `name (alias, alias)`.
func withAliases(name string, aliases []string) string {
	if len(aliases) == 0 {
		return name
	}

	return name + " (" + strings.Join(aliases, ", ") + ")"
}
