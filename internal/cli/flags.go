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
	"strings"

	"github.com/cozystack/blockstor/pkg/drbd"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// flagSet is the parsed tail of an invocation.
//
// The upstream grammar lets a flag sit before OR after the
// positionals, and both forms appear in this repository's scripts, so
// parsing walks the whole tail and collects positionals as it goes
// rather than stopping at the first non-flag.
type flagSet struct {
	// Machine selects the machine-readable envelope (-m).
	Machine bool
	// Faulty filters resource listings (--faulty).
	Faulty bool
	// Diskless marks a replica as storage-free (--diskless and its
	// DRBD-specific spelling).
	Diskless bool
	// Cancel asks for an in-flight conversion to be unwound
	// (--cancel).
	Cancel bool
	// Force overrides a safety refusal (--force).
	Force bool
	// Pastable requests the pipe-free rendering (-p/--pastable).
	Pastable bool
	// Color is the --color mode; empty means auto.
	Color string
	// Nodes/Resources filter listings (-n/-r and their long forms).
	Nodes     []string
	Resources []string
	// Positionals are the non-flag arguments, in order.
	Positionals []string
	// Values holds any other recognised `--key value` pair verbatim,
	// so handlers can read command-specific flags without this parser
	// needing to know every one of them.
	Values map[string]string
}

// Flag names that appear in more than one place.
const (
	flagColor      = "--color"
	flagNodesLong  = "--nodes"
	flagNodesShort = "-n"
	flagRscLong    = "--resources"
	flagRscDefs    = "--resource-definitions"
	flagRscShort   = "-r"
)

// valueFlags are the flags that consume the following argument when
// written in the space-separated form.
var valueFlags = map[string]struct{}{ //nolint:gochecknoglobals // static flag table
	flagColor:          {},
	flagNodesLong:      {},
	flagNodesShort:     {},
	flagRscLong:        {},
	flagRscDefs:        {},
	flagRscShort:       {},
	"--storage-pool":   {},
	"--storage-pools":  {},
	"--place-count":    {},
	"--auto-place":     {},
	"--layer-list":     {},
	"-l":               {},
	"--resource-group": {},
	"--size":           {},
	"--vlmnr":          {},
	"--limit":          {},
	"--passphrase":     {},
	"-p-value":         {},
	"--migrate-from":   {},
	"--from-resource":  {},
	"--from-snapshot":  {},
	"--to-resource":    {},
	"--node-type":      {},
	"--port":           {},
	"--pool-name":      {},
	"--provider-kind":  {},
	"--controllers":    {},
	"--output-fmt":     {},
	"--output-version": {},
	"-o":               {},
}

// parseFlags walks the argument tail.
func parseFlags(args []string) (*flagSet, error) {
	parsed := &flagSet{Values: map[string]string{}}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		// A bare `--` ends flag parsing: the scripts use it so a
		// negative volume number is not read as a flag.
		if arg == "--" {
			parsed.Positionals = append(parsed.Positionals, args[i+1:]...)

			break
		}

		if !strings.HasPrefix(arg, "-") || arg == "-" {
			parsed.Positionals = append(parsed.Positionals, arg)

			continue
		}

		name, value, inline := strings.Cut(arg, "=")

		if parsed.setBool(name) {
			continue
		}

		takesValue, err := flagTakesValue(name)
		if err != nil {
			return nil, err
		}

		if !takesValue {
			parsed.Values[strings.TrimLeft(name, "-")] = ""

			continue
		}

		if !inline {
			if i+1 >= len(args) {
				return nil, fmt.Errorf("%w: flag %q needs a value", command.ErrUsage, arg)
			}

			i++
			value = args[i]
		}

		parsed.assign(name, value)
	}

	return parsed, nil
}

// flagTakesValue reports whether a flag consumes the next argument,
// rejecting a name that is neither in the static table nor a DRBD
// knob.
//
// `drbd-options` takes its knobs as flags — `--max-buffers 36864`,
// `--unset-c-max-rate` — and there are more of those than it would be
// useful to list by hand, so they are resolved from the DRBD
// catalogue. An `--unset-` form carries no value.
func flagTakesValue(name string) (bool, error) {
	if _, known := valueFlags[name]; known {
		return true, nil
	}

	knob := strings.TrimLeft(name, "-")

	if unset, isUnset := strings.CutPrefix(knob, unsetPrefix); isUnset {
		if _, known := drbd.FlagKey(unset); known {
			return false, nil
		}

		return false, fmt.Errorf("%w: unknown DRBD option %q", command.ErrUsage, name)
	}

	if _, known := drbd.FlagKey(knob); known {
		return true, nil
	}

	return false, fmt.Errorf("%w: unknown flag %q", command.ErrUsage, name)
}

// setBool records a flag that carries no value, reporting whether the
// name was one.
func (f *flagSet) setBool(name string) bool {
	switch name {
	case "-m", "--machine-readable":
		f.Machine = true
	case "--faulty":
		f.Faulty = true
	case "--diskless", "--drbd-diskless":
		f.Diskless = true
	case "--cancel":
		f.Cancel = true
	case "--force":
		f.Force = true
	case "-p", "--pastable":
		f.Pastable = true
	case "--no-color":
		f.Color = "never"
	default:
		return false
	}

	return true
}

// assign records a flag value, folding the aliases onto one field so
// handlers read a single name.
func (f *flagSet) assign(name, value string) {
	switch name {
	case flagColor:
		f.Color = value
	case flagNodesShort, flagNodesLong:
		f.Nodes = append(f.Nodes, splitList(value)...)
	case flagRscShort, flagRscLong, flagRscDefs:
		f.Resources = append(f.Resources, splitList(value)...)
	default:
		f.Values[strings.TrimLeft(name, "-")] = value
	}
}

// splitList accepts both repeated flags and one comma-separated value,
// because both spellings appear in this repository's scripts.
func splitList(value string) []string {
	parts := strings.Split(value, ",")

	out := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}

	return out
}

// matches reports whether a value passes a filter list (empty filter
// matches everything).
func matches(filter []string, value string) bool {
	if len(filter) == 0 {
		return true
	}

	for _, want := range filter {
		if strings.EqualFold(want, value) {
			return true
		}
	}

	return false
}
