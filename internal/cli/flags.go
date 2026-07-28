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
	"strconv"
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
	// IgnoredControllers records that --controllers was given; the
	// target comes from the kubeconfig instead.
	IgnoredControllers bool
	// noColor backs --no-color, which folds into Color.
	noColor bool
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
	flagOutputFmt  = "--output-fmt"
	flagOutputAbbr = "-o"
	flagDiskless   = "--diskless"
	flagDrbdDless  = "--drbd-diskless"
	flagCancel     = "--cancel"
	flagForce      = "--force"
	flagFaulty     = "--faulty"
	flagNoColor    = "--no-color"
	flagMachine    = "--machine-readable"
	flagPastable   = "--pastable"
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

		handled, err := parsed.setBool(name, value, inline)
		if err != nil {
			return nil, err
		}

		if handled {
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

	err := parsed.resolveOutput()
	if err != nil {
		return nil, err
	}

	return parsed, nil
}

// resolveOutput folds the output-selection flags into the fields the
// handlers read, and REJECTS a value it cannot honour.
//
// A flag that is parsed and then ignored is worse than one that is
// refused: `resource list -o json | jq` would otherwise receive a
// human table with exit 0, and the script would fail somewhere else
// entirely.
func (f *flagSet) resolveOutput() error {
	// `--controllers` names REST endpoints, which this client does not
	// use. Accepting it keeps existing wrappers working; ignoring it
	// SILENTLY would not — pointing it at one cluster while the
	// kubeconfig names another would read the wrong cluster without a
	// word. The caller surfaces the notice.
	if f.Values["controllers"] != "" {
		f.IgnoredControllers = true
	}

	switch format := f.Values["output-fmt"]; format {
	case "", "table":
	case "json":
		f.Machine = true
	default:
		return fmt.Errorf("%w: unsupported output format %q (table, json)", command.ErrUsage, format)
	}

	if version := f.Values["output-version"]; version != "" && version != "v1" {
		return fmt.Errorf("%w: unsupported output version %q (v1)", command.ErrUsage, version)
	}

	return nil
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

// setBool records a value-less flag, reporting whether the name was
// one of them.
//
// An inline value is honoured when it is a boolean (`--force=false`
// must DISABLE force, not enable it) and rejected otherwise: silently
// dropping the value of `-p=secret` loses a passphrase, and silently
// ignoring `--force=false` does the opposite of what was written.
//
// The names live in ONE switch. Listing them twice — once to
// recognise, once to apply — is how a flag ends up recognised but
// never acted on.
func (f *flagSet) setBool(name, value string, inline bool) (bool, error) {
	target := f.boolTarget(name)
	if target == nil {
		return false, nil
	}

	enabled := true

	if inline {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, fmt.Errorf("%w: flag %q takes no value, got %q", command.ErrUsage, name, value)
		}

		enabled = parsed
	}

	*target = enabled

	if name == flagNoColor && enabled {
		f.Color = "never"
	}

	return true, nil
}

// boolTarget maps a value-less flag to the field it sets.
func (f *flagSet) boolTarget(name string) *bool {
	switch name {
	case "-m", flagMachine:
		return &f.Machine
	case flagFaulty:
		return &f.Faulty
	case flagDiskless, flagDrbdDless:
		return &f.Diskless
	case flagCancel:
		return &f.Cancel
	case flagForce:
		return &f.Force
	case "-p", flagPastable:
		return &f.Pastable
	case flagNoColor:
		return &f.noColor
	default:
		return nil
	}
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
	case flagOutputAbbr, flagOutputFmt:
		f.Values["output-fmt"] = value
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
