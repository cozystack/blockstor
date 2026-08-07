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
	"context"
	"fmt"
	"strings"

	"github.com/cozystack/blockstor/pkg/drbd"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// unsetPrefix is how a DRBD knob is cleared: `--unset-max-buffers`.
const unsetPrefix = "unset-"

// drbdOptions implements `<noun> drbd-options --<knob> <value> …`.
//
// The knob names are the ones in drbd.conf(5); each is stored under
// the LINSTOR property key for its section, because the section is
// what decides which `.res` block the value is rendered into. A knob
// blockstor has no section for is rejected rather than filed under a
// guess — a `net{}` option written to the resource namespace renders
// into options{}, where drbdadm rejects the whole file and every later
// adjust for that resource fails.
func drbdOptions(accessor propertyAccessor) handler {
	return func(ctx context.Context, run *runContext) error {
		if len(run.Flags.Positionals) < accessor.args {
			return fmt.Errorf("%w: drbd-options needs %d argument(s)", command.ErrUsage, accessor.args)
		}

		ident := run.Flags.Positionals[:accessor.args]

		props, err := accessor.get(ctx, run.Store, ident)
		if err != nil {
			return err
		}

		if props == nil {
			props = map[string]string{}
		}

		changed, err := applyDRBDFlags(run.Flags, props)
		if err != nil {
			return err
		}

		if !changed {
			return fmt.Errorf("%w: drbd-options needs at least one --<option>", command.ErrUsage)
		}

		return accessor.set(ctx, run.Store, ident, props)
	}
}

// applyDRBDFlags folds the parsed flags into a property bag, reporting
// whether anything was actually set or cleared.
func applyDRBDFlags(flags *flagSet, props map[string]string) (bool, error) {
	changed := false

	// A knob that is both set and unset in one invocation is REFUSED.
	// Values is a map, so resolving it would pick set or delete by
	// iteration order — the same command line giving different
	// cluster state on different runs.
	for name := range flags.Values {
		knob, isUnset := strings.CutPrefix(name, unsetPrefix)
		if !isUnset {
			continue
		}

		if _, both := flags.Values[knob]; both {
			return false, fmt.Errorf("%w: --%s and --%s%s contradict each other",
				command.ErrUsage, knob, unsetPrefix, knob)
		}
	}

	for name, value := range flags.Values {
		knob, isUnset := strings.CutPrefix(name, unsetPrefix)

		key, known := drbd.FlagKey(knob)
		if !known {
			// A non-DRBD flag on this verb (there are none today, but
			// a shared parser means one could arrive) is ignored
			// rather than written under a guessed key.
			continue
		}

		if isUnset {
			delete(props, key)
		} else {
			props[key] = value
		}

		changed = true
	}

	return changed, nil
}
