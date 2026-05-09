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

package drbd

import "strings"

// PropPrefix is the LINSTOR namespace under which DRBD options live.
// Properties under any other prefix (e.g. `Aux/...`, `StorPoolName`)
// are not DRBD options and the resolver leaves them alone.
const PropPrefix = "DrbdOptions/"

// ResolveOptions merges DRBD properties from the four upstream LINSTOR
// scopes (controller → resource-group → resource-definition →
// resource), with each lower scope overriding the upper one — exactly
// what `linstor c sp foo bar` / `linstor rg sp ...` / `linstor rd sp
// ...` / `linstor r sp ...` configure.
//
// Only keys under `DrbdOptions/` participate; everything else is
// returned unchanged from `resource` (so caller-side props like
// `StorPoolName` survive the merge). The returned map is a fresh
// allocation; the caller may mutate it without disturbing inputs.
//
// Order of `levels`: highest scope first (controller), lowest last
// (resource). nil maps are treated as empty.
func ResolveOptions(controller, resourceGroup, resourceDef, resource map[string]string) map[string]string {
	out := map[string]string{}

	for _, level := range []map[string]string{controller, resourceGroup, resourceDef, resource} {
		for key, value := range level {
			if !strings.HasPrefix(key, PropPrefix) {
				continue
			}

			out[key] = value
		}
	}

	// Pass through non-DRBD props from the most-specific scope —
	// the controller-level prop bag isn't merged in this case
	// because keys like `StorPoolName` only make sense on the
	// resource itself. Same for RG/RD: those scopes' non-DRBD
	// props are filtered out (they're config knobs for autoplace,
	// not for the .res render).
	for key, value := range resource {
		if !strings.HasPrefix(key, PropPrefix) {
			out[key] = value
		}
	}

	return out
}

// FilterDRBD returns only the `DrbdOptions/...` entries from props.
// Used by the dispatcher when it wants to push only DRBD knobs to the
// satellite without leaking unrelated props onto the wire.
func FilterDRBD(props map[string]string) map[string]string {
	out := map[string]string{}

	for key, value := range props {
		if strings.HasPrefix(key, PropPrefix) {
			out[key] = value
		}
	}

	return out
}

// SectionFor returns the `.res` block name (SectionNet, SectionDisk,
// ...) for a `DrbdOptions/<Section>/<Key>` property name. Unknown
// sections fall back to SectionOptions — DRBD's catch-all top-level
// block — so a future upstream key still lands somewhere sensible.
func SectionFor(linstorKey string) string {
	rest, ok := strings.CutPrefix(linstorKey, PropPrefix)
	if !ok {
		return SectionOptions
	}

	section, _, ok := strings.Cut(rest, "/")
	if !ok {
		return SectionOptions
	}

	switch strings.ToLower(section) {
	case "net":
		return SectionNet
	case "disk":
		return SectionDisk
	case "peerdevice", "peer-device":
		return SectionPeerDevice
	case "handlers":
		return SectionHandlers
	default:
		return SectionOptions
	}
}
