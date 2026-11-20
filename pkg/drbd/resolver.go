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

// SectionLess reports whether a `DrbdOptions/...` key has no DRBD section
// after the prefix (`DrbdOptions/auto-verify-alg` rather than
// `DrbdOptions/Net/verify-alg`). LINSTOR keeps its own controller knobs
// there; none of them is a drbd.conf option, so none is rendered as one.
// A key outside the prefix is not section-less, it is not a DRBD option.
func SectionLess(key string) bool {
	rest, ok := strings.CutPrefix(key, PropPrefix)
	if !ok {
		return false
	}

	return !strings.Contains(rest, "/")
}

// LINSTOR's automatic verify algorithm. Its controller stores the
// algorithm every diskful node of a definition supports under
// AutoVerifyAlgKey, and the satellite renders it as the net section's
// verify-alg unless one is set explicitly or AutoVerifyAlgDisableKey is
// set to anything but "false".
const (
	AutoVerifyAlgKey        = "auto-verify-alg"
	AutoVerifyAlgDisableKey = "auto-verify-algo-disable"

	// autoVerifyAlgWinDRBD marks a definition with a Windows peer;
	// WinDRBD supports only crc32c.
	autoVerifyAlgWinDRBD    = "windrbd"
	autoVerifyAlgWinDRBDAlg = "crc32c"

	verifyAlgOption = "verify-alg"
)

// FillAutoVerifyAlg sets the net section's verify-alg from LINSTOR's
// automatic algorithm in props, a resolved `DrbdOptions/...` bag, the way
// LINSTOR's satellite does: only when net carries no verify-alg of its
// own, the automatic one is set, and the disable switch is unset or
// "false". A WinDRBD peer gets crc32c, the one algorithm it supports.
//
// blockstor never stores an automatic algorithm itself; a definition
// adopted from LINSTOR does, and without this its kernels would lose the
// verify-alg they ran with.
//
// LINSTOR reads the disable switch from the definition, its group and
// the satellite bag, not from the replica or its node; props here is the
// bag already resolved across every scope, so a switch set on a single
// replica counts too. LINSTOR offers no command that sets one there.
func FillAutoVerifyAlg(net, props map[string]string) {
	for key := range net {
		if strings.EqualFold(key, verifyAlgOption) {
			return
		}
	}

	alg := props[PropPrefix+AutoVerifyAlgKey]
	if alg == "" {
		return
	}

	if disable, set := props[PropPrefix+AutoVerifyAlgDisableKey]; set && !strings.EqualFold(disable, "false") {
		return
	}

	if strings.EqualFold(alg, autoVerifyAlgWinDRBD) {
		alg = autoVerifyAlgWinDRBDAlg
	}

	net[verifyAlgOption] = alg
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
