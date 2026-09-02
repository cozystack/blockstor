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

import "sort"

// The LINSTOR property namespaces DRBD options live under. The segment
// after `DrbdOptions/` is what SectionFor routes on, so it decides
// which `.res` block the option is rendered into — put a `net{}` knob
// under `Resource` and drbdadm rejects the file.
const (
	nsNet        = PropPrefix + "Net/"
	nsDisk       = PropPrefix + "Disk/"
	nsPeerDevice = PropPrefix + "PeerDevice/"
	nsResource   = PropPrefix + "Resource/"
	nsHandlers   = PropPrefix + "Handlers/"
)

// flagNamespaces maps a `drbd-options --<name>` flag to the property
// namespace its value belongs in.
//
// It holds only the knobs Options() does NOT already carry — FlagKey
// consults the catalogue first, so listing a name in both places would
// let the two drift apart. Options() is the small set blockstor
// renders from a typed field; this table is the wider set an operator
// may legitimately set — the names and their section come
// from the drbd.conf-9.0 manual page, which is what an operator reads
// before typing the flag. Anything not listed here is rejected by the
// CLI rather than filed under a guessed namespace: a `net{}` knob
// written to `Resource` renders into the wrong block and wedges every
// subsequent drbdadm adjust for that resource.
//
//nolint:gochecknoglobals // immutable catalogue — `const` can't hold a map
var flagNamespaces = map[string]string{
	// net { }
	"cram-hmac-alg":        nsNet,
	"after-sb-2pri":        nsNet,
	"rr-conflict":          nsNet,
	"ping-timeout":         nsNet,
	"ping-int":             nsNet,
	"connect-int":          nsNet,
	"timeout":              nsNet,
	"max-epoch-size":       nsNet,
	"sndbuf-size":          nsNet,
	"rcvbuf-size":          nsNet,
	"ko-count":             nsNet,
	"allow-two-primaries":  nsNet,
	"data-integrity-alg":   nsNet,
	"verify-alg":           nsNet,
	"csums-alg":            nsNet,
	"use-rle":              nsNet,
	"socket-check-timeout": nsNet,
	"tcp-cork":             nsNet,
	"on-congestion":        nsNet,
	"congestion-fill":      nsNet,
	"congestion-extents":   nsNet,
	"transport":            nsNet,
	"load-balance-paths":   nsNet,
	"tls":                  nsNet,

	// disk { }
	"al-updates":                nsDisk,
	"md-flushes":                nsDisk,
	"disk-barrier":              nsDisk,
	"disk-flushes":              nsDisk,
	"disk-drain":                nsDisk,
	"discard-zeroes-if-aligned": nsDisk,
	"rs-discard-granularity":    nsDisk,
	"disable-write-same":        nsDisk,
	"read-balancing":            nsDisk,
	"resync-after":              nsDisk,
	"block-size":                nsDisk,

	// peer-device { }
	"resync-rate":    nsPeerDevice,
	"c-plan-ahead":   nsPeerDevice,
	"c-delay-target": nsPeerDevice,
	"c-fill-target":  nsPeerDevice,
	"c-min-rate":     nsPeerDevice,
	"bitmap":         nsPeerDevice,

	// options { } — resource scope
	"cpu-mask":                      nsResource,
	"on-no-data-accessible":         nsResource,
	"auto-promote-timeout":          nsResource,
	"peer-ack-window":               nsResource,
	"peer-ack-delay":                nsResource,
	"twopc-timeout":                 nsResource,
	"twopc-retry-timeout":           nsResource,
	"quorum-minimum-redundancy":     nsResource,
	"on-suspended-primary-outdated": nsResource,
	"max-io-depth":                  nsResource,

	// handlers { }
	"fence-peer":           nsHandlers,
	"unfence-peer":         nsHandlers,
	"before-resync-target": nsHandlers,
	"after-resync-target":  nsHandlers,
	"before-resync-source": nsHandlers,
	"after-resync-source":  nsHandlers,
	"pri-on-incon-degr":    nsHandlers,
	"pri-lost-after-sb":    nsHandlers,
	"local-io-error":       nsHandlers,
	"initial-split-brain":  nsHandlers,
	"split-brain":          nsHandlers,
	"out-of-sync":          nsHandlers,
	"quorum-lost":          nsHandlers,
	"disconnected":         nsHandlers,
}

// FlagKey resolves a `drbd-options --<name>` flag to the LINSTOR
// property key its value is stored under, reporting false for a name
// blockstor has no namespace for.
//
// The catalogue in Options() wins where it has an entry, so a knob
// that is rendered from a typed field cannot drift from the key that
// field is transcoded to.
func FlagKey(name string) (string, bool) {
	for _, opt := range Options() {
		if opt.Name == name {
			return opt.LinstorKey, true
		}
	}

	namespace, known := flagNamespaces[name]
	if !known {
		return "", false
	}

	return namespace + name, true
}

// FlagNames lists every knob FlagKey resolves, sorted, so a caller can
// tell an operator what it accepts.
func FlagNames() []string {
	seen := make(map[string]struct{}, len(flagNamespaces))
	for name := range flagNamespaces {
		seen[name] = struct{}{}
	}

	for _, opt := range Options() {
		seen[opt.Name] = struct{}{}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}
