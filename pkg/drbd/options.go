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

// Option describes one DRBD-9 configurable knob — a port of one entry
// from upstream's `drbdoptions.json` catalogue. We use it both for
// property validation (Phase 6) and so the satellite reconciler can
// route a flat string→string property bag into the right `.res` file
// section.
type Option struct {
	// Name is the bare option key (e.g. "max-buffers", "auto-promote").
	Name string

	// Section selects which `.res` block this option belongs in:
	// `SectionNet`, `SectionDisk`, `SectionPeerDevice`, `SectionOptions`
	// (top-level resource options), or `SectionHandlers`.
	Section string

	// LinstorKey is the upstream-compatible LINSTOR property name
	// (e.g. "DrbdOptions/Net/max-buffers") that callers set on
	// resource definitions. Stored verbatim so existing golinstor
	// clients keep working unmodified.
	LinstorKey string

	// Default is the value DRBD-9 uses when the option is unset.
	// Empty string means "no default; let DRBD decide" (rarely useful
	// — most are typed).
	Default string

	// Description is a one-line summary, lifted from the DRBD man pages.
	Description string
}

// Section names for `.res` block placement. Exposed so callers can
// route a flat property bag into the correct block without re-parsing
// the LINSTOR key namespace.
const (
	SectionNet        = "net"
	SectionDisk       = "disk"
	SectionPeerDevice = "peer-device"
	SectionOptions    = "options"
	SectionHandlers   = "handlers"
)

// Options is the catalogue of DRBD knobs blockstor knows about. Far
// from exhaustive — DRBD has 100+ options and we add them on demand —
// but covers the keys cozystack-style clusters actually need. Built
// up by section to keep each subset reviewable.
func Options() []Option {
	net := netOptions()
	disk := diskOptions()
	peerDev := peerDeviceOptions()
	res := resourceOptions()

	out := make([]Option, 0, len(net)+len(disk)+len(peerDev)+len(res))
	out = append(out, net...)
	out = append(out, disk...)
	out = append(out, peerDev...)
	out = append(out, res...)

	return out
}

// netOptions returns the `net { … }` section catalogue.
func netOptions() []Option {
	return []Option{
		{
			Name:        "protocol",
			Section:     SectionNet,
			LinstorKey:  "DrbdOptions/Net/protocol",
			Default:     "C",
			Description: "Replication protocol — A (async) / B (semi-sync) / C (sync). Cozystack runs C.",
		},
		{
			Name:        "shared-secret",
			Section:     SectionNet,
			LinstorKey:  "DrbdOptions/Net/shared-secret",
			Description: "HMAC secret authenticating peer-to-peer connections.",
		},
		{
			Name:        "max-buffers",
			Section:     SectionNet,
			LinstorKey:  "DrbdOptions/Net/max-buffers",
			Default:     "8000",
			Description: "Receive ring depth in 4 KiB pages.",
		},
		{
			Name:        "after-sb-0pri",
			Section:     SectionNet,
			LinstorKey:  "DrbdOptions/Net/after-sb-0pri",
			Default:     "discard-zero-changes",
			Description: "Split-brain recovery policy when neither side was Primary.",
		},
		{
			Name:        "after-sb-1pri",
			Section:     SectionNet,
			LinstorKey:  "DrbdOptions/Net/after-sb-1pri",
			Default:     "discard-secondary",
			Description: "Split-brain recovery policy when one side was Primary.",
		},
	}
}

// diskOptions returns the `disk { … }` section catalogue.
func diskOptions() []Option {
	return []Option{
		{
			Name:        "on-io-error",
			Section:     SectionDisk,
			LinstorKey:  "DrbdOptions/Disk/on-io-error",
			Default:     "detach",
			Description: "Behaviour on lower-level IO failure (pass-on / detach / call-local-io-error).",
		},
		{
			Name:        "al-extents",
			Section:     SectionDisk,
			LinstorKey:  "DrbdOptions/Disk/al-extents",
			Default:     "1237",
			Description: "Activity-log size; trades resync time against random-write throughput.",
		},
	}
}

// peerDeviceOptions returns the `peer-device { … }` section catalogue.
func peerDeviceOptions() []Option {
	return []Option{
		{
			Name:        "c-max-rate",
			Section:     SectionPeerDevice,
			LinstorKey:  "DrbdOptions/PeerDevice/c-max-rate",
			Default:     "100M",
			Description: "Upper bound on resync rate in bytes/second.",
		},
	}
}

// resourceOptions returns the top-level `options { … }` catalogue.
func resourceOptions() []Option {
	return []Option{
		{
			Name:        "auto-promote",
			Section:     SectionOptions,
			LinstorKey:  "DrbdOptions/Resource/auto-promote",
			Default:     "yes",
			Description: "Auto-promote to Primary on first open(2).",
		},
		{
			Name:        "quorum",
			Section:     SectionOptions,
			LinstorKey:  "DrbdOptions/Resource/quorum",
			Default:     "majority",
			Description: "Quorum policy — off / majority / all / <count>.",
		},
		{
			Name:        "on-no-quorum",
			Section:     SectionOptions,
			LinstorKey:  "DrbdOptions/Resource/on-no-quorum",
			Default:     "io-error",
			Description: "Behaviour when quorum is lost: io-error or suspend-io.",
		},
	}
}
