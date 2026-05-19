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

package uevent

import "strings"

// SubsystemBlock is the kernel SUBSYSTEM= value the satellite cares
// about. Block-device mutations (wipefs / pvcreate / zpool destroy
// / hotplug) all surface under this subsystem; events for `tty`,
// `net`, `usb`, etc. are filtered out at parse time so the consumer
// channel never carries traffic the discovery loop would discard
// anyway.
const SubsystemBlock = "block"

// Standard kernel ACTION= values for block subsystem events. Pinned
// as named constants so the consumer's debounce / filter logic can
// pattern-match without typos. We don't restrict the listener to
// these — any future kernel-added action would still flow through;
// the constants are intent documentation, not a whitelist.
const (
	ActionAdd     = "add"
	ActionRemove  = "remove"
	ActionChange  = "change"
	ActionMove    = "move"
	ActionOnline  = "online"
	ActionOffline = "offline"
)

// Event is the parsed shape of one block-subsystem netlink frame.
// Fields mirror the kernel's `KEY=VALUE` payload keys the satellite
// discovery loop needs to decide whether the event warrants an
// `lsblk` re-run:
//
//   - Action is `add`/`remove`/`change` (and friends — see
//     `Action*` constants).
//   - Devpath is the sysfs path under `/sys`, e.g.
//     `/devices/pci0000:00/.../block/sda/sda1`.
//   - Subsystem is always `block` for events the parser emits;
//     others get filtered out before they reach the channel.
//   - Kernel is the bare kobject name (`sda`, `sda1`, `dm-0`).
//   - Devname is the `/dev` node basename when the kernel provides
//     one (`sda`, `sda1`, `dm-0`); empty for events that don't
//     correspond to a `/dev` node (`change` on a parent kobject).
//
// All other key/value pairs from the netlink frame are discarded —
// they carry udev-rules-specific noise (ID_FS_TYPE, ID_VENDOR, …)
// the discovery loop re-derives from `lsblk` anyway.
type Event struct {
	Action    string
	Devpath   string
	Subsystem string
	Kernel    string
	Devname   string
}

// ParseFrame parses one netlink frame into an Event. Exported so the
// parser unit tests can pin frame-shape behaviour without opening a
// real netlink socket. Returns ok=false for frames the satellite
// doesn't care about:
//
//   - libudev-rebroadcast frames (start with the magic `libudev`
//     prefix). We bind only to the kernel group so these
//     shouldn't reach us, but a defensive check is cheap.
//   - Frames whose SUBSYSTEM= isn't `block`.
//   - Frames missing the mandatory header (`ACTION@DEVPATH\0`).
//
// Wire format reminder: the frame is a sequence of NUL-terminated
// strings. The first string is the human-readable header
// `ACTION@DEVPATH`; the rest are `KEY=VALUE` pairs (which re-state
// ACTION + DEVPATH redundantly). We use the KEY=VALUE form for
// extraction because it's already-typed.
func ParseFrame(frame []byte) (Event, bool) {
	// libudev rebroadcast starts with the literal bytes "libudev\0".
	// Reject so the rest of the parser can assume the kernel format.
	if len(frame) >= 8 && string(frame[:8]) == "libudev\x00" {
		return Event{}, false
	}

	// Header sanity: the first segment must contain '@' (e.g.
	// `add@/devices/pci...`). Without it the frame is not a kernel
	// uevent and parsing the rest would produce noise.
	header, rest, ok := splitNUL(frame)
	if !ok {
		return Event{}, false
	}

	if !strings.Contains(header, "@") {
		return Event{}, false
	}

	event := parseKeyValuePairs(rest)

	if event.Subsystem != SubsystemBlock {
		return Event{}, false
	}

	if event.Action == "" || event.Devpath == "" {
		return Event{}, false
	}

	// Kernel name is the basename of DEVPATH. lsblk reports the
	// same string in its KNAME column, so callers can index by it
	// without an extra `/sys` round-trip.
	event.Kernel = baseName(event.Devpath)

	return event, true
}

// parseKeyValuePairs walks the NUL-separated KEY=VALUE tail of a
// netlink frame and extracts the four keys the satellite cares
// about. Split out of ParseFrame so the gocyclo budget on the outer
// function stays comfortable — the inner switch alone burns five
// branches and grows every time a new key is added.
func parseKeyValuePairs(rest []byte) Event {
	var event Event

	for len(rest) > 0 {
		segment, remainder, ok := splitNUL(rest)
		if !ok {
			break
		}

		rest = remainder

		if segment == "" {
			continue
		}

		key, value, found := strings.Cut(segment, "=")
		if !found {
			continue
		}

		switch key {
		case "ACTION":
			event.Action = value
		case "DEVPATH":
			event.Devpath = value
		case "SUBSYSTEM":
			event.Subsystem = value
		case "DEVNAME":
			event.Devname = value
		}
	}

	return event
}

// splitNUL returns the prefix of buf up to the first NUL byte plus
// the remainder after it. ok=false when no NUL is present (the
// frame is truncated or malformed) — the caller drops the rest.
func splitNUL(buf []byte) (string, []byte, bool) {
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i]), buf[i+1:], true
		}
	}

	return "", nil, false
}

// baseName returns the final path segment of a sysfs DEVPATH. We
// avoid `path/filepath.Base` because the netlink frames use
// forward-slashes regardless of OS and a future Windows-host
// developer running `go vet` would see backslash handling kick in
// where the actual on-wire data is always forward-slash. The
// satellite only builds on Linux but the netlink parser is plain
// enough to be unit-testable on every platform.
func baseName(devpath string) string {
	for i := len(devpath) - 1; i >= 0; i-- {
		if devpath[i] == '/' {
			return devpath[i+1:]
		}
	}

	return devpath
}
