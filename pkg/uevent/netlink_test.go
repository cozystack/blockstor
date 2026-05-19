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

package uevent_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/uevent"
)

// frame assembles a raw netlink frame from its on-wire shape:
//
//	"ACTION@DEVPATH\0KEY=VAL\0KEY=VAL\0…\0"
//
// Centralised here so each test case reads as a list of key=value
// strings rather than an explicit byte-slice with manual NULs.
func frame(header string, keyValues ...string) []byte {
	parts := make([]string, 0, 1+len(keyValues))
	parts = append(parts, header)
	parts = append(parts, keyValues...)

	return []byte(strings.Join(parts, "\x00") + "\x00")
}

// TestParseFrameBlockAdd pins the happy path: an `add` event on a
// real disk produces an Event with Action / Devpath / Subsystem /
// Kernel / Devname filled in. Mirrors the frame shape the kernel
// emits after `echo 1 > /sys/block/sda/device/rescan`.
func TestParseFrameBlockAdd(t *testing.T) {
	t.Parallel()

	raw := frame(
		"add@/devices/pci0000:00/0000:00:1f.2/ata1/host0/target0:0:0/0:0:0:0/block/sda",
		"ACTION=add",
		"DEVPATH=/devices/pci0000:00/0000:00:1f.2/ata1/host0/target0:0:0/0:0:0:0/block/sda",
		"SUBSYSTEM=block",
		"MAJOR=8",
		"MINOR=0",
		"DEVNAME=sda",
		"DEVTYPE=disk",
		"SEQNUM=1234",
	)

	event, ok := uevent.ParseFrame(raw)
	if !ok {
		t.Fatalf("ParseFrame(add@sda): ok=false; want a parsed Event")
	}

	if event.Action != uevent.ActionAdd {
		t.Errorf("Action: got %q, want %q", event.Action, uevent.ActionAdd)
	}

	if event.Subsystem != uevent.SubsystemBlock {
		t.Errorf("Subsystem: got %q, want %q", event.Subsystem, uevent.SubsystemBlock)
	}

	if event.Kernel != "sda" {
		t.Errorf("Kernel: got %q, want %q (basename of Devpath)", event.Kernel, "sda")
	}

	if event.Devname != "sda" {
		t.Errorf("Devname: got %q, want %q", event.Devname, "sda")
	}
}

// TestParseFrameBlockChangeWipefs pins the operator-facing scenario
// the whole feature targets: `wipefs -a /dev/sdb` triggers a
// `change` event on the disk's parent kobject. The parser must
// surface it so the discovery loop can re-run `lsblk` immediately.
func TestParseFrameBlockChangeWipefs(t *testing.T) {
	t.Parallel()

	raw := frame(
		"change@/devices/pci0000:00/0000:00:1f.2/ata2/host1/target1:0:0/1:0:0:0/block/sdb",
		"ACTION=change",
		"DEVPATH=/devices/pci0000:00/0000:00:1f.2/ata2/host1/target1:0:0/1:0:0:0/block/sdb",
		"SUBSYSTEM=block",
		"DEVNAME=sdb",
		"SEQNUM=5678",
	)

	event, ok := uevent.ParseFrame(raw)
	if !ok {
		t.Fatalf("ParseFrame(change@sdb): ok=false; wipefs-triggered events must parse")
	}

	if event.Action != uevent.ActionChange {
		t.Errorf("Action: got %q, want %q", event.Action, uevent.ActionChange)
	}

	if event.Kernel != "sdb" {
		t.Errorf("Kernel: got %q, want %q", event.Kernel, "sdb")
	}
}

// TestParseFrameRejectsNonBlockSubsystem pins the filter: the
// listener subscribes to every kernel event but we don't want
// non-block frames clogging the consumer channel. A USB hotplug
// (subsystem=usb) must drop at parse time.
func TestParseFrameRejectsNonBlockSubsystem(t *testing.T) {
	t.Parallel()

	raw := frame(
		"add@/devices/pci0000:00/0000:00:14.0/usb1/1-1",
		"ACTION=add",
		"DEVPATH=/devices/pci0000:00/0000:00:14.0/usb1/1-1",
		"SUBSYSTEM=usb",
		"DEVNAME=bus/usb/001/002",
	)

	_, ok := uevent.ParseFrame(raw)
	if ok {
		t.Errorf("ParseFrame(usb hotplug): ok=true; want false (non-block must be filtered)")
	}
}

// TestParseFrameRejectsLibudevRebroadcast pins the defence against
// systemd-udevd's secondary multicast group. We only bind to the
// kernel group (bit 0), but if a misconfigured environment ever
// flipped that, libudev frames carry a binary header that would
// confuse the kernel-frame parser.
func TestParseFrameRejectsLibudevRebroadcast(t *testing.T) {
	t.Parallel()

	// libudev rebroadcast: magic "libudev\0" prefix followed by
	// the binary frame metadata. Exact contents past the magic
	// don't matter — the parser must reject as soon as it sees the
	// magic.
	raw := []byte("libudev\x00\x00\x00\x00\x00arbitrary payload follows\x00")

	_, ok := uevent.ParseFrame(raw)
	if ok {
		t.Errorf("ParseFrame(libudev): ok=true; want false (rebroadcast frames must be filtered)")
	}
}

// TestParseFrameRejectsMalformedHeader pins the sanity check: a
// frame whose first segment doesn't contain '@' isn't a kernel
// uevent (the prefix is always `ACTION@DEVPATH`). Without the
// guard the key/value loop would still extract garbage.
func TestParseFrameRejectsMalformedHeader(t *testing.T) {
	t.Parallel()

	raw := []byte("not-a-uevent-frame\x00SUBSYSTEM=block\x00ACTION=add\x00DEVPATH=/devices/x\x00")

	_, ok := uevent.ParseFrame(raw)
	if ok {
		t.Errorf("ParseFrame(malformed header): ok=true; want false")
	}
}

// TestParseFrameRequiresAction pins the contract: a frame missing
// ACTION= is unusable (the consumer's debounce can't classify
// add vs remove), so the parser drops it.
func TestParseFrameRequiresAction(t *testing.T) {
	t.Parallel()

	raw := frame(
		"add@/devices/x/block/sda",
		"DEVPATH=/devices/x/block/sda",
		"SUBSYSTEM=block",
		// no ACTION= key
	)

	_, ok := uevent.ParseFrame(raw)
	if ok {
		t.Errorf("ParseFrame(no ACTION): ok=true; want false")
	}
}

// TestParseFrameKernelFromDevpath pins the kernel-name extraction.
// DEVNAME= isn't always present (some kobjects don't have a /dev
// node) but the kernel name lives in the DEVPATH basename, which
// lsblk uses as KNAME. Matching them lets the discovery loop key
// by the same string the satellite already has.
func TestParseFrameKernelFromDevpath(t *testing.T) {
	t.Parallel()

	raw := frame(
		"change@/devices/virtual/block/dm-0",
		"ACTION=change",
		"DEVPATH=/devices/virtual/block/dm-0",
		"SUBSYSTEM=block",
		// no DEVNAME — the kernel name still comes from DEVPATH
	)

	event, ok := uevent.ParseFrame(raw)
	if !ok {
		t.Fatalf("ParseFrame: ok=false; want parsed even without DEVNAME")
	}

	if event.Kernel != "dm-0" {
		t.Errorf("Kernel: got %q, want %q (DEVPATH basename)", event.Kernel, "dm-0")
	}
}
