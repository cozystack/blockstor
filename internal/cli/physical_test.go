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

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

// create-device-pool is the one CLI verb that destroys data on
// purpose: the attach it records carries Wipe: true and the satellite
// acts on that with `wipefs --all --force`. These tests pin the
// refusals, because every one of them is a disk that would otherwise
// be erased on a plausible operator mistake — a stale /dev/sdX after a
// device-letter reshuffle, or a path typed from the wrong host's
// lsblk.

// seedDevice puts one discovered device on node-1 with the discovery
// verdict a test wants to exercise.
func seedDevice(dev apiv1.PhysicalDevice) func(context.Context, store.Store) {
	return func(ctx context.Context, backend store.Store) {
		dev.NodeName = "node-1"
		_ = backend.PhysicalDevices().Create(ctx, &dev)
	}
}

func devicePtrBool(v bool) *bool { return &v }

// getDevice reads back what actually landed on the device, so a test
// asserts on the store rather than on the message.
func getDevice(t *testing.T, app *cli.App, name string) apiv1.PhysicalDevice {
	t.Helper()

	dev, err := appStore(t, app).PhysicalDevices().Get(t.Context(), name)
	if err != nil {
		t.Fatalf("get device %s: %v", name, err)
	}

	return dev
}

// TestCreateDevicePoolRefusesADeviceCarryingASignature is the case the
// REST handler already refused and this path did not: discovery found
// a filesystem or LVM signature and stamped Free=False, so attaching
// would wipe a disk that is in use by something.
func TestCreateDevicePoolRefusesADeviceCarryingASignature(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDevice(apiv1.PhysicalDevice{
		Name:           "node-1-sdb",
		DevicePath:     "/dev/disk/by-id/scsi-SATA_disk_b",
		CurrentDevPath: "/dev/sdb",
		Phase:          apiv1.PhysicalDevicePhaseAvailable,
		Free:           devicePtrBool(false),
		FreeReason:     "SignatureFound",
		FreeMessage:    "found existing LVM2_member signature",
	}))

	got := app.Run(t.Context(), []string{
		"ps", "cdp", "zfs", "node-1", "/dev/sdb", "--pool-name", "data",
	})
	if got == 0 {
		t.Fatalf("create-device-pool on a signatured device exited 0; want a refusal")
	}

	if msg := errBuf.String(); !strings.Contains(msg, "SignatureFound") {
		t.Errorf("refusal does not name what discovery found: %s", msg)
	}

	if dev := getDevice(t, app, "node-1-sdb"); dev.AttachTo != nil {
		t.Errorf("a refused attach still stamped the device: %+v", dev.AttachTo)
	}
}

// TestCreateDevicePoolRefusesANonAvailablePhase: a device mid-attach or
// failed is not a candidate, and the REST path skips it. Stamping it
// again would re-trigger the wipe underneath the attach already
// running.
func TestCreateDevicePoolRefusesANonAvailablePhase(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, seedDevice(apiv1.PhysicalDevice{
		Name:           "node-1-sdc",
		CurrentDevPath: "/dev/sdc",
		Phase:          "Attaching",
		Free:           devicePtrBool(true),
	}))

	got := app.Run(t.Context(), []string{
		"ps", "cdp", "zfs", "node-1", "/dev/sdc", "--pool-name", "data",
	})
	if got == 0 {
		t.Fatalf("create-device-pool on a device mid-attach exited 0; want a refusal")
	}

	if dev := getDevice(t, app, "node-1-sdc"); dev.AttachTo != nil {
		t.Errorf("a refused attach still stamped the device: %+v", dev.AttachTo)
	}
}

// TestCreateDevicePoolRefusesAnAmbiguousToken: /dev/sdX is volatile, so
// two records can carry the same CurrentDevPath after a reshuffle.
// Stamping every match would wipe both disks from one word.
func TestCreateDevicePoolRefusesAnAmbiguousToken(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		for _, name := range []string{"node-1-sdb", "node-1-sdd"} {
			_ = backend.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
				Name:           name,
				NodeName:       "node-1",
				CurrentDevPath: "/dev/sdb",
				Phase:          apiv1.PhysicalDevicePhaseAvailable,
				Free:           devicePtrBool(true),
			})
		}
	})

	got := app.Run(t.Context(), []string{
		"ps", "cdp", "zfs", "node-1", "/dev/sdb", "--pool-name", "data",
	})
	if got == 0 {
		t.Fatalf("create-device-pool on an ambiguous token exited 0; want a refusal")
	}

	if msg := errBuf.String(); !strings.Contains(msg, "ambiguous") {
		t.Errorf("refusal does not say the token was ambiguous: %s", msg)
	}

	for _, name := range []string{"node-1-sdb", "node-1-sdd"} {
		if dev := getDevice(t, app, name); dev.AttachTo != nil {
			t.Errorf("%s was stamped despite the ambiguity: %+v", name, dev.AttachTo)
		}
	}
}

// TestCreateDevicePoolAcceptsAFreeDevice keeps the gate from becoming a
// blanket refusal: a device discovery declared free must still attach,
// or the verb is useless.
func TestCreateDevicePoolAcceptsAFreeDevice(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDevice(apiv1.PhysicalDevice{
		Name:           "node-1-sdb",
		CurrentDevPath: "/dev/sdb",
		Phase:          apiv1.PhysicalDevicePhaseAvailable,
		Free:           devicePtrBool(true),
		FreeReason:     "FreeBlockDevice",
	}))

	got := app.Run(t.Context(), []string{
		"ps", "cdp", "zfs", "node-1", "/dev/sdb", "--pool-name", "data",
	})
	if got != 0 {
		t.Fatalf("create-device-pool on a free device exit = %d (stderr: %s)", got, errBuf.String())
	}

	dev := getDevice(t, app, "node-1-sdb")
	if dev.AttachTo == nil {
		t.Fatalf("a free device was not stamped")
	}

	if dev.AttachTo.StoragePoolName != "data" {
		t.Errorf("attach names pool %q, want data", dev.AttachTo.StoragePoolName)
	}
}

// TestCreateDevicePoolAcceptsADeviceWithNoDiscoveryVerdict mirrors the
// REST handler's bootstrap carve-out: Free=nil means no scan has run,
// not "carries data". Refusing it would block a cluster that has never
// completed discovery — the exact moment an operator needs this verb.
func TestCreateDevicePoolAcceptsADeviceWithNoDiscoveryVerdict(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDevice(apiv1.PhysicalDevice{
		Name:           "node-1-sdb",
		CurrentDevPath: "/dev/sdb",
	}))

	got := app.Run(t.Context(), []string{
		"ps", "cdp", "zfs", "node-1", "/dev/sdb", "--pool-name", "data",
	})
	if got != 0 {
		t.Fatalf("create-device-pool before discovery exit = %d (stderr: %s)", got, errBuf.String())
	}

	if dev := getDevice(t, app, "node-1-sdb"); dev.AttachTo == nil {
		t.Errorf("a pre-discovery device was not stamped")
	}
}
