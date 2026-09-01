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
	"errors"
	"fmt"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli/command"
)

var errNoSuchDevice = errors.New("no discovered device matches")

// errAmbiguousDevice refuses one operator token that names several
// devices. The attach carries Wipe, so guessing which disk was meant
// is not a recoverable mistake.
var errAmbiguousDevice = errors.New("ambiguous device")

// errDeviceNotFree refuses a device the satellite reported as carrying
// something, or one that is not in the Available phase.
var errDeviceNotFree = errors.New("device is not free")

// errPoolProviderMismatch and errPoolBackingMismatch refuse continuing into
// an existing pool of the same name that describes a different backing store.
var (
	errPoolProviderMismatch = errors.New("a storage pool of that name already exists with another provider")
	errPoolBackingMismatch  = errors.New("a storage pool of that name already exists with another backing store")
)

// physicalStorageCreateDevicePool implements `physical-storage
// create-device-pool <provider> <node> <device>... --pool-name <name>`.
//
// The request is expressed on the discovered device: the satellite
// owns the wipe-and-create, so the CLI records what to attach and
// registers the pool it will become. Creating the pool without
// stamping the device would register a pool the satellite can never
// probe.
func physicalStorageCreateDevicePool(ctx context.Context, run *runContext) error {
	const wantArgs = 3 // provider, node, at least one device

	if len(run.Flags.Positionals) < wantArgs {
		return fmt.Errorf("%w: create-device-pool needs a provider, a node and a device", command.ErrUsage)
	}

	token := strings.ToLower(run.Flags.Positionals[0])

	provider, known := storageProviders[token]
	if !known {
		return fmt.Errorf("%w: unknown storage provider %q", command.ErrUsage, run.Flags.Positionals[0])
	}

	node := run.Flags.Positionals[1]
	devices := run.Flags.Positionals[2:]

	poolName := run.Flags.Values["pool-name"]
	if poolName == "" {
		return fmt.Errorf("%w: create-device-pool needs --pool-name", command.ErrUsage)
	}

	attach := attachRequest(provider, poolName, token)

	pool := &apiv1.StoragePool{
		NodeName:        node,
		StoragePoolName: poolName,
		ProviderKind:    provider.kind,
		Props:           attachProps(provider, attach),
	}

	// The pool is registered BEFORE the devices are stamped, because the
	// stamp is what points a disk at a pool and the two writes are not
	// atomic. Stamping first meant a failure partway through — a typo in
	// the fourth of four device names — left the earlier disks claimed by
	// a pool that was never created, with nothing to reconcile them and
	// no way to retry: the corrected re-run met its own half-finished
	// state. This order leaves the harmless remainder instead, a
	// registered pool with fewer devices than intended, which the same
	// command completes on re-run.
	err := run.Store.StoragePools().Create(ctx, pool)
	if err != nil {
		if !isAlreadyExists(err) {
			return fmt.Errorf("create storage pool %s on %s: %w", poolName, node, err)
		}

		// The name is taken. Tolerating that is what makes a re-run
		// finish an interrupted attempt — but only when the pool that
		// exists is the one this command is building. A pool of another
		// provider wearing the same name is somebody else's: continuing
		// would stamp the device with Wipe: true for the requested
		// backing while the registered pool keeps pointing at theirs, so
		// the disk is erased under a pool that never claimed it.
		existErr := checkAdoptablePool(ctx, run, pool)
		if existErr != nil {
			return existErr
		}
	}

	return stampDevices(ctx, run, node, devices, attach)
}

// checkAdoptablePool refuses to continue into a pool this command did not
// create, and would not have created.
//
// Compared on the fields that decide where the data lands. A difference in
// any of them means the registered pool describes a different backing store,
// and the devices about to be stamped would be wiped for a backing nothing
// reconciles them into.
func checkAdoptablePool(ctx context.Context, run *runContext, wanted *apiv1.StoragePool) error {
	existing, err := run.Store.StoragePools().Get(ctx, wanted.NodeName, wanted.StoragePoolName)
	if err != nil {
		return fmt.Errorf("get storage pool %s on %s: %w",
			wanted.StoragePoolName, wanted.NodeName, err)
	}

	if existing.ProviderKind != wanted.ProviderKind {
		return fmt.Errorf("%w: %s on %s is %s, not %s",
			errPoolProviderMismatch, wanted.StoragePoolName, wanted.NodeName,
			existing.ProviderKind, wanted.ProviderKind)
	}

	for key, want := range wanted.Props {
		if got := existing.Props[key]; got != want {
			return fmt.Errorf("%w: %s on %s has %s=%q, not %q",
				errPoolBackingMismatch, wanted.StoragePoolName, wanted.NodeName,
				key, got, want)
		}
	}

	return nil
}

// attachRequest names the backing the satellite has to create. The
// pool name doubles as the volume-group / zpool name, which is what
// `--pool-name` means to an operator running this verb.
func attachRequest(provider storageProvider, poolName, token string) *apiv1.PhysicalDeviceAttachTo {
	attach := &apiv1.PhysicalDeviceAttachTo{
		StoragePoolName: poolName,
		ProviderKind:    provider.kind,
		// The operator ran create-device-pool, which is the explicit
		// opt-in to the satellite reusing the device.
		Wipe: true,
	}

	switch {
	case strings.HasPrefix(token, "zfs"):
		attach.ZPoolName = poolName
	case strings.HasPrefix(token, "file"):
		attach.Directory = poolName
	case strings.HasPrefix(token, "lvm"):
		attach.VGName = poolName

		if provider.splitThinPool {
			attach.ThinPoolName = poolName
		}
	}

	return attach
}

// attachProps mirrors the attach request into the pool's own
// StorDriver keys, which is where the satellite's provider factory
// reads the backing from.
func attachProps(provider storageProvider, attach *apiv1.PhysicalDeviceAttachTo) map[string]string {
	props := map[string]string{}

	if attach.VGName != "" {
		props[propLvmVG] = attach.VGName
	}

	if attach.ThinPoolName != "" {
		props[propThinPool] = attach.ThinPoolName
	}

	if attach.ZPoolName != "" {
		key := propZPool
		if provider.kind == apiv1.StoragePoolKindZFSThin {
			key = propZPoolThin
		}

		props[key] = attach.ZPoolName
	}

	if attach.Directory != "" {
		props[propFileDir] = attach.Directory
	}

	return props
}

// stampDevices records the attach request on each named device.
//
// A device is matched by any of the names an operator might have in
// front of them — the stable id, the by-id path, or the volatile
// /dev/sdN one that `lsblk` just printed.
func stampDevices(
	ctx context.Context, run *runContext, node string, devices []string, attach *apiv1.PhysicalDeviceAttachTo,
) error {
	known, err := run.Store.PhysicalDevices().ListForNode(ctx, node)
	if err != nil {
		return fmt.Errorf("list devices on %s: %w", node, err)
	}

	// Resolve every device before stamping any of them. The attach is
	// not atomic across devices, so a typo in the fourth of four names
	// would otherwise leave the first three claimed by a pool that is
	// never created — and a corrected re-run is then refused, because
	// those three now look like someone else's claim. The operator is
	// stuck editing CRs by hand. The REST path validates all targets
	// before its first write for the same reason, as does this CLI's
	// own resource-group spawn.
	names := make([]string, 0, len(devices))

	for _, wanted := range devices {
		name, resolveErr := resolveDevice(known, wanted, node)
		if resolveErr != nil {
			return resolveErr
		}

		names = append(names, name)
	}

	for i, wanted := range devices {
		// The device list was read once, up front. Writing the whole
		// spec back from that snapshot reverts whatever else changed
		// on the device since; patch only the field this command is
		// about, and decide against the freshly fetched state.
		err = run.Store.PhysicalDevices().PatchPhysicalDeviceSpec(ctx, names[i],
			func(dev *apiv1.PhysicalDevice) error {
				return claimDevice(dev, wanted, node, attach)
			})
		if err != nil {
			return fmt.Errorf("attach %s on %s: %w", wanted, node, err)
		}
	}

	return nil
}

// resolveDevice picks the single record an operator token names.
//
// A token can legitimately match more than one record — two rows share
// a volatile CurrentDevPath after a /dev/sdX reshuffle, and
// deviceMatches compares that path — and the attach that follows
// carries Wipe: true. Stamping every match would wipe several disks
// from one ambiguous word, so refuse and make the operator name the
// stable id instead.
func resolveDevice(known []apiv1.PhysicalDevice, wanted, node string) (string, error) {
	matched := make([]string, 0, 1)

	for i := range known {
		if deviceMatches(&known[i], wanted) {
			matched = append(matched, known[i].Name)
		}
	}

	switch len(matched) {
	case 0:
		return "", fmt.Errorf("%s on %s: %w", wanted, node, errNoSuchDevice)
	case 1:
		return matched[0], nil
	default:
		return "", fmt.Errorf("%w: %s on %s names %d devices (%s); name the stable id",
			errAmbiguousDevice, wanted, node, len(matched), strings.Join(matched, ", "))
	}
}

// claimDevice is the attach decision, evaluated against the state the
// write lands on rather than the list read up front.
//
// Every refusal here is a disk that would otherwise be wiped: the
// attach carries Wipe: true and the satellite acts on that flag with
// `wipefs --all --force` and no guard of its own. The REST handler for
// this same verb gates on the same three signals, and this path writes
// the CRD directly rather than going through it, so the gate has to be
// repeated here or it is simply absent.
func claimDevice(
	dev *apiv1.PhysicalDevice, wanted, node string, attach *apiv1.PhysicalDeviceAttachTo,
) error {
	// Refuse a device some other pool already claimed. The check
	// belongs HERE rather than before the patch, and that is the whole
	// point of it: two concurrent create-device-pool runs both see
	// AttachTo=nil when they look, so only a check made against the
	// state the write lands on can reject the loser.
	if dev.AttachTo != nil {
		// Already pointing at the pool this command is building: that is
		// the previous attempt's work, not someone else's claim. Treat it
		// as done so a corrected re-run finishes the job instead of
		// stalling on its own leftovers.
		if sameAttach(dev.AttachTo, attach) {
			return nil
		}

		return fmt.Errorf("%w: device %s on %s is already attached to pool %s",
			store.ErrAlreadyExists, wanted, node, dev.AttachTo.StoragePoolName)
	}

	if dev.Phase != "" && dev.Phase != apiv1.PhysicalDevicePhaseAvailable {
		return fmt.Errorf("%w: device %s on %s is in phase %s, not %s",
			errDeviceNotFree, wanted, node, dev.Phase, apiv1.PhysicalDevicePhaseAvailable)
	}

	// The satellite-stamped Free condition is the source of truth for
	// "this disk already carries something". False ⇒ refuse, quoting
	// the reason discovery gave, which is the same cause `physical-
	// storage list` filtered the device out on. nil ⇒ no scan has run
	// yet, so fall through rather than block a bootstrap that has no
	// discovery data to offer.
	if dev.Free != nil && !*dev.Free {
		return fmt.Errorf("%w: device %s on %s (%s)",
			errDeviceNotFree, wanted, node, freeDetail(dev))
	}

	dev.AttachTo = attach

	return nil
}

// sameAttach reports whether an existing claim is the one this command would
// write. Compared on the fields that decide where the data lands; a claim that
// differs in any of them belongs to a different pool and must not be
// overwritten, because the attach carries Wipe.
func sameAttach(existing, wanted *apiv1.PhysicalDeviceAttachTo) bool {
	if existing == nil || wanted == nil {
		return false
	}

	return existing.StoragePoolName == wanted.StoragePoolName &&
		existing.ProviderKind == wanted.ProviderKind &&
		existing.VGName == wanted.VGName &&
		existing.ThinPoolName == wanted.ThinPoolName &&
		existing.ZPoolName == wanted.ZPoolName &&
		existing.Directory == wanted.Directory
}

// freeDetail renders whatever explanation discovery left behind, so the
// refusal names the signature it found rather than only saying no.
func freeDetail(dev *apiv1.PhysicalDevice) string {
	switch {
	case dev.FreeReason != "" && dev.FreeMessage != "":
		return dev.FreeReason + ": " + dev.FreeMessage
	case dev.FreeReason != "":
		return dev.FreeReason
	case dev.FreeMessage != "":
		return dev.FreeMessage
	default:
		return "no reason recorded"
	}
}

func deviceMatches(device *apiv1.PhysicalDevice, wanted string) bool {
	return device.Name == wanted ||
		device.StableID == wanted ||
		device.DevicePath == wanted ||
		device.CurrentDevPath == wanted
}
