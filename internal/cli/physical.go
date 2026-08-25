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

	"github.com/cozystack/blockstor/internal/cli/command"
)

var errNoSuchDevice = errors.New("no discovered device matches")

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

	err := stampDevices(ctx, run, node, devices, attach)
	if err != nil {
		return err
	}

	pool := &apiv1.StoragePool{
		NodeName:        node,
		StoragePoolName: poolName,
		ProviderKind:    provider.kind,
		Props:           attachProps(provider, attach),
	}

	err = run.Store.StoragePools().Create(ctx, pool)
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("create storage pool %s on %s: %w", poolName, node, err)
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

	for _, wanted := range devices {
		found := false

		for i := range known {
			if !deviceMatches(&known[i], wanted) {
				continue
			}

			found = true

			// The device list was read once, up front. Writing the
			// whole spec back from that snapshot reverts whatever
			// else changed on the device since; patch only the field
			// this command is about.
			err = run.Store.PhysicalDevices().PatchPhysicalDeviceSpec(ctx, known[i].Name,
				func(dev *apiv1.PhysicalDevice) error {
					dev.AttachTo = attach

					return nil
				})
			if err != nil {
				return fmt.Errorf("attach %s on %s: %w", wanted, node, err)
			}
		}

		if !found {
			return fmt.Errorf("%s on %s: %w", wanted, node, errNoSuchDevice)
		}
	}

	return nil
}

func deviceMatches(device *apiv1.PhysicalDevice, wanted string) bool {
	return device.Name == wanted ||
		device.StableID == wanted ||
		device.DevicePath == wanted ||
		device.CurrentDevPath == wanted
}
