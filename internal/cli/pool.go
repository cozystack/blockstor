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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/command"
	"github.com/cozystack/blockstor/internal/cli/view"
)

// The `StorDriver/*` property keys the satellite's provider factory
// reads to find a pool's backing storage. A pool registered without
// the key its kind expects is permanently un-reconcilable — the
// satellite rejects every attach and capacity is never probed — so the
// mapping below is pinned by test rather than inferred at runtime.
const (
	propLvmVG     = "StorDriver/LvmVg"
	propThinPool  = "StorDriver/ThinPool"
	propZPool     = "StorDriver/ZPool"
	propZPoolThin = "StorDriver/ZPoolThin"
	propFileDir   = "StorDriver/FileDir"
)

// storageProvider describes one `storage-pool create` provider token:
// the kind it registers and where the backing name goes.
type storageProvider struct {
	kind string
	// backingKey is the property the backing name is written to. Empty
	// means the provider takes no backing name (diskless).
	backingKey string
	// splitThinPool marks the providers whose backing name is written
	// `<volume-group>/<thin-pool>` and has to be taken apart.
	splitThinPool bool
}

// storageProviders maps the provider tokens an operator types to what
// gets persisted.
var storageProviders = map[string]storageProvider{ //nolint:gochecknoglobals // static provider table
	"lvm":      {kind: apiv1.StoragePoolKindLVM, backingKey: propLvmVG},
	"lvmthin":  {kind: apiv1.StoragePoolKindLVMThin, backingKey: propLvmVG, splitThinPool: true},
	"zfs":      {kind: apiv1.StoragePoolKindZFS, backingKey: propZPool},
	"zfsthin":  {kind: apiv1.StoragePoolKindZFSThin, backingKey: propZPoolThin},
	"file":     {kind: apiv1.StoragePoolKindFile, backingKey: propFileDir},
	"filethin": {kind: apiv1.StoragePoolKindFileThin, backingKey: propFileDir},
	"diskless": {kind: apiv1.StoragePoolKindDiskless},
}

// storagePoolCreate implements `storage-pool create <provider> <node>
// <pool> [<backing>]`.
func storagePoolCreate(ctx context.Context, run *runContext) error {
	const wantArgs = 3 // provider, node, pool

	if len(run.Flags.Positionals) < wantArgs {
		return fmt.Errorf("%w: storage-pool create needs a provider, a node and a pool name", command.ErrUsage)
	}

	token := strings.ToLower(run.Flags.Positionals[0])

	provider, known := storageProviders[token]
	if !known {
		return fmt.Errorf("%w: unknown storage provider %q", command.ErrUsage, run.Flags.Positionals[0])
	}

	backing := ""
	if len(run.Flags.Positionals) > wantArgs {
		backing = run.Flags.Positionals[wantArgs]
	}

	props, err := poolProps(provider, backing)
	if err != nil {
		return err
	}

	pool := &apiv1.StoragePool{
		NodeName:        run.Flags.Positionals[1],
		StoragePoolName: run.Flags.Positionals[2],
		ProviderKind:    provider.kind,
		Props:           props,
	}

	err = run.Store.StoragePools().Create(ctx, pool)
	if err != nil {
		return fmt.Errorf("create storage pool %s on %s: %w", pool.StoragePoolName, pool.NodeName, err)
	}

	return nil
}

// poolProps places the backing name under the key its provider reads.
func poolProps(provider storageProvider, backing string) (map[string]string, error) {
	props := map[string]string{}

	if provider.backingKey == "" || backing == "" {
		if provider.backingKey != "" {
			return nil, fmt.Errorf("%w: this provider needs a backing name", command.ErrUsage)
		}

		return props, nil
	}

	if !provider.splitThinPool {
		props[provider.backingKey] = backing

		return props, nil
	}

	// A thin LVM pool is addressed as `<volume-group>/<thin-pool>`.
	// Guessing the thin-pool name from the volume group would register
	// a pool that points at storage which does not exist, so a missing
	// half is rejected instead.
	group, thin, split := strings.Cut(backing, "/")
	if !split || group == "" || thin == "" {
		return nil, fmt.Errorf("%w: a thin pool is named <volume-group>/<thin-pool>, got %q",
			command.ErrUsage, backing)
	}

	props[provider.backingKey] = group
	props[propThinPool] = thin

	return props, nil
}

// storagePoolDelete implements `storage-pool delete <node> <pool>`.
func storagePoolDelete(ctx context.Context, run *runContext) error {
	const wantArgs = 2

	if len(run.Flags.Positionals) < wantArgs {
		return fmt.Errorf("%w: storage-pool delete needs a node and a pool name", command.ErrUsage)
	}

	node, pool := run.Flags.Positionals[0], run.Flags.Positionals[1]

	err := run.Store.StoragePools().Delete(ctx, node, pool)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete storage pool %s on %s: %w", pool, node, err)
	}

	return nil
}

// volumeGroupCreate implements `volume-group create <resource-group>`.
//
// A volume group is a per-volume template nested inside its resource
// group, so this appends to the parent. Without an explicit --vlmnr
// the next free number is taken: reusing an existing one would silently
// rewrite the template every future resource spawns from.
func volumeGroupCreate(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: volume-group create needs a resource group", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	group, err := run.Store.ResourceGroups().Get(ctx, name)
	if err != nil {
		return fmt.Errorf("get resource group %s: %w", name, err)
	}

	number, err := volumeGroupNumber(run, &group)
	if err != nil {
		return err
	}

	group.VolumeGroups = append(group.VolumeGroups, apiv1.VolumeGroup{VolumeNumber: number})

	err = run.Store.ResourceGroups().Update(ctx, &group)
	if err != nil {
		return fmt.Errorf("create volume group %s/%d: %w", name, number, err)
	}

	return nil
}

// volumeGroupNumber resolves the requested volume number, rejecting one
// that is already taken.
func volumeGroupNumber(run *runContext, group *apiv1.ResourceGroup) (int32, error) {
	next := int32(0)
	taken := map[int32]bool{}

	for i := range group.VolumeGroups {
		taken[group.VolumeGroups[i].VolumeNumber] = true

		if group.VolumeGroups[i].VolumeNumber >= next {
			next = group.VolumeGroups[i].VolumeNumber + 1
		}
	}

	raw := run.Flags.Values["vlmnr"]
	if raw == "" {
		return next, nil
	}

	number, err := parseInt32(raw, "--vlmnr")
	if err != nil {
		return 0, err
	}

	if taken[number] {
		return 0, fmt.Errorf("%w: volume group %s/%d already exists", command.ErrUsage, group.Name, number)
	}

	return number, nil
}

// volumeGroupList implements `volume-group list`.
func volumeGroupList(ctx context.Context, run *runContext) error {
	groups, err := run.Store.ResourceGroups().List(ctx)
	if err != nil {
		return fmt.Errorf("list resource groups: %w", err)
	}

	wanted := run.Flags.Values["resource-group"]

	tbl := &metav1.Table{ColumnDefinitions: view.VolumeGroupColumns()}
	rows := make([]apiv1.VolumeGroup, 0)

	for i := range groups {
		if wanted != "" && !strings.EqualFold(wanted, groups[i].Name) {
			continue
		}

		rows = append(rows, groups[i].VolumeGroups...)
		tbl.Rows = append(tbl.Rows, view.VolumeGroupRows(&groups[i])...)
	}

	if run.Flags.Machine {
		return machineOut(run, rows)
	}

	return run.render(tbl)
}

// errNoErrorReports explains why this one listing cannot be served
// from Kubernetes.
//
// Error reports are held in the controller process's own memory ring
// (`pkg/rest/error_reports.go`), not in any API object. Rendering an
// empty table would read as "no errors", which is the opposite of the
// truth during an incident — so the command says where the reports
// actually are and fails.
var errNoErrorReports = errors.New(
	"error reports are held in the controller's memory, not in Kubernetes objects; " +
		"read them from the controller's /v1/error-reports endpoint")

// errorReportsList implements `error-reports list`.
func errorReportsList(_ context.Context, _ *runContext) error {
	return errNoErrorReports
}
