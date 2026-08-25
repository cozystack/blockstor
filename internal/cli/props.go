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
	"fmt"
	"maps"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// deleteProperty implements `<noun> delete-property`.
//
// It is set-property with no value, which the shared accessor already
// treats as a delete — expressing it that way keeps the two verbs from
// disagreeing about what "gone" means.
func deleteProperty(accessor propertyAccessor) handler {
	return func(ctx context.Context, run *runContext) error {
		if len(run.Flags.Positionals) < accessor.args+1 {
			return fmt.Errorf("%w: delete-property needs %d argument(s) plus a key", command.ErrUsage, accessor.args)
		}

		// The key is the LAST argument this verb accepts. Passing the
		// positionals through unchanged would let a stray extra one be
		// read as a value by the shared setter, turning a delete into a
		// silent set of the very key the operator wanted gone.
		trimmed := *run
		flags := *run.Flags
		flags.Positionals = run.Flags.Positionals[:accessor.args+1]
		trimmed.Flags = &flags

		return setProperty(accessor)(ctx, &trimmed)
	}
}

// resourceProps accesses one replica's property bag, identified as
// `<node> <resource-definition>` — the same order `resource create`
// and `resource delete` use.
//
//nolint:gochecknoglobals,dupl // static accessor table; the parallel shape is the point
var resourceProps = objectProps("resource", 2, // (node, definition)
	func(ctx context.Context, st store.Store, ident []string) (apiv1.Resource, error) {
		return st.Resources().Get(ctx, ident[1], ident[0])
	},
	func(res *apiv1.Resource) *map[string]string { return &res.Props },
	func(ctx context.Context, st store.Store, ident []string, mutate func(*apiv1.Resource) error) error {
		return st.Resources().PatchResourceSpec(ctx, ident[1], ident[0], mutate)
	},
)

// storagePoolProps accesses a pool's property bag, identified as
// `<node> <pool>`.
//
//nolint:gochecknoglobals,dupl // static accessor table; the parallel shape is the point
var storagePoolProps = objectProps("storage pool", 2, // (node, pool)
	func(ctx context.Context, st store.Store, ident []string) (apiv1.StoragePool, error) {
		return st.StoragePools().Get(ctx, ident[0], ident[1])
	},
	func(pool *apiv1.StoragePool) *map[string]string { return &pool.Props },
	func(ctx context.Context, st store.Store, ident []string, mutate func(*apiv1.StoragePool) error) error {
		return st.StoragePools().PatchStoragePoolSpec(ctx, ident[0], ident[1], mutate)
	},
)

// resourceGroupProps accesses a group's property bag.
//
//nolint:gochecknoglobals,dupl // static accessor table; the parallel shape is the point
var resourceGroupProps = objectProps("resource group", 1,
	func(ctx context.Context, st store.Store, ident []string) (apiv1.ResourceGroup, error) {
		return st.ResourceGroups().Get(ctx, ident[0])
	},
	func(group *apiv1.ResourceGroup) *map[string]string { return &group.Props },
	func(ctx context.Context, st store.Store, ident []string, mutate func(*apiv1.ResourceGroup) error) error {
		return st.ResourceGroups().PatchResourceGroup(ctx, ident[0], mutate)
	},
)

// volumeDefinitionProps accesses a volume definition's property bag,
// identified as `<resource-definition> <volume-number>`.
//
//nolint:gochecknoglobals // static accessor table
var volumeDefinitionProps = propertyAccessor{
	args: 2, // (definition, volume number)
	get: func(ctx context.Context, st store.Store, ident []string) (map[string]string, error) {
		vd, err := getVolumeDefinition(ctx, st, ident)
		if err != nil {
			return nil, err
		}

		return maps.Clone(vd.Props), nil
	},
	edit: func(ctx context.Context, st store.Store, ident []string, change func(map[string]string) error) error {
		number, err := parseInt32(ident[1], "volume number")
		if err != nil {
			return err
		}

		err = st.VolumeDefinitions().PatchVolumeDefinitionSpec(ctx, ident[0], number,
			func(vd *apiv1.VolumeDefinition) error {
				if vd.Props == nil {
					vd.Props = map[string]string{}
				}

				return change(vd.Props)
			})
		if err != nil {
			return fmt.Errorf("update volume definition %s/%s: %w", ident[0], ident[1], err)
		}

		return nil
	},
}

func getVolumeDefinition(ctx context.Context, st store.Store, ident []string) (apiv1.VolumeDefinition, error) {
	number, err := parseInt32(ident[1], "volume number")
	if err != nil {
		return apiv1.VolumeDefinition{}, err
	}

	vd, err := st.VolumeDefinitions().Get(ctx, ident[0], number)
	if err != nil {
		return apiv1.VolumeDefinition{}, fmt.Errorf("get volume definition %s/%s: %w", ident[0], ident[1], err)
	}

	return vd, nil
}

// volumeGroupProps accesses a volume group's property bag, identified
// as `<resource-group> <volume-number>`.
//
// A volume group is a template nested inside its resource group rather
// than an object of its own, so both halves fetch the parent and index
// into it. A missing volume number is a real failure, not an implicit
// create: silently appending a template would change what every future
// resource spawned from the group looks like.
//
//nolint:gochecknoglobals // static accessor table
var volumeGroupProps = propertyAccessor{
	args: 2, // (resource group, volume number)
	get: func(ctx context.Context, st store.Store, ident []string) (map[string]string, error) {
		group, index, err := findVolumeGroup(ctx, st, ident)
		if err != nil {
			return nil, err
		}

		return maps.Clone(group.VolumeGroups[index].Props), nil
	},
	edit: func(ctx context.Context, st store.Store, ident []string, change func(map[string]string) error) error {
		number, err := parseInt32(ident[1], "volume number")
		if err != nil {
			return err
		}

		// The index lookup lives INSIDE the patch: the templates are a
		// slice, so a concurrent `volume-group create` shifts them and
		// an index resolved beforehand would edit the wrong template.
		err = st.ResourceGroups().PatchResourceGroup(ctx, ident[0],
			func(group *apiv1.ResourceGroup) error {
				for i := range group.VolumeGroups {
					if group.VolumeGroups[i].VolumeNumber != number {
						continue
					}

					if group.VolumeGroups[i].Props == nil {
						group.VolumeGroups[i].Props = map[string]string{}
					}

					return change(group.VolumeGroups[i].Props)
				}

				return fmt.Errorf("resource group %s has no volume group %d: %w",
					ident[0], number, store.ErrNotFound)
			})
		if err != nil {
			return fmt.Errorf("update volume group %s/%s: %w", ident[0], ident[1], err)
		}

		return nil
	},
}

func findVolumeGroup(ctx context.Context, st store.Store, ident []string) (apiv1.ResourceGroup, int, error) {
	number, err := parseInt32(ident[1], "volume number")
	if err != nil {
		return apiv1.ResourceGroup{}, 0, err
	}

	group, err := st.ResourceGroups().Get(ctx, ident[0])
	if err != nil {
		return apiv1.ResourceGroup{}, 0, fmt.Errorf("get resource group %s: %w", ident[0], err)
	}

	for i := range group.VolumeGroups {
		if group.VolumeGroups[i].VolumeNumber == number {
			return group, i, nil
		}
	}

	return apiv1.ResourceGroup{}, 0, fmt.Errorf("resource group %s has no volume group %d: %w",
		ident[0], number, store.ErrNotFound)
}
