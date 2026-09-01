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
	"maps"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli/command"
	"github.com/cozystack/blockstor/internal/cli/view"
)

// propertyAccessor reads and writes one object's property bag. Every
// noun that supports set-property/list-properties plugs into the same
// two handlers through this, so the empty-value-deletes rule below
// cannot drift between them.
type propertyAccessor struct {
	// args is how many positionals identify the object (1 for a node
	// or definition, 0 for the controller).
	args int
	get  func(context.Context, store.Store, []string) (map[string]string, error)
	// edit applies `change` to the object's own property map inside
	// the store's fetch → mutate → patch retry loop.
	//
	// It replaced a `set(whole map)` shape, which read the bag, edited
	// it locally and wrote the result back wholesale: a key another
	// writer added in between was reverted, silently and with no
	// error. That is not hypothetical — the satellite and the
	// migration reconciler both stamp per-replica properties, and a
	// `resource set-property` racing one of them undid its work.
	edit func(ctx context.Context, st store.Store, ident []string, change func(map[string]string) error) error
}

// setProperty implements `<noun> set-property`.
//
// An EMPTY value DELETES the key. That is the upstream behaviour this
// repo's replay workflows depend on: they restore a cluster's
// automatic behaviour by setting a property to "" and then assert the
// key is gone.
func setProperty(accessor propertyAccessor) handler {
	return func(ctx context.Context, run *runContext) error {
		want := accessor.args + 1

		if len(run.Flags.Positionals) < want {
			return fmt.Errorf("%w: set-property needs %d argument(s) plus a key", command.ErrUsage, accessor.args)
		}

		ident := run.Flags.Positionals[:accessor.args]
		key := run.Flags.Positionals[accessor.args]

		value := ""
		if len(run.Flags.Positionals) > want {
			value = run.Flags.Positionals[want]
		}

		return accessor.edit(ctx, run.Store, ident, func(props map[string]string) error {
			if value == "" {
				delete(props, key)
			} else {
				props[key] = value
			}

			return nil
		})
	}
}

// listProperties implements `<noun> list-properties`.
func listProperties(accessor propertyAccessor) handler {
	return func(ctx context.Context, run *runContext) error {
		if len(run.Flags.Positionals) < accessor.args {
			return fmt.Errorf("%w: list-properties needs %d argument(s)", command.ErrUsage, accessor.args)
		}

		props, err := accessor.get(ctx, run.Store, run.Flags.Positionals[:accessor.args])
		if err != nil {
			return err
		}

		keys := make([]string, 0, len(props))
		for key := range props {
			keys = append(keys, key)
		}

		sort.Strings(keys)

		if run.Flags.Machine {
			pairs := make([]map[string]string, 0, len(keys))
			for _, key := range keys {
				pairs = append(pairs, map[string]string{"key": key, "value": props[key]})
			}

			return machineOut(run, pairs)
		}

		tbl := &metav1.Table{ColumnDefinitions: view.PropertyColumns()}
		for _, key := range keys {
			tbl.Rows = append(tbl.Rows, metav1.TableRow{Cells: []any{key, props[key]}})
		}

		return run.render(tbl)
	}
}

// objectProps builds an accessor for any kind whose property bag is a
// plain field on a fetch-mutate-update object. Keeping the shape in
// one place is what stops the empty-value-deletes rule from drifting
// between nouns.
//
// `args` is how many positionals name the object: one for a node or a
// definition, two for the composite-keyed kinds (a storage pool is
// (node, pool), a volume definition is (definition, volume number)).
func objectProps[T any](
	kind string,
	args int,
	get func(context.Context, store.Store, []string) (T, error),
	bag func(*T) *map[string]string,
	patch func(ctx context.Context, st store.Store, ident []string, mutate func(*T) error) error,
	guards ...func(before, after map[string]string) error,
) propertyAccessor {
	return propertyAccessor{
		args: args,
		get: func(ctx context.Context, st store.Store, ident []string) (map[string]string, error) {
			obj, err := get(ctx, st, ident)
			if err != nil {
				return nil, fmt.Errorf("get %s %s: %w", kind, strings.Join(ident, "/"), err)
			}

			return maps.Clone(*bag(&obj)), nil
		},
		edit: func(ctx context.Context, st store.Store, ident []string, change func(map[string]string) error) error {
			err := patch(ctx, st, ident, func(obj *T) error {
				props := bag(obj)
				if *props == nil {
					*props = map[string]string{}
				}

				// Snapshot before the caller edits, so a guard can judge
				// the resulting state rather than the requested operation.
				// set, delete and delete-namespace reach the bag by
				// different routes; comparing states catches all of them,
				// including whichever route is added next.
				before := maps.Clone(*props)

				changeErr := change(*props)
				if changeErr != nil {
					return changeErr
				}

				for _, guard := range guards {
					guardErr := guard(before, *props)
					if guardErr != nil {
						return guardErr
					}
				}

				return nil
			})
			if err != nil {
				return fmt.Errorf("update %s %s: %w", kind, strings.Join(ident, "/"), err)
			}

			return nil
		},
	}
}

// rdProps accesses a resource definition's property bag.
//
//nolint:gochecknoglobals,dupl // static accessor table; the parallel shape is the point
var rdProps = objectProps("resource definition", 1,
	func(ctx context.Context, st store.Store, ident []string) (apiv1.ResourceDefinition, error) {
		return st.ResourceDefinitions().Get(ctx, ident[0])
	},
	func(def *apiv1.ResourceDefinition) *map[string]string { return &def.Props },
	func(ctx context.Context, st store.Store, ident []string, mutate func(*apiv1.ResourceDefinition) error) error {
		return st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, ident[0], mutate)
	},
)

// nodeProps accesses a node's property bag.
//
//nolint:gochecknoglobals,dupl // static accessor table; the parallel shape is the point
var nodeProps = objectProps("node", 1,
	func(ctx context.Context, st store.Store, ident []string) (apiv1.Node, error) {
		return st.Nodes().Get(ctx, ident[0])
	},
	func(node *apiv1.Node) *map[string]string { return &node.Props },
	func(ctx context.Context, st store.Store, ident []string, mutate func(*apiv1.Node) error) error {
		return st.Nodes().PatchNodeSpec(ctx, ident[0], mutate)
	},
)

// controllerProps accesses the cluster-wide property bag.
//
//nolint:gochecknoglobals // static accessor table
var controllerProps = propertyAccessor{
	args: 0,
	get: func(ctx context.Context, st store.Store, _ []string) (map[string]string, error) {
		props, err := st.ControllerProps().Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("get controller properties: %w", err)
		}

		return props, nil
	},
	edit: func(ctx context.Context, st store.Store, _ []string, change func(map[string]string) error) error {
		err := st.ControllerProps().PatchProps(ctx, change)
		if err != nil {
			return fmt.Errorf("set controller properties: %w", err)
		}

		return nil
	},
}

// resourceDefinitionCreate implements `resource-definition create`.
func resourceDefinitionCreate(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: create needs a resource-definition name", command.ErrUsage)
	}

	def := &apiv1.ResourceDefinition{
		Name:              run.Flags.Positionals[0],
		ResourceGroupName: run.Flags.Values["resource-group"],
	}

	if layers := run.Flags.Values["layer-list"]; layers != "" {
		parsed, err := parseLayerList(layers)
		if err != nil {
			return err
		}

		luksErr := checkLUKSPrerequisite(ctx, run, parsed)
		if luksErr != nil {
			return luksErr
		}

		def.LayerStack = parsed
	}

	err := run.Store.ResourceDefinitions().Create(ctx, def)
	if err != nil {
		return fmt.Errorf("create resource definition %s: %w", def.Name, err)
	}

	return nil
}

// resourceDefinitionDelete implements `resource-definition delete`.
//
// Deleting something that is already gone SUCCEEDS: the upstream
// behaviour this repo pins is idempotent, and teardown paths rely on
// it — failing there would break cleanup runs that are otherwise fine.
func resourceDefinitionDelete(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: delete needs a resource-definition name", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	err := run.Store.ResourceDefinitions().Delete(ctx, name)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete resource definition %s: %w", name, err)
	}

	return nil
}

// controllerVersion implements `controller version`.
func controllerVersion(_ context.Context, run *runContext) error {
	_, err := fmt.Fprintf(run.Out, "blockstor %s\n", Version)
	if err != nil {
		return fmt.Errorf("write version: %w", err)
	}

	return nil
}

// Version is stamped at build time; the default keeps a dev build
// honest about what it is.
//
//nolint:gochecknoglobals // set via -ldflags
var Version = "dev"

// isNotFound recognises both the Kubernetes NotFound and the store's
// own sentinel, so idempotent deletes work against either backend.
func isNotFound(err error) bool {
	return apierrors.IsNotFound(err) || errors.Is(err, store.ErrNotFound)
}

// isAlreadyExists is the create-side counterpart, across both
// backends.
func isAlreadyExists(err error) bool {
	return apierrors.IsAlreadyExists(err) || errors.Is(err, store.ErrAlreadyExists)
}
