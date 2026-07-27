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
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/command"
	"github.com/cozystack/blockstor/internal/cli/view"
)

// nodeFlagEvicted is the latched "this node is being drained" marker.
// The autoplacer refuses to pick an EVICTED node for new replicas and
// the migration reconciler watches for it.
const nodeFlagEvicted = "EVICTED"

var (
	errResourcesInUse = errors.New("demote or stop the consumers first")
	errSatelliteAlive = errors.New(
		"its satellite is still reporting ONLINE; use `node evacuate` or `node delete`, or pass --force")
)

// nodeEvacuate implements `node evacuate <node>` and its `node evict`
// alias.
//
// Evacuating a node whose resources are still mounted would let the
// autoplacer and the migration reconciler strand a live volume, so an
// in-use resource refuses the drain. A replica whose satellite has not
// reported yet (`in_use` unset) is NOT a refusal: an operator may
// legitimately drain a node before any observation has landed.
func nodeEvacuate(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: evacuate needs a node", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	if !run.Flags.Force {
		inUse, err := resourcesInUseOn(ctx, run, name)
		if err != nil {
			return err
		}

		if len(inUse) > 0 {
			return fmt.Errorf("cannot evacuate: %d resource(s) on node %s are in use (%s): %w",
				len(inUse), name, strings.Join(inUse, ", "), errResourcesInUse)
		}
	}

	return patchNodeFlags(ctx, run, name, nodeFlagEvicted, true)
}

// nodeRestore implements `node restore <node>`: the drained node came
// back before the migration finished.
func nodeRestore(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: restore needs a node", command.ErrUsage)
	}

	return patchNodeFlags(ctx, run, run.Flags.Positionals[0], nodeFlagEvicted, false)
}

// nodeLost implements `node lost <node>`: the satellite is gone for
// good, so its replicas and pools are cascade-deleted here.
//
// The cascade cannot be left to the satellite's finalizer — the
// satellite that would run it is gone with the node, so a plain
// deletion would hang every orphan forever and brick the next
// definition that recycles the name. Pools on a lost node can never be
// probed again, and leaving them skews the autoplacer's free-space
// ranking. Surviving peer replicas are left alone so the tiebreaker
// reconciler can stamp a fresh witness.
//
// Running this against a live satellite orphans its resources and
// leaves the DRBD state on the host, so an ONLINE node is refused
// unless --force is given.
func nodeLost(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: lost needs a node", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	err := checkNodeLostAllowed(ctx, run, name)
	if err != nil {
		return err
	}

	err = cascadeNodeObjects(ctx, run, name)
	if err != nil {
		return err
	}

	err = run.Store.Nodes().Delete(ctx, name)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete node %s: %w", name, err)
	}

	return nil
}

// checkNodeLostAllowed refuses the cleanup while the satellite is
// still alive. An absent node is allowed through so a re-run of a
// teardown script stays idempotent.
func checkNodeLostAllowed(ctx context.Context, run *runContext, name string) error {
	if run.Flags.Force {
		return nil
	}

	node, err := run.Store.Nodes().Get(ctx, name)
	if isNotFound(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("get node %s: %w", name, err)
	}

	// Anything other than the literal ONLINE the satellite stamps on
	// every heartbeat counts as not-online, so a partial outage
	// (CONNECTING, OFFLINE, or a node whose satellite never checked
	// in) still lets the operator clean up without the escape hatch.
	if !strings.EqualFold(node.ConnectionStatus, apiv1.NodeTypeOnline) {
		return nil
	}

	return fmt.Errorf("node %s cannot be lost: %w", name, errSatelliteAlive)
}

// cascadeNodeObjects removes the replicas and pools that can never be
// reconciled again.
func cascadeNodeObjects(ctx context.Context, run *runContext, name string) error {
	resources, err := run.Store.Resources().List(ctx)
	if err != nil {
		return fmt.Errorf("list resources: %w", err)
	}

	for i := range resources {
		if resources[i].NodeName != name {
			continue
		}

		err = run.Store.Resources().Delete(ctx, resources[i].Name, name)
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("delete resource %s on %s: %w", resources[i].Name, name, err)
		}
	}

	pools, err := run.Store.StoragePools().ListByNode(ctx, name)
	if err != nil {
		return fmt.Errorf("list storage pools on %s: %w", name, err)
	}

	for i := range pools {
		err = run.Store.StoragePools().Delete(ctx, name, pools[i].StoragePoolName)
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("delete storage pool %s on %s: %w", pools[i].StoragePoolName, name, err)
		}
	}

	return nil
}

// resourcesInUseOn names the replicas a consumer currently holds
// Primary on the node, sorted so the message is stable.
func resourcesInUseOn(ctx context.Context, run *runContext, name string) ([]string, error) {
	resources, err := run.Store.Resources().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list resources: %w", err)
	}

	var inUse []string

	for i := range resources {
		res := &resources[i]
		if res.NodeName == name && res.State.InUse != nil && *res.State.InUse {
			inUse = append(inUse, res.Name)
		}
	}

	sort.Strings(inUse)

	return inUse, nil
}

// patchNodeFlags adds or removes one flag on a node.
func patchNodeFlags(ctx context.Context, run *runContext, name, flag string, want bool) error {
	node, err := run.Store.Nodes().Get(ctx, name)
	if err != nil {
		return fmt.Errorf("get node %s: %w", name, err)
	}

	node.Flags = setFlag(node.Flags, flag, want)

	err = run.Store.Nodes().Update(ctx, &node)
	if err != nil {
		return fmt.Errorf("update node %s: %w", name, err)
	}

	return nil
}

// nodeInfo implements `node info`: the fastest answer to "why didn't
// autoplace pick this node?".
//
// The capability sets come from the node's own reported lists when the
// satellite has published them, and from the build's supported sets
// otherwise — a node that has never checked in still shows what the
// satellite would support rather than an empty table.
func nodeInfo(ctx context.Context, run *runContext) error {
	nodes, err := run.Store.Nodes().List(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	tbl := &metav1.Table{ColumnDefinitions: view.NodeInfoColumns()}
	infos := make([]apiv1.NodeInfo, 0, len(nodes))

	for i := range nodes {
		node := &nodes[i]

		if !matches(run.Flags.Nodes, node.Name) {
			continue
		}

		info := apiv1.NodeInfo{
			Name:                 node.Name,
			SupportedProviders:   fallbackList(node.StorageProviders, apiv1.SupportedStorageProviders),
			SupportedLayers:      fallbackList(node.ResourceLayers, apiv1.SupportedResourceLayers),
			UnsupportedProviders: node.UnsupportedProviders,
			UnsupportedLayers:    node.UnsupportedLayers,
		}

		infos = append(infos, info)
		tbl.Rows = append(tbl.Rows, view.NodeInfoRows(&info)...)
	}

	if run.Flags.Machine {
		return machineOut(run, infos)
	}

	return run.render(tbl)
}

func fallbackList(reported, build []string) []string {
	if len(reported) > 0 {
		return reported
	}

	return build
}
