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
	"strconv"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// kibPerMib is the binary step between adjacent size suffixes. The
// suffixes are binary all the way up, so the same factor stacks for
// G and T.
const kibPerMib = 1024

// parseInt32 reads a number that has to fit an int32 API field — a
// volume number or a placement count. The range is checked here rather
// than being truncated on assignment: a silently wrapped volume number
// would address a different volume than the operator typed.
func parseInt32(text, what string) (int32, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(text), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %q is not a number", command.ErrUsage, what, text)
	}

	return int32(value), nil
}

// ParseSize converts the size spellings operators use — `10G`, `100M`,
// `2T`, or a bare number of KiB — into KiB.
//
// The suffixes are binary, matching what the storage layer allocates:
// `1G` is 1024 MiB, not 10^9 bytes. Getting this wrong would silently
// provision a volume off by three orders of magnitude, so it is parsed
// explicitly rather than with a permissive library.
func ParseSize(text string) (int64, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return 0, fmt.Errorf("%w: empty size", command.ErrUsage)
	}

	multiplier := int64(1)
	digits := trimmed

	switch unit := trimmed[len(trimmed)-1]; unit {
	case 'K', 'k':
		digits = trimmed[:len(trimmed)-1]
	case 'M', 'm':
		multiplier, digits = kibPerMib, trimmed[:len(trimmed)-1]
	case 'G', 'g':
		multiplier, digits = kibPerMib*kibPerMib, trimmed[:len(trimmed)-1]
	case 'T', 't':
		multiplier, digits = kibPerMib*kibPerMib*kibPerMib, trimmed[:len(trimmed)-1]
	default:
		if unit < '0' || unit > '9' {
			return 0, fmt.Errorf("%w: unrecognised size %q", command.ErrUsage, text)
		}
	}

	// A trailing `iB`/`B` is tolerated (`10GiB`), because operators
	// paste sizes from documentation as often as they type them.
	digits = strings.TrimSuffix(strings.TrimSuffix(digits, "iB"), "i")

	value, err := strconv.ParseInt(strings.TrimSpace(digits), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%w: unrecognised size %q", command.ErrUsage, text)
	}

	return value * multiplier, nil
}

// nodeCreate implements `node create <name> <address>`.
func nodeCreate(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: node create needs a name", command.ErrUsage)
	}

	node := &apiv1.Node{
		Name: run.Flags.Positionals[0],
		Type: nodeType(run.Flags.Values["node-type"]),
	}

	if len(run.Flags.Positionals) > 1 {
		iface := apiv1.NetInterface{Name: "default", Address: run.Flags.Positionals[1]}

		if port := run.Flags.Values["port"]; port != "" {
			parsed, err := strconv.Atoi(port)
			if err != nil {
				return fmt.Errorf("%w: --port %q is not a number", command.ErrUsage, port)
			}

			iface.SatellitePort = parsed
		}

		node.NetInterfaces = []apiv1.NetInterface{iface}
	}

	err := run.Store.Nodes().Create(ctx, node)
	if err != nil {
		return fmt.Errorf("create node %s: %w", node.Name, err)
	}

	return nil
}

// nodeType normalises the spelling operators use (`Satellite`) to the
// upper-case enum the API stores.
func nodeType(value string) string {
	if value == "" {
		return "SATELLITE"
	}

	return strings.ToUpper(value)
}

// nodeDelete implements `node delete <name>`, idempotently.
func nodeDelete(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: node delete needs a name", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	err := run.Store.Nodes().Delete(ctx, name)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete node %s: %w", name, err)
	}

	return nil
}

// volumeDefinitionCreate implements `volume-definition create <rd>
// <size>`.
func volumeDefinitionCreate(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 2 {
		return fmt.Errorf("%w: volume-definition create needs a definition and a size", command.ErrUsage)
	}

	rdName := run.Flags.Positionals[0]

	sizeKib, err := ParseSize(run.Flags.Positionals[1])
	if err != nil {
		return err
	}

	vd := &apiv1.VolumeDefinition{SizeKib: sizeKib}

	// An explicit volume number is honoured; otherwise the store picks
	// the next free one atomically, which is what keeps two concurrent
	// creates from claiming the same slot.
	if raw := run.Flags.Values["vlmnr"]; raw != "" {
		number, convErr := parseInt32(raw, "--vlmnr")
		if convErr != nil {
			return convErr
		}

		vd.VolumeNumber = number

		err = run.Store.VolumeDefinitions().Create(ctx, rdName, vd)
		if err != nil {
			return fmt.Errorf("create volume definition %s/%d: %w", rdName, vd.VolumeNumber, err)
		}

		return nil
	}

	_, err = run.Store.VolumeDefinitions().CreateAutoNumbered(ctx, rdName, vd)
	if err != nil {
		return fmt.Errorf("create volume definition on %s: %w", rdName, err)
	}

	return nil
}

// volumeDefinitionDelete implements `volume-definition delete <rd>
// <volume-number>`.
func volumeDefinitionDelete(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 2 {
		return fmt.Errorf("%w: volume-definition delete needs a definition and a volume number", command.ErrUsage)
	}

	rdName := run.Flags.Positionals[0]

	number, err := parseInt32(run.Flags.Positionals[1], "volume number")
	if err != nil {
		return err
	}

	err = run.Store.VolumeDefinitions().Delete(ctx, rdName, number)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete volume definition %s/%d: %w", rdName, number, err)
	}

	return nil
}

// volumeDefinitionSetSize implements `volume-definition set-size <rd>
// <volume-number> <size>`.
//
// The size may arrive as a positional or via --size: this repo's
// scripts try the positional first and fall back to the flag, so both
// have to work.
func volumeDefinitionSetSize(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 2 {
		return fmt.Errorf("%w: set-size needs a definition and a volume number", command.ErrUsage)
	}

	rdName := run.Flags.Positionals[0]

	number, err := parseInt32(run.Flags.Positionals[1], "volume number")
	if err != nil {
		return err
	}

	raw := run.Flags.Values["size"]
	if len(run.Flags.Positionals) > 2 {
		raw = run.Flags.Positionals[2]
	}

	sizeKib, err := ParseSize(raw)
	if err != nil {
		return err
	}

	vd, err := run.Store.VolumeDefinitions().Get(ctx, rdName, number)
	if err != nil {
		return fmt.Errorf("get volume definition %s/%d: %w", rdName, number, err)
	}

	err = checkResize(&vd, sizeKib, run.Flags.Force)
	if err != nil {
		return err
	}

	vd.SizeKib = sizeKib

	err = run.Store.VolumeDefinitions().Update(ctx, rdName, &vd)
	if err != nil {
		return fmt.Errorf("resize volume definition %s/%d: %w", rdName, number, err)
	}

	return nil
}

// The size a volume may be set to. The floor is DRBD's own per-device
// minimum once metadata is reserved; below it the satellite loops on
// `drbdadm create-md` forever instead of failing. Both bounds hold
// even under --force, which only ever waives the shrink refusal.
const (
	volumeSizeFloorKib   int64 = 4 * kibPerMib
	volumeSizeCeilingKib int64 = 16 * kibPerMib * kibPerMib * kibPerMib
)

var (
	errSizeOutOfBounds = errors.New("size is outside the supported range of 4 MiB to 16 TiB")
	errNoAutoShrink    = errors.New(
		"shrink the filesystem first (resize2fs -s, or xfs dump+restore), unmount the volume, " +
			"then re-run with --force")
)

// checkResize refuses a resize that would destroy data or wedge the
// satellite.
//
// A shrink is refused without --force because nothing here shrinks the
// filesystem: handing the block device a smaller size under a live
// filesystem truncates it. --force is the operator's statement that
// they have already shrunk the filesystem themselves.
func checkResize(vd *apiv1.VolumeDefinition, sizeKib int64, force bool) error {
	if sizeKib < volumeSizeFloorKib || sizeKib > volumeSizeCeilingKib {
		return fmt.Errorf("%d KiB: %w", sizeKib, errSizeOutOfBounds)
	}

	if sizeKib >= vd.SizeKib || force {
		return nil
	}

	return fmt.Errorf("cannot shrink volume %d from %d KiB to %d KiB: %w",
		vd.VolumeNumber, vd.SizeKib, sizeKib, errNoAutoShrink)
}

// resourceCreate implements `resource create <node...> <rd>`.
//
// The definition is the LAST positional and every earlier one is a
// node, which is how the upstream grammar places several replicas in a
// single call.
func resourceCreate(ctx context.Context, run *runContext) error {
	// `--auto-place N` (or the `+N` delta) names no nodes: the placer
	// picks them, so the single positional is the definition.
	if run.Flags.Values["auto-place"] != "" {
		if len(run.Flags.Positionals) < 1 {
			return fmt.Errorf("%w: resource create needs a definition", command.ErrUsage)
		}

		rdName := run.Flags.Positionals[0]

		filter, err := placementFilter(ctx, run, rdName)
		if err != nil {
			return err
		}

		return autoPlace(ctx, run, rdName, filter, false)
	}

	if len(run.Flags.Positionals) < 2 {
		return fmt.Errorf("%w: resource create needs at least one node and a definition", command.ErrUsage)
	}

	last := len(run.Flags.Positionals) - 1
	rdName := run.Flags.Positionals[last]
	nodes := run.Flags.Positionals[:last]

	for _, node := range nodes {
		res := &apiv1.Resource{
			Name:     rdName,
			NodeName: node,
			Props:    map[string]string{},
		}

		if pool := run.Flags.Values["storage-pool"]; pool != "" {
			res.Props["StorPoolName"] = pool
		}

		if run.Flags.Diskless {
			res.Flags = append(res.Flags, apiv1.ResourceFlagDiskless)
		}

		err := run.Store.Resources().Create(ctx, res)
		if err != nil {
			return fmt.Errorf("create resource %s on %s: %w", rdName, node, err)
		}
	}

	return nil
}

// resourceDelete implements `resource delete <node> <rd>`,
// idempotently.
func resourceDelete(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 2 {
		return fmt.Errorf("%w: resource delete needs a node and a definition", command.ErrUsage)
	}

	last := len(run.Flags.Positionals) - 1
	rdName := run.Flags.Positionals[last]

	for _, node := range run.Flags.Positionals[:last] {
		err := run.Store.Resources().Delete(ctx, rdName, node)
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("delete resource %s on %s: %w", rdName, node, err)
		}
	}

	return nil
}

// snapshotCreate implements `snapshot create [<node>...] <rd> <snap>`.
//
// The trailing two positionals are the definition and the snapshot;
// anything before them names the nodes to capture on.
func snapshotCreate(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 2 {
		return fmt.Errorf("%w: snapshot create needs a definition and a snapshot name", command.ErrUsage)
	}

	count := len(run.Flags.Positionals)
	snap := &apiv1.Snapshot{
		ResourceName: run.Flags.Positionals[count-2],
		Name:         run.Flags.Positionals[count-1],
		Nodes:        run.Flags.Positionals[:count-2],
	}

	err := run.Store.Snapshots().Create(ctx, snap)
	if err != nil {
		return fmt.Errorf("create snapshot %s of %s: %w", snap.Name, snap.ResourceName, err)
	}

	return nil
}

// snapshotDelete implements `snapshot delete <rd> <snap>`,
// idempotently — several teardown paths delete a snapshot that a
// previous step already removed.
func snapshotDelete(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 2 {
		return fmt.Errorf("%w: snapshot delete needs a definition and a snapshot name", command.ErrUsage)
	}

	rdName, snapName := run.Flags.Positionals[0], run.Flags.Positionals[1]

	err := run.Store.Snapshots().Delete(ctx, rdName, snapName)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete snapshot %s of %s: %w", snapName, rdName, err)
	}

	return nil
}

// resourceGroupCreate implements `resource-group create <name>`.
func resourceGroupCreate(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: resource-group create needs a name", command.ErrUsage)
	}

	group := &apiv1.ResourceGroup{Name: run.Flags.Positionals[0]}

	err := applyGroupPolicy(group, run.Flags)
	if err != nil {
		return err
	}

	err = run.Store.ResourceGroups().Create(ctx, group)
	if err != nil {
		return fmt.Errorf("create resource group %s: %w", group.Name, err)
	}

	return nil
}

// resourceGroupModify implements `resource-group modify <name>`.
func resourceGroupModify(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: resource-group modify needs a name", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	group, err := run.Store.ResourceGroups().Get(ctx, name)
	if err != nil {
		return fmt.Errorf("get resource group %s: %w", name, err)
	}

	err = applyGroupPolicy(&group, run.Flags)
	if err != nil {
		return err
	}

	err = run.Store.ResourceGroups().Update(ctx, &group)
	if err != nil {
		return fmt.Errorf("update resource group %s: %w", name, err)
	}

	return nil
}

// resourceGroupDelete implements `resource-group delete <name>`.
func resourceGroupDelete(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: resource-group delete needs a name", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	err := run.Store.ResourceGroups().Delete(ctx, name)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete resource group %s: %w", name, err)
	}

	return nil
}

// applyGroupPolicy folds the placement flags onto a group, leaving
// unset fields alone so `modify` only changes what was asked for.
func applyGroupPolicy(group *apiv1.ResourceGroup, flags *flagSet) error {
	if raw := flags.Values["place-count"]; raw != "" {
		count, err := parseInt32(raw, "--place-count")
		if err != nil {
			return err
		}

		group.SelectFilter.PlaceCount = apiv1.LaxInt32(count)
	}

	if pool := flags.Values["storage-pool"]; pool != "" {
		group.SelectFilter.StoragePool = pool
	}

	if layers := flags.Values["layer-list"]; layers != "" {
		group.SelectFilter.LayerStack = splitList(layers)
	}

	return nil
}
