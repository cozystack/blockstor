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
	"math"
	"net"
	"slices"
	"strconv"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"

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

// parseLayerList splits a --layer-list value and holds it to the layer rules
// every writer has to obey.
//
// The satellite asks whether the stack CONTAINS a layer, so an unrecognised
// token is not a loud failure downstream — it is a silently absent layer.
// `DRDB,STORAGE` brings the volume up as a single local copy while the
// operator believes it is replicated, and `DRBD,LUSK,STORAGE` writes
// plaintext while the operator believes it is encrypted. One character is the
// whole distance. The REST path has refused these since it was written; this
// CLI writes the CRDs directly, and the field carries no enum or CEL rule, so
// without this check there is no backstop anywhere on this path.
//
// The value is stored as the operator typed it, matching what REST persists;
// the satellite compares case-insensitively, so only the spelling is at issue
// here, not the case.
func parseLayerList(raw string) ([]string, error) {
	layers := splitList(raw)

	err := validate.LayerStack(layers)
	if err != nil {
		//nolint:wrapcheck // a semantic refusal; see checkVolumeSize
		return nil, err
	}

	return layers, nil
}

// minVolumeNumber / maxVolumeNumber bound DRBD-9's addressable volume
// range, matching the REST validator.
const (
	minVolumeNumber int32 = 0
	maxVolumeNumber int32 = 65535
)

// parseVolumeNumber parses a volume number and holds it inside the
// range DRBD can actually address.
//
// Bare parseInt32 accepts every int32, including negatives. A negative
// number reaches the CRD, the satellite derives a device node from it,
// and the replica hangs waiting for a DRBD-ID that can never be
// allocated — the REST path rejects the same input for exactly that
// reason. This CLI writes the CRD directly, so the bound has to be
// repeated here or it is simply absent on this path.
func parseVolumeNumber(text, what string) (int32, error) {
	number, err := parseInt32(text, what)
	if err != nil {
		return 0, err
	}

	if number < minVolumeNumber || number > maxVolumeNumber {
		return 0, fmt.Errorf("%w: %s %d is outside the addressable range [%d, %d]",
			command.ErrUsage, what, number, minVolumeNumber, maxVolumeNumber)
	}

	return number, nil
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

	// A trailing `iB`, `i` or `B` is tolerated (`10GiB`, `10Gi`,
	// `10GB`), because operators paste sizes from documentation as
	// often as they type them. Trimming happens BEFORE the unit is
	// read: the unit is the last byte, so `10GiB` would otherwise be
	// rejected for ending in `B`.
	// Matched case-insensitively: an operator who types `16gib` means
	// the same thing as one who pastes `16GiB`, and rejecting one
	// spelling of a size is a papercut with no upside.
	trimmed, inBytes := trimSizeSuffix(trimmed)

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

	value, err := strconv.ParseInt(strings.TrimSpace(digits), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%w: unrecognised size %q", command.ErrUsage, text)
	}

	// The multiplication is checked, not assumed. `17179869184T`
	// overflows int64 to exactly zero, and a zero size is the one
	// value the satellite cannot fail on: it loops on `drbdadm
	// create-md` forever instead.
	if value > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("%w: size %q does not fit", command.ErrUsage, text)
	}

	// A byte-denominated size with no scale letter is bytes, not KiB.
	// Read as KiB it comes out 1024x too large — `512B` became half a
	// megabyte — so convert it, and refuse the ones that are not a
	// whole number of KiB rather than rounding a size silently.
	if inBytes && multiplier == 1 {
		if value%kibPerMib != 0 {
			return 0, fmt.Errorf("%w: size %q is not a whole number of KiB", command.ErrUsage, text)
		}

		return value / kibPerMib, nil
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

		addrErr := validateNodeAddress(iface.Address)
		if addrErr != nil {
			return addrErr
		}

		if port := run.Flags.Values["port"]; port != "" {
			parsed, err := parseSatellitePort(port)
			if err != nil {
				return err
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

	// A node still carrying replicas or pools is refused, not silently
	// emptied: dropping it leaves those objects pointing at a node that no
	// longer exists, and nothing reaps them afterwards. --force is the
	// explicit "this node is gone" decision, and it cascades.
	if run.Flags.Force {
		cascadeErr := store.CascadeOrphansForLostNode(ctx, run.Store, name)
		if cascadeErr != nil {
			return fmt.Errorf("delete objects on %s: %w", name, cascadeErr)
		}
	} else {
		rscRefs, poolRefs, refErr := store.ReferencesOnNode(ctx, run.Store, name)
		if refErr != nil {
			return fmt.Errorf("list objects on %s: %w", name, refErr)
		}

		if len(rscRefs) > 0 || len(poolRefs) > 0 {
			return fmt.Errorf("%w: %s still carries replicas of [%s] and storage pools [%s]; "+
				"remove them or pass --force",
				errNodeStillReferenced, name,
				strings.Join(rscRefs, ", "), strings.Join(poolRefs, ", "))
		}
	}

	err := run.Store.Nodes().Delete(ctx, name)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete node %s: %w", name, err)
	}

	return nil
}

// errNodeStillReferenced refuses a plain node delete while replicas or pools
// still name the node, matching what the REST door answers on the same input.
var errNodeStillReferenced = errors.New("node is still referenced")

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

	err = checkVolumeSize(sizeKib)
	if err != nil {
		return err
	}

	vd := &apiv1.VolumeDefinition{SizeKib: sizeKib}

	// An explicit volume number is honoured; otherwise the store picks
	// the next free one atomically, which is what keeps two concurrent
	// creates from claiming the same slot.
	if raw := run.Flags.Values["vlmnr"]; raw != "" {
		number, convErr := parseVolumeNumber(raw, "--vlmnr")
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

	// The shrink comparison has to run against the state the write
	// lands on. Reading the size, deciding, and then writing an
	// absolute value leaves a window: a concurrent grow (the CSI
	// resizing a live volume) lands in between, the decision was made
	// against the smaller pre-grow size, and the absolute write
	// truncates a volume that is now larger and in use. Deciding
	// inside the patch closes it — on a conflict the store re-runs
	// this closure against freshly fetched state, so the check and the
	// write always see the same size.
	err = run.Store.VolumeDefinitions().PatchVolumeDefinitionSpec(ctx, rdName, number,
		func(vd *apiv1.VolumeDefinition) error {
			boundErr := checkVolumeSize(sizeKib)
			if boundErr != nil {
				return boundErr
			}

			resizeErr := checkResize(vd, sizeKib, run.Flags.Force)
			if resizeErr != nil {
				return resizeErr
			}

			vd.SizeKib = sizeKib

			return nil
		})
	if err != nil {
		return fmt.Errorf("resize volume definition %s/%d: %w", rdName, number, err)
	}

	return nil
}

var errNoAutoShrink = errors.New(
	"shrink the filesystem first (resize2fs -s, or xfs dump+restore), unmount the volume, " +
		"then re-run with --force")

// checkVolumeSize enforces the bounds on EVERY path that writes a
// volume size, not just resize. A size below DRBD's per-device
// minimum makes the satellite loop on `drbdadm create-md` forever
// rather than fail, and nothing downstream catches it: the CRD has no
// `minimum`, no CEL rule and no admission webhook.
// checkVolumeSize holds a size inside the range DRBD can serve.
//
// The rule is shared with the REST path rather than restated here, and it is
// the only place the bound is enforced now: it used to sit on the CRD as
// well, but spec.volumeDefinitions is an atomic list, so any update touching
// it re-validated every element — a single grandfathered sub-floor volume
// then rejected every later write to that definition, the controller's own
// included.
func checkVolumeSize(sizeKib int64) error {
	//nolint:wrapcheck // a semantic refusal, surfaced verbatim: wrapping it
	// in ErrUsage would reclassify the exit code from 10 to 2, and the
	// replay workflows assert on that boundary.
	return validate.VolumeSizeKib(sizeKib)
}

// checkResize refuses a resize that would destroy data or wedge the
// satellite.
//
// A shrink is refused without --force because nothing here shrinks the
// filesystem: handing the block device a smaller size under a live
// filesystem truncates it. --force is the operator's statement that
// they have already shrunk the filesystem themselves.
func checkResize(vd *apiv1.VolumeDefinition, sizeKib int64, force bool) error {
	err := checkVolumeSize(sizeKib)
	if err != nil {
		return err
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

		if run.Flags.Diskless {
			res.Flags = append(res.Flags, apiv1.ResourceFlagDiskless)
		}

		poolErr := stampStorPool(ctx, run, res, rdName)
		if poolErr != nil {
			return poolErr
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
		err := checkDeletable(ctx, run, rdName, node)
		if err != nil {
			return err
		}

		err = run.Store.Resources().Delete(ctx, rdName, node)
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("delete resource %s on %s: %w", rdName, node, err)
		}
	}

	return nil
}

// checkDeletable refuses a delete that would strand a peer still resyncing
// from this replica — upstream U130.
//
// Deleting the last complete copy while another diskful replica is mid-sync
// leaves that replica with no source: it stays Inconsistent forever, the
// deleting node hangs in Connecting, and if the Primary is diskless there is
// no current copy of the data anywhere in the cluster. The REST path has
// answered 409 on this since it was written; the CLI deletes the CRD directly,
// so the same judgement has to run here.
//
// --force overrides, for the case where the operator knows the peer is being
// discarded anyway. A missing replica is not an error: the delete is
// idempotent, and there is nothing to strand.
func checkDeletable(ctx context.Context, run *runContext, rdName, node string) error {
	if run.Flags.Force {
		return nil
	}

	target, err := run.Store.Resources().Get(ctx, rdName, node)
	if err != nil {
		if isNotFound(err) {
			return nil
		}

		return fmt.Errorf("get resource %s on %s: %w", rdName, node, err)
	}

	siblings, err := run.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return fmt.Errorf("list replicas of %s: %w", rdName, err)
	}

	if validate.MidSyncDeleteRefusal(&target, siblings) {
		return fmt.Errorf("refusing to delete %s on %s: %w (pass --force to override)",
			rdName, node, validate.ErrLastSyncSourceDelete)
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

	err := hydrateSnapshot(ctx, run, snap)
	if err != nil {
		return err
	}

	err = run.Store.Snapshots().Create(ctx, snap)
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

	err := applyGroupPolicy(ctx, run, group, run.Flags)
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

	// The policy edit is partial — it touches only the fields the
	// flags name — so it has to be applied to current state, not to a
	// snapshot. Otherwise a concurrent `volume-group create` on the
	// same group is reverted by this write.
	err := run.Store.ResourceGroups().PatchResourceGroup(ctx, name,
		func(group *apiv1.ResourceGroup) error {
			return applyGroupPolicy(ctx, run, group, run.Flags)
		})
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
func applyGroupPolicy(
	ctx context.Context, run *runContext, group *apiv1.ResourceGroup, flags *flagSet,
) error {
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
		parsed, err := parseLayerList(layers)
		if err != nil {
			return err
		}

		// A group's stack is inherited by every definition spawned from
		// it, so an unguarded LUKS here reaches more volumes than an
		// unguarded one on a single definition.
		luksErr := checkLUKSPrerequisite(ctx, run, parsed)
		if luksErr != nil {
			return luksErr
		}

		group.SelectFilter.LayerStack = parsed
	}

	return nil
}

// stampStorPool records which pool a diskful replica belongs in,
// resolving an omitted `--storage-pool` the way the REST path does.
//
// The satellite binds a diskful replica to Props["StorPoolName"] and
// fails with `unknown storage pool ""` when it is empty, leaving the
// replica in Provisioning for good. Nothing downstream fills it in: the
// store does no defaulting, so a CLI that writes the CRD directly has
// to carry the resolution itself or reintroduce the wedge the REST side
// already fixed.
//
// The chain matches resolveTakeoverStorPool: an explicit flag wins, then
// a diskful sibling's pool, then the resource group's SelectFilter —
// first the single StoragePool, then the first entry of StoragePoolList,
// which is where linstor-csi lands the storage class's pool name.
func stampStorPool(ctx context.Context, run *runContext, res *apiv1.Resource, rdName string) error {
	// A diskless replica has no backing storage by definition, and
	// stamping a pool on one would contradict the flag the operator
	// passed.
	if slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) {
		return nil
	}

	if pool := run.Flags.Values["storage-pool"]; pool != "" {
		stampProp(res, storPoolNameProp, pool)

		return nil
	}

	pool, err := resolveStorPool(ctx, run, rdName, res.NodeName)
	if err != nil {
		return err
	}

	if pool == "" {
		return nil
	}

	stampProp(res, storPoolNameProp, pool)

	return nil
}

// resolveStorPool walks the same fallback chain the REST handler does.
// An empty result is not an error here: a cluster with no resource
// group and no sibling has nothing to infer from, and refusing would
// break the bootstrap shapes that pass the pool explicitly on the
// storage pool itself.
func resolveStorPool(ctx context.Context, run *runContext, rdName, node string) (string, error) {
	siblings, err := run.Store.Resources().ListByDefinition(ctx, rdName)
	if err == nil {
		for i := range siblings {
			if siblings[i].NodeName == node ||
				slices.Contains(siblings[i].Flags, apiv1.ResourceFlagDiskless) {
				continue
			}

			if pool := siblings[i].Props[storPoolNameProp]; pool != "" {
				return pool, nil
			}
		}
	}

	def, err := run.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		if isNotFound(err) {
			return "", nil
		}

		return "", fmt.Errorf("get resource definition %s: %w", rdName, err)
	}

	if def.ResourceGroupName == "" {
		return "", nil
	}

	group, err := run.Store.ResourceGroups().Get(ctx, def.ResourceGroupName)
	if err != nil {
		if isNotFound(err) {
			return "", nil
		}

		return "", fmt.Errorf("get resource group %s: %w", def.ResourceGroupName, err)
	}

	if pool := group.SelectFilter.StoragePool; pool != "" {
		return pool, nil
	}

	if len(group.SelectFilter.StoragePoolList) > 0 {
		return group.SelectFilter.StoragePoolList[0], nil
	}

	return "", nil
}

// validateNodeAddress refuses an address the satellite could never
// bind. The field is carried verbatim into the DRBD config and the
// controller's connection attempts, so a typo surfaces as a node that
// simply never comes online rather than as a rejected command.
func validateNodeAddress(address string) error {
	if address == "" {
		return fmt.Errorf("%w: node create needs an address", command.ErrUsage)
	}

	if net.ParseIP(address) != nil {
		return nil
	}

	// A hostname is legitimate — the satellite resolves it — but it
	// still has to look like one.
	if isPlausibleHostname(address) {
		return nil
	}

	return fmt.Errorf("%w: %q is neither an IP address nor a hostname", command.ErrUsage, address)
}

// isPlausibleHostname accepts the shapes DNS allows, so an operator can
// name a node by hostname without this rejecting it.
func isPlausibleHostname(address string) bool {
	// The DNS limits: 253 bytes for a name, 63 for one label.
	const (
		maxHostname = 253
		maxLabel    = 63
	)

	if len(address) > maxHostname {
		return false
	}

	for label := range strings.SplitSeq(address, ".") {
		if label == "" || len(label) > maxLabel {
			return false
		}

		for i, r := range label {
			alnum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
			if alnum {
				continue
			}

			if r == '-' && i != 0 && i != len(label)-1 {
				continue
			}

			return false
		}
	}

	return true
}

// parseSatellitePort holds the port inside the range TCP can express.
//
// Atoi accepts any int, and the field is an int32 on the wire, so
// `--port 4294970000` silently truncated to 2704 and the node was
// created pointing at a port nobody listens on.
func parseSatellitePort(text string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return 0, fmt.Errorf("%w: --port %q is not a number", command.ErrUsage, text)
	}

	const maxPort = 65535

	if parsed < 1 || parsed > maxPort {
		return 0, fmt.Errorf("%w: --port %d is outside [1, %d]", command.ErrUsage, parsed, maxPort)
	}

	return parsed, nil
}

// trimSizeSuffix strips the byte-unit suffix and reports whether what
// remains is denominated in bytes rather than in the scale letter that
// precedes it.
//
// Matched case-insensitively: an operator who types `16gib` means the
// same thing as one who pastes `16GiB`, and rejecting one spelling of a
// size is a papercut with no upside.
func trimSizeSuffix(text string) (string, bool) {
	for _, suffix := range []string{"ib", "i", "b"} {
		if len(text) > 1 && strings.HasSuffix(strings.ToLower(text), suffix) {
			// A bare `b` is the only one that changes the unit: `512B`
			// is 512 bytes, while `512iB` and `512i` are the binary
			// marker on whatever letter precedes them.
			return text[:len(text)-len(suffix)], suffix == "b"
		}
	}

	return text, false
}
