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

// Package effectiveprops resolves the DRBD-options bag for one
// Resource by walking the upstream-LINSTOR override hierarchy:
// Controller → ResourceGroup → ResourceDefinition → Resource.
//
// Lower scopes override upper, per non-nil field. Each scope is
// best-effort — a missing ControllerConfig / missing RG /
// missing RD degrades to "empty" rather than blocking the
// dispatch, so a partially-migrated cluster still produces a
// usable .res file.
//
// Lifted out of `internal/controller.ResourceReconciler` in
// Phase 10.1 so both the controller-side dispatcher AND the new
// satellite-side `pkg/satellite/controllers.ResourceReconciler`
// share one implementation.
package effectiveprops

import (
	"context"
	"maps"
	"strings"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
)

// Resolve walks the four scopes and returns the merged Props
// map. The `c` reader is typically a controller-runtime
// `manager.GetClient()` — both controller and satellite hold
// one. `target` is the Resource whose effective props we want;
// `rd` is the parent ResourceDefinition (may be nil; the
// reconciler usually fetched it already).
//
// Phase 10.1 step.
func Resolve(ctx context.Context, c client.Reader, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) (map[string]string, error) {
	if target == nil {
		return map[string]string{}, nil
	}

	ctrlCfg, err := controllerConfig(ctx, c)
	if err != nil {
		return nil, err
	}

	var (
		ctrlTyped  *blockstoriov1alpha1.DRBDOptions
		ctrlExtras map[string]string
	)

	if ctrlCfg != nil {
		ctrlTyped = ctrlCfg.Spec.DRBDOptions
		ctrlExtras = ctrlCfg.Spec.ExtraProps
	}

	rgInfo, rdInfo, err := scopeInputs(ctx, c, rd)
	if err != nil {
		return nil, err
	}

	// Build one flat DRBD-props map per scope — typed fields emitted
	// back to their `DrbdOptions/...` wire keys, unioned with that
	// scope's untyped ExtraProps. A scope's typed/extras keysets are
	// disjoint by construction (the store transcoder routes each
	// recognised key into the typed struct and everything else into
	// ExtraProps), so the union is unambiguous within a scope.
	//
	// The controller scope is special: `controller drbd-options`
	// persists into ControllerConfig.Spec.ExtraProps (raw, untyped —
	// see pkg/rest/controller_props.go), so ctrlTyped is normally nil
	// and ctrlExtras carries the cluster-wide knobs. We still feed
	// ctrlTyped through emitScopeProps for completeness.
	ctrlProps := emitScopeProps(ctrlTyped, ctrlExtras)
	rgProps := emitScopeProps(rgInfo.Typed, rgInfo.Extras)
	rdProps := emitScopeProps(rdInfo.Typed, rdInfo.Extras)
	resProps := emitScopeProps(target.Spec.DRBDOptions, target.Spec.ExtraProps)

	// Merge the DRBD knobs in upstream-LINSTOR precedence order:
	// Controller → ResourceGroup → ResourceDefinition → Resource, each
	// lower (closer-to-the-resource) scope overriding the one above.
	// ResolveOptions also threads through the non-DRBD raw Spec.Props
	// (StorPoolName, Aux/*) from each scope. Because every scope's DRBD
	// knobs are funneled through the *same* precedence walk here, a
	// closer scope's explicit override always wins regardless of
	// whether the value was stored typed or as an ExtraProp — the
	// "closer to the resource wins" rule (C2) holds across the whole
	// chain, including the controller tier (C1).
	ctrlMerged := mergeScopeProps(ctrlProps, nil)
	rgMerged := mergeScopeProps(rgProps, rgInfo.Props)
	rdMerged := mergeScopeProps(rdProps, rdInfo.Props)
	resMerged := mergeScopeProps(resProps, target.Spec.Props)

	out := drbd.ResolveOptions(ctrlMerged, rgMerged, rdMerged, resMerged)

	// ResolveOptions only threads through NON-DRBD props from the
	// most-specific (resource) scope — by design for keys like
	// StorPoolName that only make sense on the resource. But the RG and
	// RD scopes carry load-bearing NON-DRBD ExtraProps that the
	// satellite reads off the dispatched resource: most importantly
	// `FileSystem/Type` (+ `FileSystem/MkfsParams`), which the
	// `linstor rd set-property … FileSystem/Type ext4` CLI and the
	// linstor-csi CreateVolume path stamp on the RD and which gate the
	// satellite's mkfs / `primary --force` seed path
	// (pkg/satellite/reconciler.go hasFileSystemConfigured). Dropping
	// them silently disables mkfs → a fresh replica never writes → the
	// DRBD current-UUID never rotates past the day0 GI → the
	// controller's RD.Spec.Initialized latch never flips.
	//
	// Re-overlay the non-DRBD ExtraProps from the RG then RD scope
	// (closer scope last, so RD wins over RG; the resource scope already
	// won via ResolveOptions). DRBD keys are intentionally skipped here —
	// the precedence walk above already resolved them. The controller
	// scope is deliberately NOT re-overlaid: cluster-wide non-DRBD knobs
	// like `Aux/zone` must not leak onto every resource (C1 contract).
	overlayNonDRBDExtras(out, rgInfo.Extras)
	overlayNonDRBDExtras(out, rdInfo.Extras)

	return out, nil
}

// overlayNonDRBDExtras copies the non-`DrbdOptions/...` entries of a
// scope's ExtraProps into out (overriding on key collision). DRBD keys
// are skipped — those are resolved by the precedence walk in
// ResolveOptions and must not be re-applied here (that would let an
// upper scope's DRBD ExtraProp clobber a closer scope's resolved
// value). nil src is a no-op.
func overlayNonDRBDExtras(out, src map[string]string) {
	for key, value := range src {
		if strings.HasPrefix(key, drbd.PropPrefix) {
			continue
		}

		out[key] = value
	}
}

// emitScopeProps flattens one scope's typed DRBDOptions plus its
// untyped ExtraProps into a single `DrbdOptions/...` wire-key map.
// Typed fields win over an ExtraProp of the same key within the scope
// (the transcoder keeps the two keysets disjoint, so this only ever
// matters for hand-crafted inputs). Returns a fresh map.
func emitScopeProps(typed *blockstoriov1alpha1.DRBDOptions, extras map[string]string) map[string]string {
	out := map[string]string{}

	maps.Copy(out, extras)
	maps.Copy(out, drbd.TypedDRBDOptionsToProps(typed))

	return out
}

// mergeScopeProps unions a scope's flattened DRBD knobs (drbdProps)
// with its raw, non-DRBD Spec.Props (StorPoolName, Aux/*). The DRBD
// knobs take precedence on key collision — a `DrbdOptions/...` key
// only ever appears in drbdProps, so in practice the two are disjoint.
// Returns a fresh map; nil inputs are treated as empty.
func mergeScopeProps(drbdProps, rawProps map[string]string) map[string]string {
	out := map[string]string{}

	maps.Copy(out, rawProps)
	maps.Copy(out, drbdProps)

	return out
}

// scopeInputs gathers the RG + RD scope inputs the hierarchy
// resolver needs. Returns zero-valued info structs for missing
// scopes — a missing RG / RD softly degrades to "no input at
// this level" rather than blocking dispatch.
type scopeInfo struct {
	Props  map[string]string
	Typed  *blockstoriov1alpha1.DRBDOptions
	Extras map[string]string
}

func scopeInputs(ctx context.Context, c client.Reader, rd *blockstoriov1alpha1.ResourceDefinition) (scopeInfo, scopeInfo, error) {
	var (
		rgInfo scopeInfo
		rdInfo scopeInfo
	)

	if rd == nil {
		return rgInfo, rdInfo, nil
	}

	rdInfo = scopeInfo{
		Props:  rd.Spec.Props,
		Typed:  rd.Spec.DRBDOptions,
		Extras: rd.Spec.ExtraProps,
	}

	rgObj, err := resourceGroup(ctx, c, rd)
	if err != nil {
		return rgInfo, rdInfo, err
	}

	if rgObj != nil {
		rgInfo = scopeInfo{
			Props:  rgObj.Spec.Props,
			Typed:  rgObj.Spec.DRBDOptions,
			Extras: rgObj.Spec.ExtraProps,
		}
	}

	return rgInfo, rdInfo, nil
}

// diskOptionPrefix is the LINSTOR namespace of a DRBD disk section.
const diskOptionPrefix = drbd.PropPrefix + "Disk/"

// VolumeDiskOptions resolves each volume's own disk options: the
// DrbdOptions/Disk/* keys set on its volume definition or on the
// resource group's volume group, which LINSTOR renders into a disk
// section inside the volume rather than at resource scope. The result
// maps a volume number to bare option names (block-size, not
// DrbdOptions/Disk/block-size); a volume with none is absent.
//
// Each value follows the precedence LINSTOR's ConfFileBuilder uses for
// a volume (appendVlmIfPresent): volume, resource, volume definition,
// volume group, resource definition, resource group, node, controller,
// closer scope first. blockstor has no per-volume props on a replica,
// so the resource's own props are the closest. A key set at volume
// definition or volume group level outranks everything below it, so
// only the resource can override it; a key set at none of those two
// levels is not a volume option and stays in the resource-scope
// disk section, resolved there by Resolve.
//
// rs-discard-granularity is the exception LINSTOR's
// ConfFileBuilderAutoRules makes: while
// DrbdOptions/auto-rs-discard-granularity is unset or true, the
// controller owns the volume's value and keeps it on the volume
// definition, so the volume definition alone decides it and a value on
// the volume group or the replica does not reach the volume. effective
// is what Resolve returned for target, the levels above the volume's
// own that the switch can be set on.
func VolumeDiskOptions(ctx context.Context, c client.Reader, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, effective map[string]string) (map[int32]map[string]string, error) {
	if target == nil || rd == nil || len(rd.Spec.VolumeDefinitions) == 0 {
		return nil, nil //nolint:nilnil // no volumes, nothing to resolve
	}

	rg, err := resourceGroup(ctx, c, rd)
	if err != nil {
		return nil, err
	}

	resProps := mergeScopeProps(emitScopeProps(target.Spec.DRBDOptions, target.Spec.ExtraProps), target.Spec.Props)

	out := map[int32]map[string]string{}

	for i := range rd.Spec.VolumeDefinitions {
		vd := &rd.Spec.VolumeDefinitions[i]

		opts := map[string]string{}
		vgProps := volumeGroupProps(rg, vd.VolumeNumber)

		// Lowest of the per-volume levels first, so a closer one
		// overwrites it.
		for _, level := range []map[string]string{vgProps, vd.Props} {
			for key, value := range level {
				name, ok := strings.CutPrefix(key, diskOptionPrefix)
				if !ok || name == "" {
					continue
				}

				opts[name] = value
			}
		}

		for name := range opts {
			if value, ok := resProps[diskOptionPrefix+name]; ok {
				opts[name] = value
			}
		}

		if autoRsDiscardGranularity(resProps, vd.Props, vgProps, effective) {
			delete(opts, rsDiscardGranularity)

			if value, ok := vd.Props[diskOptionPrefix+rsDiscardGranularity]; ok {
				opts[rsDiscardGranularity] = value
			}
		}

		if len(opts) > 0 {
			out[vd.VolumeNumber] = opts
		}
	}

	return out, nil
}

// FileSystemPrefix is LINSTOR's FileSystem namespace
// (ApiConsts.NAMESPC_FILESYSTEM): FileSystem/Type, FileSystem/MkfsParams.
const FileSystemPrefix = "FileSystem/"

// VolumeFileSystemProps resolves each volume's FileSystem/* props, the
// ones that decide whether and how the satellite formats the volume.
// The result maps a volume number to full keys (FileSystem/Type); a
// volume none of the levels gives one is absent.
//
// LINSTOR reads them per volume (MkfsUtils.needsToCreateFs and
// makeFileSystemOnMarked) over resource, volume definition, volume
// group, resource definition, resource group, closer scope first, and
// nothing above the resource group. linstor-csi v1.13.2 stamps them on
// the volume definition, earlier releases on the resource definition,
// and a StorageClass parameter puts them on the resource group, so
// every one of those levels has to count. Per volume, because the
// volumes of one definition need not agree: a consistency group holds
// a block member, which must not be formatted, next to a filesystem
// one.
func VolumeFileSystemProps(ctx context.Context, c client.Reader, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) (map[int32]map[string]string, error) {
	if target == nil || rd == nil || len(rd.Spec.VolumeDefinitions) == 0 {
		return nil, nil //nolint:nilnil // no volumes, nothing to resolve
	}

	rg, err := resourceGroup(ctx, c, rd)
	if err != nil {
		return nil, err
	}

	var rgLevel map[string]string
	if rg != nil {
		rgLevel = fileSystemKeys(rg.Spec.Props, rg.Spec.ExtraProps)
	}

	rdLevel := fileSystemKeys(rd.Spec.Props, rd.Spec.ExtraProps)
	resLevel := fileSystemKeys(target.Spec.Props, target.Spec.ExtraProps)

	out := map[int32]map[string]string{}

	for i := range rd.Spec.VolumeDefinitions {
		vd := &rd.Spec.VolumeDefinitions[i]

		props := map[string]string{}

		// Farthest level first, so a closer one overwrites it.
		for _, level := range []map[string]string{
			rgLevel,
			rdLevel,
			fileSystemKeys(volumeGroupProps(rg, vd.VolumeNumber)),
			fileSystemKeys(vd.Props),
			resLevel,
		} {
			maps.Copy(props, level)
		}

		if len(props) > 0 {
			out[vd.VolumeNumber] = props
		}
	}

	return out, nil
}

// fileSystemKeys returns the FileSystem/* entries of the given maps,
// later maps winning on a collision.
func fileSystemKeys(sources ...map[string]string) map[string]string {
	out := map[string]string{}

	for _, src := range sources {
		for key, value := range src {
			if strings.HasPrefix(key, FileSystemPrefix) {
				out[key] = value
			}
		}
	}

	return out
}

const (
	// rsDiscardGranularity is the disk option LINSTOR's auto rule owns.
	rsDiscardGranularity = "rs-discard-granularity"

	// autoRsDiscardGranularityKey switches that rule off when false.
	autoRsDiscardGranularityKey = drbd.PropPrefix + "auto-rs-discard-granularity"
)

// autoRsDiscardGranularity resolves DrbdOptions/auto-rs-discard-granularity
// over a volume's levels, closest first, and reports whether the rule is
// on: unset and true are both on, as LINSTOR reads it.
func autoRsDiscardGranularity(levels ...map[string]string) bool {
	for _, level := range levels {
		if value, ok := level[autoRsDiscardGranularityKey]; ok {
			return value == "" || strings.EqualFold(value, "true")
		}
	}

	return true
}

// volumeGroupProps returns the props of the resource group's volume
// group for one volume number, nil when there is none.
func volumeGroupProps(rg *blockstoriov1alpha1.ResourceGroup, volumeNumber int32) map[string]string {
	if rg == nil {
		return nil
	}

	for i := range rg.Spec.VolumeGroups {
		if rg.Spec.VolumeGroups[i].VolumeNumber == volumeNumber {
			return rg.Spec.VolumeGroups[i].Props
		}
	}

	return nil
}

// resourceGroup fetches the definition's resource group, nil when it
// names none or the group is gone (see the package doc).
func resourceGroup(ctx context.Context, c client.Reader, rd *blockstoriov1alpha1.ResourceDefinition) (*blockstoriov1alpha1.ResourceGroup, error) {
	if rd.Spec.ResourceGroupName == "" {
		return nil, nil //nolint:nilnil // a definition without a group
	}

	var rg blockstoriov1alpha1.ResourceGroup

	// The RD keeps the group's name as the operator spelled it; the CR is
	// stored under the folded one.
	err := c.Get(ctx, client.ObjectKey{Name: naming.Key(rd.Spec.ResourceGroupName)}, &rg)
	switch {
	case err == nil:
		return &rg, nil
	case apierrors.IsNotFound(err):
		return nil, nil //nolint:nilnil // soft-fail; see package doc
	default:
		return nil, errors.Wrapf(err, "get ResourceGroup %q", rd.Spec.ResourceGroupName)
	}
}

// controllerConfig fetches the singleton ControllerConfig CRD.
// Returns (nil, nil) when missing — caller falls through to
// the legacy KVEntry path. Phase 10.4 step 1.
func controllerConfig(ctx context.Context, c client.Reader) (*blockstoriov1alpha1.ControllerConfig, error) {
	var cfg blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil //nolint:nilnil // optional singleton
		}

		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	return &cfg, nil
}
