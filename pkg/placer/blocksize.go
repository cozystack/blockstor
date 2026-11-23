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

package placer

import (
	"context"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/store"
)

// filterByBlockSize drops the pools LINSTOR's autoplacer would not offer
// a placed definition whose block size may no longer change: those whose
// minimum I/O size is above the smallest size the definition's volumes
// run at. A replica there would come up with larger sectors than its
// peers, which is what LINSTOR's refusal to create one guards against.
func (p *Placer) filterByBlockSize(
	ctx context.Context, rdName string, existing []apiv1.Resource, candidates []apiv1.StoragePool,
) ([]apiv1.StoragePool, error) {
	if len(existing) == 0 || len(candidates) == 0 {
		return candidates, nil
	}

	canChange, floor, hasFloor, err := p.blockSizeBounds(ctx, rdName, existing)
	if err != nil {
		return nil, err
	}

	if canChange || !hasFloor {
		return candidates, nil
	}

	out := make([]apiv1.StoragePool, 0, len(candidates))

	for i := range candidates {
		if blocksize.PoolFits(blocksize.PoolIoSize(candidates[i].Props), canChange, floor, hasFloor) {
			out = append(out, candidates[i])
		}
	}

	return out, nil
}

// blockSizeBounds reads what the filter needs from the definition and the
// levels above it.
func (p *Placer) blockSizeBounds(
	ctx context.Context, rdName string, existing []apiv1.Resource,
) (bool, int64, bool, error) {
	rd, err := p.store.ResourceDefinitions().Get(ctx, rdName)
	if errors.Is(err, store.ErrNotFound) {
		return true, 0, false, nil
	}

	if err != nil {
		return false, 0, false, errors.Wrap(err, "get resource definition")
	}

	vds, err := p.store.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		return false, 0, false, errors.Wrap(err, "list volume definitions")
	}

	auto, err := p.allAutoBlockSize(ctx, &rd, vds)
	if err != nil {
		return false, 0, false, err
	}

	vdProps := make([]map[string]string, 0, len(vds))
	for i := range vds {
		vdProps = append(vdProps, vds[i].Props)
	}

	primary := false

	for i := range existing {
		if in := existing[i].State.InUse; in != nil && *in {
			primary = true
		}
	}

	canChange := blocksize.CanChange(true, auto, primary, vdProps)
	floor, hasFloor := blocksize.Floor(true, blocksize.HasSpecialLayers(rd.LayerStack), auto, vdProps)

	return canChange, floor, hasFloor, nil
}

// allAutoBlockSize is LINSTOR's definition-wide auto-block-size switch:
// on only when it resolves on for every volume definition, over volume
// definition, resource definition, volume group, resource group and
// controller.
func (p *Placer) allAutoBlockSize(ctx context.Context, rd *apiv1.ResourceDefinition, vds []apiv1.VolumeDefinition) (bool, error) {
	var rg *apiv1.ResourceGroup

	if rd.ResourceGroupName != "" {
		got, err := p.store.ResourceGroups().Get(ctx, rd.ResourceGroupName)

		switch {
		case err == nil:
			rg = &got
		case !errors.Is(err, store.ErrNotFound):
			return false, errors.Wrap(err, "get resource group")
		}
	}

	ctrlProps, err := p.store.ControllerProps().Get(ctx)
	if err != nil {
		return false, errors.Wrap(err, "get controller props")
	}

	for i := range vds {
		levels := []map[string]string{vds[i].Props, rd.Props}

		if rg != nil {
			for j := range rg.VolumeGroups {
				if rg.VolumeGroups[j].VolumeNumber == vds[i].VolumeNumber {
					levels = append(levels, rg.VolumeGroups[j].Props)
				}
			}

			levels = append(levels, rg.Props)
		}

		if !blocksize.AutoEnabled(append(levels, ctrlProps)...) {
			return false, nil
		}
	}

	return true, nil
}
