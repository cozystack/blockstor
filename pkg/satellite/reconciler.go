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

package satellite

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"

	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
)

// ReconcilerConfig parametrises a Reconciler. Providers maps the
// satellite's local storage-pool names to provisioned `storage.Provider`
// instances; an unknown pool fails the per-resource Apply with a
// surfaced error message.
type ReconcilerConfig struct {
	Providers map[string]storage.Provider
}

// Reconciler turns a controller-pushed DesiredResource set into local
// state. Phase-3 cut: storage provisioning only — Build/.res / drbdadm
// follow in the next slice.
type Reconciler struct {
	cfg ReconcilerConfig
}

// NewReconciler constructs a Reconciler from cfg.
func NewReconciler(cfg ReconcilerConfig) *Reconciler {
	return &Reconciler{cfg: cfg}
}

// Apply walks res and brings local storage in line with each item.
// Each input gets a ResourceApplyResult — partial success is the norm
// (one missing pool shouldn't sink the rest of a batch).
//
// The signature returns an error too, but reserves it for context
// cancellation — per-resource failures land in the Result entries.
func (r *Reconciler) Apply(ctx context.Context, res []*satellitepb.DesiredResource) ([]*satellitepb.ResourceApplyResult, error) {
	results := make([]*satellitepb.ResourceApplyResult, 0, len(res))

	for _, dr := range res {
		err := ctx.Err()
		if err != nil {
			return results, errors.Wrap(err, "apply: context cancelled")
		}

		results = append(results, r.applyOne(ctx, dr))
	}

	return results, nil
}

// applyOne reconciles a single DesiredResource. Diskless replicas skip
// storage entirely (they're memory-backed by the DRBD stack); everything
// else routes one CreateVolume per DesiredVolume.
func (r *Reconciler) applyOne(ctx context.Context, dr *satellitepb.DesiredResource) *satellitepb.ResourceApplyResult {
	res := &satellitepb.ResourceApplyResult{
		Name:     dr.GetName(),
		NodeName: dr.GetNodeName(),
		Ok:       true,
	}

	if isDiskless(dr.GetFlags()) {
		return res
	}

	for _, vol := range dr.GetVolumes() {
		provider, ok := r.cfg.Providers[vol.GetStoragePool()]
		if !ok {
			res.Ok = false
			res.Message = fmt.Sprintf("unknown storage pool %q", vol.GetStoragePool())

			return res
		}

		err := provider.CreateVolume(ctx, storage.Volume{
			ResourceName: dr.GetName(),
			VolumeNumber: vol.GetVolumeNumber(),
			SizeKib:      vol.GetSizeKib(),
		})
		if err != nil {
			res.Ok = false
			res.Message = err.Error()

			return res
		}
	}

	return res
}

// isDiskless returns true when the DRBD-layer "DISKLESS" flag is set.
// Diskless replicas live entirely in DRBD memory and have no backing
// storage, so the reconciler must skip the storage path for them.
func isDiskless(flags []string) bool {
	return slices.Contains(flags, "DISKLESS")
}
