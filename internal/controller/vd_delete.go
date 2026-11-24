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

package controller

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"
)

// finishVolumeDefinitionDeletes removes each volume definition flagged DELETE
// whose replicas have all confirmed deleting its volume. It is the
// controller's half of LINSTOR's volume definition delete, which drops the
// definition once every satellite answered: the REST door tries the same
// right after `vd d`, and this picks it up when a satellite confirms later,
// after a restart, or once `node lost` took away a replica that never would.
func (r *ResourceDefinitionReconciler) finishVolumeDefinitionDeletes(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	for i := range rd.Spec.VolumeDefinitions {
		vd := &rd.Spec.VolumeDefinitions[i]
		if !vd.Deleting() {
			continue
		}

		_, err := store.FinishVolumeDefinitionDelete(ctx, r.Store, rd.Name, vd.VolumeNumber)
		if err != nil {
			return errors.Wrapf(err, "volume definition %s/%d", rd.Name, vd.VolumeNumber)
		}
	}

	return nil
}

// deletedVolumesChanged reports whether a replica's volume definition delete
// confirmations changed.
func deletedVolumesChanged(oldObj, newObj client.Object) bool {
	oldRes, okOld := oldObj.(*blockstoriov1alpha1.Resource)
	newRes, okNew := newObj.(*blockstoriov1alpha1.Resource)

	if !okOld || !okNew {
		return false
	}

	return !slices.Equal(oldRes.Status.DeletedVolumes, newRes.Status.DeletedVolumes)
}
