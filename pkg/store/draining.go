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

package store

import (
	"slices"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// ErrDefinitionReplicasDraining refuses a definition whose name still has
// replicas of an earlier definition being deleted under it.
var ErrDefinitionReplicasDraining = errors.New("replicas of an earlier definition under that name are still being deleted")

// RefuseOverDrainingReplicas is the check both stores run before they create
// a definition: none of the replicas under its name may be on their way out.
//
// LINSTOR keeps a deleted definition, flagged DELETE, until every satellite
// has confirmed its replica gone, so a definition of the same name cannot be
// created while a node holding one of them is offline. linstor-csi retries
// such a CreateVolume until the name is free. Here the definition object goes
// at once and its replicas drain on their own; a definition created over them
// would count a replica of the old one as its own. The old replica carries the
// old volume's DRBD node id, disk state and generation, and the new replicas
// then elect it as their initial UpToDate source, take a seed generation from
// it, and size their block from it, and come up Inconsistent with nothing to
// sync from once it is gone.
//
// A replica that is not being deleted is not refused: an orphan left by a
// cascade that did not finish, or one placed under a name on purpose, is the
// restore doors' to judge (RefuseReplicasWithoutDefinition).
func RefuseOverDrainingReplicas(name string, replicas []apiv1.Resource) error {
	var draining []string

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDelete) {
			draining = append(draining, replicas[i].NodeName)
		}
	}

	if len(draining) == 0 {
		return nil
	}

	slices.Sort(draining)

	return errors.Wrapf(errors.Mark(ErrAlreadyExists, ErrDefinitionReplicasDraining),
		"resource definition %q is still being deleted: its replicas on %s are not gone yet; retry once they are",
		name, strings.Join(draining, ", "))
}
