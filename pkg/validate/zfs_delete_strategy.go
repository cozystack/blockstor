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

package validate

import (
	"errors"
	"strings"
)

// ZFSDeleteStrategyProp is LINSTOR's property choosing what a delete does
// with a ZFS volume whose definition has snapshots. It is read from the
// resource definition, then its resource group, then the controller.
const ZFSDeleteStrategyProp = "StorDriver/Zfs/DeleteStrategy"

// ZFSDeleteStrategy is a value of ZFSDeleteStrategyProp.
type ZFSDeleteStrategy string

// The values LINSTOR accepts, case-insensitively.
const (
	// ZFSDeleteStrategyDelete refuses to delete a replica or a volume
	// definition while the resource definition has snapshots.
	ZFSDeleteStrategyDelete ZFSDeleteStrategy = "DELETE"

	// ZFSDeleteStrategyRename lets the delete through: a zvol that still has
	// snapshots is renamed and destroyed when its last snapshot goes.
	ZFSDeleteStrategyRename ZFSDeleteStrategy = "RENAME"

	// ZFSDeleteStrategyDynamic is DELETE without snapshots and RENAME with
	// them, so it never refuses. It is the default.
	ZFSDeleteStrategyDynamic ZFSDeleteStrategy = "DYNAMIC"
)

// ErrZFSDeleteStrategy refuses a delete under the DELETE strategy while the
// resource definition has snapshots.
var ErrZFSDeleteStrategy = errors.New("the DELETE strategy refuses a delete while the resource definition has snapshots")

// EffectiveZFSDeleteStrategy resolves the strategy the way LINSTOR does: the
// first scope that sets the property wins, and a value that is not one of
// the three reads as DYNAMIC, as an unset one does.
func EffectiveZFSDeleteStrategy(scopes ...map[string]string) ZFSDeleteStrategy {
	for _, props := range scopes {
		value, ok := props[ZFSDeleteStrategyProp]
		if !ok {
			continue
		}

		for _, strategy := range []ZFSDeleteStrategy{ZFSDeleteStrategyDelete, ZFSDeleteStrategyRename, ZFSDeleteStrategyDynamic} {
			if strings.EqualFold(value, string(strategy)) {
				return strategy
			}
		}

		return ZFSDeleteStrategyDynamic
	}

	return ZFSDeleteStrategyDynamic
}

// ZFSDeleteRefusal reports whether the strategy refuses a replica or volume
// definition delete. Like LINSTOR's, the check does not look at which
// provider the volumes are on: under DELETE, any snapshot of the definition
// refuses.
func ZFSDeleteRefusal(strategy ZFSDeleteStrategy, hasSnapshots bool) bool {
	return strategy == ZFSDeleteStrategyDelete && hasSnapshots
}
