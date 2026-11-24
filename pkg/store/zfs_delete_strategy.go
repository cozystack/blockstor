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
	"context"
	"errors"
	"fmt"

	"github.com/cozystack/blockstor/pkg/validate"
)

// ZFSDeleteRefused reports whether LINSTOR's ZFS delete strategy refuses a
// replica or volume definition delete under rdName: the strategy resolved
// from the definition, its group and the controller is DELETE, and the
// definition has snapshots. Shared by the REST and CLI doors so both give the
// same answer.
//
// A definition that is already gone refuses nothing; the caller's own delete
// decides what that means.
func ZFSDeleteRefused(ctx context.Context, st Store, rdName string) (bool, error) {
	rd, err := st.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("get resource definition %s: %w", rdName, err)
	}

	var groupProps map[string]string

	if rd.ResourceGroupName != "" {
		rg, err := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)

		switch {
		case err == nil:
			groupProps = rg.Props
		case !errors.Is(err, ErrNotFound):
			return false, fmt.Errorf("get resource group %s: %w", rd.ResourceGroupName, err)
		}
	}

	controllerProps, err := st.ControllerProps().Get(ctx)
	if err != nil {
		return false, fmt.Errorf("get controller props: %w", err)
	}

	strategy := validate.EffectiveZFSDeleteStrategy(rd.Props, groupProps, controllerProps)
	if strategy != validate.ZFSDeleteStrategyDelete {
		return false, nil
	}

	// Uncached, as the definition delete's own check: a snapshot that has
	// not reached the informer yet is still one.
	snapshots, err := st.Snapshots().ListByDefinitionUncached(ctx, rdName)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return false, fmt.Errorf("list snapshots of %s: %w", rdName, err)
	}

	return validate.ZFSDeleteRefusal(strategy, len(snapshots) > 0), nil
}
