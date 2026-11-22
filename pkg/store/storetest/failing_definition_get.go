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

package storetest

import (
	"context"
	"errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// ErrDefinitionGet is the error FailingDefinitionGet answers every
// definition read with.
var ErrDefinitionGet = errors.New("storetest: definition read failed")

// FailingDefinitionGet wraps a store whose definition reads fail with
// something other than NotFound. A delete guard that has to read the
// definition is tested with it: a read it cannot make must refuse the
// delete, not let it through.
type FailingDefinitionGet struct {
	store.Store
}

// ResourceDefinitions returns the failing definition store.
func (f FailingDefinitionGet) ResourceDefinitions() store.ResourceDefinitionStore {
	return failingDefinitions{ResourceDefinitionStore: f.Store.ResourceDefinitions()}
}

type failingDefinitions struct {
	store.ResourceDefinitionStore
}

func (failingDefinitions) Get(context.Context, string) (apiv1.ResourceDefinition, error) {
	return apiv1.ResourceDefinition{}, ErrDefinitionGet
}
