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

package k8s_test

import (
	"errors"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The REST clone and restore doors resolve a definition's stack through the
// store. A definition naming its group in the operator's spelling resolves
// to the group's CR, stored under the folded name: undecided while the
// group's stack is not pinned onto it, and the pinned stack after, never
// the default stack the satellite would build for a group it cannot find.
func TestTheStackOfADefinitionNamingItsGroupInAnotherCase(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()
	st := k8s.New(fixture.client)
	luks := []string{"DRBD", "LUKS", "STORAGE"}

	err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "LuksRG", SelectFilter: apiv1.AutoSelectFilter{LayerStack: luks},
	})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}

	rd := apiv1.ResourceDefinition{Name: "pvc-case", ResourceGroupName: "LuksRG"}

	_, err = store.EffectiveLayerStack(ctx, st, &rd)
	if !errors.Is(err, store.ErrLayerStackUndecided) {
		t.Fatalf("before the pin: err = %v, want ErrLayerStackUndecided", err)
	}

	rd.LayerStack = luks

	got, err := store.EffectiveLayerStack(ctx, st, &rd)
	if err != nil || !slices.Equal(got, luks) {
		t.Fatalf("after the pin: = %v, %v; want %v", got, err, luks)
	}
}
