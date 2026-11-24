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

package cli_test

import (
	"context"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A replica with a disk beyond what the definition's peer slots hold is
// refused, as by the REST door.
func TestResourceCreateRefusesAReplicaThePeerSlotsCannotHold(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, st store.Store) {
		_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x", PeerSlots: 1})

		for _, node := range []string{"n1", "n2", "n3"} {
			_ = st.Nodes().Create(ctx, &apiv1.Node{Name: node})
			_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{NodeName: node, StoragePoolName: "sp", ProviderKind: "LVM_THIN"})
		}

		for _, node := range []string{"n1", "n2"} {
			_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: node})
		}
	})

	if got := app.Run(t.Context(), []string{"r", "c", "n3", "pvc-x", "--storage-pool", "sp"}); got == 0 {
		t.Errorf("a third replica with a disk on a one-slot definition was accepted")
	}

	if _, err := appStore(t, app).Resources().Get(t.Context(), "pvc-x", "n3"); err == nil {
		t.Errorf("the refused replica was created")
	}

	if got := app.Run(t.Context(), []string{"r", "c", "n3", "pvc-x", "--diskless"}); got != 0 {
		t.Errorf("a diskless replica was refused")
	}
}
