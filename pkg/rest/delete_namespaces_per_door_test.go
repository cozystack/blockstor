// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"maps"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// namespaceCase is one delete_namespaces entry and what it must leave of
// seededNamespaceProps: a namespace takes the keys below it, not a key spelled
// like it, and trailing slashes name the same namespace.
type namespaceCase struct {
	ns         string
	gone, kept []string
}

func namespaceCases() []namespaceCase {
	return []namespaceCase{
		{ns: "DrbdOptions", gone: []string{"DrbdOptions/Net/protocol"}, kept: []string{"DrbdOptions", "DrbdOptionsOther"}},
		{ns: "Aux//", gone: []string{"Aux/a"}, kept: []string{"Aux", "Auxiliary"}},
	}
}

func seededNamespaceProps() map[string]string {
	return map[string]string{
		"DrbdOptions": "x", "DrbdOptions/Net/protocol": "C", "DrbdOptionsOther": "y",
		"Aux": "own", "Aux/a": "1", "Auxiliary": "keep",
	}
}

func checkNamespaceCase(t *testing.T, door string, tc namespaceCase, props map[string]string) {
	t.Helper()

	for _, key := range tc.gone {
		if _, ok := props[key]; ok {
			t.Errorf("%s, delete_namespaces=[%q]: %q survived", door, tc.ns, key)
		}
	}

	for _, key := range tc.kept {
		if _, ok := props[key]; !ok {
			t.Errorf("%s, delete_namespaces=[%q]: %q went", door, tc.ns, key)
		}
	}
}

// Every door reads delete_namespaces the one way, through the shared deleter;
// a door that kept its own loop would answer differently on one of these.
func TestDeleteNamespacesReadsTheSameOnEveryDoor(t *testing.T) {
	t.Parallel()

	for _, tc := range namespaceCases() {
		modify := apiv1.GenericPropsModify{DeleteNamespace: []string{tc.ns}}

		t.Run("volume-definition "+tc.ns, func(t *testing.T) {
			t.Parallel()

			vd := apiv1.VolumeDefinition{Props: seededNamespaceProps()}
			mergeVolumeDefinitionPatch(&vd, &volumeDefinitionModifyBody{DeleteNamespaces: modify.DeleteNamespace})
			checkNamespaceCase(t, "volume definition", tc, vd.Props)
		})

		t.Run("per-resource-volume "+tc.ns, func(t *testing.T) {
			t.Parallel()

			vol := apiv1.Volume{Props: seededNamespaceProps()}
			mergeVolumePropsPatch(&vol, &modify)
			checkNamespaceCase(t, "per-resource volume", tc, vol.Props)
		})

		t.Run("node-connection "+tc.ns, func(t *testing.T) {
			t.Parallel()

			conn := map[string]map[string]string{"a": seededNamespaceProps()}
			mergeNodeConnectionPropsInto(conn, "a", &modify)
			checkNamespaceCase(t, "node connection", tc, conn["a"])
		})

		t.Run("pool-definition "+tc.ns, func(t *testing.T) {
			t.Parallel()

			def := store.StoragePoolDefinition{Props: seededNamespaceProps()}
			applySPDfnPropsPatch(&def, &modify)
			checkNamespaceCase(t, "pool definition", tc, def.Props)
		})

		t.Run("pool "+tc.ns, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
				t.Fatalf("seed node: %v", err)
			}

			if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
				NodeName: "n1", StoragePoolName: "p1", ProviderKind: apiv1.StoragePoolKindZFSThin,
				Props: maps.Clone(seededNamespaceProps()),
			}); err != nil {
				t.Fatalf("seed pool: %v", err)
			}

			if code := putNamespaceDelete(t, st, "/v1/nodes/n1/storage-pools/p1", tc.ns); code != http.StatusOK {
				t.Fatalf("pool modify = %d, want 200", code)
			}

			sp, err := st.StoragePools().Get(ctx, "n1", "p1")
			if err != nil {
				t.Fatalf("read the pool: %v", err)
			}

			checkNamespaceCase(t, "pool", tc, sp.Props)
		})
	}
}
