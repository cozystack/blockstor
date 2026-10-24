package rest

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The props-modify fields edit props a definition already has. On a door
// that creates the definition there is nothing to edit, and answering
// success while dropping them claimed an edit that never happened.
func propsModifyCases() map[string]func(override *map[string]string, del, ns *[]string) {
	return map[string]func(override *map[string]string, del, ns *[]string){
		"override_props":    func(o *map[string]string, _, _ *[]string) { *o = map[string]string{"Aux/k": "v"} },
		"delete_props":      func(_ *map[string]string, d, _ *[]string) { *d = []string{"Aux/k"} },
		"delete_namespaces": func(_ *map[string]string, _, n *[]string) { *n = []string{"Aux"} },
	}
}

func TestRDCreateRefusesPropsModifyFields(t *testing.T) {
	t.Parallel()

	for field, set := range propsModifyCases() {
		t.Run(field, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			base, stop := startServerWithStore(t, st)
			defer stop()

			body := apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{Name: "rd-modify"},
			}
			set(&body.OverrideProps, &body.DeleteProps, &body.DeleteNamespace)

			assertRefusedNamingField(t, base+"/v1/resource-definitions", body, field)

			_, err := st.ResourceDefinitions().Get(t.Context(), "rd-modify")
			if !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("a refused create left the definition behind: err=%v", err)
			}
		})
	}
}

func TestSpawnRefusesPropsModifyFields(t *testing.T) {
	t.Parallel()

	for field, set := range propsModifyCases() {
		t.Run(field, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-modify"}); err != nil {
				t.Fatalf("seed group: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body := apiv1.ResourceGroupSpawn{ResourceDefinitionName: "rd-modify", VolumeSizes: []int64{4096}}
			set(&body.OverrideProps, &body.DeleteProps, &body.DeleteNamespace)

			assertRefusedNamingField(t, base+"/v1/resource-groups/rg-modify/spawn", body, field)

			_, err := st.ResourceDefinitions().Get(t.Context(), "rd-modify")
			if !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("a refused spawn left the definition behind: err=%v", err)
			}
		})
	}
}

// `linstor rg spawn --definitions-only` asks for the definition and its
// volumes and no placement. The handler placed replicas anyway.
func TestSpawnDefinitionsOnlyPlacesNothing(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-defs",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	}); err != nil {
		t.Fatalf("seed group: %v", err)
	}

	for _, node := range []string{"n1", "n2"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool", NodeName: node, ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 1_000_000, TotalCapacity: 10_000_000,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	raw, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "rd-defs",
		VolumeSizes:            []int64{4096},
		DefinitionsOnly:        true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-defs/spawn", raw)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d, want 201: %s", resp.StatusCode, b)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "rd-defs")
	if err != nil || len(vds) != 1 {
		t.Fatalf("volume definitions: %+v, err=%v; want one", vds, err)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "rd-defs")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Fatalf("definitions_only spawn placed %d replicas, want none", len(replicas))
	}
}

func assertRefusedNamingField(t *testing.T, url string, body any, field string) {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, url, raw)
	defer func() { _ = resp.Body.Close() }()

	got, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", resp.StatusCode, got)
	}

	if !strings.Contains(string(got), field) {
		t.Fatalf("the refusal does not name %s: %s", field, got)
	}
}
