// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

// countingRDs records how the listing reaches the layer stacks.
type countingRDs struct {
	store.ResourceDefinitionStore

	gets  atomic.Int64
	lists atomic.Int64
}

func (c *countingRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	c.gets.Add(1)

	return c.ResourceDefinitionStore.Get(ctx, name) //nolint:wrapcheck // test helper
}

func (c *countingRDs) List(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	c.lists.Add(1)

	return c.ResourceDefinitionStore.List(ctx) //nolint:wrapcheck // test helper
}

type rdCountingStore struct {
	store.Store

	rds *countingRDs
}

func (c *rdCountingStore) ResourceDefinitions() store.ResourceDefinitionStore { return c.rds }

// seedLayered seeds one replica per definition, each reported the way
// the store projects it: the default DRBD chain on the replica, and the
// definition's own stack only on the definition.
func seedLayered(t *testing.T, stacks map[string][]string) *rdCountingStore {
	t.Helper()

	backend := store.NewInMemory()
	ctx := t.Context()

	for name, stack := range stacks {
		if err := backend.ResourceDefinitions().Create(ctx,
			&apiv1.ResourceDefinition{Name: name, LayerStack: stack}); err != nil {
			t.Fatalf("seed definition: %v", err)
		}

		rep := &apiv1.Resource{
			Name:        name,
			NodeName:    "node-1",
			State:       apiv1.ResourceState{InUse: new(bool)},
			LayerObject: &apiv1.ResourceLayer{Type: apiv1.LayerKindDRBD, Drbd: &apiv1.DrbdResourceLayer{}},
			Volumes: []apiv1.Volume{{
				VolumeNumber:  0,
				StoragePool:   "data",
				DevicePath:    "/dev/zvol/data/" + name + "_00000",
				LayerDataList: []apiv1.VolumeLayerData{{Type: "DRBD"}, {Type: "STORAGE"}},
			}},
		}

		if err := backend.Resources().Create(ctx, rep); err != nil {
			t.Fatalf("seed replica: %v", err)
		}
	}

	return &rdCountingStore{
		Store: backend,
		rds:   &countingRDs{ResourceDefinitionStore: backend.ResourceDefinitions()},
	}
}

func runLayered(t *testing.T, st store.Store, args ...string) string {
	t.Helper()

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return st, nil
		},
	}

	if got := app.Run(t.Context(), args); got != 0 {
		t.Fatalf("%v: exit = %d (stderr: %s)", args, got, errBuf.String())
	}

	return out.String()
}

// rowOf returns the rendered row that names the definition.
func rowOf(t *testing.T, out, name string) string {
	t.Helper()

	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "| "+name+" ") {
			return line
		}
	}

	t.Fatalf("no row for %s in:\n%s", name, out)

	return ""
}

// A local volume — STORAGE alone, or LUKS over STORAGE — has no DRBD
// disk state to report, and `linstor r l` shows it Created. The replica
// CR does not carry the stack, so the CLI has to read it off the
// definition; reading the store's default DRBD chain instead showed
// every such volume as Unknown, in `resource list` and in both volume
// listings alike.
func TestListingsReadTheDefinitionsLayerStack(t *testing.T) {
	t.Parallel()

	st := seedLayered(t, map[string][]string{
		"pvc-local":     {"STORAGE"},
		"pvc-encrypted": {"LUKS", "STORAGE"},
		"pvc-drbd":      {"DRBD", "STORAGE"},
	})

	for _, args := range [][]string{
		{"resource", "list"},
		{"volume", "list"},
		{"resource", "list-volumes"},
	} {
		out := runLayered(t, st, args...)

		for name, want := range map[string]string{
			"pvc-local":     "Created",
			"pvc-encrypted": "Created",
			"pvc-drbd":      "Unknown",
		} {
			row := rowOf(t, out, name)
			if !strings.Contains(row, "| "+want+" ") {
				t.Errorf("%v: %s row = %q, want State %s", args, name, row, want)
			}
		}
	}

	// The stamp lands on the machine output as well, which carries the
	// layer data rather than the rendered state.
	machine := runLayered(t, st, "resource", "list", "-m", "-r", "pvc-local")
	if strings.Contains(machine, `"DRBD"`) {
		t.Errorf("machine output for a STORAGE-only replica still names DRBD: %s", machine)
	}
}

// The layer-stack read follows the volume-size read: a narrowed
// listing asks about the definitions it shows, and a wide one reads
// every definition once instead of once each.
func TestLayerStackReadsFollowTheListingsWidth(t *testing.T) {
	t.Parallel()

	stacks := map[string][]string{}
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r"} {
		stacks["pvc-"+name] = []string{"STORAGE"}
	}

	narrow := seedLayered(t, stacks)
	runLayered(t, narrow, "resource", "list", "-r", "pvc-a")

	if gets, lists := narrow.rds.gets.Load(), narrow.rds.lists.Load(); gets != 1 || lists != 0 {
		t.Errorf("narrowed listing: %d gets and %d lists, want 1 and 0", gets, lists)
	}

	wide := seedLayered(t, stacks)
	runLayered(t, wide, "volume", "list")

	if gets, lists := wide.rds.gets.Load(), wide.rds.lists.Load(); gets != 0 || lists != 1 {
		t.Errorf("wide listing: %d gets and %d lists, want 0 and 1", gets, lists)
	}
}
