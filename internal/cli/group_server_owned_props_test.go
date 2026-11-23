// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A group's props are copied onto every definition spawned from it, so the CLI
// door refuses a prop blockstor reads back off a definition the way `rg c` and
// `rg modify` refuse it on the REST door, and still sets an ordinary one and
// still deletes a server-owned one a group already carries.
func TestResourceGroupSetPropertyRefusesServerOwnedProps(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:  "rg-owned",
			Props: map[string]string{store.RestoreAdoptedProp: "left-from-before"},
		})
	})

	if got := app.Run(t.Context(), []string{"rg", "set-property", "rg-owned", "Aux/team", "storage"}); got != 0 {
		t.Fatalf("setting an ordinary prop exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"rg", "set-property", "rg-owned", store.RestoreFromSnapshotProp, "src:snap"}); got == 0 {
		t.Fatal("a group took a restore marker every spawned definition would inherit")
	}

	if !strings.Contains(errBuf.String(), "blockstor sets this property itself") {
		t.Errorf("stderr %q does not say why", errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"rg", "delete-property", "rg-owned", store.RestoreAdoptedProp}); got != 0 {
		t.Fatalf("deleting a server-owned prop from a group exit = %d (stderr: %s)", got, errBuf.String())
	}

	rg, err := appStore(t, app).ResourceGroups().Get(t.Context(), "rg-owned")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if rg.Props["Aux/team"] != "storage" {
		t.Errorf("the ordinary prop was not set: %v", rg.Props)
	}

	if _, ok := rg.Props[store.RestoreFromSnapshotProp]; ok {
		t.Errorf("the refused marker reached the group: %v", rg.Props)
	}

	if _, ok := rg.Props[store.RestoreAdoptedProp]; ok {
		t.Errorf("the server-owned prop survived its delete: %v", rg.Props)
	}
}
