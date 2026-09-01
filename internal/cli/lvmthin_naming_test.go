// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A volume group and the thin LV inside it are different objects with
// different namespaces, which is why upstream LINSTOR prefixes a bare pool
// name for the group and keeps the bare name for the LV. This verb wrote the
// bare name into both fields, and wrote a `vg/thin` value into the
// volume-group field whole — a slash is not legal in a VG name.
//
// The REST door for the same verb has always followed upstream, so the two
// doors produced different backing for the same command.
func TestCreateDevicePoolNamesLvmThinTheWayRESTDoes(t *testing.T) {
	t.Parallel()

	seed := func(ctx context.Context, backend store.Store) {
		_ = backend.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
			Name: "node-1-sdb", NodeName: "node-1",
			DevicePath: "/dev/disk/by-id/wwn-0x1", CurrentDevPath: "/dev/sdb",
			SizeBytes: 1 << 40,
		})
	}

	for name, tc := range map[string]struct {
		poolName string
		wantVG   string
		wantThin string
	}{
		"a bare name gets the upstream prefix": {"data", "linstor_data", "data"},
		"an explicit vg/thin is split":         {"vg0/thin0", "vg0", "thin0"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, seed)

			argv := []string{
				"ps", "cdp", "lvmthin", "node-1", "/dev/sdb", "--pool-name", tc.poolName,
			}
			if got := app.Run(t.Context(), argv); got != 0 {
				t.Fatalf("create-device-pool exit = %d (stderr: %s)", got, errBuf.String())
			}

			backend := appStore(t, app)

			device, err := backend.PhysicalDevices().Get(t.Context(), "node-1-sdb")
			if err != nil {
				t.Fatalf("get device: %v", err)
			}

			if device.AttachTo.VGName != tc.wantVG {
				t.Errorf("volume group = %q, want %q", device.AttachTo.VGName, tc.wantVG)
			}

			if device.AttachTo.ThinPoolName != tc.wantThin {
				t.Errorf("thin pool = %q, want %q", device.AttachTo.ThinPoolName, tc.wantThin)
			}

			pool, err := backend.StoragePools().Get(t.Context(), "node-1", tc.poolName)
			if err != nil {
				t.Fatalf("get pool: %v", err)
			}

			if got := pool.Props["StorDriver/LvmVg"]; got != tc.wantVG {
				t.Errorf("pool volume group = %q, want %q", got, tc.wantVG)
			}

			if got := pool.Props["StorDriver/ThinPool"]; got != tc.wantThin {
				t.Errorf("pool thin pool = %q, want %q", got, tc.wantThin)
			}
		})
	}
}
