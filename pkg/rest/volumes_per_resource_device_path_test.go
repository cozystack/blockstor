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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestGetVolumeWaitsForDevicePath: the satellite's observer reports a new
// replica's disk state before its apply pass stamps the device path, so a
// volume read can find the volume with no path. linstor-csi fails the
// attach on an empty path ("could not determine device path"), so the read
// waits for the path the way it waits for the volume itself.
func TestGetVolumeWaitsForDevicePath(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-dp"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-dp", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	res := &apiv1.Resource{
		Name:     "pvc-dp",
		NodeName: "n1",
		Flags:    []string{apiv1.ResourceFlagDiskless},
		Volumes:  []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "Diskless"}}},
	}
	if err := st.Resources().Create(ctx, res); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	go func() {
		time.Sleep(300 * time.Millisecond)

		reported := *res
		reported.Volumes = []apiv1.Volume{{
			VolumeNumber: 0,
			DevicePath:   "/dev/drbd1000",
			State:        apiv1.VolumeState{DiskState: "Diskless"},
		}}
		_ = st.Resources().Update(ctx, &reported)
	}()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-dp/resources/n1/volumes/0")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", resp.StatusCode)
	}

	var vol apiv1.Volume
	if err := json.NewDecoder(resp.Body).Decode(&vol); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if vol.DevicePath != "/dev/drbd1000" {
		t.Errorf("device_path = %q, want /dev/drbd1000", vol.DevicePath)
	}
}
