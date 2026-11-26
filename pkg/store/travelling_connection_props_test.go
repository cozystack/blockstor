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

package store_test

import (
	"maps"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A clone or a restore of a volume caught mid live migration gets the
// definition's props, as LINSTOR copies them, but not the migration: the
// copy runs on no VM and nobody will close a migration on it. The
// connection that allowed two Primaries stays behind, and what the
// migration wrote on the definition is reverted as unmake-available would.
func TestTravellingPropsLeaveTheMigrationBehind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  map[string]string
		want map[string]string
	}{
		{
			name: "connection scope",
			src: map[string]string{
				apiv1.ResourceConnectionPropKey("node-a", "node-b", "DrbdOptions/Net/allow-two-primaries"): "yes",
				apiv1.ResourceConnectionPropKey("node-a", "node-b", "DrbdOptions/Net/protocol"):            "C",
				"Internal/Drbd/LiveMigrate/SourceNode":                                                     "node-a",
				"Internal/Drbd/LiveMigrate/TargetNode":                                                     "node-b",
				"Internal/Drbd/LiveMigrate/ProtocolSetOn":                                                  "RSC_CONN",
				"DrbdOptions/Net/protocol":                                                                 "A",
			},
			want: map[string]string{"DrbdOptions/Net/protocol": "A"},
		},
		{
			name: "two diskless ends, on the definition",
			src: map[string]string{
				"DrbdOptions/Net/allow-two-primaries":     "yes",
				"DrbdOptions/Net/protocol":                "C",
				"Internal/Drbd/LiveMigrate/SourceNode":    "node-a",
				"Internal/Drbd/LiveMigrate/TargetNode":    "node-b",
				"Internal/Drbd/LiveMigrate/ProtocolSetOn": "RSC_DFN",
				"Internal/Drbd/LiveMigrate/PrevProtocol":  "A",
			},
			want: map[string]string{"DrbdOptions/Net/protocol": "A"},
		},
		{
			name: "allow-two-primaries a client set, without marks",
			src: map[string]string{
				"DrbdOptions/Net/allow-two-primaries": "yes",
				"DrbdOptions/Net/protocol":            "C",
			},
			want: map[string]string{
				"DrbdOptions/Net/allow-two-primaries": "yes",
				"DrbdOptions/Net/protocol":            "C",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := store.TravellingProps(tc.src); !maps.Equal(got, tc.want) {
				t.Fatalf("travelling props\n got %v\nwant %v", got, tc.want)
			}
		})
	}
}
