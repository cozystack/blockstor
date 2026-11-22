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
// definition's props, as LINSTOR copies them, but not the resource
// connection that allowed two Primaries: the copy's replicas are its own.
func TestTravellingPropsLeaveTheResourceConnectionsBehind(t *testing.T) {
	t.Parallel()

	src := map[string]string{
		apiv1.ResourceConnectionPropKey("node-a", "node-b", "DrbdOptions/Net/allow-two-primaries"): "yes",
		apiv1.ResourceConnectionPropKey("node-a", "node-b", "DrbdOptions/Net/protocol"):            "C",
		"Internal/Drbd/LiveMigrate/SourceNode":                                                     "node-a",
		"Internal/Drbd/LiveMigrate/TargetNode":                                                     "node-b",
		"DrbdOptions/Net/protocol":                                                                 "A",
	}

	want := map[string]string{
		"Internal/Drbd/LiveMigrate/SourceNode": "node-a",
		"Internal/Drbd/LiveMigrate/TargetNode": "node-b",
		"DrbdOptions/Net/protocol":             "A",
	}

	if got := store.TravellingProps(src); !maps.Equal(got, want) {
		t.Fatalf("travelling props\n got %v\nwant %v", got, want)
	}
}
