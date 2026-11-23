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
	"errors"
	"maps"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

func replicaInUse(node string, using bool) apiv1.Resource {
	return apiv1.Resource{Name: "vm-disk", NodeName: node, State: apiv1.ResourceState{InUse: &using}}
}

func refusalCode(t *testing.T, err error) int64 {
	t.Helper()

	var refusal *dualPrimaryRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("error %v is not a dual-primary refusal", err)
	}

	return refusal.rc.RetCode
}

func TestMigrationSource(t *testing.T) {
	t.Parallel()

	marks := map[string]string{liveMigrateSourceProp: "node-a", liveMigrateTargetProp: "node-b"}

	tests := []struct {
		name     string
		props    map[string]string
		replicas []apiv1.Resource
		target   string
		want     string
		refused  bool
	}{
		{
			name:     "in use nowhere is a plain attach",
			replicas: []apiv1.Resource{replicaInUse("node-a", false), replicaInUse("node-c", false)},
			target:   "node-b",
		},
		{
			name:     "in use on one other node is the source",
			replicas: []apiv1.Resource{replicaInUse("node-a", true), replicaInUse("node-c", false)},
			target:   "node-b",
			want:     "node-a",
		},
		{
			name:     "in use on the target itself needs nothing",
			replicas: []apiv1.Resource{replicaInUse("node-b", true)},
			target:   "Node-B",
		},
		{
			name:     "the repeat of the marked migration finds the same source",
			props:    marks,
			replicas: []apiv1.Resource{replicaInUse("node-a", true)},
			target:   "node-b",
			want:     "node-a",
		},
		{
			name:     "a mark naming another migration refuses",
			props:    marks,
			replicas: []apiv1.Resource{replicaInUse("node-a", true)},
			target:   "node-c",
			refused:  true,
		},
		{
			name:     "in use on both ends of the marked migration repeats it",
			props:    marks,
			replicas: []apiv1.Resource{replicaInUse("node-b", true), replicaInUse("node-a", true)},
			target:   "node-b",
			want:     "node-a",
		},
		{
			name:     "in use on two nodes without a mark refuses",
			replicas: []apiv1.Resource{replicaInUse("node-a", true), replicaInUse("node-c", true)},
			target:   "node-b",
			refused:  true,
		},
		{
			name:     "in use on a node the mark does not name refuses",
			props:    marks,
			replicas: []apiv1.Resource{replicaInUse("node-a", true), replicaInUse("node-c", true)},
			target:   "node-b",
			refused:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := migrationSource("vm-disk", tc.props, tc.replicas, tc.target)

			if tc.refused {
				if code := refusalCode(t, err); code != apiCallRcError|apiCallRcFailExistsLiveMigrate {
					t.Fatalf("ret_code %#x, want FAIL_EXISTS_LIVE_MIGRATE", code)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected refusal: %v", err)
			}

			if got != tc.want {
				t.Fatalf("source %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDualPrimaryPlanOpensTheConnection(t *testing.T) {
	t.Parallel()

	conn := func(key string) string { return apiv1.ResourceConnectionPropKey("node-a", "node-b", key) }

	tests := []struct {
		name  string
		plan  dualPrimaryPlan
		props map[string]string
		want  map[string]string
	}{
		{
			name:  "protocol C stays, only allow-two-primaries on the connection",
			plan:  dualPrimaryPlan{source: "node-a", target: "node-b"},
			props: map[string]string{},
			want: map[string]string{
				conn(allowTwoPrimariesProp): "yes",
				liveMigrateSourceProp:       "node-a",
				liveMigrateTargetProp:       "node-b",
			},
		},
		{
			name: "an inherited protocol A is overridden on the connection only",
			plan: dualPrimaryPlan{
				source: "node-a", target: "node-b",
				ends: [2]protocolSources{{inherited: "A"}, {inherited: "A"}},
			},
			props: map[string]string{},
			want: map[string]string{
				conn(allowTwoPrimariesProp):  "yes",
				conn(drbdProtocolProp):       "C",
				liveMigrateSourceProp:        "node-a",
				liveMigrateTargetProp:        "node-b",
				liveMigrateProtocolSetOnProp: protocolSetOnConnection,
			},
		},
		{
			name: "the definition's protocol is judged from the props being patched",
			plan: dualPrimaryPlan{source: "node-b", target: "node-a"},
			props: map[string]string{
				drbdProtocolProp: "B",
			},
			want: map[string]string{
				drbdProtocolProp:             "B",
				conn(allowTwoPrimariesProp):  "yes",
				conn(drbdProtocolProp):       "C",
				liveMigrateSourceProp:        "node-b",
				liveMigrateTargetProp:        "node-a",
				liveMigrateProtocolSetOnProp: protocolSetOnConnection,
			},
		},
		{
			name:  "a protocol the connection sets itself is recorded to be put back",
			plan:  dualPrimaryPlan{source: "node-a", target: "node-b"},
			props: map[string]string{conn(drbdProtocolProp): "A"},
			want: map[string]string{
				conn(allowTwoPrimariesProp):  "yes",
				conn(drbdProtocolProp):       "C",
				liveMigrateSourceProp:        "node-a",
				liveMigrateTargetProp:        "node-b",
				liveMigrateProtocolSetOnProp: protocolSetOnConnection,
				liveMigratePrevProtocolProp:  "A",
			},
		},
		{
			name: "two diskless ends are configured on the definition",
			plan: dualPrimaryPlan{
				source: "node-a", target: "node-b", bothDiskless: true,
				ends: [2]protocolSources{{}, {inherited: "A"}},
			},
			props: map[string]string{},
			want: map[string]string{
				allowTwoPrimariesProp:        "yes",
				drbdProtocolProp:             "C",
				liveMigrateSourceProp:        "node-a",
				liveMigrateTargetProp:        "node-b",
				liveMigrateProtocolSetOnProp: protocolSetOnDefinition,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			props := maps.Clone(tc.props)
			tc.plan.rdName = "vm-disk"

			msgs := tc.plan.apply(props)

			if !maps.Equal(props, tc.want) {
				t.Fatalf("props\n got %v\nwant %v", props, tc.want)
			}

			if len(msgs) == 0 {
				t.Fatal("no message answered")
			}
		})
	}
}

func TestRevertDualPrimary(t *testing.T) {
	t.Parallel()

	ab := func(key string) string { return apiv1.ResourceConnectionPropKey("node-a", "node-b", key) }
	cd := func(key string) string { return apiv1.ResourceConnectionPropKey("node-c", "node-d", key) }

	tests := []struct {
		name        string
		node        string
		props       map[string]string
		want        map[string]string
		wantChanged bool
	}{
		{
			name: "the connection override and its marks come off",
			node: "node-a",
			props: map[string]string{
				ab(allowTwoPrimariesProp):    "yes",
				ab(drbdProtocolProp):         "C",
				liveMigrateSourceProp:        "node-a",
				liveMigrateTargetProp:        "node-b",
				liveMigrateProtocolSetOnProp: protocolSetOnConnection,
				drbdProtocolProp:             "A",
			},
			want:        map[string]string{drbdProtocolProp: "A"},
			wantChanged: true,
		},
		{
			name: "a protocol the connection had is put back",
			node: "node-a",
			props: map[string]string{
				ab(allowTwoPrimariesProp):    "yes",
				ab(drbdProtocolProp):         "C",
				liveMigrateSourceProp:        "node-a",
				liveMigrateTargetProp:        "node-b",
				liveMigrateProtocolSetOnProp: protocolSetOnConnection,
				liveMigratePrevProtocolProp:  "B",
			},
			want:        map[string]string{ab(drbdProtocolProp): "B"},
			wantChanged: true,
		},
		{
			name: "the definition scope of two diskless ends is reverted",
			node: "node-b",
			props: map[string]string{
				allowTwoPrimariesProp:        "yes",
				drbdProtocolProp:             "C",
				liveMigrateSourceProp:        "node-a",
				liveMigrateTargetProp:        "node-b",
				liveMigrateProtocolSetOnProp: protocolSetOnDefinition,
				liveMigratePrevProtocolProp:  "A",
			},
			want:        map[string]string{drbdProtocolProp: "A"},
			wantChanged: true,
		},
		{
			name: "allow-two-primaries a pre-1.13 driver left on the definition comes off",
			node: "node-a",
			props: map[string]string{
				allowTwoPrimariesProp: "yes",
				"Aux/keep":            "1",
			},
			want:        map[string]string{"Aux/keep": "1"},
			wantChanged: true,
		},
		{
			name: "a connection deleted with its replica is not brought back",
			node: "node-a",
			props: map[string]string{
				liveMigrateSourceProp:        "node-a",
				liveMigrateTargetProp:        "node-b",
				liveMigrateProtocolSetOnProp: protocolSetOnConnection,
				liveMigratePrevProtocolProp:  "A",
			},
			want:        map[string]string{},
			wantChanged: true,
		},
		{
			name: "a connection between two other nodes is left alone",
			node: "node-a",
			props: map[string]string{
				cd(allowTwoPrimariesProp): "yes",
				ab(drbdProtocolProp):      "A",
			},
			want: map[string]string{
				cd(allowTwoPrimariesProp): "yes",
				ab(drbdProtocolProp):      "A",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			props := maps.Clone(tc.props)

			changed, _ := revertDualPrimary("vm-disk", tc.node, props)

			if !maps.Equal(props, tc.want) {
				t.Fatalf("props\n got %v\nwant %v", props, tc.want)
			}

			if changed != tc.wantChanged {
				t.Fatalf("changed %v, want %v", changed, tc.wantChanged)
			}
		})
	}
}

// Two diskless ends take protocol C from the definition, and a protocol a
// replica sets on itself is rendered over the definition's: the override
// could not take effect, so the migration is refused before anything is
// written. Between a diskful and a diskless end the connection carries
// the override, which wins over both replicas.
func TestDualPrimaryPlanRefusesAShadowedOverride(t *testing.T) {
	t.Parallel()

	shadowed := dualPrimaryPlan{
		rdName: "vm-disk", source: "node-a", target: "node-b", bothDiskless: true,
		ends: [2]protocolSources{{}, {resource: "A"}},
	}
	if code := refusalCode(t, shadowed.refuseShadowedOverride()); code != apiCallRcError|apiCallRcFailInvldProp {
		t.Fatalf("rc %d, want FAIL_INVLD_PROP", code)
	}

	for _, plan := range []dualPrimaryPlan{
		{rdName: "vm-disk", source: "node-a", target: "node-b", ends: [2]protocolSources{{}, {resource: "A"}}},
		{rdName: "vm-disk", source: "node-a", target: "node-b", bothDiskless: true, ends: [2]protocolSources{{resource: "c"}, {inherited: "A"}}},
	} {
		if err := plan.refuseShadowedOverride(); err != nil {
			t.Errorf("plan %+v refused: %v", plan, err)
		}
	}
}
