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

package linstormigrate

import (
	"strings"
	"testing"
)

// authDump is one DRBD definition with a secret and two replicas, plus
// whatever props the case adds.
func authDump(props ...PropsContainerRow) *Dump {
	return &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
			{NodeName: "NODE-B", NodeDspName: "node-b", NodeType: 2, UUID: "n-b"},
		},
		ResourceGroups: []ResourceGroupRow{
			{ResourceGroupName: "SC", ResourceGroupDspName: "sc", UUID: "rg"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-AUTH", ResourceDspName: "pvc-auth", ResourceGroupName: "SC", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-AUTH", VlmNr: 0, VlmSize: 1048576, UUID: "vd"},
		},
		Resources: []ResourceRow{
			{NodeName: "NODE-A", ResourceName: "PVC-AUTH", UUID: "r-a"},
			{NodeName: "NODE-B", ResourceName: "PVC-AUTH", UUID: "r-b"},
		},
		LayerDrbdResourceDefinitions: []LayerDrbdResourceDefinitionRow{
			{ResourceName: "PVC-AUTH", Secret: "fixture-secret-auth"},
		},
		PropsContainers: props,
	}
}

// TestSharedSecretFollowsWhatLinstorRendered: LINSTOR writes the
// cram-hmac-alg / shared-secret pair only for a replica whose
// auto-verify-alg resolves to something other than windrbd. blockstor
// writes cram-hmac-alg only when it is set, so the converter carries the
// secret and stamps the digest exactly where LINSTOR's kernels
// authenticate, and adoption changes nothing.
func TestSharedSecretFollowsWhatLinstorRendered(t *testing.T) {
	const avaKey = "DrbdOptions/auto-verify-alg"

	for _, tc := range []struct {
		name    string
		props   []PropsContainerRow
		carried bool
		warning string
	}{
		{
			name:    "set on the definition",
			props:   []PropsContainerRow{{PropsInstance: "/RSC_DFNS/PVC-AUTH", PropKey: avaKey, PropValue: "crc32c"}},
			carried: true,
		},
		{
			name:    "set in the satellite bag",
			props:   []PropsContainerRow{{PropsInstance: PropsInstanceSatellite, PropKey: avaKey, PropValue: "sha256"}},
			carried: true,
		},
		{
			name:    "set on the group",
			props:   []PropsContainerRow{{PropsInstance: "/RSC_GRPS/SC", PropKey: avaKey, PropValue: "sha256"}},
			carried: true,
		},
		{
			name:    "not set anywhere",
			carried: false,
			warning: "pvc-auth: LINSTOR rendered no peer authentication for it",
		},
		{
			name:    "windrbd",
			props:   []PropsContainerRow{{PropsInstance: "/RSC_DFNS/PVC-AUTH", PropKey: avaKey, PropValue: "windrbd"}},
			carried: false,
			warning: "pvc-auth: LINSTOR rendered no peer authentication for it",
		},
		{
			// A closer scope wins: the replica's windrbd on node-b
			// shadows the definition's value there only.
			name: "one replica overrides it",
			props: []PropsContainerRow{
				{PropsInstance: "/RSC_DFNS/PVC-AUTH", PropKey: avaKey, PropValue: "crc32c"},
				{PropsInstance: "/RSCS/NODE-B/PVC-AUTH", PropKey: avaKey, PropValue: "windrbd"},
			},
			carried: true,
			warning: "pvc-auth: LINSTOR authenticated peers on 1 of 2 replicas",
		},
		{
			name:    "set on one node only",
			props:   []PropsContainerRow{{PropsInstance: "/NODES/NODE-A", PropKey: avaKey, PropValue: "crc32c"}},
			carried: true,
			warning: "pvc-auth: LINSTOR authenticated peers on 1 of 2 replicas",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := convertForTest(t, authDump(tc.props...))
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}

			rd := findRD(t, res, "pvc-auth")

			_, carried := rd.Spec.ExtraProps[drbdSharedSecretProp]
			if carried != tc.carried {
				t.Errorf("shared secret carried = %v, want %v", carried, tc.carried)
			}

			alg, stamped := rd.Spec.ExtraProps[drbdCramHMACAlgProp]
			if stamped != tc.carried || (stamped && alg != "sha1") {
				t.Errorf("cram-hmac-alg = %q (set %v), want sha1 only where the secret is carried", alg, stamped)
			}

			if tc.warning != "" && !hasWarning(res, tc.warning) {
				t.Errorf("missing %q; warnings: %v", tc.warning, res.Warnings)
			}

			if tc.warning == "" && hasWarning(res, "peer authentication") {
				t.Errorf("unexpected authentication warning; warnings: %v", res.Warnings)
			}
		})
	}
}

// cramHMACAlgAnywhere lists every converted object that names a
// cram-hmac-alg, at any scope blockstor resolves DRBD options from.
func cramHMACAlgAnywhere(res *Result) []string {
	var found []string

	scan := func(what string, bags ...map[string]string) {
		for _, bag := range bags {
			if hasKeyFold(bag, drbdCramHMACAlgProp) {
				found = append(found, what)
			}
		}
	}

	if res.ControllerConfig != nil {
		scan("ControllerConfig", res.ControllerConfig.Spec.ExtraProps)
	}

	for i := range res.ResourceGroups {
		rg := &res.ResourceGroups[i]
		scan("ResourceGroup "+rg.Name, rg.Spec.ExtraProps, rg.Spec.Props)
	}

	for i := range res.ResourceDefinitions {
		rd := &res.ResourceDefinitions[i]
		scan("ResourceDefinition "+rd.Name, rd.Spec.ExtraProps, rd.Spec.Props)
	}

	for i := range res.Resources {
		r := &res.Resources[i]
		scan("Resource "+r.Name, r.Spec.ExtraProps, r.Spec.Props)
	}

	return found
}

// TestAUserSecretDoesNotTurnAuthenticationOn: LINSTOR lets an operator
// set DrbdOptions/Net/shared-secret, and where auto-verify-alg does not
// resolve it renders that key without cram-hmac-alg, so the peers run
// unauthenticated. The converter carries the prop as it is and must not
// add the digest, or adoption would switch authentication on one node at
// a time.
func TestAUserSecretDoesNotTurnAuthenticationOn(t *testing.T) {
	for _, instance := range []string{"/RSC_DFNS/PVC-AUTH", "/RSC_GRPS/SC", PropsInstanceSatellite} {
		t.Run(instance, func(t *testing.T) {
			res, err := convertForTest(t, authDump(PropsContainerRow{
				PropsInstance: instance, PropKey: drbdSharedSecretProp, PropValue: "operator-secret",
			}))
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}

			if found := cramHMACAlgAnywhere(res); len(found) > 0 {
				t.Errorf("cram-hmac-alg written for a definition LINSTOR did not authenticate: %s", strings.Join(found, ", "))
			}
		})
	}
}

// TestADefinitionsOwnCramHMACAlgIsKept: a digest the definition names
// itself is carried with its props and is not replaced by sha1.
func TestADefinitionsOwnCramHMACAlgIsKept(t *testing.T) {
	res, err := convertForTest(t, authDump(
		PropsContainerRow{PropsInstance: "/RSC_DFNS/PVC-AUTH", PropKey: "DrbdOptions/auto-verify-alg", PropValue: "crc32c"},
		PropsContainerRow{PropsInstance: "/RSC_DFNS/PVC-AUTH", PropKey: drbdCramHMACAlgProp, PropValue: "sha256"},
	))
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	rd := findRD(t, res, "pvc-auth")

	if got := rd.Spec.ExtraProps[drbdCramHMACAlgProp]; got != "sha256" {
		t.Errorf("cram-hmac-alg = %q, want the definition's sha256; extraProps: %v", got, rd.Spec.ExtraProps)
	}
}
