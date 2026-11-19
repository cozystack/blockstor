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
	"testing"
)

// TestSatellitePropsReachControllerConfig: `linstor controller
// set-property DrbdOptions/Net/ping-int …`, and piraeus-operator's
// LinstorCluster.spec.properties with it, lands in /STLT, not /CTRL. The
// satellites render every .res from that bag, so leaving it behind takes
// the option off every adopted resource at once.
func TestSatellitePropsReachControllerConfig(t *testing.T) {
	res := convertFixture(t)

	cc := res.ControllerConfig
	if cc == nil {
		t.Fatal("ControllerConfig not emitted")
	}

	for key, want := range map[string]string{
		"DrbdOptions/Net/connect-int":  "15",
		"DrbdOptions/Net/ping-int":     "12",
		"DrbdOptions/Net/ping-timeout": "10",
		"DrbdOptions/Net/timeout":      "90",
		"DrbdOptions/Net/verify-alg":   "crc32c",
		// /CTRL keeps carrying over next to it.
		"DrbdOptions/auto-add-quorum-tiebreaker": "True",
	} {
		if got := cc.Spec.ExtraProps[key]; got != want {
			t.Errorf("%s = %q, want %q; extraProps: %v", key, got, want, cc.Spec.ExtraProps)
		}
	}

	if _, ok := cc.Spec.ExtraProps["Cluster/LocalID"]; ok {
		t.Error("/STLT Cluster/LocalID leaked into ControllerConfig")
	}
}

// TestControllerAndSatelliteDisagreement: LINSTOR files some keys in both
// bags. Where they differ, the value kept is the one LINSTOR acted on —
// the satellite's for a .res option, the controller's for a controller
// knob — and the other is reported, not dropped quietly.
func TestControllerAndSatelliteDisagreement(t *testing.T) {
	dump := &Dump{
		PropsContainers: []PropsContainerRow{
			{PropsInstance: PropsInstanceController, PropKey: "DrbdOptions/Net/max-buffers", PropValue: "8000"},
			{PropsInstance: PropsInstanceSatellite, PropKey: "DrbdOptions/Net/max-buffers", PropValue: "10000"},
			{PropsInstance: PropsInstanceController, PropKey: "DrbdOptions/auto-verify-algo-disable", PropValue: "true"},
			{PropsInstance: PropsInstanceSatellite, PropKey: "DrbdOptions/auto-verify-algo-disable", PropValue: "false"},
			{PropsInstance: PropsInstanceController, PropKey: "DrbdOptions/Net/ko-count", PropValue: "7"},
			{PropsInstance: PropsInstanceSatellite, PropKey: "DrbdOptions/Net/ko-count", PropValue: "7"},
		},
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	cc := res.ControllerConfig
	if cc == nil {
		t.Fatal("ControllerConfig not emitted")
	}

	// max-buffers is typed; a disagreement on it must still resolve to
	// the value the satellites rendered.
	if cc.Spec.DRBDOptions == nil || cc.Spec.DRBDOptions.Net == nil || cc.Spec.DRBDOptions.Net.MaxBuffers == nil ||
		*cc.Spec.DRBDOptions.Net.MaxBuffers != 10000 {
		t.Errorf("max-buffers did not keep the /STLT value: %+v", cc.Spec.DRBDOptions)
	}

	if got := cc.Spec.ExtraProps["DrbdOptions/auto-verify-algo-disable"]; got != "true" {
		t.Errorf("auto-verify-algo-disable = %q, want the /CTRL value true", got)
	}

	if got := cc.Spec.ExtraProps["DrbdOptions/Net/ko-count"]; got != "7" {
		t.Errorf("ko-count = %q, want 7", got)
	}

	for _, want := range []string{
		`DrbdOptions/Net/max-buffers: /CTRL and /STLT disagree ("8000" vs "10000") — kept the /STLT value`,
		`DrbdOptions/auto-verify-algo-disable: /CTRL and /STLT disagree ("true" vs "false") — kept the /CTRL value`,
	} {
		if !hasWarning(res, want) {
			t.Errorf("missing %q; warnings: %v", want, res.Warnings)
		}
	}

	if hasWarning(res, "ko-count") {
		t.Errorf("agreeing values were reported; warnings: %v", res.Warnings)
	}
}

// TestSparseFlagAtSatelliteScopeIsSeen: `linstor controller set-property
// StorDriver/ZfscreateOptions -s` is stored in /STLT, which is the bag the
// satellite creates zvols from.
func TestSparseFlagAtSatelliteScopeIsSeen(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "CLUSTERWIDE", DriverName: "ZFS", UUID: "sp-1"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: PropsInstanceSatellite, PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.StoragePools) != 1 || res.StoragePools[0].Spec.ProviderKind != "ZFS_THIN" {
		t.Fatalf("a satellite-scope sparse flag was not seen: %+v", res.StoragePools)
	}
}
