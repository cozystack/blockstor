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

import "testing"

// peerSlotsDump is a two-replica DRBD definition whose DRBD layer has
// definition slots rdSlots (0: no DRBD layer row) and whose replicas were
// created with replicaSlots.
func peerSlotsDump(rdSlots, replicaSlots int32) *Dump {
	dump := stackDump(`["DRBD","STORAGE"]`,
		layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE"))

	if rdSlots != 0 {
		dump.LayerDrbdResourceDefinitions = []LayerDrbdResourceDefinitionRow{
			{ResourceName: "PVC-S", PeerSlots: rdSlots, AlStripes: 1, AlStripeSize: 32, TransportType: "IP"},
		}
	}

	// Each replica row carries its port, as a dump read at the storage
	// version does; one without would be refused as a pruned dump.
	dump.LayerDrbdResources = []LayerDrbdResourceRow{
		{LayerResourceID: 1, NodeID: 0, PeerSlots: replicaSlots, AlStripes: 1, AlStripeSize: 32, TCPPortList: ptr("[7000]")},
		{LayerResourceID: 10, NodeID: 1, PeerSlots: replicaSlots, AlStripes: 1, AlStripeSize: 32, TCPPortList: ptr("[7000]")},
	}

	return dump
}

// An adopted definition keeps the peer slots its DRBD layer has, and each
// adopted replica the slots its metadata was created with, so a new
// replica is created like LINSTOR's and a grow sizes each lower disk for
// the metadata on it. Without them the definition and its replicas would
// read as the 15 of a definition blockstor created before it recorded a
// count.
func TestAdoptedDefinitionKeepsItsPeerSlots(t *testing.T) {
	t.Parallel()

	res := convertTwoReplicas(t, peerSlotsDump(12, 9))

	if len(res.ResourceDefinitions) != 1 || len(res.Resources) != 2 {
		t.Fatalf("converted %d definitions and %d replicas; warnings %v",
			len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
	}

	if got := res.ResourceDefinitions[0].Spec.PeerSlots; got == nil || *got != 12 {
		t.Errorf("definition peer slots: %v, want the DRBD layer's 12", got)
	}

	for i := range res.Resources {
		if got := res.Resources[i].Spec.PeerSlots; got == nil || *got != 9 {
			t.Errorf("replica %s peer slots: %v, want the 9 its metadata has", res.Resources[i].Name, got)
		}
	}
}

// A definition LINSTOR never created a DRBD layer for gets what LINSTOR
// would give it when it does: PeerSlotsNewResource on the definition, its
// group and the controller, else 7.
func TestADefinitionWithoutADRBDLayerResolvesItsPeerSlots(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		props []PropsContainerRow
		want  int32
	}{
		"default": {want: 7},
		"controller": {
			props: []PropsContainerRow{{PropsInstance: PropsInstanceController, PropKey: "PeerSlotsNewResource", PropValue: "5"}},
			want:  5,
		},
		"group over controller": {
			props: []PropsContainerRow{
				{PropsInstance: PropsInstanceController, PropKey: "PeerSlotsNewResource", PropValue: "5"},
				{PropsInstance: "/RSC_GRPS/RG", PropKey: "PeerSlotsNewResource", PropValue: "6"},
			},
			want: 6,
		},
		"definition over group": {
			props: []PropsContainerRow{
				{PropsInstance: "/RSC_GRPS/RG", PropKey: "PeerSlotsNewResource", PropValue: "6"},
				{PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "PeerSlotsNewResource", PropValue: "4"},
			},
			want: 4,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := convertTwoReplicas(t, withProps(peerSlotsDump(0, 7), tc.props...))

			if len(res.ResourceDefinitions) != 1 {
				t.Fatalf("converted %d definitions; warnings %v", len(res.ResourceDefinitions), res.Warnings)
			}

			if got := res.ResourceDefinitions[0].Spec.PeerSlots; got == nil || *got != tc.want {
				t.Errorf("definition peer slots: %v, want %d", got, tc.want)
			}
		})
	}
}

// A group's `--peer-slots` is carried, so definitions spawned from it
// after the migration get it, as they would have in LINSTOR.
func TestAGroupKeepsItsPeerSlots(t *testing.T) {
	t.Parallel()

	dump := peerSlotsDump(7, 7)
	dump.ResourceGroups[0].PeerSlots = ptr(int32(11))

	res := convertTwoReplicas(t, dump)

	if len(res.ResourceGroups) != 1 || res.ResourceGroups[0].Spec.PeerSlots != 11 {
		t.Fatalf("group: %+v; warnings %v", res.ResourceGroups, res.Warnings)
	}
}
