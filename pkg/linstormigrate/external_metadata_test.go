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
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// A replica whose DRBD metadata lives on a separate volume is never adopted:
// blockstor would bring it up with internal metadata and write it over the
// end of the data. The definition and all its replicas stay behind, and the
// report says which replica and why. The evidence is a ".meta" layer child,
// a meta pool on a DRBD volume, or, for a replica the dump has no chain
// for, StorPoolNameDrbdMeta in any scope LINSTOR reads it from.
func TestADefinitionWithExternalMetadataIsSkipped(t *testing.T) {
	t.Parallel()

	meta := LayerResourceIDRow{
		LayerResourceID: 20, LayerResourceKind: "STORAGE", LayerResourceParentID: ptr(int32(1)),
		LayerResourceSuffix: ".meta", NodeName: "NODE-A", ResourceName: "PVC-S",
	}
	chained := func() *Dump {
		return stackDump("[]", layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE"))
	}
	// The definition names its own stack, so #223 lets replicas with no
	// chain through, and only the property can tell where their metadata is.
	unchained := func(instance string) *Dump {
		return withProps(stackDump(`["DRBD","STORAGE"]`),
			PropsContainerRow{PropsInstance: instance, PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"})
	}

	for name, tc := range map[string]struct {
		dump *Dump
		want string
	}{
		"meta layer": {
			dump: stackDump("[]", append(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"), meta), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE")),
			want: "the replica on node-a keeps its DRBD metadata on a separate volume",
		},
		"meta pool on a DRBD volume": {
			dump: func() *Dump {
				d := chained()
				d.LayerDrbdVolumes = []LayerDrbdVolumeRow{{LayerResourceID: 10, VlmNr: 0, NodeName: "NODE-B", PoolName: "meta"}}

				return d
			}(),
			want: "the replica on node-b keeps its DRBD metadata on a separate volume",
		},
		"no chain, pool on the volume definition": {dump: unchained("/VLM_DFNS/PVC-S/0"), want: "on volume definition pvc-s/0"},
		"no chain, pool on the volume group":      {dump: unchained("/VLM_GRPS/RG/0"), want: "on volume group RG/0"},
		"no chain, pool on the replica":           {dump: unchained("/RSCS/NODE-A/PVC-S"), want: "on the replica on node-a"},
		"no chain, pool on the definition":        {dump: unchained("/RSC_DFNS/PVC-S"), want: "on the definition"},
		"no chain, pool on the resource group":    {dump: unchained("/RSC_GRPS/RG"), want: "on resource group RG"},
		"no chain, pool on the node":              {dump: unchained("/NODES/NODE-B"), want: "on node node-b"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := convertTwoReplicas(t, tc.dump)

			if len(res.ResourceDefinitions) != 0 || len(res.Resources) != 0 {
				t.Fatalf("converted %d definitions and %d replicas, want none; warnings %v",
					len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
			}

			if !hasWarning(res, tc.want) {
				t.Errorf("the skip does not say %q; warnings %v", tc.want, res.Warnings)
			}

			if !hasWarning(res, "copy its data to a volume with internal metadata") {
				t.Errorf("the skip does not say how to move the data; warnings %v", res.Warnings)
			}
		})
	}
}

// StorPoolNameDrbdMeta on replicas whose chains have no ".meta" child does
// not move their metadata: LINSTOR applies it to replicas created later, and
// ".internal" asks for internal metadata anyway. The definition converts,
// the property is left off the definition and its replicas, the scopes the
// dispatcher reads it from, and the report says the metadata stays where it
// is.
func TestAMetaPoolOverInternalMetadataConvertsWithoutIt(t *testing.T) {
	t.Parallel()

	for name, prop := range map[string]PropsContainerRow{
		"pool on the volume definition": {PropsInstance: "/VLM_DFNS/PVC-S/0", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"},
		"pool on the volume group":      {PropsInstance: "/VLM_GRPS/RG/0", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"},
		"pool on a replica":             {PropsInstance: "/RSCS/NODE-B/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"},
		"pool on the definition":        {PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"},
		"pool on the resource group":    {PropsInstance: "/RSC_GRPS/RG", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"},
		"pool on a node":                {PropsInstance: "/NODES/NODE-A", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"},
		"internal on the definition":    {PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: ".internal"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := convertTwoReplicas(t, withProps(stackDump("[]",
				layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE")), prop))

			if len(res.ResourceDefinitions) != 1 || len(res.Resources) != 2 {
				t.Fatalf("converted %d definitions and %d replicas, want the definition and both replicas; warnings %v",
					len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
			}

			if v, ok := res.ResourceDefinitions[0].Spec.Props[storPoolNameDrbdMeta]; ok {
				t.Errorf("the definition carries %s %q", storPoolNameDrbdMeta, v)
			}

			for i := range res.Resources {
				if v, ok := res.Resources[i].Spec.Props[storPoolNameDrbdMeta]; ok {
					t.Errorf("replica %s carries %s %q", res.Resources[i].Name, storPoolNameDrbdMeta, v)
				}
			}

			if !hasWarning(res, "keep their DRBD metadata on the data device") || !hasWarning(res, "not carried over") {
				t.Errorf("the report does not say the metadata stays internal; warnings %v", res.Warnings)
			}

			if hasWarning(res, "separate volume") || hasWarning(res, "copy its data") || hasWarning(res, "skipped") {
				t.Errorf("the report says the definition was skipped or the metadata is external; warnings %v", res.Warnings)
			}
		})
	}
}

// LINSTOR resolves the meta pool per volume, and matches ".internal" in any
// case but untrimmed: spaces around it make it a pool name. A replica without
// a chain is external when any of its volumes resolves to a separate pool,
// and internal when every one asks for internal metadata.
func TestAMetaPoolIsResolvedPerVolumeAsLINSTORReadsIt(t *testing.T) {
	t.Parallel()

	twoVolumes := func(props ...PropsContainerRow) *Dump {
		d := withProps(stackDump(`["DRBD","STORAGE"]`), props...)
		d.VolumeDefinitions = append(d.VolumeDefinitions, VolumeDefinitionRow{ResourceName: "PVC-S", VlmNr: 1, VlmSize: 1048576, UUID: "vd-s1"})

		return d
	}

	t.Run("internal on volume 0, a pool on the definition", func(t *testing.T) {
		t.Parallel()

		res := convertTwoReplicas(t, twoVolumes(
			PropsContainerRow{PropsInstance: "/VLM_DFNS/PVC-S/0", PropKey: "StorPoolNameDrbdMeta", PropValue: ".internal"},
			PropsContainerRow{PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"}))

		if len(res.ResourceDefinitions) != 0 {
			t.Fatalf("converted %d definitions, want none: volume 1 keeps its metadata on meta; warnings %v",
				len(res.ResourceDefinitions), res.Warnings)
		}

		if !hasWarning(res, "meta on the definition would put its DRBD metadata on a separate volume") {
			t.Errorf("the skip does not name the pool volume 1 resolves to; warnings %v", res.Warnings)
		}
	})

	t.Run("internal in another case", func(t *testing.T) {
		t.Parallel()

		res := convertTwoReplicas(t, twoVolumes(
			PropsContainerRow{PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: ".INTERNAL"}))

		if len(res.ResourceDefinitions) != 1 || len(res.Resources) != 2 {
			t.Fatalf("converted %d definitions and %d replicas, want the definition and both replicas; warnings %v",
				len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
		}
	})

	t.Run("internal with spaces around it is a pool name", func(t *testing.T) {
		t.Parallel()

		res := convertTwoReplicas(t, twoVolumes(
			PropsContainerRow{PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: " .INTERNAL "}))

		if len(res.ResourceDefinitions) != 0 {
			t.Fatalf("converted %d definitions, want none: LINSTOR reads %q as a pool; warnings %v",
				len(res.ResourceDefinitions), " .INTERNAL ", res.Warnings)
		}
	})

	t.Run("a blank value is unset", func(t *testing.T) {
		t.Parallel()

		res := convertTwoReplicas(t, twoVolumes(
			PropsContainerRow{PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "  "}))

		if len(res.ResourceDefinitions) != 1 || len(res.Resources) != 2 {
			t.Fatalf("converted %d definitions and %d replicas, want the definition and both replicas; warnings %v",
				len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
		}
	})
}

// Rows that do not convert say nothing about where a live replica keeps its
// metadata: a diskless replica, a replica marked DELETE and a snapshot's
// rows, whatever their layout or property.
func TestRowsThatDoNotConvertDoNotDecideTheMetadata(t *testing.T) {
	t.Parallel()

	meta := func(node string, id int32) LayerResourceIDRow {
		return LayerResourceIDRow{
			LayerResourceID: id + 1, LayerResourceKind: "STORAGE", LayerResourceParentID: ptr(id),
			LayerResourceSuffix: ".meta", NodeName: node, ResourceName: "PVC-S",
		}
	}
	// The definition names its stack, so a replica without a chain is let
	// through by #223 and only the metadata check can stop it.
	named := func(chains ...[]LayerResourceIDRow) *Dump { return stackDump(`["DRBD","STORAGE"]`, chains...) }

	for name, tc := range map[string]struct {
		dump     *Dump
		replicas []ResourceRow
	}{
		"diskless replica with a pool and no chain": {
			dump: withProps(named(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE")),
				PropsContainerRow{PropsInstance: "/RSCS/NODE-B/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"}),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", ResourceFlags: resourceFlagDiskless, UUID: "r-b"},
			},
		},
		"snapshot placement with a pool and no chain": {
			dump: withProps(named(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE")),
				PropsContainerRow{PropsInstance: "/RSCS/NODE-B/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"}),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", SnapshotName: "SNAP", UUID: "s-b"},
			},
		},
		"snapshot volume definition with a pool": {
			dump: func() *Dump {
				d := withProps(named(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE")),
					PropsContainerRow{PropsInstance: "/VLM_DFNS/PVC-S/7", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"})
				d.VolumeDefinitions = append(d.VolumeDefinitions,
					VolumeDefinitionRow{ResourceName: "PVC-S", SnapshotName: "SNAP", VlmNr: 7, VlmSize: 1048576, UUID: "svd"})

				return d
			}(),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", UUID: "r-b"},
			},
		},
		"snapshot layer row with a meta child": {
			dump: func() *Dump {
				d := named(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE"))
				m := meta("NODE-A", 40)
				m.SnapshotName = "SNAP"
				d.LayerResourceIDs = append(d.LayerResourceIDs, m)

				return d
			}(),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", UUID: "r-b"},
			},
		},
		"meta child on a replica marked DELETE": {
			dump: named(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"),
				append(layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE"), meta("NODE-B", 10))),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", ResourceFlags: resourceFlagDelete, UUID: "r-b"},
			},
		},
		"internal pool and no chain": {
			dump: withProps(named(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE")),
				PropsContainerRow{PropsInstance: "/RSCS/NODE-B/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: ".internal"}),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", UUID: "r-b"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tc.dump.Resources = tc.replicas

			res, err := Convert(tc.dump)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}

			if len(res.ResourceDefinitions) != 1 {
				t.Fatalf("converted %d definitions, want pvc-s; warnings %v", len(res.ResourceDefinitions), res.Warnings)
			}

			if hasWarning(res, "separate volume") {
				t.Errorf("the metadata was reported external; warnings %v", res.Warnings)
			}
		})
	}
}

// The meta pool of a DRBD volume loads from the dump as kubectl prints it,
// a null pool included.
func TestTheDRBDVolumeMetaPoolLoadsFromTheDump(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	body := `{"items":[` +
		`{"spec":{"layer_resource_id":10,"vlm_nr":0,"node_name":"NODE-B","pool_name":"meta"}},` +
		`{"spec":{"layer_resource_id":11,"vlm_nr":0,"node_name":null,"pool_name":null}}]}`

	for table, data := range map[string]string{
		"layerdrbdvolumes": body,
		"nodes":            `{"items":[{"spec":{"node_name":"NODE-B","node_dsp_name":"node-b","node_type":2,"uuid":"n-b"}}]}`,
	} {
		err := os.WriteFile(filepath.Join(dir, table+dumpFileSuffix), []byte(data), 0o600)
		if err != nil {
			t.Fatal(err)
		}
	}

	dump, err := LoadDump(dir)
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	want := []LayerDrbdVolumeRow{{LayerResourceID: 10, NodeName: "NODE-B", PoolName: "meta"}, {LayerResourceID: 11}}
	if !slices.Equal(dump.LayerDrbdVolumes, want) {
		t.Fatalf("loaded %+v, want %+v", dump.LayerDrbdVolumes, want)
	}
}

// One definition's metadata says nothing about another's: a neighbour on
// other nodes with internal metadata still converts.
func TestExternalMetadataSkipsOnlyItsOwnDefinition(t *testing.T) {
	t.Parallel()

	meta := LayerResourceIDRow{
		LayerResourceID: 20, LayerResourceKind: "STORAGE", LayerResourceParentID: ptr(int32(1)),
		LayerResourceSuffix: ".meta", NodeName: "NODE-A", ResourceName: "PVC-S",
	}

	for name, dump := range map[string]*Dump{
		"meta layer on the other definition": stackDump("[]",
			append(layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"), meta), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE")),
		// PVC-T has no replica on NODE-B, so the node's property is not its.
		// PVC-S names its stack and has no chain on NODE-B, so the property
		// decides for it.
		"meta pool on a node the other definition is not on": withProps(stackDump(`["DRBD","STORAGE"]`,
			layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE")),
			PropsContainerRow{PropsInstance: "/NODES/NODE-B", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dump.ResourceDefinitions = append(dump.ResourceDefinitions,
				ResourceDefinitionRow{ResourceName: "PVC-T", ResourceDspName: "pvc-t", ResourceGroupName: "RG", LayerStack: "[]", UUID: "rd-t"})
			dump.VolumeDefinitions = append(dump.VolumeDefinitions, VolumeDefinitionRow{ResourceName: "PVC-T", VlmNr: 0, VlmSize: 1048576, UUID: "vd-t"})
			dump.LayerResourceIDs = append(dump.LayerResourceIDs, layerChain(30, "NODE-A", "PVC-T", "DRBD", "STORAGE")...)
			dump.Resources = []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", UUID: "r-b"},
				{NodeName: "NODE-A", ResourceName: "PVC-T", UUID: "r-t"},
			}

			res, err := Convert(dump)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}

			if len(res.ResourceDefinitions) != 1 || res.ResourceDefinitions[0].Name != "pvc-t" || len(res.Resources) != 1 {
				t.Fatalf("converted %d definitions and %d replicas, want pvc-t and its replica; warnings %v",
					len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
			}
		})
	}
}

// convertTwoReplicas places a replica of PVC-S on each node of dump and
// converts it.
func convertTwoReplicas(t *testing.T, dump *Dump) *Result {
	t.Helper()

	dump.Resources = []ResourceRow{
		{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
		{NodeName: "NODE-B", ResourceName: "PVC-S", UUID: "r-b"},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	return res
}

func withProps(dump *Dump, rows ...PropsContainerRow) *Dump {
	dump.PropsContainers = append(dump.PropsContainers, rows...)

	return dump
}

// A meta pool says nothing about a replica that is going away or a
// definition with no DRBD layer: neither keeps DRBD metadata, so the
// definition converts.
func TestAMetaPoolWithoutDRBDMetadataBehindItConverts(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		dump     *Dump
		replicas []ResourceRow
	}{
		"on a replica marked DELETE": {
			// NODE-B has no chain, so without the DELETE filter its property
			// would decide alone.
			dump: withProps(stackDump(`["DRBD","STORAGE"]`, layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE")),
				PropsContainerRow{PropsInstance: "/RSCS/NODE-B/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"}),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", ResourceFlags: resourceFlagDelete, UUID: "r-b"},
			},
		},
		"on a definition without DRBD": {
			dump: withProps(stackDump(`["STORAGE"]`),
				PropsContainerRow{PropsInstance: "/RSC_GRPS/RG", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"}),
			replicas: []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tc.dump.Resources = tc.replicas

			res, err := Convert(tc.dump)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}

			if len(res.ResourceDefinitions) != 1 || len(res.Resources) != 1 {
				t.Fatalf("converted %d definitions and %d replicas, want the definition and its live replica; warnings %v",
					len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
			}

			if hasWarning(res, storPoolNameDrbdMeta) {
				t.Errorf("the meta pool was reported; warnings %v", res.Warnings)
			}
		})
	}
}
