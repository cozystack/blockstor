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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
)

// dumpFileSuffix is the file-name suffix each table dump carries:
// `<plural>.internal.linstor.linbit.com.json`.
const dumpFileSuffix = ".internal.linstor.linbit.com.json"

// ErrEmptyDump is returned when the input directory contains none of
// the expected `*.internal.linstor.linbit.com.json` table files.
var ErrEmptyDump = errors.New("no LINSTOR table dumps found")

// listEnvelope is the kubectl `-ojson` List wrapper. Each item's spec
// is one SQL row. Its apiVersion is the version the API server served
// the row at, which decides the fields it kept; the rest is transport.
type listEnvelope struct {
	Items []struct {
		APIVersion string          `json:"apiVersion"`
		Spec       json.RawMessage `json:"spec"`
	} `json:"items"`
}

// loadTable reads `<dir>/<table>.internal.linstor.linbit.com.json` and
// decodes every item's spec into out (a pointer to a slice of row
// structs), and records the apiVersions the rows were read at in
// versions. A missing file is not an error — older LINSTOR schemas
// lack some tables and empty tables may be omitted from a dump.
func loadTable[T any](dir, table string, out *[]T, versions map[string][]string) error {
	path := filepath.Join(dir, table+dumpFileSuffix)

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("read %s: %w", path, err)
	}

	var list listEnvelope

	err = json.Unmarshal(data, &list)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}

	rows := make([]T, 0, len(list.Items))
	seen := map[string]bool{}

	for i, item := range list.Items {
		if item.APIVersion != "" && !seen[item.APIVersion] {
			seen[item.APIVersion] = true
			versions[table] = append(versions[table], item.APIVersion)
		}

		var row T

		err = json.Unmarshal(item.Spec, &row)
		if err != nil {
			return fmt.Errorf("parse %s item %d spec: %w", path, i, err)
		}

		rows = append(rows, row)
	}

	slices.Sort(versions[table])

	*out = rows

	return nil
}

// LoadDump reads every table the converter consumes from dir.
func LoadDump(dir string) (*Dump, error) {
	dump := &Dump{APIVersions: map[string][]string{}}
	versions := dump.APIVersions

	err := errors.Join(
		loadTable(dir, "nodes", &dump.Nodes, versions),
		loadTable(dir, "nodenetinterfaces", &dump.NodeNetInterfaces, versions),
		loadTable(dir, "nodestorpool", &dump.NodeStorPools, versions),
		loadTable(dir, "storpooldefinitions", &dump.StorPoolDefinitions, versions),
		loadTable(dir, "resourcegroups", &dump.ResourceGroups, versions),
		loadTable(dir, "volumegroups", &dump.VolumeGroups, versions),
		loadTable(dir, "resourcedefinitions", &dump.ResourceDefinitions, versions),
		loadTable(dir, "volumedefinitions", &dump.VolumeDefinitions, versions),
		loadTable(dir, "resources", &dump.Resources, versions),
		loadTable(dir, "volumes", &dump.Volumes, versions),
		loadTable(dir, "propscontainers", &dump.PropsContainers, versions),
		loadTable(dir, "layerresourceids", &dump.LayerResourceIDs, versions),
		loadTable(dir, "layerdrbdresourcedefinitions", &dump.LayerDrbdResourceDefinitions, versions),
		loadTable(dir, "layerdrbdresources", &dump.LayerDrbdResources, versions),
		loadTable(dir, "layerdrbdvolumedefinitions", &dump.LayerDrbdVolumeDefinitions, versions),
		loadTable(dir, "layerdrbdvolumes", &dump.LayerDrbdVolumes, versions),
		loadTable(dir, "layerstoragevolumes", &dump.LayerStorageVolumes, versions),
		loadTable(dir, "layerluksvolumes", &dump.LayerLuksVolumes, versions),
		loadTable(dir, "linstorremotes", &dump.LinstorRemotes, versions),
	)
	if err != nil {
		return nil, err
	}

	if len(dump.Nodes) == 0 && len(dump.ResourceDefinitions) == 0 {
		return nil, fmt.Errorf("%w in %s (expected *%s files)", ErrEmptyDump, dir, dumpFileSuffix)
	}

	return dump, nil
}
