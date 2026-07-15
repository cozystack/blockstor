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
)

// dumpFileSuffix is the file-name suffix each table dump carries:
// `<plural>.internal.linstor.linbit.com.json`.
const dumpFileSuffix = ".internal.linstor.linbit.com.json"

// ErrEmptyDump is returned when the input directory contains none of
// the expected `*.internal.linstor.linbit.com.json` table files.
var ErrEmptyDump = errors.New("no LINSTOR table dumps found")

// listEnvelope is the kubectl `-ojson` List wrapper. Each item's spec
// is one SQL row; everything else (metadata, apiVersion) is transport.
type listEnvelope struct {
	Items []struct {
		Spec json.RawMessage `json:"spec"`
	} `json:"items"`
}

// loadTable reads `<dir>/<table>.internal.linstor.linbit.com.json` and
// decodes every item's spec into out (a pointer to a slice of row
// structs). A missing file is not an error — older LINSTOR schemas
// lack some tables and empty tables may be omitted from a dump.
func loadTable[T any](dir, table string, out *[]T) error {
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

	for i, item := range list.Items {
		var row T

		err = json.Unmarshal(item.Spec, &row)
		if err != nil {
			return fmt.Errorf("parse %s item %d spec: %w", path, i, err)
		}

		rows = append(rows, row)
	}

	*out = rows

	return nil
}

// LoadDump reads every table the converter consumes from dir.
func LoadDump(dir string) (*Dump, error) {
	dump := &Dump{}

	err := errors.Join(
		loadTable(dir, "nodes", &dump.Nodes),
		loadTable(dir, "nodenetinterfaces", &dump.NodeNetInterfaces),
		loadTable(dir, "nodestorpool", &dump.NodeStorPools),
		loadTable(dir, "storpooldefinitions", &dump.StorPoolDefinitions),
		loadTable(dir, "resourcegroups", &dump.ResourceGroups),
		loadTable(dir, "volumegroups", &dump.VolumeGroups),
		loadTable(dir, "resourcedefinitions", &dump.ResourceDefinitions),
		loadTable(dir, "volumedefinitions", &dump.VolumeDefinitions),
		loadTable(dir, "resources", &dump.Resources),
		loadTable(dir, "volumes", &dump.Volumes),
		loadTable(dir, "propscontainers", &dump.PropsContainers),
		loadTable(dir, "layerresourceids", &dump.LayerResourceIDs),
		loadTable(dir, "layerdrbdresourcedefinitions", &dump.LayerDrbdResourceDefinitions),
		loadTable(dir, "layerdrbdresources", &dump.LayerDrbdResources),
		loadTable(dir, "layerdrbdvolumedefinitions", &dump.LayerDrbdVolumeDefinitions),
		loadTable(dir, "layerdrbdvolumes", &dump.LayerDrbdVolumes),
		loadTable(dir, "layerstoragevolumes", &dump.LayerStorageVolumes),
		loadTable(dir, "layerluksvolumes", &dump.LayerLuksVolumes),
	)
	if err != nil {
		return nil, err
	}

	if len(dump.Nodes) == 0 && len(dump.ResourceDefinitions) == 0 {
		return nil, fmt.Errorf("%w in %s (expected *%s files)", ErrEmptyDump, dir, dumpFileSuffix)
	}

	return dump, nil
}
