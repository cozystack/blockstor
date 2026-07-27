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

package v1alpha1_test

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

// Every CRD carries printer columns so `kubectl get` is useful on its
// own and the CLI can lean on server-side table printing. Without them
// `kubectl get resources` shows NAME/AGE and nothing else — an operator
// debugging a stuck replica learns nothing from it.
//
// The columns are also a compatibility surface: the CLI's tables and
// kubectl's tables should not disagree about what a resource's state
// is, so the set is pinned here rather than left to drift.
func TestCRDsCarryPrinterColumns(t *testing.T) {
	t.Parallel()

	want := map[string][]string{
		"blockstor.cozystack.io_nodes.yaml":               {"Type", "Address", "Status", "Age"},
		"blockstor.cozystack.io_storagepools.yaml":        {"Node", "Pool", "Provider", "Free-KiB", "Total-KiB", "Age"},
		"blockstor.cozystack.io_resourcedefinitions.yaml": {"Group", "Port", "Layers", "Age"},
		"blockstor.cozystack.io_resources.yaml":           {"Definition", "Node", "Pool", "Node-ID", "Port", "State", "In-Use", "Age"},
		"blockstor.cozystack.io_snapshots.yaml":           {"Definition", "Snapshot", "Nodes", "Age"},
		"blockstor.cozystack.io_resourcegroups.yaml":      {"Place-Count", "Storage-Pool", "Layers", "Age"},
	}

	for file, wantCols := range want {
		t.Run(file, func(t *testing.T) {
			t.Parallel()

			got := printerColumnNames(t, file)
			if len(got) != len(wantCols) {
				t.Fatalf("printer columns = %v, want %v", got, wantCols)
			}

			for i := range wantCols {
				if got[i] != wantCols[i] {
					t.Errorf("column[%d] = %q, want %q (full set %v)", i, got[i], wantCols[i], got)
				}
			}
		})
	}
}

// printerColumnNames reads the generated CRD and returns the served
// version's printer-column names in declaration order.
func printerColumnNames(t *testing.T, file string) []string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", file))
	if err != nil {
		t.Fatalf("read CRD: %v (run `make manifests`)", err)
	}

	var crd apiextv1.CustomResourceDefinition

	err = yaml.Unmarshal(data, &crd)
	if err != nil {
		t.Fatalf("parse CRD %s: %v", file, err)
	}

	for i := range crd.Spec.Versions {
		version := &crd.Spec.Versions[i]
		if !version.Served {
			continue
		}

		names := make([]string, 0, len(version.AdditionalPrinterColumns))
		for j := range version.AdditionalPrinterColumns {
			names = append(names, version.AdditionalPrinterColumns[j].Name)
		}

		return names
	}

	t.Fatalf("CRD %s has no served version", file)

	return nil
}
