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
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"
)

// WriteManifests renders the converted object set as one multi-document
// YAML stream in apply order (nodes → pools → groups → definitions →
// resources → snapshots) with a stable sort inside each kind, so the
// output is deterministic and diffable between runs.
func WriteManifests(w io.Writer, res *Result) error {
	docs := make([]any, 0,
		1+len(res.Nodes)+len(res.StoragePools)+len(res.ResourceGroups)+
			len(res.ResourceDefinitions)+len(res.Resources)+len(res.Snapshots))

	if res.ControllerConfig != nil {
		docs = append(docs, res.ControllerConfig)
	}

	for i := range res.Nodes {
		docs = append(docs, &res.Nodes[i])
	}

	for i := range res.StoragePools {
		docs = append(docs, &res.StoragePools[i])
	}

	for i := range res.ResourceGroups {
		docs = append(docs, &res.ResourceGroups[i])
	}

	for i := range res.ResourceDefinitions {
		docs = append(docs, &res.ResourceDefinitions[i])
	}

	for i := range res.Resources {
		docs = append(docs, &res.Resources[i])
	}

	for i := range res.Snapshots {
		docs = append(docs, &res.Snapshots[i])
	}

	for _, doc := range docs {
		data, err := marshalManifest(doc)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "---\n%s", data)
		if err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}

	return nil
}

// marshalManifest renders one CRD as YAML, pruning the noise a direct
// struct marshal would leak into manifests meant for `kubectl apply`:
// the zero `metadata.creationTimestamp: null` and the empty `status`.
func marshalManifest(obj any) ([]byte, error) {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	var tree map[string]any

	err = json.Unmarshal(jsonBytes, &tree)
	if err != nil {
		return nil, fmt.Errorf("reparse manifest: %w", err)
	}

	delete(tree, "status")

	if meta, ok := tree["metadata"].(map[string]any); ok {
		if ts, present := meta["creationTimestamp"]; present && ts == nil {
			delete(meta, "creationTimestamp")
		}
	}

	data, err := yaml.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("render manifest yaml: %w", err)
	}

	return data, nil
}

// WriteReport renders the migration warnings, one per line, prefixed so
// they read cleanly on stderr next to the manifest stream on stdout.
func WriteReport(w io.Writer, res *Result) error {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "converted: %d nodes, %d storage pools, %d resource groups, %d resource definitions, %d resources, %d snapshots\n",
		len(res.Nodes), len(res.StoragePools), len(res.ResourceGroups),
		len(res.ResourceDefinitions), len(res.Resources), len(res.Snapshots))

	for _, warning := range res.Warnings {
		fmt.Fprintf(&buf, "warning: %s\n", warning)
	}

	_, err := w.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	return nil
}
