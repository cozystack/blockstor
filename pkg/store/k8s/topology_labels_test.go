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

package k8s

import (
	"testing"
)

// TestFoldTopologyLabelsAddsAuxProp pins the migration shim: a Node
// with `topology.blockstor.io/zone=us-east-1a` set as a native
// Kubernetes label MUST surface as `Props["Aux/zone"]` on the wire
// so the autoplacer's existing replicas_on_same / replicas_on_different
// filters keep working without changes. Phase 10.3.
func TestFoldTopologyLabelsAddsAuxProp(t *testing.T) {
	t.Parallel()

	props := map[string]string{}
	labels := map[string]string{
		"topology.blockstor.io/zone": "us-east-1a",
		"topology.blockstor.io/rack": "r5",
		"unrelated":                  "ignored",
	}

	foldTopologyLabels(props, labels)

	if props["Aux/zone"] != "us-east-1a" {
		t.Errorf("Aux/zone: got %q, want us-east-1a", props["Aux/zone"])
	}

	if props["Aux/rack"] != "r5" {
		t.Errorf("Aux/rack: got %q, want r5", props["Aux/rack"])
	}

	if _, ok := props["unrelated"]; ok {
		t.Errorf("unrelated label leaked into props")
	}
}

// TestFoldTopologyLabelsPropsWinOnConflict pins the precedence
// rule: when both the Props key and the matching label are set,
// Props wins (matches the autoplacer's auxKey() lookup, which
// reads Props verbatim — the labels path is purely additive).
// A regression that overwrote Props would silently change
// existing placement decisions.
func TestFoldTopologyLabelsPropsWinOnConflict(t *testing.T) {
	t.Parallel()

	props := map[string]string{
		"Aux/zone": "explicit-from-props",
	}
	labels := map[string]string{
		"topology.blockstor.io/zone": "from-label-should-lose",
	}

	foldTopologyLabels(props, labels)

	if props["Aux/zone"] != "explicit-from-props" {
		t.Errorf("Aux/zone: got %q, want explicit-from-props (Props overrides labels)", props["Aux/zone"])
	}
}

// TestHasTopologyLabelsTrue / False pin the cheap pre-check: skip
// the props-clone allocation entirely when there's nothing to fold
// in. A regression that always allocated would slow every
// crdToWireNode call on clusters with no topology labels at all.
func TestHasTopologyLabelsTrue(t *testing.T) {
	t.Parallel()

	if !hasTopologyLabels(map[string]string{"topology.blockstor.io/zone": "us-east-1a"}) {
		t.Errorf("expected true for prefixed label")
	}
}

func TestHasTopologyLabelsFalse(t *testing.T) {
	t.Parallel()

	cases := []map[string]string{
		nil,
		{},
		{"unrelated": "x"},
		{"topology.kubernetes.io/zone": "x"}, // standard k8s label, NOT ours
	}

	for i, labels := range cases {
		if hasTopologyLabels(labels) {
			t.Errorf("case %d: expected false for %v", i, labels)
		}
	}
}
