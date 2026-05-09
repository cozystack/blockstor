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

package placer

import (
	"reflect"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestAuxKey: keys are auto-prefixed with "Aux/" because that's the
// LINSTOR convention for operator-supplied node labels (Aux/zone,
// Aux/rack, …). An explicit "Aux/x" passes through unchanged.
func TestAuxKey(t *testing.T) {
	cases := []struct{ in, want string }{
		{"zone", "Aux/zone"},
		{"rack", "Aux/rack"},
		{"Aux/zone", "Aux/zone"},
		{"", "Aux/"},
	}
	for _, c := range cases {
		got := auxKey(c.in)
		if got != c.want {
			t.Errorf("auxKey(%q): got %q, want %q", c.in, got, c.want)
		}
	}
}

// TestPoolKey: composite (node, pool) key uses 0x1f as separator so
// neither half can spoof the other via collision in the joined string.
func TestPoolKey(t *testing.T) {
	a := poolKey("n1", "thin")
	b := poolKey("n", "1\x1fthin")
	if a == b {
		t.Errorf("poolKey collision: %q == %q", a, b)
	}

	if a != "n1\x1fthin" {
		t.Errorf("poolKey: got %q, want n1\\x1fthin", a)
	}
}

// TestPoolsByKey: every input pool must be reachable in the output map
// under its (node, pool) composite key.
func TestPoolsByKey(t *testing.T) {
	pools := []apiv1.StoragePool{
		{NodeName: "n1", StoragePoolName: "thin"},
		{NodeName: "n2", StoragePoolName: "thin"},
		{NodeName: "n1", StoragePoolName: "zfs"},
	}
	out := poolsByKey(pools)

	if len(out) != 3 {
		t.Errorf("len: got %d, want 3", len(out))
	}

	for _, p := range pools {
		got, ok := out[poolKey(p.NodeName, p.StoragePoolName)]
		if !ok {
			t.Errorf("missing key for %s/%s", p.NodeName, p.StoragePoolName)
			continue
		}

		if got.NodeName != p.NodeName || got.StoragePoolName != p.StoragePoolName {
			t.Errorf("wrong value for key: got %+v", got)
		}
	}
}

// TestLookupKeys: the helper extracts named (Aux-prefixed) props from
// a node's label map. Missing keys land as empty strings so downstream
// matching treats "node has no zone label" as a distinct group.
func TestLookupKeys(t *testing.T) {
	props := map[string]string{
		"Aux/zone": "us-east-1a",
		"Aux/rack": "r17",
		"other":    "ignored",
	}

	got := lookupKeys(props, []string{"zone", "rack", "missing"})

	want := map[string]string{
		"Aux/zone":    "us-east-1a",
		"Aux/rack":    "r17",
		"Aux/missing": "",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("lookupKeys: got %+v, want %+v", got, want)
	}
}

// TestMatchesTuple: a node matches when every requested key has the
// same value as in `want`. Nil `want` is treated as no-constraint
// (match everything).
func TestMatchesTuple(t *testing.T) {
	nodeUSEast := map[string]string{"Aux/zone": "us-east-1a"}
	nodeUSWest := map[string]string{"Aux/zone": "us-west-1b"}
	keys := []string{"zone"}

	if !matchesTuple(nodeUSEast, keys, nil) {
		t.Errorf("nil want must match")
	}

	want := map[string]string{"Aux/zone": "us-east-1a"}
	if !matchesTuple(nodeUSEast, keys, want) {
		t.Errorf("matching node failed; node=%+v want=%+v", nodeUSEast, want)
	}

	if matchesTuple(nodeUSWest, keys, want) {
		t.Errorf("non-matching node passed; node=%+v want=%+v", nodeUSWest, want)
	}
}

// TestCollidesWithDiff: the diff-set check fires when a node's tuple
// matches an already-placed replica's tuple. Used to enforce
// replicas_on_different (anti-affinity).
func TestCollidesWithDiff(t *testing.T) {
	nodeProps := map[string]string{"Aux/zone": "us-east-1a"}
	keys := []string{"zone"}

	empty := map[string]struct{}{}
	if collidesWithDiff(nodeProps, keys, empty) {
		t.Errorf("empty seen-set must not collide")
	}

	seen := map[string]struct{}{"zone=us-east-1a": {}}
	if !collidesWithDiff(nodeProps, keys, seen) {
		t.Errorf("matching seen-set must collide")
	}

	differentZone := map[string]string{"Aux/zone": "us-west-1b"}
	if collidesWithDiff(differentZone, keys, seen) {
		t.Errorf("different value must not collide; node=%+v seen=%+v",
			differentZone, seen)
	}
}

// TestTupleKey: the canonical string serialisation must be order-
// independent (sorted by key) so two equal tuples always map to the
// same string.
func TestTupleKey(t *testing.T) {
	a := map[string]string{"Aux/zone": "us-east", "Aux/rack": "r17"}
	b := map[string]string{"Aux/rack": "r17", "Aux/zone": "us-east"}

	keyA := tupleKey(a)
	keyB := tupleKey(b)

	if keyA != keyB {
		t.Errorf("tupleKey order-dependent: %q != %q", keyA, keyB)
	}

	if keyA == "" {
		t.Errorf("tupleKey of non-empty tuple is empty")
	}

	// Nil and empty must produce the empty string.
	if got := tupleKey(nil); got != "" {
		t.Errorf("tupleKey(nil): got %q, want empty", got)
	}
}

// TestTopologyTupleAndSeen: build the seen-set (for diff anti-affinity)
// and the tuple of a single existing replica from a pair of replicas
// and the node label map. Pins the autoplacer's anti-affinity input.
func TestTopologyTupleAndSeen(t *testing.T) {
	existing := []apiv1.Resource{
		{NodeName: "n1"},
		{NodeName: "n2"},
	}

	nodes := map[string]map[string]string{
		"n1": {"Aux/zone": "us-east-1a"},
		"n2": {"Aux/zone": "us-east-1b"},
	}

	keys := []string{"zone"}

	tuple := topologyTuple(existing, nodes, keys)
	if tuple["Aux/zone"] != "us-east-1a" {
		t.Errorf("topologyTuple: got %+v, want zone=us-east-1a (first replica)", tuple)
	}

	seen := topologySeen(existing, nodes, keys)

	// Both replicas' zones must be in the seen-set so a 3rd replica
	// in either of those zones is rejected.
	for _, want := range []string{"zone=us-east-1a", "zone=us-east-1b"} {
		_, ok := seen[want]
		if !ok {
			t.Errorf("topologySeen missing %q; got %+v", want, seen)
		}
	}
}
