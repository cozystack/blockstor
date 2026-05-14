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

// TestParseWeight pins the four behaviours of the weight decoder
// consumed by loadWeights (scenario 2.W01):
//   - empty string → 1.0 (the UG9 default; clusters that never touch
//     the knobs get all four strategies equally weighted)
//   - unparseable garbage → 1.0 (operator typo must not break placement)
//   - negative value → 0 (clamped, so a fat-fingered "-1" disables the
//     strategy instead of inverting it)
//   - valid non-negative float → exact value
//
// The defaults matter: a regression that returned 0 on the empty path
// would silently disable every strategy on a fresh cluster, and Place
// would degenerate to the NodeName-alphabetical tiebreaker.
func TestParseWeight(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"", 1.0},
		{"garbage", 1.0},
		{"-1", 0},
		{"-0.5", 0},
		{"0", 0},
		{"1", 1.0},
		{"2.5", 2.5},
		{"10", 10.0},
	}
	for _, c := range cases {
		got := parseWeight(c.in)
		if got != c.want {
			t.Errorf("parseWeight(%q): got %v, want %v", c.in, got, c.want)
		}
	}
}

// TestThroughputHintRoundTrip verifies the per-SP
// `Autoplacer/MaxThroughput` decoder (scenario 6.W11). The prop is
// bytes/sec as a long; we decode through float64 so the scoring path
// can normalise without a second cast. Missing / unparseable /
// negative all collapse to 0 (the "unknown" sentinel that makes the
// MaxThroughput strategy a no-op for that pool).
func TestThroughputHintRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want float64
	}{
		{"missing", "", 0},
		{"zero", "0", 0},
		{"valid-int", "104857600", 104857600},       // 100 MiB/s
		{"valid-large", "10737418240", 10737418240}, // 10 GiB/s
		{"garbage", "1e3MB/s", 0},
		{"negative", "-1", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pool := apiv1.StoragePool{Props: map[string]string{}}
			if c.raw != "" {
				pool.Props[apiv1.PropAutoplacerMaxThroughput] = c.raw
			}

			got := throughputHint(&pool)
			if got != c.want {
				t.Errorf("throughputHint(%q): got %v, want %v", c.raw, got, c.want)
			}
		})
	}
}

// TestReservedKibRoundTrip verifies the per-pool reserved-KiB hint
// decoder consumed by the MinReservedSpace strategy (scenario 2.W01).
// The same fail-soft contract as throughputHint: missing / garbage /
// negative all collapse to 0 ("no reservation reported"), so the
// strategy degrades to the same score as a pool that literally has
// nothing reserved.
func TestReservedKibRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int64
	}{
		{"missing", "", 0},
		{"zero", "0", 0},
		{"valid", "1048576", 1048576},
		{"garbage", "lots", 0},
		{"negative", "-100", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pool := apiv1.StoragePool{Props: map[string]string{}}
			if c.raw != "" {
				pool.Props[apiv1.PropAuxPoolReservedKib] = c.raw
			}

			got := reservedKib(&pool)
			if got != c.want {
				t.Errorf("reservedKib(%q): got %d, want %d", c.raw, got, c.want)
			}
		})
	}
}

// TestCompositeMaxThroughputDominates pins the MaxThroughput scoring
// half of scenario 6.W11. Three pools with identical Free/Total +
// zero existing resources, advertising MaxThroughput hints of 100 /
// 200 / 400 MB/s respectively. With Weights/MaxThroughput=10 and the
// other three weights at their defaults (1.0), the 400-hint pool
// must score strictly higher than the 100-hint pool — the per-pool
// normalised hint contribution (1.0 vs 0.25) is multiplied by 10,
// dwarfing the 1.0 max contribution any other strategy can offer.
func TestCompositeMaxThroughputDominates(t *testing.T) {
	pools := []apiv1.StoragePool{
		{NodeName: "n1", FreeCapacity: 1000, TotalCapacity: 1000, Props: map[string]string{
			apiv1.PropAutoplacerMaxThroughput: "104857600", // 100 MB/s
		}},
		{NodeName: "n2", FreeCapacity: 1000, TotalCapacity: 1000, Props: map[string]string{
			apiv1.PropAutoplacerMaxThroughput: "209715200", // 200 MB/s
		}},
		{NodeName: "n3", FreeCapacity: 1000, TotalCapacity: 1000, Props: map[string]string{
			apiv1.PropAutoplacerMaxThroughput: "419430400", // 400 MB/s
		}},
	}

	w := weights{
		maxFreeSpace:     1.0,
		minReservedSpace: 1.0,
		minRscCount:      1.0,
		maxThroughput:    10.0,
	}

	maxT := 0.0

	for i := range pools {
		if t := throughputHint(&pools[i]); t > maxT {
			maxT = t
		}
	}

	scores := make(map[string]float64, len(pools))
	for i := range pools {
		scores[pools[i].NodeName] = composite(&pools[i], w, map[string]int{}, maxT)
	}

	if scores["n3"] <= scores["n2"] {
		t.Errorf("MaxThroughput=10 must rank 400 MB/s above 200 MB/s; n3=%.4f n2=%.4f",
			scores["n3"], scores["n2"])
	}

	if scores["n2"] <= scores["n1"] {
		t.Errorf("MaxThroughput=10 must rank 200 MB/s above 100 MB/s; n2=%.4f n1=%.4f",
			scores["n2"], scores["n1"])
	}
}

// TestCompositeWeightZeroDisablesStrategy pins that a controller-
// scope weight set to 0 removes that strategy from the composite
// entirely — the strategy's per-pool score is multiplied by 0 and
// drops out, leaving only the remaining strategies to break ties.
// Scenario 2.W01 explicitly relies on this: setting
// `Weights/MaxFreeSpace=0` plus `Weights/MinRscCount=1` makes the
// placer pick the least-busy node even when it has the smallest pool.
func TestCompositeWeightZeroDisablesStrategy(t *testing.T) {
	pool := apiv1.StoragePool{
		NodeName:      "n1",
		FreeCapacity:  1000,
		TotalCapacity: 1000,
		Props: map[string]string{
			apiv1.PropAutoplacerMaxThroughput: "104857600",
			apiv1.PropAuxPoolReservedKib:      "100",
		},
	}

	// MaxFreeSpace contribution would be 1.0; with weight 0 it drops out.
	w := weights{maxFreeSpace: 0, minReservedSpace: 1.0, minRscCount: 1.0, maxThroughput: 1.0}

	got := composite(&pool, w, map[string]int{}, 104857600)

	// Expected: MinReservedSpace(1*0.9) + MinRscCount(1*1.0) +
	// MaxThroughput(1*1.0) = 2.9 — no MaxFreeSpace contribution.
	want := (1.0 - 100.0/1000.0) + 1.0 + 1.0
	if got != want {
		t.Errorf("composite with weight=0 strategy must drop out: got %v, want %v", got, want)
	}
}

// TestCompositeNoThroughputHintIsZero pins the fail-soft branch when
// no pool in the candidate set advertises a MaxThroughput hint
// (maxThroughput parameter is 0). The MaxThroughput strategy must
// contribute exactly 0 to every pool — never divide-by-zero, never
// silently double-weight another strategy. This is the realistic
// fresh-cluster path: operators don't set `Autoplacer/MaxThroughput`
// on every SP from day one.
func TestCompositeNoThroughputHintIsZero(t *testing.T) {
	pool := apiv1.StoragePool{
		NodeName:      "n1",
		FreeCapacity:  500,
		TotalCapacity: 1000,
		Props:         map[string]string{}, // no MaxThroughput
	}

	w := weights{maxFreeSpace: 1.0, minReservedSpace: 1.0, minRscCount: 1.0, maxThroughput: 100.0}

	// maxThroughput sentinel = 0 → strategy short-circuits, no NaN.
	got := composite(&pool, w, map[string]int{}, 0)
	want := 0.5 /* maxFree */ + 1.0 /* minReserved */ + 1.0 /* minRscCount */
	if got != want {
		t.Errorf("no hint must yield zero throughput contribution: got %v, want %v", got, want)
	}
}

// TestRankCandidatesStableTiebreak pins the NodeName-ASC tiebreaker
// applied by rankCandidates after the composite-score compare. Two
// pools with identical scores must come back in alphabetical NodeName
// order so downstream tests (and operators reading the placement
// order) get a deterministic answer instead of map-iteration roulette.
func TestRankCandidatesStableTiebreak(t *testing.T) {
	pools := []apiv1.StoragePool{
		{NodeName: "n-c", FreeCapacity: 1000, TotalCapacity: 1000},
		{NodeName: "n-a", FreeCapacity: 1000, TotalCapacity: 1000},
		{NodeName: "n-b", FreeCapacity: 1000, TotalCapacity: 1000},
	}

	w := weights{maxFreeSpace: 1.0, minReservedSpace: 1.0, minRscCount: 1.0, maxThroughput: 1.0}

	rankCandidates(pools, w, map[string]int{})

	want := []string{"n-a", "n-b", "n-c"}
	for i, p := range pools {
		if p.NodeName != want[i] {
			t.Errorf("rankCandidates[%d]: got %s, want %s", i, p.NodeName, want[i])
		}
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
