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

package stream_test

// Scenario 3.10 — satellite respects `--controller-bind-address` from
// the active NetInterface.
//
// Upstream LINSTOR's UG9 §"Managing network interface cards"
// (lines 2167-2169) documents: `linstor node interface modify ...
// --active` re-points the satellite at a new controller-facing IP
// without restarting the satellite process.
//
// Pinned current behaviour (DEFERRED — Outcome B):
//
//  1. blockstor's satellite has NO `--controller-bind-address` flag.
//     The only flags on cmd/satellite/main.go are `--node-name` and
//     `--state-dir`. Verified in TestSatelliteFlagsLackControllerBindAddress
//     below.
//
//  2. Phase 10.6 retired every satellite→controller gRPC wire — the
//     satellite now talks to the Kubernetes apiserver via the standard
//     in-cluster config (controller-runtime `ctrl.GetConfig()`). The
//     "controller endpoint" the satellite needs to reach is therefore
//     the cluster's `kubernetes.default.svc` Service VIP, NOT a
//     LINSTOR controller TCP endpoint, NOT a value drawn from this
//     node's Spec.NetInterfaces. The Service VIP is supplied by the
//     kubelet (envvar / projected SA token) and stays valid for the
//     lifetime of the pod regardless of what the operator writes to
//     blockstor's own Node.Spec.NetInterfaces.
//
//  3. Blockstor's `api/v1alpha1.NodeNetInterface` CRD type carries
//     `Name`, `Address`, `SatellitePort`, `SatelliteEncryptionType` —
//     but NO `Active` / `IsActive` field. The wire `IsActive` flag
//     that `linstor node interface list` shows is synthesised at
//     conversion time by pkg/store/k8s/nodes.go as `i == 0` (first
//     interface wins) — pure presentation, no behaviour attached.
//     Verified in TestNodeCRDHasNoActiveField below.
//
//  4. The only satellite code that reads peer NetInterfaces at
//     runtime is the cross-node snapshot fetcher
//     (pkg/satellite/controllers/snapshot_fetcher.go) — that's
//     satellite→satellite snapshot transport, not satellite→controller.
//     Even there, the selector is "Name == default" or "first
//     non-empty Address", NOT an Active flag.
//
// The spec test therefore documents the deferred gap: an operator
// changing `Spec.NetInterfaces` on a node CRD MUST restart the
// satellite pod for the change to affect anything operationally
// useful (and even then, only the local DRBD `on <node>` address +
// the peer snapshot-fetch path are affected — the apiserver
// connection still goes via kubernetes.default.svc).
//
// Tracking:
//
//   - tests/scenarios/03-networking.md §3.10
//   - docs/known-issues.md → "Bug 49: satellite ignores runtime
//     NetInterface changes (deferred from scenario 3.10)"
//
// Outcome A (active re-dial loop) was considered and rejected: the
// blockstor architecture does not have a satellite→controller dial
// to re-target, so implementing the upstream-LINSTOR semantics would
// require resurrecting the Phase 10.6-retired gRPC wire purely so we
// have something to re-dial. Not worth the complexity for a P2.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	blockstorv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestSatelliteFlagsLackControllerBindAddress pins the deferred state
// of scenario 3.10: the satellite binary today exposes neither a
// `--controller-bind-address` flag nor any other CLI knob that would
// let it re-dial the controller via an alternate NIC. The flag set
// is intentionally minimal — surfacing `controller-bind-address`
// (Outcome A) would require also wiring runtime reconciliation of
// the satellite's own Node.Spec.NetInterfaces, which Phase 10.6's
// "kube-apiserver as the only controller wire" design makes
// architecturally meaningless.
//
// If this test ever starts failing because the operator added the
// flag, that's a strong signal that Outcome A is being implemented —
// at which point this whole file should be replaced with a positive
// test that asserts the re-dial actually happens.
func TestSatelliteFlagsLackControllerBindAddress(t *testing.T) {
	t.Parallel()

	mainPath := satelliteMainPath(t)

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, mainPath, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", mainPath, err)
	}

	flags := collectFlagNames(f)

	// Sanity: we actually parsed flags out of main.go. If main.go
	// stops using flag.StringVar / flag.BoolVar entirely (e.g.
	// migrated to cobra) this slice will be empty and the test
	// becomes meaningless — fail loudly so a follow-up audit picks
	// it up.
	if len(flags) == 0 {
		t.Fatalf("no flag.*Var calls found in %s — has the binary been ported to a new flag library? "+
			"Update this spec test to read whichever new mechanism declares CLI flags.", mainPath)
	}

	// The forbidden-by-current-spec flag names. Any of these
	// appearing means someone implemented Outcome A — replace this
	// test with a positive re-dial assertion at that point.
	forbidden := []string{
		"controller-bind-address",
		"controller-address",
		"controller-endpoint",
		"controller-url",
		"controller",
	}

	for _, name := range flags {
		for _, f := range forbidden {
			if name == f {
				t.Fatalf("flag %q appeared on satellite binary — scenario 3.10 was implemented (Outcome A); "+
					"replace this spec test with a positive re-dial assertion against the new wiring", name)
			}
		}
	}

	// Belt-and-braces: pin the exact flag set we expect today, so
	// any drift (added or removed) trips review. The order matches
	// the order declared in cmd/satellite/main.go.
	// Bug 207 added health-probe-bind-address for kubelet livenessProbe.
	want := []string{"node-name", "state-dir", "health-probe-bind-address"}
	if !equalStringSlices(flags, want) {
		t.Fatalf("satellite flag set drifted: got %v, want %v. "+
			"If this is intentional, update the want list AND update tests/scenarios/03-networking.md §3.10 "+
			"to describe what the new flag does for the active-interface re-dial story.", flags, want)
	}
}

// TestNodeCRDHasNoActiveField pins the second half of the deferred
// scenario 3.10: blockstor's `NodeNetInterface` CRD type carries no
// `Active` (or `IsActive`) field. Without that field there is no
// place on the CRD for the operator to express "this is the
// controller-facing NIC now"; the corresponding wire flag is
// synthesised as `i == 0` on the way out (see pkg/store/k8s/nodes.go).
//
// Adding the field is the prerequisite for Outcome A. If this test
// ever fails because the field was added, the contract for what
// happens when an operator flips it MUST be specified somewhere in
// pkg/satellite/ before the field is allowed to land — otherwise we
// ship a CRD knob with no behaviour.
func TestNodeCRDHasNoActiveField(t *testing.T) {
	t.Parallel()

	// Zero-value the struct and assert via reflection on the
	// existing fields. Using a struct literal with field names
	// also gives us a compile-time check: if the type later gains
	// an Active field, this literal still compiles but the
	// has-field check below trips.
	ni := blockstorv1alpha1.NodeNetInterface{
		Name:                    "n",
		Address:                 "10.0.0.1",
		SatellitePort:           0,
		SatelliteEncryptionType: "PLAIN",
	}

	_ = ni // suppress unused — the assertion is the type literal above.

	// Defensive reflection check covering both common spellings.
	if hasField(ni, "Active") {
		t.Fatalf("NodeNetInterface gained an Active field — scenario 3.10 Outcome A is being implemented. " +
			"Wire the satellite Node reconciler to act on changes to that field before merging the CRD update.")
	}

	if hasField(ni, "IsActive") {
		t.Fatalf("NodeNetInterface gained an IsActive field — scenario 3.10 Outcome A is being implemented. " +
			"Wire the satellite Node reconciler to act on changes to that field before merging the CRD update.")
	}
}

// satelliteMainPath resolves cmd/satellite/main.go relative to the
// running test binary. The test runs under
// pkg/satellite/stream/, so we walk two directories up then
// into cmd/satellite/.
func satelliteMainPath(t *testing.T) string {
	t.Helper()

	// runtime.Caller returns the path of this test file inside the
	// module — anchor the relative walk on it rather than on
	// os.Getwd which would be the per-test temp dir.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed — cannot locate cmd/satellite/main.go")
	}

	// thisFile = .../pkg/satellite/stream/redial_spec_test.go
	// Walk up to the module root (4 levels: stream → satellite → pkg → root).
	root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))

	return filepath.Join(root, "cmd", "satellite", "main.go")
}

// collectFlagNames walks the parsed AST of cmd/satellite/main.go and
// returns every flag name passed to flag.StringVar / flag.BoolVar /
// flag.IntVar / flag.Float64Var / flag.DurationVar in declaration
// order. Pure AST inspection — does NOT execute main(), which would
// register the flags into flag.CommandLine and pollute other tests.
func collectFlagNames(f *ast.File) []string {
	var names []string

	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "flag" {
			return true
		}

		// Match Var-style flag declarations only (the form main.go
		// actually uses). Bare flag.String / flag.Bool return a
		// pointer and aren't used here; if a future change moves to
		// them, expand this list.
		fn := sel.Sel.Name
		if !strings.HasSuffix(fn, "Var") {
			return true
		}

		// Signature: flag.XxxVar(target, name, default, usage)
		// — name is the second arg, always a string literal.
		if len(call.Args) < 2 {
			return true
		}

		lit, ok := call.Args[1].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}

		// Strip surrounding quotes from the AST literal.
		name := strings.Trim(lit.Value, `"`)
		names = append(names, name)

		return true
	})

	return names
}

// hasField reports whether a struct value has a field by name.
// NodeNetInterface is a plain struct so reflection is stable here;
// using ast inspection of the CRD package would be overkill for a
// single negative assertion.
func hasField(v any, name string) bool {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	_, found := t.FieldByName(name)

	return found
}

// equalStringSlices reports whether two []string have identical
// length and content in the same order.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
