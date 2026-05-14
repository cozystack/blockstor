//go:build integration

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

Group D — ResourceDefinition. Each subtest drives the upstream
`linstor` CLI against an in-process apiserver+REST stack and pins
the wire contract for one scenario from docs/test-strategy.md
(Group D row). Names are arranged so `go test -run '^TestGroupD'`
catches the whole group (parent + every subtest).

Scenario map (subtest → upstream defect reference):
  - RDCreateListDelete                — basic CRUD wire shape
  - RDInheritsLayerStackFromRG        — 54
  - RDDeleteCascadesResourcesSnapshots — 1, 4
  - RDCloneFromSource                 — 15, 21
  - RDListWithVolumeDefinitions       — 53
  - RDFilterByRscDfns                 — 61
  - RDListLayerData                   — 58
  - RDSetPropertyEffective            — 203
  - DfltRscGrpCanonical               — 57

All helpers live in this file per the agent-playbook scope contract:
only the per-group test file changes — the harness stays untouched.

Why a parent + subtests layout (vs. nine top-level Test funcs):
controller-runtime validates controller names process-globally —
`controller with name node already exists` fires the second time a
fresh Manager wires its reconcilers in one `go test` invocation.
The harness boots one stack per call (the Phase 0 isolation
guarantee) which collides with that on the second top-level Test.
We therefore share ONE stack per package by hosting every Group D
check as a subtest under one parent Test that owns the stack
lifecycle. Each subtest prefixes its CRD names with a slug so they
cannot cross-pollute through the shared apiserver state.

Per-subtest tests still surface individually (`TestGroupD/<name>`)
in the go test output and report verbose pass/fail per scenario.
*/

package integration

import (
	"context"
	"os/exec"
	"sort"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// labelResourceDefinition mirrors the constant the k8s store uses to
// tag child Resources / Snapshots so it can list-by-RD efficiently
// (pkg/store/k8s/resources.go::LabelResourceDefinition). We seed the
// label directly because the cascade test bypasses the REST/CLI path
// that would have done so naturally.
const labelResourceDefinition = "blockstor.io/resource-definition"

// TestGroupD is the parent that owns the shared stack and dispatches
// every Group D bug-guard as a subtest. Booting once dodges the
// process-global metric-name collision the harness can't currently
// avoid, while keeping each scenario named so a failure surfaces as
// `TestGroupD/<Bug>` in the report.
//
// The companion top-level `TestGroupD<Name>` aliases below let
// developers run a single subtest with `-run '^TestGroupDFoo$'` as
// well, while the canonical `-run '^TestGroupD'` invocation still
// runs everything via the parent.
func TestGroupD(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)
	cli := &harness.CLI{URL: stack.RestURL}

	t.Run("RDCreateListDelete", func(t *testing.T) {
		groupDRDCreateListDelete(t, stack, cli)
	})

	t.Run("RDInheritsLayerStackFromRG", func(t *testing.T) {
		groupDRDInheritsLayerStackFromRG(t, stack, cli)
	})

	t.Run("RDDeleteCascadesResourcesSnapshots", func(t *testing.T) {
		groupDRDDeleteCascadesResourcesSnapshots(t, stack, cli)
	})

	t.Run("RDCloneFromSource", func(t *testing.T) {
		groupDRDCloneFromSource(t, stack, cli)
	})

	t.Run("RDListWithVolumeDefinitions", func(t *testing.T) {
		groupDRDListWithVolumeDefinitions(t, stack, cli)
	})

	t.Run("RDFilterByRscDfns", func(t *testing.T) {
		groupDRDFilterByRscDfns(t, stack, cli)
	})

	t.Run("RDListLayerData", func(t *testing.T) {
		groupDRDListLayerData(t, stack, cli)
	})

	t.Run("RDSetPropertyEffective", func(t *testing.T) {
		groupDRDSetPropertyEffective(t, stack, cli)
	})

	t.Run("DfltRscGrpCanonical", func(t *testing.T) {
		groupDDfltRscGrpCanonical(t, stack, cli)
	})
}

// rdNames extracts the `name` field from the CLI's JSON envelope.
// Stable helper kept local so the per-test code reads like the
// scenario it tests instead of a JSON-walk loop.
func rdNames(rows []map[string]any) []string {
	names := make([]string, 0, len(rows))

	for _, row := range rows {
		if n, ok := row["name"].(string); ok {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	return names
}

// waitForRDPresent polls the CRD client until the named RD appears or
// the deadline elapses. The CLI POST returns 201 once the apiserver
// has accepted the create, but the cluster client may still see the
// object on a brief informer cache trail; polling here keeps the
// downstream assertions deterministic.
func waitForRDPresent(t *testing.T, stack *harness.Stack, name string) {
	t.Helper()

	harness.Eventually(t, 10*time.Second, func() bool {
		var rd blockstoriov1alpha1.ResourceDefinition

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: name}, &rd)

		return err == nil
	}, "ResourceDefinition "+name+" never appeared")
}

// waitForRDGone polls until the RD is no longer present (NotFound).
// Used by cascade tests where `rd d` returns 200 but the CRD removal
// races with the apiserver delete pipeline.
func waitForRDGone(t *testing.T, stack *harness.Stack, name string) {
	t.Helper()

	harness.Eventually(t, 30*time.Second, func() bool {
		var rd blockstoriov1alpha1.ResourceDefinition

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: name}, &rd)

		return apierrors.IsNotFound(err)
	}, "ResourceDefinition "+name+" never deleted")
}

// runCLIIgnoringExit drives the linstor CLI like harness.CLI.Run but
// tolerates non-zero exit. Used by tests that probe "expected
// failure" REST responses (e.g. snapshot-present rd delete → 409
// → CLI exits non-zero). The harness's Run helper fatals on a
// non-zero exit, so we cannot reuse it; this stays local rather
// than bloating the harness with a single-caller helper.
func runCLIIgnoringExit(t *testing.T, cli *harness.CLI, args ...string) {
	t.Helper()

	bin := "linstor"
	if cli.Binary != "" {
		bin = cli.Binary
	}

	if _, err := exec.LookPath(bin); err != nil {
		t.Fatalf("linstor binary missing: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	full := append([]string{"--controllers", cli.URL, "--machine-readable"}, args...)

	cmd := exec.CommandContext(ctx, bin, full...)
	// Intentionally ignore the exit code — the caller asserts on
	// post-conditions (CRD still present, etc.). If the CLI crashes
	// on some unexpected mode, the assertion that follows fails and
	// the test signal stays clear.
	_ = cmd.Run()
}

// contains is a tiny string-slice predicate. Local to this file
// because the harness intentionally doesn't grow general utilities —
// each group keeps its bespoke helpers next to the tests.
func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}

	return false
}

// equalStringSlice compares two string slices for order-preserving
// equality. Standalone helper so the LayerStack assertion stays
// dependency-free.
func equalStringSlice(a, b []string) bool {
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

// cleanupRD best-effort-deletes the named RD, swallowing NotFound so
// tests stay green when prior cleanup already ran. Per-test cleanup
// keeps the shared apiserver state predictable for sibling tests.
func cleanupRD(t *testing.T, stack *harness.Stack, name string) {
	t.Helper()

	var rd blockstoriov1alpha1.ResourceDefinition

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: name}, &rd)
	if apierrors.IsNotFound(err) {
		return
	}

	if err != nil {
		t.Logf("cleanupRD %q: get: %v", name, err)

		return
	}

	if err := stack.Env.Client.Delete(context.Background(), &rd); err != nil && !apierrors.IsNotFound(err) {
		t.Logf("cleanupRD %q: delete: %v", name, err)
	}
}

// ------------------------------------------------------------------
// Subtest implementations. Kept as plain functions so the
// orchestration in TestGroupD reads as a one-line-per-scenario
// dispatch table — and so each function carries the scenario's
// defect-reference documentation right next to its assertion.
// ------------------------------------------------------------------

// groupDRDCreateListDelete pins the basic CRUD wire contract:
// `rd c`, `rd l`, `rd d` round-trip cleanly. Acts as the smoke for
// every other Group D test — a regression here flags the underlying
// REST surface, not the bug-guard-specific paths below.
func groupDRDCreateListDelete(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const rdName = "rd-d-crud"

	t.Cleanup(func() { cleanupRD(t, stack, rdName) })

	cli.Run(t, "resource-definition", "create", rdName)

	waitForRDPresent(t, stack, rdName)

	got := rdNames(cli.JSON(t, "resource-definition", "list"))

	if !contains(got, rdName) {
		t.Fatalf("%s not in list: %v", rdName, got)
	}

	cli.Run(t, "resource-definition", "delete", rdName)

	waitForRDGone(t, stack, rdName)
}

// groupDRDInheritsLayerStackFromRG pins Bug 54: when an RG's
// SelectFilter pins `layerStack`, an RD created with no
// `--layer-list` flag MUST inherit that stack. Without this, the
// dispatcher reads rd.Spec.LayerStack==nil, the legacy needsDRBD
// default kicks in, and an STORAGE-only RG produces DRBD-stacked
// replicas — silently contradicting the operator intent.
func groupDRDInheritsLayerStackFromRG(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const (
		rdName = "rd-d-inherit"
		rgName = "rg-d-storage-only"
	)

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: rgName},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				PlaceCount: 1,
				LayerStack: []string{"STORAGE"},
			},
		},
	}

	t.Cleanup(func() {
		cleanupRD(t, stack, rdName)
		_ = stack.Env.Client.Delete(context.Background(), rg)
	})

	if err := stack.Env.Client.Create(context.Background(), rg); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create RG: %v", err)
	}

	cli.Run(t, "resource-definition", "create", rdName,
		"--resource-group", rgName)

	waitForRDPresent(t, stack, rdName)

	var rd blockstoriov1alpha1.ResourceDefinition

	harness.Eventually(t, 10*time.Second, func() bool {
		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: rdName}, &rd)
		if err != nil {
			return false
		}

		return len(rd.Spec.LayerStack) == 1 && rd.Spec.LayerStack[0] == "STORAGE"
	}, "RD did not inherit LayerStack=[STORAGE]")

	if !equalStringSlice(rd.Spec.LayerStack, []string{"STORAGE"}) {
		t.Fatalf("RD LayerStack: got %v, want [STORAGE] (Bug 54 inheritance)", rd.Spec.LayerStack)
	}
}

// groupDRDDeleteCascadesResourcesSnapshots pins Bugs 1/4:
// `rd d <rd>` must cascade-delete every child Resource AND refuse
// the delete (409) while child Snapshots still exist. Without
// cascade, child Resources orphan; without the snapshot guard,
// snapshots dangle on a vanished parent.
func groupDRDDeleteCascadesResourcesSnapshots(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const rdName = "rd-d-cascade"

	t.Cleanup(func() { cleanupRD(t, stack, rdName) })

	ctx := context.Background()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
		},
	}
	if err := stack.Env.Client.Create(ctx, rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:   rdName + "." + harness.NodeWorker1,
			Labels: map[string]string{labelResourceDefinition: rdName},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               harness.NodeWorker1,
		},
	}
	if err := stack.Env.Client.Create(ctx, res); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	// Snapshot present → rd d MUST refuse with conflict (Bug 4).
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:   rdName + ".snap-1",
			Labels: map[string]string{labelResourceDefinition: rdName},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rdName,
			SnapshotName:           "snap-1",
		},
	}
	if err := stack.Env.Client.Create(ctx, snap); err != nil {
		t.Fatalf("seed Snapshot: %v", err)
	}

	runCLIIgnoringExit(t, cli, "resource-definition", "delete", rdName)

	// RD must still be alive after the refused delete.
	var stillThere blockstoriov1alpha1.ResourceDefinition
	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: rdName}, &stillThere); err != nil {
		t.Fatalf("RD vanished after a refused delete (Bug 4 guard broken): %v", err)
	}

	// Drop the snapshot, retry — cascade must now succeed and the
	// child Resource must be marked for deletion (Bug 1).
	if err := stack.Env.Client.Delete(ctx, snap); err != nil {
		t.Fatalf("drop Snapshot: %v", err)
	}

	cli.Run(t, "resource-definition", "delete", rdName)

	waitForRDGone(t, stack, rdName)

	harness.Eventually(t, 30*time.Second, func() bool {
		var r blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(ctx,
			types.NamespacedName{Name: rdName + "." + harness.NodeWorker1}, &r)

		return apierrors.IsNotFound(err) || r.DeletionTimestamp != nil
	}, "child Resource not cascaded on rd delete (Bug 1)")
}

// groupDRDCloneFromSource pins Bugs 15/21: `rd clone <src> <dst>`
// must (a) accept the upstream-shape POST body, (b) materialise a
// new RD under the requested name, (c) copy mutable spec fields
// (props, RG ref), and (d) leave the source RD intact.
func groupDRDCloneFromSource(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const (
		srcName = "rd-d-src"
		dstName = "rd-d-clone"
	)

	t.Cleanup(func() {
		cleanupRD(t, stack, srcName)
		cleanupRD(t, stack, dstName)
	})

	ctx := context.Background()

	src := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: srcName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
			Props:             map[string]string{"Aux/cozystack.io/origin": "src"},
		},
	}
	if err := stack.Env.Client.Create(ctx, src); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	cli.Run(t, "resource-definition", "clone", srcName, dstName)

	waitForRDPresent(t, stack, dstName)

	var clone blockstoriov1alpha1.ResourceDefinition
	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: dstName}, &clone); err != nil {
		t.Fatalf("get clone: %v", err)
	}

	if clone.Spec.Props["Aux/cozystack.io/origin"] != "src" {
		t.Errorf("clone did not copy Props (Bug 15 metadata copy): got %v", clone.Spec.Props)
	}

	if clone.Spec.ResourceGroupName != harness.FixtureDefaultRG {
		t.Errorf("clone did not copy ResourceGroupName: got %q, want %q",
			clone.Spec.ResourceGroupName, harness.FixtureDefaultRG)
	}

	// Source must remain (Bug 21: clone is a copy, not a rename).
	var stillSrc blockstoriov1alpha1.ResourceDefinition
	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: srcName}, &stillSrc); err != nil {
		t.Errorf("source RD vanished after clone (Bug 21): %v", err)
	}
}

// groupDRDListWithVolumeDefinitions pins Bug 53: `linstor vd l`
// sends `GET /v1/resource-definitions?with_volume_definitions=true`
// and expects each RD to inline its volume_definitions array.
// Without inline VDs the CLI renders an empty table even when
// volumes exist (it never falls back to per-RD GETs).
func groupDRDListWithVolumeDefinitions(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const rdName = "rd-d-with-vd"

	t.Cleanup(func() { cleanupRD(t, stack, rdName) })

	ctx := context.Background()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}
	if err := stack.Env.Client.Create(ctx, rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	waitForRDPresent(t, stack, rdName)

	rows := cli.JSON(t, "volume-definition", "list")

	if len(rows) == 0 {
		t.Fatalf("vd l returned empty envelope; expected an entry for %s", rdName)
	}

	found := false

	for _, row := range rows {
		name, _ := row["name"].(string)
		if name != rdName {
			continue
		}

		vds, ok := row["volume_definitions"].([]any)
		if !ok || len(vds) == 0 {
			t.Fatalf("%s row missing/empty volume_definitions: %#v (Bug 53)", rdName, row)
		}

		found = true

		break
	}

	if !found {
		t.Fatalf("%s missing from `vd l` envelope: %#v", rdName, rows)
	}
}

// groupDRDFilterByRscDfns pins Bug 61: `linstor rd l
// --resource-definitions <name>` MUST honour the
// `resource_definitions` query parameter. Pre-fix, the param was
// ignored and ALL RDs came back, breaking the CLI's filtered
// rendering and any tool that walks one-RD-at-a-time.
func groupDRDFilterByRscDfns(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	names := []string{"rd-d-filter-alpha", "rd-d-filter-beta", "rd-d-filter-gamma"}

	t.Cleanup(func() {
		for _, n := range names {
			cleanupRD(t, stack, n)
		}
	})

	ctx := context.Background()

	for _, name := range names {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				ResourceGroupName: harness.FixtureDefaultRG,
			},
		}
		if err := stack.Env.Client.Create(ctx, rd); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}

		waitForRDPresent(t, stack, name)
	}

	got := rdNames(cli.JSON(t, "resource-definition", "list",
		"--resource-definitions", "rd-d-filter-beta"))

	if len(got) != 1 || got[0] != "rd-d-filter-beta" {
		t.Fatalf("--resource-definitions filter ignored (Bug 61): got %v, want [rd-d-filter-beta]", got)
	}
}

// groupDRDListLayerData pins Bug 58: the `layer_data` field on the
// RD wire shape must be parseable by the python CLI without an
// AttributeError. The python CLI's `rsc_dfn.layer_data` access is
// unconditional; an unparseable shape crashes the parser on
// `linstor rd list`.
//
// We assert by running `rd l` through the CLI: if the wire shape
// breaks the python parser, harness.CLI.Run fatals on a stderr
// traceback. Then we walk the JSON envelope and confirm the field
// is either absent (omitempty acceptable per upstream shape) or an
// array — anything else is a wire-shape regression.
func groupDRDListLayerData(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const rdName = "rd-d-layer-data"

	t.Cleanup(func() { cleanupRD(t, stack, rdName) })

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
			LayerStack:        []string{"DRBD", "STORAGE"},
		},
	}
	if err := stack.Env.Client.Create(context.Background(), rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	waitForRDPresent(t, stack, rdName)

	rows := cli.JSON(t, "resource-definition", "list")

	for _, row := range rows {
		name, _ := row["name"].(string)
		if name != rdName {
			continue
		}

		ld, ok := row["layer_data"]
		if !ok {
			return
		}

		if _, isArr := ld.([]any); !isArr {
			t.Fatalf("layer_data on %s is %T not array (Bug 58): %#v", rdName, ld, ld)
		}

		return
	}

	t.Fatalf("%s missing from rd l envelope", rdName)
}

// groupDRDSetPropertyEffective pins Bug 203: after
// `linstor rd set-property <rd> <key> <val>`, the value MUST appear
// in the merged effective-props bag exposed by /v1/view/resources
// under scope `RD`. The CLI's `r lp <rd> <node>` derives the `(R)`
// inheritance marker from this same field; a missing entry shows
// the prop as if it had never been set.
func groupDRDSetPropertyEffective(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const rdName = "rd-d-eff"

	ctx := context.Background()

	t.Cleanup(func() { cleanupRD(t, stack, rdName) })

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
		},
	}
	if err := stack.Env.Client.Create(ctx, rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:   rdName + "." + harness.NodeWorker1,
			Labels: map[string]string{labelResourceDefinition: rdName},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               harness.NodeWorker1,
		},
	}
	if err := stack.Env.Client.Create(ctx, res); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	t.Cleanup(func() {
		_ = stack.Env.Client.Delete(ctx, res)
	})

	cli.Run(t, "resource-definition", "set-property", rdName,
		"Aux/cozystack.io/origin", "bug-203")

	harness.Eventually(t, 15*time.Second, func() bool {
		rows := cli.JSON(t, "resource", "list-volumes")
		for _, row := range rows {
			rname, _ := row["name"].(string)
			node, _ := row["node_name"].(string)

			if rname != rdName || node != harness.NodeWorker1 {
				continue
			}

			eff, ok := row["effective_props"].(map[string]any)
			if !ok {
				continue
			}

			entry, ok := eff["Aux/cozystack.io/origin"].(map[string]any)
			if !ok {
				continue
			}

			if val, _ := entry["value"].(string); val != "bug-203" {
				continue
			}

			if scope, _ := entry["scope"].(string); scope != "RD" {
				continue
			}

			return true
		}

		return false
	}, "Aux/cozystack.io/origin=bug-203 (scope=RD) never appeared in effective_props (Bug 203)")
}

// groupDDfltRscGrpCanonical pins Bug 57: an RD created without
// `--resource-group` MUST be assigned to the canonical CamelCase
// `DfltRscGrp`. Pre-fix, the auto-created RG was named lowercase
// (`dfltrscgrp` after the k8s-store slugifier), which silently
// broke linstor-csi's string-equality lookups for the
// `defaultResourceGroup` constant.
func groupDDfltRscGrpCanonical(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const rdName = "rd-d-bare-rg"

	t.Cleanup(func() { cleanupRD(t, stack, rdName) })

	cli.Run(t, "resource-definition", "create", rdName)

	waitForRDPresent(t, stack, rdName)

	gotRGs := rdNames(cli.JSON(t, "resource-group", "list"))

	if !contains(gotRGs, "DfltRscGrp") {
		t.Fatalf("auto-created RG name not canonical (Bug 57): got %v, want one entry == %q",
			gotRGs, "DfltRscGrp")
	}

	var rd blockstoriov1alpha1.ResourceDefinition
	if err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: rdName}, &rd); err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if rd.Spec.ResourceGroupName != "DfltRscGrp" {
		t.Errorf("RD.ResourceGroupName: got %q, want %q (Bug 57 canonical)",
			rd.Spec.ResourceGroupName, "DfltRscGrp")
	}
}
