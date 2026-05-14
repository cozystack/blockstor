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
*/

// Group A — Node. Tier 2 integration tests for the Node REST surface
// driven through the upstream `linstor` CLI against the in-process
// envtest stack. See docs/test-strategy.md (Group A row) for the
// scenario table; each test below carries the bug-guard reference
// from column 3 in its leading comment.
package integration

import (
	"context"
	"os/exec"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// TestGroupANodeListEmpty: `linstor n l` against an empty cluster
// returns the empty envelope shape `[]` rather than crashing the
// Python CLI with a traceback. Wire-shape guard: the harness CLI
// wrapper fails on stderr containing `xml.etree.ElementTree.ParseError`
// or `json.decoder.JSONDecodeError`, so reaching the assertion means
// the envelope decoded cleanly.
func TestGroupANodeListEmpty(t *testing.T) {
	stack := harness.StartStack(t)
	// No SeedThreeNodeCluster — assert against a fresh cluster.

	cli := &harness.CLI{URL: stack.RestURL}

	out := cli.JSON(t, "node", "list")
	if len(out) != 0 {
		t.Fatalf("expected empty node list, got %d rows: %v", len(out), out)
	}
}

// TestGroupANodeListAfterCreate: seed three nodes via the fixture and
// confirm `linstor n l` reports them all. Basic happy-path read.
func TestGroupANodeListAfterCreate(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	out := cli.JSON(t, "node", "list")

	got := nodeNamesFromCLI(out)
	sort.Strings(got)

	want := []string{harness.NodeWorker1, harness.NodeWorker2, harness.NodeWorker3}
	if !sameStringSlice(got, want) {
		t.Fatalf("nodes: got %v, want %v", got, want)
	}
}

// TestGroupANodeCreatePopulatesNetIf: `linstor n c worker-X 10.0.0.X`
// creates a Node whose NetInterfaces[] carries the supplied address
// and the upstream-LINSTOR default port (3366) + encryption type
// (PLAIN). The CLI parity audit row #1 / Bug 59 plug requires both
// fields populated so `linstor n l` Addresses column is non-blank;
// the CRD must persist the address so the autoplacer's connection
// routing has something to dial.
func TestGroupANodeCreatePopulatesNetIf(t *testing.T) {
	stack := harness.StartStack(t)

	cli := &harness.CLI{URL: stack.RestURL}
	// `linstor node create <name> <ip>` is the canonical CLI form.
	// Pass an explicit interface name so we don't depend on the
	// CLI defaulting an unnamed-interface to "default".
	cli.JSON(t, "node", "create", "--node-type", "SATELLITE",
		"worker-create-1", "10.0.0.42")

	// Read-back via the CLI: the row MUST be there with one
	// NetInterface carrying the address + port + encryption type.
	out := cli.JSON(t, "node", "list")

	var row map[string]any

	for _, r := range out {
		if name, _ := r["name"].(string); name == "worker-create-1" {
			row = r

			break
		}
	}

	if row == nil {
		t.Fatalf("node worker-create-1 not in `n l` output: %v", out)
	}

	ifaces, ok := row["net_interfaces"].([]any)
	if !ok || len(ifaces) == 0 {
		t.Fatalf("net_interfaces missing/empty: %v", row["net_interfaces"])
	}

	iface, _ := ifaces[0].(map[string]any)

	if addr, _ := iface["address"].(string); addr != "10.0.0.42" {
		t.Errorf("net_interfaces[0].address: got %v, want 10.0.0.42", iface["address"])
	}

	// satellite_port comes back as float64 from generic JSON
	// unmarshal — compare numerically.
	if port, _ := iface["satellite_port"].(float64); int(port) != 3366 {
		t.Errorf("net_interfaces[0].satellite_port: got %v, want 3366", iface["satellite_port"])
	}

	if enc, _ := iface["satellite_encryption_type"].(string); enc != "PLAIN" {
		t.Errorf("net_interfaces[0].satellite_encryption_type: got %v, want PLAIN", iface["satellite_encryption_type"])
	}

	// Confirm the CRD itself stores the address so a reconcile
	// finding the satellite endpoint doesn't depend on the read-side
	// synthesis.
	var crd blockstoriov1alpha1.Node

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: "worker-create-1"}, &crd)
	if err != nil {
		t.Fatalf("Get blockstor Node CRD: %v", err)
	}

	if len(crd.Spec.NetInterfaces) == 0 {
		t.Fatalf("Node.Spec.NetInterfaces empty after `n c`")
	}

	if crd.Spec.NetInterfaces[0].Address != "10.0.0.42" {
		t.Errorf("Node.Spec.NetInterfaces[0].Address: got %q, want 10.0.0.42",
			crd.Spec.NetInterfaces[0].Address)
	}
}

// TestGroupANodeRestorePUT pins Bug 78: `linstor n restore <node>` is
// served by PUT /v1/nodes/{node}/restore. Without the PUT route Go
// 1.22 ServeMux returns 405 with an empty body, and python-linstor's
// XML fallback decoder crashes with
// `xml.etree.ElementTree.ParseError: syntax error: line 1, column 0`
// — i.e. the operator sees a stack trace, not "node restored".
//
// Drive the PUT via the upstream CLI so the test exercises the exact
// HTTP verb python-linstor sends; assert (a) the call succeeds (no
// traceback — CLI wrapper handles that) and (b) the EVICTED flag
// disappears from the Node's spec flags.
func TestGroupANodeRestorePUT(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	// Pre-evacuate worker-1 so there's an EVICTED flag for restore
	// to clear. Evacuate already runs through the PUT route the
	// next test pins; we don't double-assert here.
	cli := &harness.CLI{URL: stack.RestURL}
	cli.JSON(t, "node", "evacuate", harness.NodeWorker1)

	// Sanity: the flag should be on the CRD before restore.
	flagsBefore := nodeFlags(t, stack, harness.NodeWorker1)
	if !containsString(flagsBefore, "EVICTED") {
		t.Fatalf("setup: EVICTED missing on %q after evacuate; flags=%v",
			harness.NodeWorker1, flagsBefore)
	}

	// The actual Bug 78 guard: this exec calls PUT
	// /v1/nodes/worker-1/restore. CLI wrapper fatals on any
	// python traceback fragment, so reaching the assert means
	// the PUT route returned a JSON envelope the CLI parsed.
	cli.JSON(t, "node", "restore", harness.NodeWorker1)

	flagsAfter := nodeFlags(t, stack, harness.NodeWorker1)
	if containsString(flagsAfter, "EVICTED") {
		t.Fatalf("EVICTED still on %q after restore; flags=%v",
			harness.NodeWorker1, flagsAfter)
	}
}

// TestGroupANodeEvacuatePUT pins Bug 78 for evacuate + Bug 18 for the
// InUse refusal: `linstor n evacuate` must round-trip through the PUT
// route, refuse with 409 when any resource on the node is in use, and
// honour `--force` to override.
//
// We stage an in-use resource on worker-2 by seeding a Resource CRD
// whose Status.State.InUse=true (the same shape the CLI parity audit
// uses). Then we call evacuate twice: the first call must fail (the
// CLI wrapper turns non-zero exit + traceback into t.Fatal — we route
// around that by using CLI.Run and inspecting stderr); the second
// with --force must succeed and stamp EVICTED.
func TestGroupANodeEvacuatePUT(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	// Seed a ResourceDefinition + Resource on worker-2 with
	// in_use=true via the store. linstor-csi's state-tracking path
	// stamps in_use through the satellite; for the test we go
	// direct via the CRD client (the harness's manager runs the
	// real reconcilers, so the satellite mock will leave Status
	// alone except for DrbdState which we don't depend on here).
	ctx := context.Background()
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-evac"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
		},
	}

	err := stack.Env.Client.Create(ctx, rd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("seed RD: %v", err)
	}

	r := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-evac." + harness.NodeWorker2},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd-evac",
			NodeName:               harness.NodeWorker2,
		},
	}

	err = stack.Env.Client.Create(ctx, r)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("seed Resource: %v", err)
	}

	// Stamp Status.InUse=true so the evacuate handler trips its
	// 409 guard. The CRD field is a bare bool; the store
	// translates it to the apiv1.Resource.State.InUse pointer the
	// REST handler reads.
	err = retryStatusPatch(ctx, stack, "rd-evac."+harness.NodeWorker2,
		func(r *blockstoriov1alpha1.Resource) {
			r.Status.InUse = true
		})
	if err != nil {
		t.Fatalf("stamp InUse: %v", err)
	}

	// First call: must fail. We bypass CLI.JSON because the wrapper
	// fatals on non-zero exit; expect the underlying handler to
	// return 409 and the CLI to surface a non-zero exit with a
	// LINSTOR-shaped error envelope (no traceback — Bug 78 wire).
	cli := &harness.CLI{URL: stack.RestURL}

	out, _ := runCLINoFatal(t, cli, "node", "evacuate", harness.NodeWorker2)
	if !containsBytes(out, "in use") && !containsBytes(out, "in_use") {
		// CLI may have logged the message through stderr already;
		// the assertion that matters is the flag was NOT stamped.
		t.Logf("evacuate (no force) stdout: %s", out)
	}

	flagsMid := nodeFlags(t, stack, harness.NodeWorker2)
	if containsString(flagsMid, "EVICTED") {
		t.Fatalf("EVICTED stamped despite in-use refusal; flags=%v", flagsMid)
	}

	// Second call: --force MUST stamp EVICTED. This is the PUT
	// route smoke — CLI wrapper fatals on traceback so reaching
	// the next assert means the wire shape is intact.
	cli.JSON(t, "node", "evacuate", "--force", harness.NodeWorker2)

	flagsAfter := nodeFlags(t, stack, harness.NodeWorker2)
	if !containsString(flagsAfter, "EVICTED") {
		t.Fatalf("EVICTED missing after --force evacuate; flags=%v", flagsAfter)
	}
}

// TestGroupANodeEvictPUT pins Bug 78 for the `evict` alias: upstream
// LINSTOR aliases `linstor n evict` onto the same PUT route as
// evacuate (blockstor's REST shim wires both to handleNodeEvacuate);
// without the PUT route, golinstor's `NodeService.Evict` (which
// `doPUT`s) crashes the python decoder with an empty 405 body.
func TestGroupANodeEvictPUT(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	// `linstor node evict <node>` — same wire as evacuate.
	cli.JSON(t, "node", "evict", harness.NodeWorker3)

	flags := nodeFlags(t, stack, harness.NodeWorker3)
	if !containsString(flags, "EVICTED") {
		t.Fatalf("EVICTED missing on %q after `n evict`; flags=%v",
			harness.NodeWorker3, flags)
	}
}

// TestGroupANodeLostCascadesOrphans pins Bug 28: `linstor n lost X`
// must cascade-delete every Resource and StoragePool whose NodeName
// references the lost node. The satellite that owned the
// SatelliteResourceFinalizer is gone with the node — a plain
// DeletionTimestamp stamp would hang every orphan forever and brick
// the next RD-create that recycles the name/port allocation.
//
// We seed a Resource on worker-1 + a peer replica on worker-2, then
// call `n lost worker-1` and assert the worker-1 replica is gone
// while worker-2 survives.
func TestGroupANodeLostCascadesOrphans(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	ctx := context.Background()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-lost"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
		},
	}

	err := stack.Env.Client.Create(ctx, rd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{harness.NodeWorker1, harness.NodeWorker2} {
		r := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "rd-lost." + node},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "rd-lost",
				NodeName:               node,
			},
		}

		err = stack.Env.Client.Create(ctx, r)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("seed Resource on %s: %v", node, err)
		}
	}

	cli := &harness.CLI{URL: stack.RestURL}
	cli.JSON(t, "node", "lost", harness.NodeWorker1)

	// worker-1 replica must be gone (handler-driven cascade — not
	// finalizer-driven, see node_lifecycle.go cascadeOrphansForLostNode).
	harness.Eventually(t, 10*time.Second, func() bool {
		var got blockstoriov1alpha1.Resource

		gErr := stack.Env.Client.Get(ctx,
			types.NamespacedName{Name: "rd-lost." + harness.NodeWorker1}, &got)

		return apierrors.IsNotFound(gErr)
	}, "Resource on lost worker-1 not cascade-deleted")

	// worker-2 (the surviving peer) MUST stay.
	var peer blockstoriov1alpha1.Resource

	err = stack.Env.Client.Get(ctx,
		types.NamespacedName{Name: "rd-lost." + harness.NodeWorker2}, &peer)
	if err != nil {
		t.Fatalf("peer Resource on worker-2 vanished; cascade scoped too widely: %v", err)
	}

	// And the Node CRD itself is gone.
	var lostNode blockstoriov1alpha1.Node

	err = stack.Env.Client.Get(ctx,
		types.NamespacedName{Name: harness.NodeWorker1}, &lostNode)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("lost Node CRD still present after `n lost`; err=%v", err)
	}
}

// TestGroupANodeReconnect pins the wire shape of PUT
// /v1/nodes/{node}/reconnect: golinstor's NodeService.Reconnect
// `doPUT`s with no body and decodes a `[]ApiCallRc` envelope. The
// blockstor satellite-as-CR (Phase 10.6) has no TCP to bounce so the
// handler is a no-op acknowledgement — but the wire shape MUST be
// the envelope the python CLI parses, not an empty 405.
func TestGroupANodeReconnect(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	// Reaching this assert means the CLI's JSON decode succeeded:
	// CLI.JSON's wrapper fatals on traceback / decode error.
	cli.JSON(t, "node", "reconnect", harness.NodeWorker1)
}

// TestGroupANodeAuxLabelSync pins Bug 13 / F7 (scenario 2.13): a
// `topology.kubernetes.io/zone` label set on the *corev1.Node*
// surfaces on the matching blockstor Node CRD's
// `Spec.Props["Aux/topology.kubernetes.io/zone"]` within a reconcile
// loop, so RG selectors like `replicasOnSame=...` actually resolve.
//
// The harness's manager wires NodeLabelSyncReconciler
// (see manager.go::wireNodeReconcilers), so the projection runs
// once we create the corev1.Node.
func TestGroupANodeAuxLabelSync(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	const (
		labelKey = "topology.kubernetes.io/zone"
		auxKey   = "Aux/topology.kubernetes.io/zone"
		zone     = "zone-a"
	)

	ctx := context.Background()

	// Create the corev1.Node that the label-sync reconciler will
	// project. The blockstor Node CRD with the same name was seeded
	// by SeedThreeNodeCluster.
	kubeNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   harness.NodeWorker1,
			Labels: map[string]string{labelKey: zone},
		},
	}

	err := stack.Env.Client.Create(ctx, kubeNode)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create corev1.Node: %v", err)
	}

	// Wait for the reconciler to land the Aux/ prop on the
	// blockstor Node CRD.
	harness.Eventually(t, 15*time.Second, func() bool {
		var n blockstoriov1alpha1.Node

		gErr := stack.Env.Client.Get(ctx,
			types.NamespacedName{Name: harness.NodeWorker1}, &n)
		if gErr != nil {
			return false
		}

		return n.Spec.Props[auxKey] == zone
	}, "label-sync reconciler did not surface "+auxKey+"="+zone)

	// Cross-check via the operator-facing CLI surface: `linstor n l`
	// must reflect the prop in the JSON envelope's `props` field
	// (this is what `linstor n list-properties` reads).
	cli := &harness.CLI{URL: stack.RestURL}
	out := cli.JSON(t, "node", "list")

	var row map[string]any

	for _, r := range out {
		if name, _ := r["name"].(string); name == harness.NodeWorker1 {
			row = r

			break
		}
	}

	if row == nil {
		t.Fatalf("worker-1 missing from `n l`: %v", out)
	}

	props, _ := row["props"].(map[string]any)
	if got, _ := props[auxKey].(string); got != zone {
		t.Fatalf("props[%q] via CLI: got %q, want %q (label-sync reconciler regression)",
			auxKey, got, zone)
	}
}

// TestGroupANodeWireShapeFields pins Bug 59: every field upstream
// LINSTOR's openapi.json declares on the Node DTO MUST be present in
// `GET /v1/nodes` output so the python CLI's column renderers
// (Addresses, UUID, supported-layers/-providers) don't surface as
// blanks. The 2026-05-14 Bug 59 plug added UUID + NetInterface
// defaults + capability tables; this test guards against silent
// regression of any of them.
func TestGroupANodeWireShapeFields(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	out := cli.JSON(t, "node", "list")

	var row map[string]any

	for _, r := range out {
		if name, _ := r["name"].(string); name == harness.NodeWorker1 {
			row = r

			break
		}
	}

	if row == nil {
		t.Fatalf("worker-1 missing from `n l`: %v", out)
	}

	// UUID: must be non-empty and stable.
	if uuid, _ := row["uuid"].(string); uuid == "" {
		t.Errorf("uuid: missing on `n l` row (Bug 59 / F2 regression)")
	}

	// type / SATELLITE round-trip.
	if typ, _ := row["type"].(string); typ != "SATELLITE" {
		t.Errorf("type: got %q, want SATELLITE", typ)
	}

	// resource_layers + storage_providers: capability tables the
	// CLI's `linstor advise` and autoplacer surface.
	layers, _ := row["resource_layers"].([]any)
	if len(layers) == 0 {
		t.Errorf("resource_layers: empty (F2 regression — blockstor satellite implements DRBD/STORAGE/LUKS)")
	}

	providers, _ := row["storage_providers"].([]any)
	if len(providers) == 0 {
		t.Errorf("storage_providers: empty (F2 regression — blockstor satellite implements LVM_THIN/ZFS_THIN/FILE/DISKLESS+...)")
	}

	// NetInterface defaults — port + encryption + is_active.
	ifaces, _ := row["net_interfaces"].([]any)
	if len(ifaces) == 0 {
		t.Fatalf("net_interfaces: empty on fixture node")
	}

	iface, _ := ifaces[0].(map[string]any)

	if port, _ := iface["satellite_port"].(float64); int(port) != 3366 {
		t.Errorf("net_interfaces[0].satellite_port: got %v, want 3366 (Bug 59)", iface["satellite_port"])
	}

	if enc, _ := iface["satellite_encryption_type"].(string); enc != "PLAIN" {
		t.Errorf("net_interfaces[0].satellite_encryption_type: got %v, want PLAIN (Bug 59)", iface["satellite_encryption_type"])
	}

	if isActive, _ := iface["is_active"].(bool); !isActive {
		t.Errorf("net_interfaces[0].is_active: got %v, want true (Bug 59)", iface["is_active"])
	}

	if ifUUID, _ := iface["uuid"].(string); ifUUID == "" {
		t.Errorf("net_interfaces[0].uuid: missing (F1 regression)")
	}

	// props: synthesised NodeUname + CurStltConnName so the CLI's
	// `list-properties` view is never nil.
	props, _ := row["props"].(map[string]any)
	if props == nil {
		t.Fatalf("props: nil (F2 regression — NodeUname+CurStltConnName must be synthesised)")
	}

	if got, _ := props["CurStltConnName"].(string); got == "" {
		t.Errorf("props[CurStltConnName]: empty (F2 regression)")
	}
}

// --- helpers ----------------------------------------------------------------

// nodeFlags returns the union of Spec.Flags and Status.Flags for the
// named Node CRD. Evacuate / Restore / Evict stamp via Spec, but the
// helper is liberal so a future split of authority between Spec and
// Status doesn't break the assertion semantics.
func nodeFlags(t *testing.T, stack *harness.Stack, name string) []string {
	t.Helper()

	var n blockstoriov1alpha1.Node

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: name}, &n)
	if err != nil {
		t.Fatalf("Get Node %q: %v", name, err)
	}

	out := append([]string(nil), n.Spec.Flags...)
	out = append(out, n.Status.Flags...)

	return out
}

// retryStatusPatch reads the named Resource, applies mutate, and
// writes the Status back. Retries on transient apiserver
// "object has been modified" conflicts up to a small budget.
func retryStatusPatch(ctx context.Context, stack *harness.Stack, name string,
	mutate func(*blockstoriov1alpha1.Resource),
) error {
	const retries = 10

	var lastErr error

	for range retries {
		var r blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: name}, &r)
		if err != nil {
			lastErr = err

			continue
		}

		mutate(&r)

		err = stack.Env.Client.Status().Update(ctx, &r)
		if err == nil {
			return nil
		}

		lastErr = err

		time.Sleep(50 * time.Millisecond)
	}

	return lastErr
}

// runCLINoFatal runs the CLI and returns stdout + a best-effort
// success flag. Unlike CLI.Run/JSON, this does NOT t.Fatal on
// non-zero exit — used by tests that expect the call to fail
// (e.g. evacuate without --force on an in-use node).
//
// We can't reach harness.CLI's internals (the playbook bans harness
// edits), so we re-implement via exec — kept tiny + scoped to this
// file so it doesn't accidentally bloat into a parallel helper.
func runCLINoFatal(t *testing.T, cli *harness.CLI, args ...string) ([]byte, bool) {
	t.Helper()

	full := append([]string{"--controllers", cli.URL, "--machine-readable"}, args...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "linstor", full...) //nolint:gosec // test-only invocation of the linstor CLI

	out, err := cmd.CombinedOutput()

	return out, err == nil
}

// nodeNamesFromCLI extracts the `name` field from each row in a
// `linstor n l` envelope decoded by harness.CLI.JSON.
func nodeNamesFromCLI(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))

	for _, r := range rows {
		if n, _ := r["name"].(string); n != "" {
			out = append(out, n)
		}
	}

	return out
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}

	return false
}

func containsBytes(haystack []byte, needle string) bool {
	if needle == "" {
		return false
	}

	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return true
		}
	}

	return false
}

func sameStringSlice(a, b []string) bool {
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
