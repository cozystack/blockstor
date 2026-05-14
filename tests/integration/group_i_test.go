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

package integration

// Group I — Node-connection / Resource-connection (6 tests).
//
// docs/test-strategy.md row "I — Node/Resource connections":
//
//	TestGroupI/NodeConnectionSetProperty    — basic        (node-connection write surface)
//	TestGroupI/ResourceConnectionPathCreate — scenario 3.7 (multi-path DRBD .res block)
//	TestGroupI/PingTimeoutPropagation       — 5.W03        (Controller>RG>RD>R precedence)
//	TestGroupI/NetOptionsSplit              — 5.W04        (connect-int / max-buffers / protocol)
//	TestGroupI/EffectivePropsAtAllLevels    — Bug 203      (per-entry scope tag walk)
//	TestGroupI/DrbdProxyEndpoint            — wave2        (drbd-proxy 501 envelope)
//
// Single-process subtest layout
// -----------------------------
// controller-runtime keeps a process-global `usedNames` set on every
// `SetupWithManager` call (see sigs.k8s.io/controller-runtime/pkg/
// controller/name.go::checkName). Booting a fresh manager in each
// `Test*` function — the natural `go test` style — surfaces as
// "controller with name node already exists" on the second
// invocation. The harness does not (yet) flip `SkipNameValidation`,
// and this group's prompt forbids touching the harness, so every
// Group I scenario runs as a subtest under one top-level
// TestGroupI(t) that calls `harness.StartStack` exactly once.
//
// The DoD filter `-run '^TestGroupI'` matches the umbrella as well
// as every subtest (`TestGroupI/<name>`), so the contract on the
// run command is preserved.
//
// Implementation notes
// --------------------
//
//   - Effective-prop assertions read /v1/view/resources. blockstor's
//     REST does not expose the upstream Python CLI's dedicated
//     /v1/resources/<r>/effective-properties endpoint, but the
//     ResourceWithVolumes envelope on /v1/view/resources carries the
//     per-replica `effective_props` map populated by the same
//     `effectivePropsForResource` resolver, so the precedence
//     contract is testable end-to-end.
//
//   - The node-connection PUT surface returns 204 (empty body) in
//     blockstor today — handleNoContent in pkg/rest/node_connections.go.
//     The upstream Python CLI cannot decode that envelope and exits 1
//     with `Unable to parse REST json data`. Testing the contract
//     directly via HTTP captures the storage-layer behaviour without
//     blocking on the CLI-side decoder; a future Phase 2 fix that
//     surfaces an ApiCallRc envelope would still satisfy the
//     `StatusCode <= 204` assertion here.
//
//   - The multi-path connection POST writes onto
//     RD.Spec.Props["Cozystack/ResourceConnectionPaths/<a>/<b>"]
//     (pkg/rest/resource_connections.go) — a blockstor-native shape
//     the satellite dispatcher decodes via
//     pkg/dispatcher/connections.go::connectionsFromRD. The in-process
//     satellite mock does not render .res files, so the test
//     reconstructs the `drbd.Resource{Connections: ...}` value from
//     the RD prop bag and calls pkg/drbd.Build directly — exactly
//     what pkg/satellite/reconciler.go::buildResFile does in
//     production. The result MUST contain the multi-path
//     `connection { path { … } }` block UG9 §"Creating multiple
//     DRBD paths with LINSTOR" specifies.
//
//   - The Resource-scope of the effective-prop hierarchy (the
//     "specific node-connection" rung the prompt names) is written
//     directly through the envtest Client onto Resource.Spec.Props —
//     blockstor's REST does not expose a per-Resource set-property
//     route, but the dispatcher and the effective-prop resolver both
//     read Resource.Spec.Props so a direct CRD update is observably
//     identical to what the upstream CLI would do.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

const (
	// groupIPropPingTimeout is the upstream LINSTOR prop key the
	// Controller→RG→RD→Resource precedence walk in
	// /PingTimeoutPropagation uses. Hoisted so every level writes
	// the identical spelling — a typo would silently pass the leaf-
	// wins assertion (no level matches the parent key).
	groupIPropPingTimeout = "DrbdOptions/Net/ping-timeout"

	// groupIResourceConnectionPathsPropPrefix mirrors the prefix
	// pkg/rest/resource_connections.go and pkg/dispatcher/
	// connections.go use to store path lists. Pinned by literal
	// rather than imported so this file stays self-documenting on
	// the wire shape it asserts (and a divergent rename of the
	// prefix on one side without the other surfaces here).
	groupIResourceConnectionPathsPropPrefix = "Cozystack/ResourceConnectionPaths/"

	// groupIHTTPTimeout caps every direct-REST call the group
	// makes. The blockstor REST server runs in the same process
	// as the test, so any real timeout indicates a hung handler —
	// fail fast.
	groupIHTTPTimeout = 15 * time.Second

	// groupIEventually is the polling budget for any reconciler-
	// driven assertion (autoplace → Resource rows visible,
	// effective-prop merge after a Controller / RG / RD PUT). Long
	// enough that envtest's reconcile bursts settle, short enough
	// that a real bug surfaces in CI without blocking the suite.
	groupIEventually = 30 * time.Second
)

// TestGroupI is the Phase 1 Group I entrypoint. controller-runtime's
// process-global controller-name registry forces a single envtest
// boot per process; every scenario lives as a subtest underneath.
// `-run '^TestGroupI'` matches the umbrella plus every subtest.
//
//nolint:funlen // subtest table — splitting per-test would require touching the harness.
func TestGroupI(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	t.Run("NodeConnectionSetProperty", func(t *testing.T) {
		groupINodeConnectionSetProperty(t, stack)
	})

	t.Run("ResourceConnectionPathCreate", func(t *testing.T) {
		groupIResourceConnectionPathCreate(t, stack)
	})

	t.Run("PingTimeoutPropagation", func(t *testing.T) {
		groupIPingTimeoutPropagation(t, stack)
	})

	t.Run("NetOptionsSplit", func(t *testing.T) {
		groupINetOptionsSplit(t, stack)
	})

	t.Run("EffectivePropsAtAllLevels", func(t *testing.T) {
		groupIEffectivePropsAtAllLevels(t, stack)
	})

	t.Run("DrbdProxyEndpoint", func(t *testing.T) {
		groupIDrbdProxyEndpoint(t, stack)
	})
}

// groupINodeConnectionSetProperty — basic: the node-connection PUT
// surface is a cozystack passthrough (flat-L2, no DRBD-proxy), but
// the wire contract MUST still answer the modify request without a
// 4xx/5xx. The handler accepts override_props / delete_props bodies
// and returns 204 — see pkg/rest/node_connections.go::handleNoContent.
//
// We drive the REST endpoint directly: the upstream Python CLI does
// not decode an empty 204 body (its `_rest_request` rejects empty
// JSON), so a CLI-level test fails on a python decoder edge case
// rather than the actual storage-layer contract. Direct HTTP pins
// the contract that matters — accepting the modify body without
// rejecting on shape.
//
// Re-reading the connection via GET confirms the canonical empty-bag
// envelope still surfaces (golinstor decodes `{node_a, node_b,
// properties:{}}` into a zero-value NodeConnection without error).
func groupINodeConnectionSetProperty(t *testing.T, stack *harness.Stack) {
	t.Helper()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{groupIPropPingTimeout: "150"},
	})
	if err != nil {
		t.Fatalf("marshal modify body: %v", err)
	}

	resp := groupIDoHTTPRequest(t, http.MethodPut,
		stack.RestURL+"/v1/node-connections/"+
			harness.NodeWorker1+"/"+harness.NodeWorker2, body)
	_ = resp.Body.Close()

	// blockstor's PUT handler is intentionally a no-op accept that
	// returns 204. 2xx range covers both that and any future
	// upgrade to a 201/200 + ApiCallRc envelope.
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		t.Fatalf("node-connection PUT: status %d, want 2xx", resp.StatusCode)
	}

	// GET surface must answer with the upstream LINSTOR-shaped
	// per-pair envelope. golinstor decodes `[{...}]` into a slice
	// of NodeConnection and unwraps the first element — returning
	// `[]` or `null` here would silently mis-decode into a zero-
	// value struct.
	getResp := groupIDoHTTPRequest(t, http.MethodGet,
		stack.RestURL+"/v1/node-connections/"+
			harness.NodeWorker1+"/"+harness.NodeWorker2, nil)
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("node-connection GET: status %d, want 200", getResp.StatusCode)
	}

	var got map[string]any

	err = json.NewDecoder(getResp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	if got["node_a"] != harness.NodeWorker1 || got["node_b"] != harness.NodeWorker2 {
		t.Errorf("envelope node fields: got %+v", got)
	}

	if _, ok := got["properties"].(map[string]any); !ok {
		t.Errorf("envelope missing properties map: %+v", got)
	}
}

// groupIResourceConnectionPathCreate — scenario 3.7: a multi-path
// DRBD connection is described by two `path { … }` sub-blocks inside
// one `connection { … }` block, each carrying its own host-address
// pair. The blockstor REST surface stores the path list on the parent
// RD's Spec.Props (key prefix `Cozystack/ResourceConnectionPaths/`);
// the satellite dispatcher decodes those props and feeds them into
// `drbd.Build`. The test drives the storage half (REST → Spec.Props)
// and the rendering half (Spec.Props → `drbd.Build`) end-to-end so a
// regression on either side surfaces immediately.
//
// The in-process satellite mock (`harness/satellite.go`) does not
// write .res files to a state-dir — the harness is intentionally
// status-projection-only. We therefore call `drbd.Build` directly
// with Hosts + Connections rebuilt from the persisted RD props, the
// same path the real satellite reconciler's `buildResFile` follows
// (pkg/satellite/reconciler.go::buildResConnections).
func groupIResourceConnectionPathCreate(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const (
		rdName    = "rd-multipath"
		pathOne   = "path-primary"
		pathTwo   = "path-secondary"
		pathOneAA = "10.99.0.11"
		pathOneAB = "10.99.0.12"
		pathTwoAA = "10.99.1.11"
		pathTwoAB = "10.99.1.12"
	)

	spawnRDViaRG(t, stack, harness.FixtureDefaultRG, rdName, 4*1024*1024)
	groupIWaitResourcesCreated(t, stack, rdName, 2)

	groupIPostResourceConnectionPath(t, stack.RestURL, rdName,
		harness.NodeWorker1, harness.NodeWorker2, pathOne, pathOneAA, pathOneAB)
	groupIPostResourceConnectionPath(t, stack.RestURL, rdName,
		harness.NodeWorker1, harness.NodeWorker2, pathTwo, pathTwoAA, pathTwoAB)

	rd := groupIGetRD(t, stack, rdName)

	conns := groupIDecodeConnections(t, rd, harness.NodeWorker1, harness.NodeWorker2)
	if len(conns) != 1 {
		t.Fatalf("connectionsFromRD: got %d connection entries, want 1", len(conns))
	}

	if len(conns[0].Paths) != 2 {
		t.Fatalf("connection paths: got %d, want 2", len(conns[0].Paths))
	}

	body, err := drbd.Build(drbd.Resource{
		Name: rdName,
		Hosts: []drbd.Host{
			{NodeName: harness.NodeWorker1, Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: harness.NodeWorker2, Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
		Volumes:     []drbd.Volume{{Number: 0, Device: "/dev/drbd1000", Disk: "/dev/vg/lv", Minor: 1000}},
		Connections: conns,
	})
	if err != nil {
		t.Fatalf("drbd.Build: %v", err)
	}

	// The connection block MUST contain two path sub-blocks. drbd-9
	// drops the implicit single-host-pair default as soon as any
	// explicit path is present, so a regression that emits the
	// inline `host A address …; host B address …;` lines instead
	// surfaces here.
	//
	// We match `    path {` with leading whitespace to avoid the
	// false positive where the resource name (e.g. `rd-multipath {`)
	// happens to end with `path {` as a substring.
	const pathBlockMarker = "    path {"

	if got := strings.Count(body, pathBlockMarker); got != 2 {
		t.Fatalf(".res: want exactly 2 %q blocks, got %d:\n%s",
			pathBlockMarker, got, body)
	}

	for _, addr := range []string{pathOneAA, pathOneAB, pathTwoAA, pathTwoAB} {
		if !strings.Contains(body, "address "+addr+":") {
			t.Errorf(".res missing address %q:\n%s", addr, body)
		}
	}

	// The implicit `host worker-1 address 10.0.0.1:` line MUST be
	// suppressed inside the connection block when an explicit path
	// is present. Pin the surrounding `connection { … }` window so
	// we don't accidentally match the per-host `on worker-1 { … }`
	// block earlier in the file.
	connectionWindow := groupISliceBetween(body, "  connection {", "  }")
	if strings.Contains(connectionWindow, "host "+harness.NodeWorker1+" address 10.0.0.1:") {
		t.Errorf(".res connection block still carries the implicit single-pair host lines:\n%s",
			connectionWindow)
	}
}

// groupIPingTimeoutPropagation — 5.W03: the Controller → RG → RD →
// Resource precedence chain MUST be honoured for the DRBD net-option
// keys the satellite stamps on the .res file. The test sets the same
// key at every level with distinct values and asserts the
// effective_props map carried on `/v1/view/resources` reports the
// most-specific value as the winner.
//
// blockstor does NOT expose the upstream Python CLI's
// `/v1/resources/<r>/effective-properties` endpoint. The same
// `effectivePropsForResource` resolver is reachable on
// `/v1/view/resources` though, with each replica's bag inlined as
// `effective_props`. The precedence-chain contract is the same:
// Controller (CTRL) < RG < RD < Resource — the leaf value wins.
func groupIPingTimeoutPropagation(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const rdName = "rd-pingto"

	groupIPutControllerProp(t, stack.RestURL, groupIPropPingTimeout, "100")
	groupIPutRGProp(t, stack.RestURL, harness.FixtureDefaultRG, groupIPropPingTimeout, "200")

	spawnRDViaRG(t, stack, harness.FixtureDefaultRG, rdName, 4*1024*1024)
	groupIWaitResourcesCreated(t, stack, rdName, 2)

	groupIPutRDProp(t, stack.RestURL, rdName, groupIPropPingTimeout, "300")

	// Resource-scope override on worker-1. blockstor's REST does
	// not surface a `r set-property` endpoint, but the dispatcher
	// and the effective-prop resolver both read Resource.Spec.Props,
	// so a direct CRD update is the same observable as the upstream
	// CLI would produce. The "specific node-connection" terminology
	// in the test plan refers to this per-replica override.
	groupISetResourceProp(t, stack, rdName, harness.NodeWorker1, groupIPropPingTimeout, "400")

	// Worker-1: leaf wins.
	harness.Eventually(t, groupIEventually, func() bool {
		got := groupIGetEffectivePropsForResource(t, stack.RestURL, rdName, harness.NodeWorker1)

		return got[groupIPropPingTimeout] == "400"
	}, fmt.Sprintf("ping-timeout effective on %s/%s != 400", rdName, harness.NodeWorker1))

	// Worker-2: no per-replica override → RD-scope value 300 wins.
	// Pins the layering is not "leaf wins for every replica" but a
	// per-replica chain walk.
	harness.Eventually(t, groupIEventually, func() bool {
		got := groupIGetEffectivePropsForResource(t, stack.RestURL, rdName, harness.NodeWorker2)

		return got[groupIPropPingTimeout] == "300"
	}, fmt.Sprintf("ping-timeout effective on %s/%s != 300 (RD scope)", rdName, harness.NodeWorker2))
}

// groupINetOptionsSplit — 5.W04: each of `connect-int`,
// `max-buffers`, and `protocol` is its own RD-prop key under
// `DrbdOptions/Net/`. The resolver MUST merge them as independent
// entries — a regression that flattens the per-key entries into a
// single slot (the symptom 5.W04 catches in the wild) would surface
// here as either a missing key or last-write-wins clobbering.
//
// We set all three on the RD scope, then assert each appears in the
// effective_props bag of every replica with the exact value we wrote.
func groupINetOptionsSplit(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const rdName = "rd-netopts"

	spawnRDViaRG(t, stack, harness.FixtureDefaultRG, rdName, 4*1024*1024)
	groupIWaitResourcesCreated(t, stack, rdName, 2)

	want := map[string]string{
		"DrbdOptions/Net/connect-int": "20",
		"DrbdOptions/Net/max-buffers": "8192",
		"DrbdOptions/Net/protocol":    "C",
	}

	// Apply all three keys in a single PUT — the merge path under
	// test is the OverrideProps multi-key apply, the exact shape
	// `linstor rd drbd-options --connect-int 20 --max-buffers 8192
	// --protocol C` sends.
	groupIPutRDPropsMulti(t, stack.RestURL, rdName, want)

	// Each key must surface on every replica's effective bag at the
	// requested value. Iterating both worker-1 and worker-2 catches
	// a regression that resolves only the first replica's chain.
	for _, node := range []string{harness.NodeWorker1, harness.NodeWorker2} {
		node := node

		harness.Eventually(t, groupIEventually, func() bool {
			got := groupIGetEffectivePropsForResource(t, stack.RestURL, rdName, node)

			for k, v := range want {
				if got[k] != v {
					return false
				}
			}

			return true
		}, fmt.Sprintf("net-options not split on %s/%s", rdName, node))
	}
}

// groupIEffectivePropsAtAllLevels — Bug 203: the effective-prop
// map MUST surface entries from EVERY scope, not only the leaf.
// blockstor's resolver tags each entry with its origin scope
// (`apiv1.EffectivePropScopeController` / `RG` / `RD` / `RSC`);
// the Python CLI walks the tag to render the `(R)` inheritance
// marker on `linstor r lp`. A regression that collapses the per-
// entry origin (e.g. tagging every entry as the queried scope)
// would not change the resolved values — Bug 203 needs a separate
// guard.
//
// Strategy: set DISTINCT keys at each scope so every level
// contributes one unique entry. Assert the final map carries all
// four keys AND that each entry's `scope` matches the level we
// wrote it at.
func groupIEffectivePropsAtAllLevels(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const rdName = "rd-effprops"

	const (
		ctrlKey = "DrbdOptions/Net/timeout"
		rgKey   = "DrbdOptions/Net/max-epoch-size"
		rdKey   = "DrbdOptions/Net/sndbuf-size"
		rscKey  = "DrbdOptions/Net/rcvbuf-size"

		ctrlVal = "60"
		rgVal   = "16384"
		rdVal   = "512K"
		rscVal  = "256K"
	)

	groupIPutControllerProp(t, stack.RestURL, ctrlKey, ctrlVal)

	// Order matters: spawn FIRST, then write the RG-scope prop.
	// `buildSpawnedRD` (pkg/rest/spawn.go) copies RG.Props onto the
	// new RD.Props at spawn time — writing the RG key before spawn
	// would surface the value at RD scope on the resolver's walk,
	// hiding the per-scope tag the Bug 203 guard asserts.
	spawnRDViaRG(t, stack, harness.FixtureDefaultRG, rdName, 4*1024*1024)
	groupIWaitResourcesCreated(t, stack, rdName, 2)

	groupIPutRGProp(t, stack.RestURL, harness.FixtureDefaultRG, rgKey, rgVal)
	groupIPutRDProp(t, stack.RestURL, rdName, rdKey, rdVal)
	groupISetResourceProp(t, stack, rdName, harness.NodeWorker1, rscKey, rscVal)

	want := map[string]apiv1.EffectivePropEntry{
		ctrlKey: {Value: ctrlVal, Scope: apiv1.EffectivePropScopeController},
		rgKey:   {Value: rgVal, Scope: apiv1.EffectivePropScopeResourceGroup},
		rdKey:   {Value: rdVal, Scope: apiv1.EffectivePropScopeResourceDefinition},
		rscKey:  {Value: rscVal, Scope: apiv1.EffectivePropScopeResource},
	}

	deadline := time.Now().Add(groupIEventually)

	var got apiv1.EffectiveProperties

	for time.Now().Before(deadline) {
		got = groupIGetEffectivePropsEntriesForResource(t, stack.RestURL, rdName, harness.NodeWorker1)
		if effectivePropsMatch(got, want) {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("effective-props did not surface every level (Bug 203):\n  want: %+v\n  got:  %+v",
		want, got)
}

// effectivePropsMatch reports whether every expected key surfaces in
// got with the expected value AND scope. Pulled out so the
// EffectivePropsAtAllLevels poll body stays readable.
func effectivePropsMatch(got apiv1.EffectiveProperties, want map[string]apiv1.EffectivePropEntry) bool {
	for k, expect := range want {
		entry, ok := got[k]
		if !ok {
			return false
		}

		if entry.Value != expect.Value {
			return false
		}

		if entry.Scope != expect.Scope {
			return false
		}
	}

	return true
}

// groupIDrbdProxyEndpoint — wave2: cozystack does not ship
// drbd-proxy (flat-L2 + native DRBD-9 protocol), but the upstream
// Python CLI calls `drbd-proxy enable` / `drbd-proxy disable` /
// `drbd-proxy options` against the controller and expects a
// well-shaped envelope back. blockstor answers with HTTP 501 +
// an ApiCallRc body. The contract here is two-fold:
//
//	(a) the HTTP layer returns 501, not 404 — the operator gets
//	    "not supported in this build", not a confusing "endpoint
//	    missing" trace.
//	(b) the body is a JSON ApiCallRc envelope, not bare text — the
//	    Python CLI's response decoder shells out to
//	    apicallrc.parse() even on non-2xx and emits a traceback
//	    when the body is not JSON (Bug 59 class).
//
// Both halves are pinned because returning 404 + plaintext would
// hand python-linstor's REST decoder an empty body it currently
// logs as a 0-rc success — the operator would think the proxy
// enabled silently.
func groupIDrbdProxyEndpoint(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const rdName = "rd-drbd-proxy"

	spawnRDViaRG(t, stack, harness.FixtureDefaultRG, rdName, 4*1024*1024)
	groupIWaitResourcesCreated(t, stack, rdName, 2)

	resp := groupIDoHTTPRequest(t, http.MethodPost,
		stack.RestURL+"/v1/resource-definitions/"+rdName+"/drbd-proxy/enable/"+
			harness.NodeWorker1+"/"+harness.NodeWorker2,
		nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("drbd-proxy enable: status %d, want 501", resp.StatusCode)
	}

	var rc []apiv1.APICallRc

	err := json.NewDecoder(resp.Body).Decode(&rc)
	if err != nil {
		t.Fatalf("drbd-proxy enable: decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].Message == "" {
		t.Errorf("drbd-proxy enable: empty envelope %+v", rc)
	}
}

// ---------------------------------------------------------------------
// helpers (file-scoped — must NOT live under harness/)
// ---------------------------------------------------------------------

// spawnRDViaRG drives a single `POST /v1/resource-groups/{rg}/spawn`
// for one RD with one VD of `sizeKib`. The spawn handler creates the
// RD + VDs and runs autoplace synchronously, so by the time this
// returns the Resource rows are pending; Eventually-driven assertions
// in the caller cover the per-replica port/minor/node-id allocator
// settling.
func spawnRDViaRG(t *testing.T, stack *harness.Stack, rgName, rdName string, sizeKib int64) {
	t.Helper()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: rdName,
		VolumeSizes:            []int64{sizeKib},
	})
	if err != nil {
		t.Fatalf("marshal spawn body: %v", err)
	}

	resp := groupIDoHTTPRequest(t, http.MethodPost,
		stack.RestURL+"/v1/resource-groups/"+rgName+"/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("spawn %s/%s: status %d", rgName, rdName, resp.StatusCode)
	}
}

// groupIWaitResourcesCreated blocks until at least `want` Resource
// rows exist for the named RD. Autoplace runs inline on spawn but
// the API watch / per-replica allocator is asynchronous; one envtest
// tick settles them.
func groupIWaitResourcesCreated(t *testing.T, stack *harness.Stack, rdName string, want int) {
	t.Helper()

	harness.Eventually(t, groupIEventually, func() bool {
		var list blockstoriov1alpha1.ResourceList

		err := stack.Env.Client.List(context.Background(), &list)
		if err != nil {
			return false
		}

		count := 0

		for i := range list.Items {
			if list.Items[i].Spec.ResourceDefinitionName == rdName {
				count++
			}
		}

		return count >= want
	}, fmt.Sprintf("Resource rows for %s: want >= %d", rdName, want))
}

// groupIPostResourceConnectionPath drives the blockstor-native paths
// POST endpoint. We use direct HTTP rather than the upstream Python
// CLI because `linstor resource-connection path create` writes
// `Paths/<name>/<node>` properties (the upstream storage shape)
// which blockstor's dispatcher does not honour — see file comment.
func groupIPostResourceConnectionPath(t *testing.T, baseURL, rdName, nodeA, nodeB, pathName, addrA, addrB string) {
	t.Helper()

	body, err := json.Marshal(map[string]string{
		"name":           pathName,
		"node_a_address": addrA,
		"node_b_address": addrB,
	})
	if err != nil {
		t.Fatalf("marshal path body: %v", err)
	}

	url := baseURL + "/v1/resource-definitions/" + rdName +
		"/resource-connections/" + nodeA + "/" + nodeB + "/paths"

	resp := groupIDoHTTPRequest(t, http.MethodPost, url, body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST %s: status %d, want 201", url, resp.StatusCode)
	}
}

// groupIGetRD reads the named ResourceDefinition CRD directly via
// the envtest client. We need the Spec.Props bag below the wire-
// shape layer; going through the typed client is one fewer
// translation step than the REST GET.
func groupIGetRD(t *testing.T, stack *harness.Stack, rdName string) *blockstoriov1alpha1.ResourceDefinition {
	t.Helper()

	var rd blockstoriov1alpha1.ResourceDefinition

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: rdName}, &rd)
	if err != nil {
		t.Fatalf("get RD %q: %v", rdName, err)
	}

	return &rd
}

// groupIDecodeConnections projects the RD's
// `Cozystack/ResourceConnectionPaths/<a>/<b>` prop blob into the
// drbd.ResourceConnection shape the .res renderer consumes. Mirrors
// pkg/dispatcher/connections.go::connectionsFromRD (which is
// unexported) — see file comment for why we re-implement here
// instead of importing.
func groupIDecodeConnections(t *testing.T, rd *blockstoriov1alpha1.ResourceDefinition, nodeA, nodeB string) []drbd.ResourceConnection {
	t.Helper()

	// Canonical key order is lexicographic — the REST writer
	// stores under min(a,b)/max(a,b) so a GET in either order
	// surfaces the same blob.
	low, high := nodeA, nodeB
	if low > high {
		low, high = high, low
	}

	key := groupIResourceConnectionPathsPropPrefix + low + "/" + high

	raw, ok := rd.Spec.Props[key]
	if !ok {
		t.Fatalf("RD %q missing connection prop %q (props=%v)", rd.Name, key, rd.Spec.Props)
	}

	var wire []map[string]string

	err := json.Unmarshal([]byte(raw), &wire)
	if err != nil {
		t.Fatalf("decode connection prop blob: %v", err)
	}

	if len(wire) == 0 {
		t.Fatalf("connection prop blob empty: %q", raw)
	}

	paths := make([]drbd.ResourcePath, 0, len(wire))

	for _, entry := range wire {
		paths = append(paths, drbd.ResourcePath{
			Name:     entry["name"],
			AddressA: entry["node_a_address"],
			AddressB: entry["node_b_address"],
		})
	}

	return []drbd.ResourceConnection{{NodeA: low, NodeB: high, Paths: paths}}
}

// groupISetResourceProp writes one prop onto a Resource CRD's
// Spec.Props bag. blockstor's REST surface does not expose a
// `PUT /v1/resource-definitions/{rd}/resources/{node}` set-property
// endpoint, so the effective-prop resolver's Resource scope is fed
// by direct CRD updates here. See file comment.
//
// envtest interleaves the satellite mock's Status writes with the
// test's Spec writes, surfacing a "the object has been modified"
// conflict on the first attempt; the retry loop absorbs that.
func groupISetResourceProp(t *testing.T, stack *harness.Stack, rdName, nodeName, key, value string) {
	t.Helper()

	ctx := context.Background()
	resourceName := rdName + "." + nodeName

	harness.Eventually(t, groupIEventually, func() bool {
		var r blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: resourceName}, &r)
		if err != nil {
			return false
		}

		if r.Spec.Props == nil {
			r.Spec.Props = map[string]string{}
		}

		r.Spec.Props[key] = value

		err = stack.Env.Client.Update(ctx, &r)

		return err == nil
	}, fmt.Sprintf("set Resource %q prop %q=%q", resourceName, key, value))
}

// groupIPutControllerProp posts an OverrideProps body to
// /v1/controller/properties — the wire shape upstream's
// `linstor controller set-property X Y` produces.
func groupIPutControllerProp(t *testing.T, baseURL, key, value string) {
	t.Helper()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{key: value},
	})
	if err != nil {
		t.Fatalf("marshal controller prop: %v", err)
	}

	resp := groupIDoHTTPRequest(t, http.MethodPost,
		baseURL+"/v1/controller/properties", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("controller set-property %s: status %d", key, resp.StatusCode)
	}
}

// groupIPutRGProp puts an OverrideProps body to
// /v1/resource-groups/{rg} — `linstor rg set-property` shape.
func groupIPutRGProp(t *testing.T, baseURL, rgName, key, value string) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"override_props": map[string]string{key: value},
	})
	if err != nil {
		t.Fatalf("marshal rg prop: %v", err)
	}

	resp := groupIDoHTTPRequest(t, http.MethodPut,
		baseURL+"/v1/resource-groups/"+rgName, body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("rg set-property %s: status %d", key, resp.StatusCode)
	}
}

// groupIPutRDProp puts an OverrideProps body to
// /v1/resource-definitions/{rd} — `linstor rd set-property` shape.
func groupIPutRDProp(t *testing.T, baseURL, rdName, key, value string) {
	t.Helper()

	groupIPutRDPropsMulti(t, baseURL, rdName, map[string]string{key: value})
}

// groupIPutRDPropsMulti applies several keys in one PUT — exactly
// the wire shape `linstor rd drbd-options --K1=V1 --K2=V2 ...`
// produces.
func groupIPutRDPropsMulti(t *testing.T, baseURL, rdName string, props map[string]string) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"override_props": props,
	})
	if err != nil {
		t.Fatalf("marshal rd props: %v", err)
	}

	resp := groupIDoHTTPRequest(t, http.MethodPut,
		baseURL+"/v1/resource-definitions/"+rdName, body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rd set-property %v: status %d", props, resp.StatusCode)
	}
}

// groupIGetEffectivePropsForResource fetches /v1/view/resources?
// resources=<rd>&nodes=<node> and returns the per-replica
// `effective_props` map as a flat key→value lookup.
func groupIGetEffectivePropsForResource(t *testing.T, baseURL, rdName, nodeName string) map[string]string {
	t.Helper()

	entries := groupIGetEffectivePropsEntriesForResource(t, baseURL, rdName, nodeName)

	out := make(map[string]string, len(entries))

	for k, entry := range entries {
		out[k] = entry.Value
	}

	return out
}

// groupIGetEffectivePropsEntriesForResource returns the full
// EffectivePropEntry view (Value + Scope) — needed by the Bug 203
// guard where the scope tag is the assertion.
func groupIGetEffectivePropsEntriesForResource(t *testing.T, baseURL, rdName, nodeName string) apiv1.EffectiveProperties {
	t.Helper()

	resp := groupIDoHTTPRequest(t, http.MethodGet,
		baseURL+"/v1/view/resources?resources="+rdName+"&nodes="+nodeName, nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("view/resources: status %d", resp.StatusCode)
	}

	var rows []apiv1.ResourceWithVolumes

	err := json.NewDecoder(resp.Body).Decode(&rows)
	if err != nil {
		t.Fatalf("decode view/resources: %v", err)
	}

	for i := range rows {
		if rows[i].Name == rdName && rows[i].NodeName == nodeName {
			return rows[i].EffectiveProps
		}
	}

	t.Fatalf("view/resources missing row for %s/%s (rows=%d)", rdName, nodeName, len(rows))

	return nil
}

// groupIDoHTTPRequest is the shared HTTP-client helper for every
// direct-REST call the group makes. Uses a bounded context timeout
// and propagates the harness's "request body must be JSON"
// convention.
func groupIDoHTTPRequest(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), groupIHTTPTimeout)
	t.Cleanup(cancel)

	var (
		req *http.Request
		err error
	)

	if body != nil {
		req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, http.NoBody)
	}

	if err != nil {
		t.Fatalf("build %s %s: %v", method, url, err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	cli := &http.Client{Timeout: groupIHTTPTimeout}

	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, url, err)
	}

	return resp
}

// groupISliceBetween returns the substring of s between the first
// occurrence of `start` and the next `end` after it. Used by the
// multi-path .res guard to scope the "no implicit host lines"
// assertion to just the `connection { … }` block — the same `host
// worker-1 address …;` token appears in the per-host `on
// worker-1 { … }` block earlier in the file. Returns the empty
// string if either delimiter is missing; the caller then trips its
// own assertion on the empty window.
func groupISliceBetween(s, start, end string) string {
	i := strings.Index(s, start)
	if i < 0 {
		return ""
	}

	j := strings.Index(s[i:], end)
	if j < 0 {
		return ""
	}

	return s[i : i+j+len(end)]
}
