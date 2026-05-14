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

// Group C — Resource Group integration tests. See docs/test-strategy.md
// row "C — Resource Group" for the per-test bug-guard mapping. Every
// test in this file drives the LINSTOR REST surface via the native
// `linstor` CLI plus targeted golinstor / raw-HTTP calls; the harness
// satellite mock advances Resource.Status.DrbdState to UpToDate so
// the autoplaced replicas reach a steady state within the
// `Eventually` budget.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	lapi "github.com/LINBIT/golinstor/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// rgcHTTPTimeout caps every raw-HTTP request made by this file.
// Picked to be comfortably longer than the manager's reconcile
// debounce so a fast in-process apiserver doesn't fight the test
// while still failing fast on a wedged handler.
const rgcHTTPTimeout = 15 * time.Second

// rgcReadyTimeout caps every Eventually() loop that waits for
// reconciler-side state to converge (e.g. autoplaced Resources
// landing on satellites). Mirrors the harness asserts default.
const rgcReadyTimeout = 30 * time.Second

// TestGroupCRGListCreateDelete: basic Resource Group CRUD via the
// upstream `linstor` CLI. Wire-shape guard — the Python CLI's
// `--machine-readable` envelope must round-trip create + list +
// delete without a python traceback.
func TestGroupCRGListCreateDelete(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const rgName = "rg-crud"

	// CREATE via CLI — `linstor rg c` posts JSON to /v1/resource-groups.
	cli.Run(t, "resource-group", "create", rgName, "--place-count", "2")

	// LIST — must contain both the fixture's `default` and our new RG.
	rows := cli.JSON(t, "resource-group", "list")

	if !rgcRowsContainName(rows, rgName) {
		t.Fatalf("rg list missing %q after create; rows=%v", rgName, rgRowNames(rows))
	}

	if !rgcRowsContainName(rows, harness.FixtureDefaultRG) {
		t.Fatalf("rg list missing fixture default %q; rows=%v",
			harness.FixtureDefaultRG, rgRowNames(rows))
	}

	// DELETE via CLI — must exit 0 and remove the entry.
	cli.Run(t, "resource-group", "delete", rgName)

	rows = cli.JSON(t, "resource-group", "list")

	if rgcRowsContainName(rows, rgName) {
		t.Fatalf("rg list still contains %q after delete; rows=%v", rgName, rgRowNames(rows))
	}
}

// TestGroupCRGSpawnResources: `linstor rg spawn-resources <rg> <name>`
// must create the RD plus exactly N Resource records — where N is
// the RG's place-count. The autoplacer runs inline as part of the
// spawn (`spawnAutoplace` in pkg/rest/spawn.go), so a successful
// spawn is observably equivalent to "RD exists + 2 R landed +
// satellite has driven them to UpToDate".
func TestGroupCRGSpawnResources(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	lc := rgcLapiClient(t, stack.RestURL)

	const (
		rgName = "rg-spawn"
		rdName = "pvc-spawn"
	)

	cli.Run(t, "resource-group", "create", rgName,
		"--place-count", "2", "--storage-pool", "lvm-thin")

	// 4 MiB so the over-subscription gate (10 GiB fixture pool)
	// has zero chance of tripping us.
	cli.Run(t, "resource-group", "spawn-resources", rgName, rdName, "4M")

	// Wait for both Resources to land via the lapi client — the
	// REST handler kicks the placer synchronously but the satellite
	// mock takes one tick to stamp DrbdState=UpToDate.
	harness.Eventually(t, rgcReadyTimeout, func() bool {
		resList, err := lc.Resources.GetAll(t.Context(), rdName)
		if err != nil {
			return false
		}

		diskful := 0

		for i := range resList {
			if rgcResourceIsDiskful(&resList[i]) {
				diskful++
			}
		}

		return diskful >= 2
	}, "rg spawn-resources did not produce >=2 diskful Resources for "+rdName)

	// Sanity: the RD itself is back-pointing at our RG.
	rd, err := lc.ResourceDefinitions.Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("get RD %q: %v", rdName, err)
	}

	if rd.ResourceGroupName != rgName {
		t.Fatalf("RD %q ResourceGroupName: got %q, want %q",
			rdName, rd.ResourceGroupName, rgName)
	}
}

// TestGroupCRGDeleteRefusesIfHasRDs pins scenario 9.W02 / wave1 4.5 /
// **Bug 11**: a `rg d` while ResourceDefinitions still reference the
// group must be REFUSED with 409 + FAIL_EXISTS_RSC_DFN — there is no
// `--force`. The refusal MUST run BEFORE any persistence so the RG
// can't end up half-deleted with orphan RDs pointing at it.
func TestGroupCRGDeleteRefusesIfHasRDs(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const (
		rgName = "rg-busy"
		rdName = "pvc-blocker"
	)

	cli.Run(t, "resource-group", "create", rgName,
		"--place-count", "2", "--storage-pool", "lvm-thin")
	cli.Run(t, "resource-group", "spawn-resources", rgName, rdName, "4M")

	// Raw HTTP DELETE — the python CLI swallows the 409 envelope
	// into an exit-1 message; we want the wire status code so the
	// bug guard is unambiguous.
	resp := rgcHTTPDelete(t, stack.RestURL+"/v1/resource-groups/"+rgName)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("rg d %q: got status %d, want 409 + FAIL_EXISTS_RSC_DFN; body=%s",
			rgName, resp.StatusCode, string(body))
	}

	var rc []apiv1.APICallRc

	err := json.NewDecoder(resp.Body).Decode(&rc)
	if err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) != 1 {
		t.Fatalf("ApiCallRc envelope length: got %d, want 1; rc=%+v", len(rc), rc)
	}

	if !strings.Contains(rc[0].Message, "existing resource definitions") &&
		!strings.Contains(rc[0].Message, "resource-definitions exist") {
		t.Fatalf("refusal message missing the upstream substring; got %q", rc[0].Message)
	}

	// And critically: the RG must still be there — refusal is
	// pre-mutation.
	rows := cli.JSON(t, "resource-group", "list")

	if !rgcRowsContainName(rows, rgName) {
		t.Fatalf("RG %q vanished despite 409 refusal — half-deletion bug; rows=%v",
			rgName, rgRowNames(rows))
	}
}

// TestGroupCRGModifyReAutoplaces pins **Bug 60**: raising the
// RG.SelectFilter.PlaceCount via `rg modify` must trigger the
// deferred-rebalance pipeline. Phase 11.x split the reconciler off
// from the REST process, so the cross-process signal is the
// `blockstor.io/rebalance-pending` annotation on the parent RG —
// this is what RGRebalanceReconciler observes to re-run autoplace
// across every child RD.
func TestGroupCRGModifyReAutoplaces(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const (
		rgName = "rg-rebal"
		rdName = "pvc-rebal"
	)

	cli.Run(t, "resource-group", "create", rgName,
		"--place-count", "2", "--storage-pool", "lvm-thin")
	cli.Run(t, "resource-group", "spawn-resources", rgName, rdName, "4M")

	// Raw PUT — golinstor's RG modify hides the annotation; we want
	// the annotation read-back via the apiserver to be the assertion
	// authority for the bug guard.
	body, err := json.Marshal(apiv1.ResourceGroup{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	})
	if err != nil {
		t.Fatalf("marshal modify body: %v", err)
	}

	resp := rgcHTTPPut(t, stack.RestURL+"/v1/resource-groups/"+rgName, body)

	rbody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rg modify %q: got status %d, want 200; body=%s",
			rgName, resp.StatusCode, string(rbody))
	}

	// REST response must surface the deferred work as a second
	// ApiCallRc entry — see pkg/rest/resource_groups.go's
	// rebalanceMessage. Operators see this at the original call
	// site rather than only in the controller log.
	var reply []apiv1.APICallRc

	err = json.Unmarshal(rbody, &reply)
	if err != nil {
		t.Fatalf("decode modify reply: %v", err)
	}

	if len(reply) < 2 {
		t.Fatalf("rg modify reply must include a rebalance advisory; got %d entries: %+v",
			len(reply), reply)
	}

	// The annotation gets stamped synchronously on the persisted
	// CRD inside the REST handler. Read via the typed client to
	// pin the cross-process signal contract.
	rg := harness.MustGet(t, stack.Env.Client, rgName, &blockstoriov1alpha1.ResourceGroup{})

	stamp, ok := rg.Annotations[apiv1.AnnotationRGRebalancePending]
	if !ok {
		t.Fatalf("rebalance-pending annotation missing on RG %q after modify; annotations=%v",
			rgName, rg.Annotations)
	}

	if stamp == "" {
		t.Fatalf("rebalance-pending stamp must be a non-empty RFC3339 timestamp; got %q", stamp)
	}

	// And the persisted SelectFilter.PlaceCount reflects the new value
	// — the merge didn't clobber the rebalance trigger.
	if rg.Spec.SelectFilter.PlaceCount != 3 {
		t.Fatalf("RG SelectFilter.PlaceCount after modify: got %d, want 3",
			rg.Spec.SelectFilter.PlaceCount)
	}
}

// TestGroupCRGEffectivePropsChain pins **Bug 54**: a property set
// at RG scope must be visible to spawned RDs and to per-replica
// Resources via the merged effective-props view. `/v1/view/resources`
// returns `effective_props` per replica with the scope tag the value
// was inherited from — `RG` here, since we set the prop nowhere
// else.
func TestGroupCRGEffectivePropsChain(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const (
		rgName  = "rg-props"
		rdName  = "pvc-props"
		propKey = "Aux/group-c/inheritance-probe"
		propVal = "set-at-rg-scope"
	)

	cli.Run(t, "resource-group", "create", rgName,
		"--place-count", "2", "--storage-pool", "lvm-thin")

	// `rg sp` on an Aux/* key — the CLI translates this into a PUT
	// /v1/resource-groups/{rg} with override_props. The --aux flag
	// rewrites the prefix; we use the canonical key directly so the
	// stored value matches the lookup we do below.
	cli.Run(t, "resource-group", "set-property", rgName, propKey, propVal)

	cli.Run(t, "resource-group", "spawn-resources", rgName, rdName, "4M")

	// Wait for the placer to land replicas — effective_props is
	// computed per-Resource by `/v1/view/resources`'s buildResourceView.
	harness.Eventually(t, rgcReadyTimeout, func() bool {
		rows, err := rgcFetchView(stack.RestURL, rdName)
		if err != nil {
			return false
		}

		return len(rows) >= 2
	}, "rg spawn-resources did not produce >=2 view rows for "+rdName)

	rows, err := rgcFetchView(stack.RestURL, rdName)
	if err != nil {
		t.Fatalf("fetch /v1/view/resources: %v", err)
	}

	if len(rows) == 0 {
		t.Fatalf("/v1/view/resources returned 0 rows for %q", rdName)
	}

	for i := range rows {
		eff := rows[i].EffectiveProps
		if eff == nil {
			t.Fatalf("row[%d] (%s.%s) has nil effective_props; want %q=%q via RG",
				i, rows[i].Name, rows[i].NodeName, propKey, propVal)
		}

		entry, ok := eff[propKey]
		if !ok {
			t.Fatalf("row[%d] (%s.%s) missing effective prop %q; got keys=%v",
				i, rows[i].Name, rows[i].NodeName, propKey, sortedKeys(eff))
		}

		if entry.Value != propVal {
			t.Fatalf("row[%d] effective_props[%q].value: got %q, want %q",
				i, propKey, entry.Value, propVal)
		}

		// Bug 54: the value must propagate down the chain. Spawn
		// copies the RG.Props bag onto the freshly-built RD (see
		// pkg/rest/spawn.go's buildSpawnedRD), so the merged
		// effective-props lookup legitimately resolves to either
		// RG (untouched-on-RD branch) or RD (copy-on-spawn branch).
		// The bug guard is that the value SHOWS UP, not which scope
		// claims ownership — the latter is an implementation
		// choice the spawn handler is free to evolve.
		if entry.Scope != apiv1.EffectivePropScopeResourceGroup &&
			entry.Scope != apiv1.EffectivePropScopeResourceDefinition {
			t.Fatalf("row[%d] effective_props[%q].scope: got %q, want %q or %q (Bug 54)",
				i, propKey, entry.Scope,
				apiv1.EffectivePropScopeResourceGroup,
				apiv1.EffectivePropScopeResourceDefinition)
		}
	}
}

// TestGroupCRGSetPropertyDrbdNet pins scenario 5.W03: setting a
// DrbdOptions/Net/* property on an RG must propagate to every
// Resource spawned from it. We assert the merged effective-props
// view since the harness satellite mock is a status-projection
// shim that does NOT render .res files — but it DOES go through
// the same REST effective-props path that the production
// .res-renderer reads, so this pins the upstream contract one
// hop short of the file system.
func TestGroupCRGSetPropertyDrbdNet(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const (
		rgName  = "rg-net"
		rdName  = "pvc-net"
		propKey = "DrbdOptions/Net/ping-timeout"
		propVal = "500"
	)

	cli.Run(t, "resource-group", "create", rgName,
		"--place-count", "2", "--storage-pool", "lvm-thin")

	cli.Run(t, "resource-group", "set-property", rgName, propKey, propVal)

	// `rg list-properties` round-trip on the same prop — the Python
	// CLI parses the GET /v1/resource-groups/{rg}.props field and
	// renders it as a table. We just assert the value lands.
	props := rgcRGProps(t, stack.RestURL, rgName)

	if props[propKey] != propVal {
		t.Fatalf("RG.props[%q] after set-property: got %q, want %q",
			propKey, props[propKey], propVal)
	}

	cli.Run(t, "resource-group", "spawn-resources", rgName, rdName, "4M")

	harness.Eventually(t, rgcReadyTimeout, func() bool {
		rows, err := rgcFetchView(stack.RestURL, rdName)
		if err != nil {
			return false
		}

		return len(rows) >= 2
	}, "rg spawn-resources did not produce >=2 view rows for "+rdName)

	rows, err := rgcFetchView(stack.RestURL, rdName)
	if err != nil {
		t.Fatalf("fetch /v1/view/resources: %v", err)
	}

	for i := range rows {
		eff := rows[i].EffectiveProps

		entry, ok := eff[propKey]
		if !ok {
			t.Fatalf("row[%d] (%s.%s) missing inherited DrbdOptions/Net key %q; got keys=%v",
				i, rows[i].Name, rows[i].NodeName, propKey, sortedKeys(eff))
		}

		if entry.Value != propVal {
			t.Fatalf("row[%d] effective_props[%q].value: got %q, want %q",
				i, propKey, entry.Value, propVal)
		}

		// Inherited from the RG. Scenario 5.W03: the value must
		// reach satellite scope so the .res renderer can emit
		// `net { ping-timeout 500; }`. Spawn copies the bag from
		// RG → RD, so the highest-precedence origin is either RG
		// (untouched RD) or RD (post-copy) — both are inheritance,
		// neither is a per-Resource override. R-scope here would
		// mean a per-Resource set-property had bled into the test
		// and the inheritance chain broke.
		if entry.Scope != apiv1.EffectivePropScopeResourceGroup &&
			entry.Scope != apiv1.EffectivePropScopeResourceDefinition {
			t.Fatalf("row[%d] DrbdOptions/Net inheritance scope: got %q, want %q or %q",
				i, entry.Scope,
				apiv1.EffectivePropScopeResourceGroup,
				apiv1.EffectivePropScopeResourceDefinition)
		}
	}
}

// TestGroupCRGListPropertiesContract pins the wire shape `linstor
// rg list-properties` consumes: GET /v1/resource-groups/{rg}.props
// must be a non-nil string→string map even when the RG has zero
// properties. Bug 60-class envelope guard — a `null` body crashes
// the Python CLI's table renderer with TypeError before the
// operator sees the empty-properties row.
func TestGroupCRGListPropertiesContract(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const rgName = "rg-list-props"

	cli.Run(t, "resource-group", "create", rgName,
		"--place-count", "2", "--storage-pool", "lvm-thin")

	// Empty-props branch first — wire-shape guard.
	got := rgcRGRaw(t, stack.RestURL, rgName)

	if raw, ok := got["props"]; ok {
		// `omitempty` is allowed (key absent) or an empty object.
		// `null` is NOT — the Python CLI crashes on it. Re-encode
		// to JSON to assert the shape unambiguously.
		buf, _ := json.Marshal(raw)
		if string(buf) == "null" {
			t.Fatalf("GET RG.props rendered as JSON null on empty RG — Python CLI parser bug guard")
		}
	}

	// Populate three namespaced keys spanning the canonical
	// LINSTOR Props prefixes (DrbdOptions/Net/, Aux/, FileSystem/).
	cli.Run(t, "resource-group", "set-property", rgName,
		"DrbdOptions/Net/protocol", "C")
	cli.Run(t, "resource-group", "set-property", rgName,
		"Aux/team", "storage")
	cli.Run(t, "resource-group", "set-property", rgName,
		"FileSystem/Type", "xfs")

	props := rgcRGProps(t, stack.RestURL, rgName)

	want := map[string]string{
		"DrbdOptions/Net/protocol": "C",
		"Aux/team":                 "storage",
		"FileSystem/Type":          "xfs",
	}

	for k, v := range want {
		if props[k] != v {
			t.Fatalf("rg list-properties: got [%q]=%q, want %q (rendered map=%v)",
				k, props[k], v, props)
		}
	}
}

// ---------------- helpers ----------------

// rgcLapiClient builds a golinstor REST client against the harness
// stack — used for typed CRUD assertions that the CLI obscures.
func rgcLapiClient(t *testing.T, baseURL string) *lapi.Client {
	t.Helper()

	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse REST URL %q: %v", baseURL, err)
	}

	c, err := lapi.NewClient(lapi.BaseURL(u))
	if err != nil {
		t.Fatalf("lapi.NewClient: %v", err)
	}

	return c
}

// rgcHTTPDelete issues a context-bounded DELETE against the REST
// server. Pulled out of every test so the timeout / context wiring
// stays uniform.
func rgcHTTPDelete(t *testing.T, addr string) *http.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), rgcHTTPTimeout)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, addr, http.NoBody)
	if err != nil {
		t.Fatalf("build DELETE %s: %v", addr, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", addr, err)
	}

	return resp
}

// rgcHTTPPut issues a context-bounded PUT against the REST server.
func rgcHTTPPut(t *testing.T, addr string, body []byte) *http.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), rgcHTTPTimeout)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, addr, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build PUT %s: %v", addr, err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", addr, err)
	}

	return resp
}

// rgcHTTPGetJSON GETs `addr` and JSON-decodes the body into `out`.
func rgcHTTPGetJSON(t *testing.T, addr string, out any) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), rgcHTTPTimeout)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr, http.NoBody)
	if err != nil {
		t.Fatalf("build GET %s: %v", addr, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", addr, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: status %d; body=%s", addr, resp.StatusCode, string(body))
	}

	err = json.NewDecoder(resp.Body).Decode(out)
	if err != nil {
		t.Fatalf("decode %s: %v", addr, err)
	}
}

// rgcFetchView queries /v1/view/resources?resources=<rd> and
// returns the parsed ResourceWithVolumes slice.
func rgcFetchView(baseURL, rdName string) ([]apiv1.ResourceWithVolumes, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rgcHTTPTimeout)
	defer cancel()

	addr := baseURL + "/v1/view/resources?resources=" + url.QueryEscape(rdName)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return nil, &rgcViewError{Status: resp.StatusCode, Body: string(body)}
	}

	var rows []apiv1.ResourceWithVolumes

	err = json.NewDecoder(resp.Body).Decode(&rows)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// rgcViewError captures a non-200 from /v1/view/resources so
// callers can keep the t.Fatal-vs-Eventually distinction.
type rgcViewError struct {
	Status int
	Body   string
}

func (e *rgcViewError) Error() string {
	return "view/resources: status " + strconvI(e.Status) + " body " + e.Body
}

func strconvI(v int) string {
	const decShift = 10

	if v == 0 {
		return "0"
	}

	neg := v < 0
	if neg {
		v = -v
	}

	buf := [20]byte{}
	i := len(buf)

	for v > 0 {
		i--

		buf[i] = byte('0' + v%decShift)
		v /= decShift
	}

	if neg {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}

// rgcRGProps fetches GET /v1/resource-groups/{rg} and returns
// the parsed `props` map. Empty / absent maps degrade to a
// non-nil empty map so callers can index unconditionally.
func rgcRGProps(t *testing.T, baseURL, rgName string) map[string]string {
	t.Helper()

	raw := rgcRGRaw(t, baseURL, rgName)

	props, ok := raw["props"].(map[string]any)
	if !ok {
		return map[string]string{}
	}

	out := make(map[string]string, len(props))

	for k, v := range props {
		s, ok := v.(string)
		if !ok {
			continue
		}

		out[k] = s
	}

	return out
}

// rgcRGRaw fetches the raw JSON object for one RG.
func rgcRGRaw(t *testing.T, baseURL, rgName string) map[string]any {
	t.Helper()

	var got map[string]any

	rgcHTTPGetJSON(t, baseURL+"/v1/resource-groups/"+rgName, &got)

	return got
}

// rgcRowsContainName scans a `linstor rg l` JSON shape (post-flatten)
// for a row whose `name` equals `want`. The CLI's wire shape uses
// the field `name`, mirroring the LINSTOR `ResourceGroup` openapi
// type — see pkg/api/v1/resource_group.go.
func rgcRowsContainName(rows []map[string]any, want string) bool {
	for _, row := range rows {
		name, _ := row["name"].(string)
		if name == want {
			return true
		}
	}

	return false
}

// rgRowNames extracts the `.name` field from every row in a
// flattened CLI list response — used only in failure messages so
// the operator sees what WAS in the list when the assert tripped.
func rgRowNames(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))

	for _, row := range rows {
		name, _ := row["name"].(string)
		if name != "" {
			out = append(out, name)
		}
	}

	sort.Strings(out)

	return out
}

// sortedKeys returns the keys of an effective-props map in stable
// order — used only in failure messages.
func sortedKeys(eff apiv1.EffectiveProperties) []string {
	out := make([]string, 0, len(eff))
	for k := range eff {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// rgcResourceIsDiskful filters out DISKLESS / TIE_BREAKER replicas
// — the rg spawn-resources flow with --place-count=2 produces
// exactly 2 diskful + (optionally) 1 tiebreaker. We assert against
// diskful so an autoplacer that later adds a tiebreaker can't
// inflate the count past N.
func rgcResourceIsDiskful(rsc *lapi.Resource) bool {
	for _, f := range rsc.Flags {
		if f == "DISKLESS" || f == "TIE_BREAKER" {
			return false
		}
	}

	return true
}
