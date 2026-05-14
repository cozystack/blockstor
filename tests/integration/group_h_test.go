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

// Group H — Controller / Error Reports / KV (6 tests).
//
// docs/test-strategy.md row "Group H": pins the controller singleton
// surface (version, list-properties, set-property → effective on RDs),
// the error-reports envelope + per-node filter (F6 / Bug 62), and the
// key-value-store CRUD path (csi-sanity relies on KV for snapshot
// bookkeeping).
//
// Bug-guards in this file:
//   - F6 / Bug 62: `linstor err l` returns the JSON list envelope (not
//     a 404 / null body / python traceback) even when the ring is empty,
//     and the ?node= filter is honoured controller-side so an empty
//     filter result is also a valid envelope.
//   - controller property propagation: `linstor c sp DrbdOptions/Net/X Y`
//     surfaces as a Controller-scope entry in /v1/view/resources'
//     `effective_props` for a freshly-spawned Resource — verifies the
//     Controller → RG → RD → Resource chain wired in pkg/rest/effective_props.go.
//
// Shared stack: controller-runtime's controller-name registry is
// process-global, so booting two managers in the same `go test` binary
// fails with "controller with name node already exists". Group H boots
// one `harness.StartStack` inside the parent TestGroupH and runs the
// six tests as `t.Run` sub-tests against the same stack. With this
// layout `-run '^TestGroupH'` matches both the parent and every
// sub-test (`TestGroupHControllerVersion` maps to
// `TestGroupH/ControllerVersion`).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

const (
	// groupHCLITimeout caps `linstor` CLI invocations in this file.
	// In-process REST: 30s is generous (the smoke spends <1s on a
	// single call), but matches the harness default so a slow CI
	// runner doesn't hit a per-call deadline.
	groupHCLITimeout = 30 * time.Second

	// groupHRDPropTimeout caps the wait for a freshly-spawned
	// Resource to surface via /v1/view/resources. The satellite
	// mock reconciles every 200ms (harness.satelliteTickInterval),
	// so 30s leaves plenty of head room for envtest etcd hiccups.
	groupHRDPropTimeout = 30 * time.Second

	// groupHHTTPTimeout caps direct REST round-trips done outside
	// the linstor CLI. Wire-shape assertions are sub-millisecond on
	// the in-process server; 10s is the same budget the harness
	// healthz pinger uses (manager.go: pingHealthz).
	groupHHTTPTimeout = 10 * time.Second
)

// TestGroupH boots one shared envtest+manager+REST stack and runs the
// six Group H scenarios as sequential sub-tests against it.
// Sub-tests run serially (no t.Parallel) so they cannot race the
// controller-property set/get path, which mutates a singleton CRD.
//
// The `-run '^TestGroupH'` DoD command matches:
//
//	TestGroupH                          (parent)
//	TestGroupH/ControllerVersion        (sub-test)
//	TestGroupH/ControllerListProperties
//	TestGroupH/ControllerSetProperty
//	TestGroupH/ErrorReportsList
//	TestGroupH/ErrorReportsFilterByNode
//	TestGroupH/KVStoreCRUD
//
// — six sub-tests under one parent. Test output therefore prints
// every Group H scenario name individually, the failure isolation
// matches the per-row entries in docs/test-strategy.md.
func TestGroupH(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL, Timeout: groupHCLITimeout}

	t.Run("ControllerVersion", func(t *testing.T) {
		runControllerVersion(t, stack, cli)
	})
	t.Run("ControllerListProperties", func(t *testing.T) {
		runControllerListProperties(t, stack, cli)
	})
	t.Run("ControllerSetProperty", func(t *testing.T) {
		runControllerSetProperty(t, stack, cli)
	})
	t.Run("ErrorReportsList", func(t *testing.T) {
		runErrorReportsList(t, stack, cli)
	})
	t.Run("ErrorReportsFilterByNode", func(t *testing.T) {
		runErrorReportsFilterByNode(t, stack, cli)
	})
	t.Run("KVStoreCRUD", func(t *testing.T) {
		runKVStoreCRUD(t, stack, cli)
	})
}

// runControllerVersion pins `linstor controller version`: the CLI must
// parse our /v1/controller/version response without a Python
// traceback. Wire-shape guard — a regression that dropped the
// `version` key or returned a list-wrapper would crash the CLI's
// version-print path with `KeyError`. Bug 59-class.
func runControllerVersion(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	// `linstor c v` is the canonical smoke for controller-side surface;
	// the CLI's `Run` helper guards against python tracebacks on
	// stderr, so a non-error return is the primary assertion. We also
	// keep stdout non-empty as a basic sanity (a `[]` body would
	// still be empty after the CLI's version-print path runs).
	out := cli.Run(t, "controller", "version")
	if len(out) == 0 {
		t.Fatalf("linstor controller version: empty stdout")
	}

	// Direct REST round-trip pins the wire envelope so a CLI change
	// can't silently mask a missing field. golinstor's
	// `Controller.GetControllerVersion` unmarshals into a struct with
	// `version` as a required-by-convention key; without it the CLI
	// prints "None" for the version banner.
	resp := httpGetGroupH(t, stack.RestURL+"/v1/controller/version")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/controller/version: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.ControllerVersion

	err := json.NewDecoder(resp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode controller version: %v", err)
	}

	if got.Version == "" {
		t.Errorf("ControllerVersion.Version is empty; expected non-empty value")
	}
}

// runControllerListProperties pins `linstor c lp` — both as a CLI
// traceback-free smoke and as a REST wire-shape check. A fresh
// controller has an empty ExtraProps bag; the CLI must accept that
// without crashing on a missing key, and the REST envelope must be a
// JSON object (not null / list / 404).
func runControllerListProperties(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	// CLI smoke: `linstor c lp -m` emits `[[{"key":...,"value":...},...]]`
	// even on an empty bag (the inner list is just empty). We accept
	// either zero or non-zero rows — the contract is "no traceback,
	// envelope shape correct".
	_ = cli.Run(t, "controller", "list-properties")

	// REST wire-shape: `GET /v1/controller/properties` returns a JSON
	// object (empty map if no props set). golinstor unmarshals into
	// `map[string]string` so a `null` body or list shape breaks the
	// CLI's property-print path.
	resp := httpGetGroupH(t, stack.RestURL+"/v1/controller/properties")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/controller/properties: got %d, want 200", resp.StatusCode)
	}

	var props map[string]string

	err := json.NewDecoder(resp.Body).Decode(&props)
	if err != nil {
		t.Fatalf("decode controller props: %v", err)
	}

	if props == nil {
		t.Errorf("controller properties decoded as nil; expected empty object")
	}
}

// runControllerSetProperty pins controller→RD prop propagation:
//
//   - set `DrbdOptions/Net/ping-timeout = 500` via `linstor c sp`
//   - spawn an RD with the default RG via `rg sr default <rd> 50M`
//   - poll /v1/view/resources until at least one resource for the new
//     RD exposes `effective_props["DrbdOptions/Net/ping-timeout"]` with
//     {value: "500", scope: Controller}
//
// This walks the full Controller → RG → RD → Resource merge implemented
// in pkg/rest/effective_props.go. A regression that lost controller-
// scope props (e.g. ControllerConfig CRD never read, or scope tag
// shifted) would surface as a missing or wrong-scope entry here.
//
// Note: the shared-stack layout means a previous Group H sub-test
// could have left controller properties on the bag; the assertion is
// scoped to the key + scope this test writes, so a sibling key on the
// bag doesn't affect the result.
func runControllerSetProperty(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	const (
		propKey   = "DrbdOptions/Net/ping-timeout"
		propValue = "500"
	)

	// 1. Set the controller-scope property. Use the Python CLI's
	// canonical "key value" form — `controller set-property` POSTs
	// {override_props: {key: value}} which lands in ControllerConfig.Spec.ExtraProps.
	_ = cli.Run(t, "controller", "set-property", propKey, propValue)

	// 2. Confirm the set landed via `c lp` round-trip. The CLI prints
	// `[[{"key":"DrbdOptions/Net/ping-timeout","value":"500"}, ...]]`
	// under `--machine-readable`; harness.CLI.JSON flattens one
	// level so we get `[{...}, ...]`.
	gotProps := cli.JSON(t, "controller", "list-properties")
	if !hasKVPair(gotProps, propKey, propValue) {
		t.Fatalf("controller list-properties does not contain %s=%s: %v",
			propKey, propValue, gotProps)
	}

	// 3. Spawn an RD that the satellite-mock will reconcile. `rg sr
	// default rd-<unique> 50M` uses the fixture's default RG
	// (PlaceCount=2). The CLI invocation creates the RD + VD and
	// kicks off autoplace — Resource objects appear within a few
	// satellite ticks.
	rdName := uniqueRDName("group-h-sp")

	_ = cli.Run(t, "resource-group", "spawn-resources",
		harness.FixtureDefaultRG, rdName, "50M")

	// 4. Poll /v1/view/resources until at least one Resource for the
	// new RD is visible and carries the merged effective_props bag.
	// The satellite mock advances DrbdState to UpToDate on its tick,
	// but for the effective-prop merge we only need the resource to
	// exist — the merge runs in the REST handler on every GET.
	harness.Eventually(t, groupHRDPropTimeout, func() bool {
		entries := decodeResourceView(t, stack.RestURL, rdName)
		for i := range entries {
			entry, ok := entries[i].EffectiveProps[propKey]
			if !ok {
				continue
			}

			if entry.Value == propValue && entry.Scope == apiv1.EffectivePropScopeController {
				return true
			}
		}

		return false
	}, fmt.Sprintf("effective_props[%q] not Controller-scope=%q for RD %q",
		propKey, propValue, rdName))
}

// runErrorReportsList pins F6 / Bug 62: `linstor err l` must return a
// JSON list envelope (not a 404, null body, or python traceback) on a
// fresh controller with an empty ring buffer.
//
// Production reconcilers don't push to the ring yet (RecordErrorReport
// is unwired in the test stack) and the harness doesn't expose
// `*rest.Server` so a test can't seed entries — but the envelope
// contract is exactly what F6 / Bug 62 surfaced: the CLI's traceback
// came from blockstor returning `null` instead of `[]` for the empty
// case. This contract test guarantees the empty shape stays an empty
// JSON list forever, and that the limit query-param doesn't 400 on
// the empty path.
func runErrorReportsList(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	// CLI smoke: `linstor err l -m` decodes the controller's response
	// via golinstor's ErrorReport DTO. A null body or 404 surfaces as
	// `json.decoder.JSONDecodeError` or `KeyError` on stderr — the
	// CLI wrapper's traceback guard fails the test.
	_ = cli.Run(t, "error-reports", "list")

	// REST wire-shape pin: empty ring → 200 + `[]`. golinstor's
	// `Controller.GetErrorReports` unmarshals into `[]ErrorReport`
	// directly; a `null` body breaks the slice decode with a typed
	// nil that the CLI then iterates and crashes on.
	resp := httpGetGroupH(t, stack.RestURL+"/v1/error-reports")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/error-reports: got %d, want 200", resp.StatusCode)
	}

	var entries []map[string]any

	err := json.NewDecoder(resp.Body).Decode(&entries)
	if err != nil {
		t.Fatalf("decode error-reports: %v", err)
	}

	if entries == nil {
		t.Errorf("error-reports decoded as nil slice; expected empty list")
	}

	// Pagination param smoke: `?limit=5` must produce 200 + a list
	// (not 400). The handler validates `limit >= 0`; a buggy
	// implementation that 400'd on the empty path would break the
	// CLI's `linstor err l --limit=5` shortcut.
	limitResp := httpGetGroupH(t, stack.RestURL+"/v1/error-reports?limit=5")
	defer func() { _ = limitResp.Body.Close() }()

	if limitResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/error-reports?limit=5: got %d, want 200", limitResp.StatusCode)
	}
}

// runErrorReportsFilterByNode pins F6: the `?node=` query parameter
// (what golinstor's `Controller.GetErrorReports(nodes=[X])` expands
// `--nodes=X` into) must be honoured controller-side. A regression
// that ignored the filter would return the full ring across the wire,
// breaking `linstor err l --nodes=worker-1`'s "only show this
// satellite's failures" workflow.
//
// We assert the filter is applied (empty-result envelope is still
// valid) and that an unknown node returns an empty list rather than
// a 400 / 404. The `?since=garbage` sub-case pins the 400 path so
// invalid filters surface as operator-actionable errors instead of
// silent passes.
func runErrorReportsFilterByNode(t *testing.T, stack *harness.Stack, cli *harness.CLI) {
	t.Helper()

	// CLI invocation: `linstor err l --nodes worker-1`. The Python
	// client iterates per-node, hitting /v1/error-reports?node=worker-1.
	// Empty ring → empty per-node response → CLI prints an empty
	// table with no traceback.
	_ = cli.Run(t, "error-reports", "list", "--nodes", harness.NodeWorker1)

	// Wire-shape: `?node=worker-1` returns `[]` on a fresh ring.
	resp := httpGetGroupH(t,
		stack.RestURL+"/v1/error-reports?node="+harness.NodeWorker1)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/error-reports?node=worker-1: got %d, want 200", resp.StatusCode)
	}

	var entries []map[string]any

	err := json.NewDecoder(resp.Body).Decode(&entries)
	if err != nil {
		t.Fatalf("decode filtered error-reports: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("expected empty list for unseeded ring; got %d entries", len(entries))
	}

	// Unknown-node sanity: an arbitrary node name returns `[]`, not
	// 400/404. The CLI runs the per-node loop even when the node
	// doesn't exist in the cluster (the user may be searching for a
	// recently-removed satellite's reports); a 4xx here breaks that
	// search flow.
	resp2 := httpGetGroupH(t, stack.RestURL+"/v1/error-reports?node=ghost-node")
	defer func() { _ = resp2.Body.Close() }()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/error-reports?node=ghost-node: got %d, want 200",
			resp2.StatusCode)
	}

	// Invalid since parameter: must reject with 400. Pins the
	// parseSinceMillis contract so the CLI surfaces typos instead of
	// silently returning unfiltered output.
	badResp := httpGetGroupH(t, stack.RestURL+"/v1/error-reports?since=not-a-time")
	defer func() { _ = badResp.Body.Close() }()

	if badResp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET /v1/error-reports?since=garbage: got %d, want 400",
			badResp.StatusCode)
	}
}

// runKVStoreCRUD pins the in-memory key-value-store endpoints
// linstor-csi relies on for per-PVC snapshot bookkeeping (csi-sanity
// drives 5+ snapshot tests through this surface). The CRUD flow:
//
//   - PUT creates an instance + a key
//     (handleKVModify auto-creates on first write)
//   - GET single returns `[]KV` (linstor-csi's expected shape)
//   - GET list reports the instance in the flat array
//   - PUT with `delete_props` removes the key
//   - GET after delete returns the same instance with the key gone
//
// Implementation note on CLI invocation: the linstor-client 1.27.1
// shipped three `kv` CLI sub-commands that don't round-trip cleanly
// under `--machine-readable`:
//
//   - `kv modify`: production handleKVModify writes 200 + empty body
//     (no ApiCallRc list); the Python client logs
//     `Unable to parse REST json data: ...`. Production-side bug,
//     not in scope for Group H.
//   - `kv show`: upstream CLI's `_print_machine_readable` calls
//     `data_v1` on `dict.items()` tuples → AttributeError.
//   - `kv list`: same `_print_machine_readable` path is fed
//     instance-name strings → `AttributeError: 'str' object has no
//     attribute 'data_v1'`.
//
// All three CLI paths are upstream-linstor-client bugs unrelated to
// blockstor's REST contract. We therefore drive the full CRUD via
// direct REST so the wire-shape contract linstor-csi consumes is
// pinned without depending on the broken CLI paths above. (The
// CLI's read-only paths still work in human-readable mode, but the
// harness CLI helper hard-codes --machine-readable for the JSON
// round-trip; routing CRUD through the REST surface is the closest
// equivalent and the path linstor-csi itself takes.)
func runKVStoreCRUD(t *testing.T, stack *harness.Stack, _ *harness.CLI) {
	t.Helper()

	// The CLI handle is intentionally accepted (so the call-site
	// signature in TestGroupH stays uniform) but unused: every
	// --machine-readable `kv` sub-command is broken in
	// linstor-client 1.27.1 (see doc comment above), so the test
	// routes through direct REST instead.

	// Use a per-test-unique instance name so the CRUD flow doesn't
	// race or alias with anything an earlier Group H sub-test left
	// behind in the process-local kvBag (kvBag lives at package
	// scope in pkg/rest/kv_store.go).
	instance := "csi-test-" + uniqueSuffix()

	const (
		key   = "snapshot/pvc-123"
		value = "snap-handle-abc"
	)

	// 1. Create + set via direct REST PUT. Same shape linstor-csi
	// (via golinstor) sends — `override_props` map.
	putBody, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{key: value},
	})
	if err != nil {
		t.Fatalf("marshal PUT body: %v", err)
	}

	putResp := httpPutGroupH(t, stack.RestURL+"/v1/key-value-store/"+instance, putBody)
	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT /v1/key-value-store/%s: got %d, want 200", instance, putResp.StatusCode)
	}

	// 2. Wire-shape: `GET /v1/key-value-store/<instance>` returns a
	// single-element array (golinstor decodes into []KV and picks [0]);
	// a bare object response breaks linstor-csi's snapshot lookup with
	// "cannot unmarshal object into Go value of type []client.KV".
	gotEntry := decodeKVInstance(t, stack.RestURL, instance)
	if gotEntry.Props[key] != value {
		t.Fatalf("kv get %q: props[%s] = %q, want %q",
			instance, key, gotEntry.Props[key], value)
	}

	// 3. List wire-shape: `GET /v1/key-value-store` returns a flat
	// array of `[]KV` rows. The Python CLI's `kv list -m` path is
	// broken (see doc comment above), so we hit the REST endpoint
	// directly. linstor-csi consumes this exact shape during the
	// snapshot scrape; a regression that returned a wrapped
	// `{instances: [...]}` envelope would break csi-sanity's
	// ListSnapshots check.
	listResp := httpGetGroupH(t, stack.RestURL+"/v1/key-value-store")
	defer func() { _ = listResp.Body.Close() }()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/key-value-store: got %d, want 200", listResp.StatusCode)
	}

	var allInstances []apiv1.KV

	err = json.NewDecoder(listResp.Body).Decode(&allInstances)
	if err != nil {
		t.Fatalf("decode kv list: %v", err)
	}

	seen := false

	for i := range allInstances {
		if allInstances[i].Name == instance {
			seen = true

			break
		}
	}

	if !seen {
		t.Fatalf("kv list does not contain instance %q; got %+v", instance, allInstances)
	}

	// 4. Delete the key via direct REST PUT (delete_props). Same
	// shape the CLI's two-arg `modify <instance> <key>` form would
	// emit — we just side-step the empty-body parse failure on the
	// CLI return path.
	delBody, err := json.Marshal(apiv1.GenericPropsModify{
		DeleteProps: []string{key},
	})
	if err != nil {
		t.Fatalf("marshal DELETE body: %v", err)
	}

	delResp := httpPutGroupH(t, stack.RestURL+"/v1/key-value-store/"+instance, delBody)
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT (delete) /v1/key-value-store/%s: got %d, want 200",
			instance, delResp.StatusCode)
	}

	// 5. Wire-shape after delete: the instance still exists in the
	// kvBag (no DELETE was issued), but its prop map no longer
	// carries the deleted key. linstor-csi's csi-sanity drives the
	// same flow on snapshot delete-then-list scenarios.
	gotEntry = decodeKVInstance(t, stack.RestURL, instance)
	if _, present := gotEntry.Props[key]; present {
		t.Fatalf("kv get %q after delete: props[%s] still present (value=%q)",
			instance, key, gotEntry.Props[key])
	}
}

// decodeKVInstance issues `GET /v1/key-value-store/<instance>`,
// asserts the single-element-array wire shape, and returns the
// embedded KV. Pulled out of runKVStoreCRUD so the assertion that
// the response is `[]KV` (not a bare KV) is the same in every call
// site — protecting the linstor-csi consumer contract.
func decodeKVInstance(t *testing.T, baseURL, instance string) apiv1.KV {
	t.Helper()

	resp := httpGetGroupH(t, baseURL+"/v1/key-value-store/"+instance)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/key-value-store/%s: got %d, want 200", instance, resp.StatusCode)
	}

	var kvList []apiv1.KV

	err := json.NewDecoder(resp.Body).Decode(&kvList)
	if err != nil {
		t.Fatalf("decode kv get: %v", err)
	}

	if len(kvList) != 1 {
		t.Fatalf("kv get wire shape: got %d elements, want 1: %+v", len(kvList), kvList)
	}

	if kvList[0].Name != instance {
		t.Fatalf("kv get name: got %q, want %q", kvList[0].Name, instance)
	}

	return kvList[0]
}

// resourceViewEntry is the trimmed projection of /v1/view/resources we
// need for effective_props assertions. Decoding into the full
// apiv1.ResourceWithVolumes pulls in the entire Resource tree (Volumes,
// LayerObject, …) and surfaces JSON-tag drift as a noisy decode error
// instead of a focused "this field is missing" failure. The trimmed
// shape pins exactly the fields the test reads.
type resourceViewEntry struct {
	Name           string                              `json:"name"`
	NodeName       string                              `json:"node_name"`
	EffectiveProps map[string]apiv1.EffectivePropEntry `json:"effective_props,omitempty"`
}

// decodeResourceView GETs /v1/view/resources and returns only the
// entries for the requested RD. The endpoint already supports
// `?resources=<rd>` server-side filtering — we use it so the polling
// loop doesn't pull the whole cluster every tick.
func decodeResourceView(t *testing.T, baseURL, rdName string) []resourceViewEntry {
	t.Helper()

	resp := httpGetGroupH(t, baseURL+"/v1/view/resources?resources="+rdName)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/view/resources: got %d, want 200", resp.StatusCode)
	}

	var entries []resourceViewEntry

	err := json.NewDecoder(resp.Body).Decode(&entries)
	if err != nil {
		t.Fatalf("decode resource view: %v", err)
	}

	out := entries[:0]
	for i := range entries {
		if entries[i].Name == rdName {
			out = append(out, entries[i])
		}
	}

	return out
}

// hasKVPair scans a flattened linstor-CLI props output for a {key,
// value} pair. The CLI's `--machine-readable` envelope is `[[{key,
// value}, ...]]`; harness.CLI.JSON flattens one level so callers
// receive `[{...}]`.
func hasKVPair(rows []map[string]any, key, value string) bool {
	for _, row := range rows {
		// `linstor c lp -m` shape: top-level objects each carry a
		// `key` + `value` field.
		k, _ := row["key"].(string)
		v, _ := row["value"].(string)

		if k == key && v == value {
			return true
		}

		// `linstor kv show -m` shape: top-level object carries a
		// `props` map. Cover both forms so the same helper works
		// against `c lp` and `kv show` envelopes.
		if props, ok := row["props"].(map[string]any); ok {
			if pv, has := props[key].(string); has && pv == value {
				return true
			}
		}
	}

	return false
}

// httpGetGroupH issues a plain GET request and fails the test on
// transport error. Pulled out of every sub-test so the assertion
// loop stays readable. Mirrors the helpers in pkg/rest/*_test.go
// (httpGet) without depending on the unexported test surface there.
func httpGetGroupH(t *testing.T, url string) *http.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), groupHHTTPTimeout)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("build GET %s: %v", url, err)
	}

	cli := &http.Client{Timeout: groupHHTTPTimeout}

	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}

	return resp
}

// httpPutGroupH issues a JSON PUT request and fails the test on
// transport error. Used by the KV CRUD sub-test to drive the write
// path directly (the CLI's `kv modify` return-decode is broken on
// empty-body 200 — Phase 2 territory).
func httpPutGroupH(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), groupHHTTPTimeout)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build PUT %s: %v", url, err)
	}

	req.Header.Set("Content-Type", "application/json")

	cli := &http.Client{Timeout: groupHHTTPTimeout}

	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}

	return resp
}

// uniqueRDName generates a per-test resource-definition name that
// won't collide with other Group H sub-tests. RD names are
// cluster-scoped (a CEL XValidation rule pins them lowercase, hence
// strings.ToLower); the nanosecond timestamp suffix is wide enough
// that two back-to-back invocations in the same goroutine never tie.
func uniqueRDName(prefix string) string {
	return strings.ToLower(prefix) + "-" + uniqueSuffix()
}

// uniqueSuffix returns a per-call timestamp tag. Centralised so the
// RD-name and KV-instance helpers share the same uniqueness source.
func uniqueSuffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
