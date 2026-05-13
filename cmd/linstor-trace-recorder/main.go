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

// Command linstor-trace-recorder drives the upstream golinstor
// client through a series of operations against a live LINSTOR
// controller (the "oracle") and captures every HTTP request /
// response pair as a `tests/contract.Trace` JSON file. The corpus
// lands under tests/contract/testdata/oracle/ and the contract
// harness replays it against blockstor's REST shim to verify
// wire-shape parity.
//
// Usage:
//
//	go run ./cmd/linstor-trace-recorder \
//	    --controller http://localhost:3370 \
//	    --out tests/contract/testdata/oracle \
//	    --phase nodes
//
// Phases are independent so a partial recording (e.g. just nodes)
// can land before the full suite is wired. Each phase is
// idempotent: it tears down its own state at the end so the next
// run starts from the same oracle baseline.
//
// CLI tool: stdout is the operator-facing log, so the forbidigo
// "use a logger" rule doesn't apply. nolint at file scope.
//
//nolint:forbidigo // fmt.Print is intentional for a CLI tool
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/LINBIT/golinstor/client"
	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/tests/contract"
)

const (
	dirMode  = 0o755
	fileMode = 0o644
	// maxLabel caps the post-method/path filename slug so a long
	// query string can't push past common-filesystem name limits.
	// Each trace's lexical ordering keys off the leading `<seq>-`
	// prefix anyway.
	maxLabel = 80
)

func main() {
	var (
		controllerURL  string
		outDir         string
		phaseName      string
		keepListPrefix string
	)

	flag.StringVar(&controllerURL, "controller", "http://localhost:3370",
		"URL of the LINSTOR controller to record against.")
	flag.StringVar(&outDir, "out", "tests/contract/testdata/oracle",
		"Directory to write trace JSON files into. Created if absent.")
	flag.StringVar(&phaseName, "phase", "all",
		"Which phase to record: bootstrap, nodes, all.")
	flag.StringVar(&keepListPrefix, "keep-list-name-prefix", "trace-",
		"Drop entries from list responses whose `name` doesn't start with this. "+
			"Set to \"\" to disable. Default keeps only the fixtures the phase script created.")
	flag.Parse()

	recorderNormalizeOpts = contract.NormalizeOptions{
		KeepListNamePrefix: keepListPrefix,
	}

	err := os.MkdirAll(outDir, dirMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create out dir: %v\n", err)
		os.Exit(1)
	}

	rec := &recorder{outDir: outDir}

	cli, err := newClient(controllerURL, rec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build client: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	for _, current := range selectPhases(phaseName) {
		fmt.Printf("=== phase: %s ===\n", current.name)

		rec.phase = current.name
		rec.step = 0

		runErr := current.run(ctx, cli)
		if runErr != nil {
			fmt.Fprintf(os.Stderr, "phase %s: %v\n", current.name, runErr)
			os.Exit(1)
		}
	}

	fmt.Printf("recorded %d traces under %s\n", rec.total, outDir)
}

// recorder is the shared state every phase writes through. It owns
// the output directory + the running step counter so phases produce
// lexically-sorted filenames (`001-bootstrap-…`,
// `002-nodes-…`, …) the contract harness orders by.
type recorder struct {
	outDir string
	phase  string
	step   int
	total  int
}

// recordingTransport wraps an http.RoundTripper and writes one
// Trace JSON per request/response pair.
type recordingTransport struct {
	inner http.RoundTripper
	rec   *recorder
}

// trace mirrors tests/contract.Trace exactly. Duplicated here so
// the recorder doesn't drag a test-package import into a runtime
// binary (and so a contract.Trace schema change rejects the
// recorder at compile time if the fields drift).
type trace struct {
	Name           string          `json:"name"`
	Method         string          `json:"method"`
	Path           string          `json:"path"`
	Body           json.RawMessage `json:"body,omitempty"`
	ExpectedStatus int             `json:"expectedStatus"`
	ExpectedBody   json.RawMessage `json:"expectedBody,omitempty"`
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	reqBody, err := snapshotAndRestoreBody(req)
	if err != nil {
		return nil, errors.Wrap(err, "snapshot request body")
	}

	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		return nil, errors.Wrap(err, "round trip")
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(respBody))

	t.rec.write(req.Method, req.URL.RequestURI(), reqBody, resp.StatusCode, respBody)

	return resp, nil
}

// snapshotAndRestoreBody slurps a request body into memory and
// replaces the underlying Reader so the downstream RoundTrip still
// sees the bytes. Without this the recorded body would be empty
// after the request flushed its content onto the wire.
func snapshotAndRestoreBody(req *http.Request) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}

	buf, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	_ = req.Body.Close()
	req.Body = io.NopCloser(bytes.NewReader(buf))

	return buf, nil
}

// write emits the next trace file. Naming convention:
// `<seq>-<phase>-<slug>.json` where seq is monotonic across the
// whole run so a re-recorded subset replaces in place without
// re-ordering siblings.
func (r *recorder) write(method, path string, reqBody []byte, status int, respBody []byte) {
	r.step++
	r.total++

	name := fmt.Sprintf("%03d-%s-%s.json", r.total, r.phase, sanitisePath(method, path))

	captured := trace{
		Name:           fmt.Sprintf("%s %s", method, path),
		Method:         method,
		Path:           path,
		ExpectedStatus: status,
	}

	// Request bodies are recorder-authored fixtures — never pre-existing
	// oracle state — so the list-name filter doesn't apply.
	if len(reqBody) > 0 {
		captured.Body = normalizeOrCanonical(reqBody, false)
	}

	// Response bodies get the list-filter only on fixture-list endpoints
	// (top-level /v1/nodes, /v1/resource-groups, …). Per-fixture detail
	// endpoints like /v1/nodes/{n}/net-interfaces return arrays of the
	// fixture's own children; filtering by name would strip the
	// recorder-created "default" interface and break the trace.
	if len(respBody) > 0 {
		captured.ExpectedBody = normalizeOrCanonical(respBody, isFixtureListPath(path))
	}

	out, err := json.MarshalIndent(captured, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal trace: %v\n", err)
		os.Exit(1)
	}

	out = append(out, '\n')

	// filepath.Base belt-and-braces against any path-traversal
	// chars that snuck through sanitisePath (filenames already have
	// `/` stripped, but gosec G703 wants the explicit guard).
	err = os.WriteFile(filepath.Join(r.outDir, filepath.Base(name)), out, fileMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write trace: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("  %s\n", name)
}

// recorderNormalizeOpts captures the additional scrubbing the
// recorder applies on top of the baseline `contract.Normalize`.
// Currently just the list-name prefix filter so list endpoints
// don't bake pre-existing oracle state (e2e6 workers,
// DfltRscGrp) into the committed corpus.
//
// Set in main() once flags are parsed; consumed inside
// normalizeOrCanonical via package-global. Package globals are
// idiomatic for "config fixed at startup" in CLI binaries.
//
//nolint:gochecknoglobals // CLI startup-time config
var recorderNormalizeOpts contract.NormalizeOptions

// normalizeOrCanonical scrubs the body through `tests/contract.Normalize`
// (with the recorder's list-name prefix filter) so stand-specific
// volatile values (UUIDs, timestamps, real worker IPs, operator-
// managed props, pre-existing oracle list entries) are replaced
// with stable placeholders before the trace lands on disk. Falls
// back to the JSON canonical form (stable key ordering, same as
// encoding/json's default re-marshal) if normalisation fails —
// better to commit a slightly noisy trace than to skip the call.
func normalizeOrCanonical(in []byte, allowListFilter bool) json.RawMessage {
	opts := recorderNormalizeOpts
	if !allowListFilter {
		opts.KeepListNamePrefix = ""
	}

	scrubbed, err := contract.NormalizeWith(in, opts)
	if err == nil && len(scrubbed) > 0 {
		return scrubbed
	}

	var decoded any

	err = json.Unmarshal(in, &decoded)
	if err != nil {
		return json.RawMessage(in)
	}

	out, err := json.Marshal(decoded)
	if err != nil {
		return json.RawMessage(in)
	}

	return out
}

// isFixtureListPath reports whether a URL path returns a top-level
// array of "primary fixtures" — Nodes, RGs, RDs, or view aggregates.
// The recorder's KeepListNamePrefix filter only makes sense on these
// paths: it drops pre-existing oracle state from the committed
// corpus. Per-fixture detail endpoints (e.g.
// `/v1/nodes/{n}/net-interfaces` returns []NetInterface owned by the
// node we just created) must NOT get filtered or recorder-created
// children would silently disappear.
//
// Match by path *prefix* with the per-fixture sub-paths excluded —
// `/v1/resource-definitions` is fixture-list, but
// `/v1/resource-definitions/foo/volume-definitions` is per-fixture
// detail and slips out.
func isFixtureListPath(path string) bool {
	// Per-fixture detail paths (the leaf is a child of a named
	// fixture) — explicitly NOT filterable.
	excludePrefixes := []string{
		"/v1/nodes/",
		"/v1/resource-groups/",
		"/v1/resource-definitions/",
		"/v1/key-value-store/",
	}

	for _, prefix := range excludePrefixes {
		if strings.HasPrefix(path, prefix) {
			return false
		}
	}

	// Trim any querystring before matching — `?` shows up on RD
	// list calls with no opts.
	trimmed := path
	if idx := strings.Index(trimmed, "?"); idx >= 0 {
		trimmed = trimmed[:idx]
	}

	// Fixture-list / view paths: the response is a top-level array
	// of objects whose `name` field is the fixture name.
	return slices.Contains([]string{
		"/v1/nodes",
		"/v1/resource-groups",
		"/v1/resource-definitions",
		"/v1/key-value-store",
		"/v1/view/resources",
		"/v1/view/snapshots",
		"/v1/view/storage-pools",
		"/v1/view/volume-definitions",
	}, trimmed)
}

// sanitisePath turns an HTTP method + path into a filename-safe
// short label, e.g. `GET /v1/nodes/n1` → `get-v1-nodes-n1`.
func sanitisePath(method, path string) string {
	clean := strings.ToLower(method) + "-" + strings.TrimPrefix(path, "/")
	clean = strings.ReplaceAll(clean, "/", "-")
	clean = strings.ReplaceAll(clean, "?", "-")
	clean = strings.ReplaceAll(clean, "&", "-")
	clean = strings.ReplaceAll(clean, "=", "-")

	if len(clean) > maxLabel {
		clean = clean[:maxLabel]
	}

	return clean
}

func newClient(controllerURL string, rec *recorder) (*client.Client, error) {
	parsed, err := url.Parse(controllerURL)
	if err != nil {
		return nil, errors.Wrap(err, "parse controller URL")
	}

	httpClient := &http.Client{
		Transport: &recordingTransport{
			inner: http.DefaultTransport,
			rec:   rec,
		},
	}

	cli, err := client.NewClient(
		client.Controllers([]string{parsed.String()}),
		client.HTTPClient(httpClient),
	)
	if err != nil {
		return nil, errors.Wrap(err, "new linstor client")
	}

	return cli, nil
}
