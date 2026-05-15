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

package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/cockroachdb/errors"
)

// Result is the outcome of replaying one Trace against the server.
type Result struct {
	// Trace identifies which trace this result describes.
	Trace string

	// Match is true when status and body matched.
	Match bool

	// Diffs lists the divergences from expected — empty if Match.
	Diffs []string

	// ActualStatus / ActualBody capture what the server returned.
	ActualStatus int
	ActualBody   json.RawMessage
}

// Replay sends each trace's request to baseURL and returns one Result
// per trace. It does not stop on the first divergence — the caller
// gets the full picture.
//
// The HTTP client is supplied so tests can inject their own (e.g. one
// pointing at httptest.NewServer for in-process replay).
func Replay(ctx context.Context, client *http.Client, baseURL string, traces []Trace) ([]Result, error) {
	if client == nil {
		client = http.DefaultClient
	}

	results := make([]Result, 0, len(traces))

	for i := range traces {
		result, err := replayOne(ctx, client, baseURL, &traces[i])
		if err != nil {
			return results, errors.Wrapf(err, "replay %s", traces[i].Name)
		}

		results = append(results, result)
	}

	return results, nil
}

// replayBodyIPPlaceholder is the deterministic IP literal substituted
// for the `<ip>` placeholder in trace bodies at replay time. Recorded
// traces scrub real worker IPs to `<ip>` so the fixtures are portable
// across stands; but Bug 120 added wire-boundary IP validation on
// POST /v1/nodes, so a literal `"<ip>"` in the body now (correctly)
// returns 400. Substituting a parseable literal here keeps the
// fixtures portable while still exercising the rest of the pipeline.
const replayBodyIPPlaceholder = "10.0.0.1"

// replayOne issues a single trace against baseURL.
func replayOne(ctx context.Context, client *http.Client, baseURL string, trace *Trace) (Result, error) {
	var body io.Reader
	if len(trace.Body) > 0 {
		// Bug 120 follow-on: rehydrate the `<ip>` placeholder in the
		// request body so the wire-boundary IP validation accepts the
		// payload. The expected-body comparison still runs through
		// Normalize, which re-scrubs IPv4 literals back to `<ip>` —
		// so the fixture stays portable and the diff stays clean.
		body = bytes.NewReader(rehydrateIPPlaceholder(trace.Body))
	}

	req, err := http.NewRequestWithContext(ctx, trace.Method, baseURL+trace.Path, body)
	if err != nil {
		return Result{}, errors.Wrap(err, "build request")
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return Result{}, errors.Wrap(err, "send request")
	}

	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, errors.Wrap(err, "read response")
	}

	result := Result{
		Trace:        trace.Name,
		ActualStatus: resp.StatusCode,
		ActualBody:   respBody,
	}

	diffs := compare(trace, &result)

	result.Diffs = diffs
	result.Match = len(diffs) == 0

	return result, nil
}

// compare runs the trace's expectations against the captured response.
// Both sides go through Normalize so stand-specific volatile values
// (UUIDs, timestamps, real worker IPs, operator-managed props) don't
// produce false-positive divergences. The expected body was already
// normalised at recording time but we re-run for idempotency so a
// caller can hand-author a trace without thinking about scrubbing.
func compare(trace *Trace, result *Result) []string {
	var diffs []string

	if trace.ExpectedStatus != 0 && trace.ExpectedStatus != result.ActualStatus {
		diffs = append(diffs,
			"status: got "+itoa(result.ActualStatus)+", want "+itoa(trace.ExpectedStatus))
	}

	if len(trace.ExpectedBody) > 0 {
		want, err := Normalize(trace.ExpectedBody)
		if err != nil {
			diffs = append(diffs, "expected body: "+err.Error())

			return diffs
		}

		got, err := Normalize(result.ActualBody)
		if err != nil {
			diffs = append(diffs, "actual body: "+err.Error())

			return diffs
		}

		if !bytes.Equal(want, got) {
			diffs = append(diffs, "body diverges; want="+string(want)+" got="+string(got))
		}
	}

	return diffs
}

// rehydrateIPPlaceholder substitutes every `<ip>` placeholder in raw
// JSON with replayBodyIPPlaceholder. Operates on the raw bytes (not
// via Unmarshal) so structural ordering of the original recording is
// preserved bit-for-bit through to the replay HTTP request. Bug 120's
// IP validation rejects unparseable literals; without this rehydration
// the recorded trace fixtures (which scrub real IPs to `<ip>`) would
// fail at the wire boundary before reaching the handler we're trying
// to exercise.
//
// Two forms are substituted because json.RawMessage preserves the
// recorder's choice of escape — Go's default `json.Marshal` is
// HTML-safe and emits `<` as the Unicode escape `<` (so the
// placeholder appears as `<ip>` in the on-disk fixtures),
// while a hand-authored fixture or a marshaller with
// `SetEscapeHTML(false)` writes the literal `<ip>`. Both forms must
// rehydrate to the same parseable IP at replay time.
func rehydrateIPPlaceholder(in []byte) []byte {
	target := []byte(`"` + replayBodyIPPlaceholder + `"`)
	// Literal-`<`-`>` form: hand-authored or non-HTML-safe encoder.
	out := bytes.ReplaceAll(in, []byte(`"<ip>"`), target)
	// HTML-safe-escape form: Go's default json.Marshal emits `<` as
	// the 6-byte sequence `<` and `>` as `>` to prevent
	// script-injection in HTML-embedded JSON. Recorded oracle traces
	// went through that path, so the on-disk bytes are the escape
	// form, not the literal `<`/`>`. Use a double-quoted Go string
	// literal here so `\\u003c` produces the on-wire 6-byte sequence.
	out = bytes.ReplaceAll(out, []byte("\"\\u003cip\\u003e\""), target)

	return out
}

// itoa avoids dragging strconv into this file's import set; we only
// need it for tiny status codes. (Keeps imports tight in the harness
// so the dependency graph is one-glance reviewable.)
func itoa(num int) string {
	if num == 0 {
		return "0"
	}

	negative := false
	if num < 0 {
		negative = true
		num = -num
	}

	var buf [12]byte

	pos := len(buf)

	for num > 0 {
		pos--

		buf[pos] = byte('0' + num%10)
		num /= 10
	}

	if negative {
		pos--

		buf[pos] = '-'
	}

	return string(buf[pos:])
}
