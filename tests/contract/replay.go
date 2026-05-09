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

// replayOne issues a single trace against baseURL.
func replayOne(ctx context.Context, client *http.Client, baseURL string, trace *Trace) (Result, error) {
	var body io.Reader
	if len(trace.Body) > 0 {
		body = bytes.NewReader(trace.Body)
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
// JSON bodies are normalised (key-sorted) before byte compare so we
// don't false-positive on golang vs Java map iteration order.
func compare(trace *Trace, result *Result) []string {
	var diffs []string

	if trace.ExpectedStatus != 0 && trace.ExpectedStatus != result.ActualStatus {
		diffs = append(diffs,
			"status: got "+itoa(result.ActualStatus)+", want "+itoa(trace.ExpectedStatus))
	}

	if len(trace.ExpectedBody) > 0 {
		want, err := normaliseJSON(trace.ExpectedBody)
		if err != nil {
			diffs = append(diffs, "expected body: "+err.Error())

			return diffs
		}

		got, err := normaliseJSON(result.ActualBody)
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

// normaliseJSON re-marshals raw JSON with sorted keys so map-iteration
// order doesn't poison string compare. Returns the original bytes for
// non-object responses (arrays, scalars).
func normaliseJSON(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return raw, nil
	}

	var decoded any

	err := json.Unmarshal(raw, &decoded)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal")
	}

	out, err := json.Marshal(decoded)
	if err != nil {
		return nil, errors.Wrap(err, "marshal")
	}

	return out, nil
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
