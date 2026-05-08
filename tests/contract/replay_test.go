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

package contract_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cozystack/blockstor/tests/contract"
)

// TestReplayMatchingTrace: server response identical to the trace's
// expected → Match=true, Diffs empty.
func TestReplayMatchingTrace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"a":1,"b":2}`))
	}))
	defer srv.Close()

	traces := []contract.Trace{{
		Name:           "ok",
		Method:         http.MethodGet,
		Path:           "/v1/anything",
		ExpectedStatus: http.StatusOK,
		ExpectedBody:   json.RawMessage(`{"b":2,"a":1}`), // different order, same content
	}}

	results, err := contract.Replay(t.Context(), srv.Client(), srv.URL, traces)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("len: got %d, want 1", len(results))
	}

	if !results[0].Match {
		t.Errorf("expected Match; got Diffs=%v", results[0].Diffs)
	}
}

// TestReplayStatusDiverges: server status differs → Match=false with
// a status-diff entry.
func TestReplayStatusDiverges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	traces := []contract.Trace{{
		Name:           "wrong-status",
		Method:         http.MethodGet,
		Path:           "/v1/anything",
		ExpectedStatus: http.StatusOK,
	}}

	results, err := contract.Replay(t.Context(), srv.Client(), srv.URL, traces)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if results[0].Match {
		t.Errorf("expected Match=false")
	}

	if len(results[0].Diffs) == 0 {
		t.Errorf("expected diffs to be populated")
	}
}

// TestReplayBodyDiverges: response JSON differs in content → diff
// entry present.
func TestReplayBodyDiverges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"a":99}`))
	}))
	defer srv.Close()

	traces := []contract.Trace{{
		Name:         "body-mismatch",
		Method:       http.MethodGet,
		Path:         "/v1/anything",
		ExpectedBody: json.RawMessage(`{"a":1}`),
	}}

	results, err := contract.Replay(t.Context(), srv.Client(), srv.URL, traces)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if results[0].Match {
		t.Errorf("expected Match=false; got Diffs=%v", results[0].Diffs)
	}
}

// TestLoadTracesDir reads `.json` files from a temp dir, ignoring
// non-json + sub-directories. Order is lexical.
func TestLoadTracesDir(t *testing.T) {
	dir := t.TempDir()

	files := map[string]string{
		"01-create.json": `{"name":"create","method":"POST","path":"/v1/x","expectedStatus":201}`,
		"02-list.json":   `{"name":"list","method":"GET","path":"/v1/x","expectedStatus":200}`,
		"readme.txt":     "ignored",
	}

	for name, body := range files {
		err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600)
		if err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	got, err := contract.LoadTracesDir(dir)
	if err != nil {
		t.Fatalf("LoadTracesDir: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}

	if got[0].Name != "create" || got[1].Name != "list" {
		t.Errorf("order wrong; got %s, %s", got[0].Name, got[1].Name)
	}
}
