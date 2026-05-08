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

// Package contract is the contract-diff test harness. It replays
// captured golinstor request/response traces against blockstor's REST
// server and reports per-step JSON divergences.
//
// The harness is independent of where traces come from — feed it
// traces recorded against the Java LINSTOR oracle to verify
// API parity, or traces recorded against blockstor itself for plain
// regression testing.
package contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
)

// Trace is one captured request/response pair. Fields are JSON-tagged
// so traces can live as `.json` files in the testdata directory.
type Trace struct {
	// Name is a human-readable label for diagnostics.
	Name string `json:"name"`

	// Method / Path / Body describe the request. Body is raw JSON;
	// the harness sends it verbatim.
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Body   json.RawMessage `json:"body,omitempty"`

	// ExpectedStatus is the HTTP status the oracle returned.
	ExpectedStatus int `json:"expectedStatus"`

	// ExpectedBody is the JSON body the oracle returned. Compared
	// modulo key ordering and known-volatile fields (UUIDs, timestamps).
	ExpectedBody json.RawMessage `json:"expectedBody,omitempty"`
}

// LoadTracesDir reads every `.json` file under dir into a Trace slice.
// Lexically sorted so the replayer order stays deterministic across
// runs (otherwise diff output varies for no real reason).
func LoadTracesDir(dir string) ([]Trace, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "read traces dir %s", dir)
	}

	names := make([]string, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		names = append(names, entry.Name())
	}

	sort.Strings(names)

	out := make([]Trace, 0, len(names))

	for _, name := range names {
		path := filepath.Join(dir, name)

		body, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.Wrapf(err, "read %s", path)
		}

		var trace Trace

		err = json.Unmarshal(body, &trace)
		if err != nil {
			return nil, errors.Wrapf(err, "decode %s", path)
		}

		if trace.Name == "" {
			trace.Name = name
		}

		out = append(out, trace)
	}

	return out, nil
}
