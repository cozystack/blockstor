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
	"encoding/json"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

// NormalizeOptions tunes Normalize's behaviour for caller-specific
// needs. The zero value gives the baseline scrubber the replay
// harness uses; the trace recorder additionally sets
// KeepListNamePrefix so list responses don't leak pre-existing
// oracle state (e2e6 workers, default groups) into the committed
// corpus.
type NormalizeOptions struct {
	// KeepListNamePrefix, when non-empty, filters JSON arrays of
	// objects: only entries whose `name` field starts with the
	// given prefix are kept. Applied recursively, so a list of
	// resources containing a list of net_interfaces (each with
	// its own `name` like "default") is NOT filtered out — only
	// the outer array of objects with a top-level `name` is.
	// Set to "trace-" by the recorder so the corpus contains
	// only the fixtures the phase script created.
	KeepListNamePrefix string
}

// Normalize is the baseline scrubber. Equivalent to NormalizeWith
// with a zero-value NormalizeOptions. Kept as the package-level
// entry point so existing callers (the replay harness) don't need
// the options dance.
func Normalize(body json.RawMessage) (json.RawMessage, error) {
	return NormalizeWith(body, NormalizeOptions{})
}

// NormalizeWith rewrites a JSON body to a deterministic form so
// traces recorded against one LINSTOR controller compare equal to
// the blockstor REST response on replay. Volatile values that vary
// run-to-run (UUIDs, timestamps, real worker IPs, kernel-version
// strings) get replaced with stable placeholder tokens; operator-
// managed dynamic props get stripped entirely.
//
// The function is idempotent — `NormalizeWith(NormalizeWith(x, o), o) == NormalizeWith(x, o)` —
// so it's safe to apply at both recording time (so the committed
// corpus is reproducible) and replay time (so blockstor's response
// gets the same scrubbing before the diff). Empty / non-JSON input
// passes through unchanged.
func NormalizeWith(body json.RawMessage, opts NormalizeOptions) (json.RawMessage, error) {
	if len(body) == 0 {
		return body, nil
	}

	var decoded any

	err := json.Unmarshal(body, &decoded)
	if err != nil {
		// Non-JSON payloads pass through. Some LINSTOR endpoints
		// emit text/plain for errors; the contract harness sees
		// status codes for those, not body content.
		return body, nil //nolint:nilerr // non-JSON passthrough is intentional
	}

	scrubbed := scrubWith(decoded, opts)

	out, err := json.Marshal(scrubbed)
	if err != nil {
		return nil, errors.Wrap(err, "re-marshal scrubbed JSON")
	}

	return out, nil
}

// dropProps is the set of LINSTOR props that vary stand-to-stand
// (operator-managed, picked up from k8s) or are otherwise
// reconstructed at runtime from the current peer set. Stripping
// them lets the corpus stay stable while still asserting the rest
// of the prop map round-trips.
var dropProps = map[string]struct{}{ //nolint:gochecknoglobals // table-driven constant
	"Aux/piraeus.io/last-applied":          {},
	"Aux/piraeus.io/configured-interfaces": {},
	"Aux/topology/kubernetes.io/hostname":  {},
	"Aux/topology/linbit.com/hostname":     {},
	"Aux/registered-by":                    {},
	"CurStltConnName":                      {},
	"NodeUname":                            {},
	"StltConn/0/Active":                    {},
	"StltConn/0/Address":                   {},
	"StltConn/0/Port":                      {},
	"StltConn/0/EncryptionType":            {},
}

// volatileTopLevel is the set of top-level response keys that vary
// per-build of the oracle and would never compare equal to
// blockstor's own response. Dropped at scrub time.
var volatileTopLevel = map[string]struct{}{ //nolint:gochecknoglobals // table-driven constant
	"build_time": {},
	"git_hash":   {},
	"uuid":       {},
}

func scrubWith(value any, opts NormalizeOptions) any {
	switch typed := value.(type) {
	case map[string]any:
		return scrubMap(typed, opts)
	case []any:
		return scrubSlice(typed, opts)
	case string:
		return scrubString(typed)
	default:
		return value
	}
}

func scrubMap(input map[string]any, opts NormalizeOptions) map[string]any {
	out := make(map[string]any, len(input))

	for key, raw := range input {
		if _, drop := volatileTopLevel[key]; drop {
			continue
		}

		if key == "props" || key == "override_props" {
			out[key] = scrubProps(raw)

			continue
		}

		// Per-key value coercion: certain field names ALWAYS carry
		// volatile data regardless of the type recorded. Force the
		// placeholder so a missing field on one side doesn't show
		// up as a string-vs-null diff.
		switch key {
		case "address":
			out[key] = placeholderIP
		case "satellite_port":
			out[key] = raw // ports are stable-ish; keep
		default:
			out[key] = scrubWith(raw, opts)
		}
	}

	return out
}

func scrubProps(raw any) any {
	props, ok := raw.(map[string]any)
	if !ok {
		return raw
	}

	out := make(map[string]any, len(props))

	for key, value := range props {
		if _, drop := dropProps[key]; drop {
			continue
		}

		out[key] = value
	}

	return out
}

func scrubSlice(input []any, opts NormalizeOptions) []any {
	filtered := filterListByNamePrefix(input, opts.KeepListNamePrefix)

	out := make([]any, len(filtered))
	for i, value := range filtered {
		out[i] = scrubWith(value, opts)
	}

	return out
}

// filterListByNamePrefix drops entries from an array of objects
// whose top-level `name` field doesn't start with prefix. Entries
// without a `name` field are kept as-is so this is safe to apply
// to lists of scalars / lists of objects with different schemas.
// An empty prefix means "keep everything" — the zero-value default.
func filterListByNamePrefix(input []any, prefix string) []any {
	if prefix == "" {
		return input
	}

	out := make([]any, 0, len(input))

	for _, entry := range input {
		obj, ok := entry.(map[string]any)
		if !ok {
			out = append(out, entry)

			continue
		}

		nameRaw, hasName := obj["name"]
		if !hasName {
			out = append(out, entry)

			continue
		}

		name, ok := nameRaw.(string)
		if !ok || !strings.HasPrefix(name, prefix) {
			continue
		}

		out = append(out, entry)
	}

	return out
}

// scrubString applies regex-based placeholders. Patterns checked in
// most-to-least specific order so a UUID embedded in a longer
// string still wins over the timestamp pattern.
func scrubString(s string) string {
	if uuidPattern.MatchString(s) {
		return uuidPattern.ReplaceAllString(s, placeholderUUID)
	}

	if timestampPattern.MatchString(s) {
		return timestampPattern.ReplaceAllString(s, placeholderTimestamp)
	}

	if ipv4Pattern.MatchString(s) && !strings.HasPrefix(s, "<") {
		return ipv4Pattern.ReplaceAllString(s, placeholderIP)
	}

	return s
}

// Patterns are package-level so the regex engine compiles them
// once. Anchors are intentionally loose because some LINSTOR
// payloads embed UUIDs / timestamps inside larger strings
// (e.g. error_report ids).
// Compiled once at package init via the var-block.
var (
	uuidPattern      = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	timestampPattern = regexp.MustCompile(`[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})`)
	ipv4Pattern      = regexp.MustCompile(`(?:\b|^)(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(?:\b|$)`)
)

const (
	placeholderUUID      = "<uuid>"
	placeholderTimestamp = "<timestamp>"
	placeholderIP        = "<ip>"
)
