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
	"testing"

	"github.com/cozystack/blockstor/tests/contract"
)

// TestNormalizeIdempotent pins the idempotency contract: applying
// Normalize twice produces the same bytes as one application.
// Critical because the recorder normalises at write time and the
// replay harness normalises blockstor's response too — without
// idempotency the second pass could "double-scrub" a value.
func TestNormalizeIdempotent(t *testing.T) {
	cases := [][]byte{
		[]byte(`{"uuid":"550e8400-e29b-41d4-a716-446655440000","name":"n1"}`),
		[]byte(`{"build_time":"2025-10-13T06:37:58+00:00","version":"1.32.3"}`),
		[]byte(`[{"address":"10.244.1.6","name":"default"}]`),
		[]byte(`{}`),
		[]byte(`[]`),
		[]byte(`""`),
		[]byte(`123`),
	}

	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			once, err := contract.Normalize(in)
			if err != nil {
				t.Fatalf("first pass: %v", err)
			}

			twice, err := contract.Normalize(once)
			if err != nil {
				t.Fatalf("second pass: %v", err)
			}

			if string(once) != string(twice) {
				t.Errorf("not idempotent: once=%s twice=%s", once, twice)
			}
		})
	}
}

// TestNormalizeUUIDsReplaced pins the UUID-stripping rule. Every
// LINSTOR response carries `uuid` fields per object; without
// stripping, two recordings against the same controller would
// always diff.
func TestNormalizeUUIDsReplaced(t *testing.T) {
	in := []byte(`{"uuid":"550e8400-e29b-41d4-a716-446655440000","name":"n1"}`)

	out, err := contract.Normalize(in)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if _, present := decoded["uuid"]; present {
		t.Errorf("uuid field should be dropped at top level: %s", out)
	}

	if decoded["name"] != "n1" {
		t.Errorf("non-volatile field corrupted: %s", out)
	}
}

// TestNormalizeNestedUUIDReplaced pins UUID stripping inside
// nested objects (e.g. net_interfaces[].uuid). LINSTOR puts UUIDs
// at every layer; stripping only at top level would miss most.
func TestNormalizeNestedUUIDReplaced(t *testing.T) {
	in := []byte(`{"name":"n1","net_interfaces":[{"name":"default","uuid":"9ea5d7e8-9760-476e-afbb-846272d274fa","address":"10.0.0.1"}]}`)

	out, err := contract.Normalize(in)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	// Decode and walk to verify the nested uuid is gone.
	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	ifaces, ok := decoded["net_interfaces"].([]any)
	if !ok || len(ifaces) != 1 {
		t.Fatalf("net_interfaces shape wrong: %s", out)
	}

	iface, ok := ifaces[0].(map[string]any)
	if !ok {
		t.Fatalf("net_interfaces[0] not a map: %s", out)
	}

	if _, present := iface["uuid"]; present {
		t.Errorf("nested uuid should be stripped: %s", out)
	}

	if iface["address"] != "<ip>" {
		t.Errorf("address not normalised: %s", out)
	}

	if iface["name"] != "default" {
		t.Errorf("name corrupted: %s", out)
	}
}

// TestNormalizeTimestampReplaced pins the ISO-8601 placeholder.
// Every error_report and several layer payloads include a
// timestamp; without normalisation they'd diff every run.
func TestNormalizeTimestampReplaced(t *testing.T) {
	in := []byte(`{"created_at":"2025-10-13T06:37:58+00:00","name":"err"}`)

	out, err := contract.Normalize(in)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded["created_at"] != "<timestamp>" {
		t.Errorf("timestamp not normalised: %s", out)
	}
}

// TestNormalizeOperatorPropsDropped pins the props-filter rule for
// the piraeus-operator-managed keys that vary stand-to-stand.
// Without filtering, every node-list trace diffs on the
// per-stand topology props.
func TestNormalizeOperatorPropsDropped(t *testing.T) {
	in := []byte(`{"name":"n1","props":{"Aux/piraeus.io/last-applied":"[\"x\"]","Aux/topology/kubernetes.io/hostname":"e2e6-worker-1","NodeUname":"e2e6-worker-1","StorPoolName":"stand"}}`)

	out, err := contract.Normalize(in)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	props, ok := decoded["props"].(map[string]any)
	if !ok {
		t.Fatalf("props missing: %s", out)
	}

	for _, dropped := range []string{
		"Aux/piraeus.io/last-applied",
		"Aux/topology/kubernetes.io/hostname",
		"NodeUname",
	} {
		if _, present := props[dropped]; present {
			t.Errorf("operator-managed prop %q should be dropped: %s", dropped, out)
		}
	}

	// Domain-meaningful props survive.
	if props["StorPoolName"] != "stand" {
		t.Errorf("StorPoolName lost: %s", out)
	}
}

// TestNormalizeBuildTimeAndGitHashDropped pins the /controller/version
// rule. build_time and git_hash vary per binary; only the
// rest_api_version contract matters across stands.
func TestNormalizeBuildTimeAndGitHashDropped(t *testing.T) {
	in := []byte(`{"build_time":"2025-10-13T06:37:58+00:00","git_hash":"6dac06aed233f2c89ac7cc6b1185d6dce9ec74c4","rest_api_version":"1.26.0","version":"1.32.3"}`)

	out, err := contract.Normalize(in)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// version strings drift between LINSTOR releases; we drop
	// `version` and `rest_api_version` alongside build_time and
	// git_hash so the contract test doesn't gate on the release
	// number. linstor-csi / piraeus-operator don't gate on either
	// string either.
	for _, dropped := range []string{"build_time", "git_hash", "version", "rest_api_version"} {
		if _, present := decoded[dropped]; present {
			t.Errorf("%s should be dropped: %s", dropped, out)
		}
	}
}

// TestNormalizeNonJSONPassthrough pins the graceful fallback for
// text/plain response bodies (some LINSTOR error paths emit them).
func TestNormalizeNonJSONPassthrough(t *testing.T) {
	in := []byte(`plain text error`)

	out, err := contract.Normalize(in)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	if string(out) != string(in) {
		t.Errorf("non-JSON should pass through: got %s", out)
	}
}

// TestNormalizeEmptyBody pins the zero-input handling — Replay
// passes empty bodies for GET requests; Normalize must accept.
func TestNormalizeEmptyBody(t *testing.T) {
	out, err := contract.Normalize(nil)
	if err != nil {
		t.Fatalf("normalize nil: %v", err)
	}

	if len(out) != 0 {
		t.Errorf("nil input should yield zero-len output: got %q", out)
	}
}
