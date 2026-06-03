// SPDX-License-Identifier: Apache-2.0

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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Corner-case campaign — group B (quorum / auto-quorum property
// semantics). These pin the empty-value=delete rule (B5 / I1) on the
// RD and RG property-modify surfaces and the shared applyPropsModify
// core directly.
//
// UG9 §"Auto-quorum policies" NOTE (~4277-4279): "Setting
// `DrbdOptions/Resource/on-no-quorum` to an empty value … deletes the
// property from the object entirely." The server must converge to the
// key being ABSENT — not present with an empty string value — so a
// subsequent `list-properties` shows no key at all.

const (
	cbOnNoQuorumKey = "DrbdOptions/Resource/on-no-quorum"
	cbQuorumKey     = "DrbdOptions/Resource/quorum"
	cbAutoQuorumKey = "DrbdOptions/auto-quorum"
)

// TestCornerB_RDEmptyOverrideDeletesProp: an `override_props` entry
// with an empty value on the RD-modify PUT must DELETE the key, not
// store an empty string. Mirrors `linstor rd set-property <rd>
// DrbdOptions/Resource/on-no-quorum` (no value).
func TestCornerB_RDEmptyOverrideDeletesProp(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Seed an RD carrying the key we will clear.
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "cc-b-rd",
		Props: map[string]string{
			cbOnNoQuorumKey: "io-error",
			cbQuorumKey:     "off",
		},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	body, err := json.Marshal(map[string]any{
		"override_props": map[string]string{cbOnNoQuorumKey: ""},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/cc-b-rd", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200; body=%s", resp.StatusCode, readAll(t, resp))
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "cc-b-rd")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if v, present := got.Props[cbOnNoQuorumKey]; present {
		t.Errorf("B5: %s still present with value %q; empty override must DELETE the key",
			cbOnNoQuorumKey, v)
	}

	// The untouched key must survive.
	if got.Props[cbQuorumKey] != "off" {
		t.Errorf("B5: sibling key %s clobbered: got %q, want %q",
			cbQuorumKey, got.Props[cbQuorumKey], "off")
	}
}

// TestCornerB_RGEmptyOverrideDeletesProp: same rule on the RG-modify
// PUT. Mirrors the plan's verbatim sequence `rg set-property g
// DrbdOptions/Resource/on-no-quorum` (no value).
func TestCornerB_RGEmptyOverrideDeletesProp(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: "cc-b-rg",
		Props: map[string]string{
			cbAutoQuorumKey: "disabled",
			cbOnNoQuorumKey: "suspend-io",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	body, err := json.Marshal(map[string]any{
		"override_props": map[string]string{cbOnNoQuorumKey: ""},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-groups/cc-b-rg", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200; body=%s", resp.StatusCode, readAll(t, resp))
	}

	got, err := st.ResourceGroups().Get(t.Context(), "cc-b-rg")
	if err != nil {
		t.Fatalf("get RG: %v", err)
	}

	if v, present := got.Props[cbOnNoQuorumKey]; present {
		t.Errorf("B5: %s still present with value %q; empty override must DELETE the key",
			cbOnNoQuorumKey, v)
	}

	if got.Props[cbAutoQuorumKey] != "disabled" {
		t.Errorf("B5: sibling key %s clobbered: got %q, want %q",
			cbAutoQuorumKey, got.Props[cbAutoQuorumKey], "disabled")
	}
}

// TestCornerB_RDDeletePropsStillWorks: the explicit `delete_props`
// path keeps working alongside the empty-override path — both routes
// converge on key absence.
func TestCornerB_RDDeletePropsStillWorks(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  "cc-b-rd2",
		Props: map[string]string{cbOnNoQuorumKey: "io-error"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	body, err := json.Marshal(map[string]any{
		"delete_props": []string{cbOnNoQuorumKey},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/cc-b-rd2", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200; body=%s", resp.StatusCode, readAll(t, resp))
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "cc-b-rd2")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if _, present := got.Props[cbOnNoQuorumKey]; present {
		t.Errorf("B5: delete_props did not remove %s", cbOnNoQuorumKey)
	}
}

// TestCornerB_ApplyPropsModifyEmptyDeletes unit-pins the shared core
// directly: empty override value deletes; non-empty sets;
// delete_props strips; a nil starting bag is allocated lazily.
func TestCornerB_ApplyPropsModifyEmptyDeletes(t *testing.T) {
	t.Parallel()

	start := map[string]string{"keep": "v", "drop-empty": "old", "drop-explicit": "old"}

	out := applyPropsModify(start,
		map[string]string{"drop-empty": "", "add": "new"},
		[]string{"drop-explicit"})

	if _, present := out["drop-empty"]; present {
		t.Errorf("empty override must delete key, got %q", out["drop-empty"])
	}

	if _, present := out["drop-explicit"]; present {
		t.Errorf("delete_props must strip key")
	}

	if out["add"] != "new" {
		t.Errorf("non-empty override must set: got %q", out["add"])
	}

	if out["keep"] != "v" {
		t.Errorf("untouched key must survive: got %q", out["keep"])
	}

	// nil bag + only-deletes => stays a usable (empty) map, no panic.
	got := applyPropsModify(nil, nil, []string{"ghost"})
	if got == nil {
		t.Errorf("nil bag with delete should allocate, got nil")
	}
}

// TestCornerB_DefaultQuorumPairIsDeliberateDelta pins corner-case B6
// and the DELIBERATE DELTA it records.
//
// UG9 §"Auto-quorum policies" TIP (~4251-4253): the upstream LINSTOR
// default policy pair is `quorum majority` + `on-no-quorum io-error`.
// blockstor INTENTIONALLY diverges on the second half — it seeds
// `on-no-quorum=suspend-io` instead (Bug 297, P1 data-loss class:
// io-error freezes the minority replica in a state that survives
// partition heal and breaks auto-promote; suspend-io blocks I/O until
// quorum returns, then resumes cleanly). This divergence is recorded
// in docs/cli-parity-known-deltas.md and must NOT be silently
// "fixed" back to io-error.
//
// This test is the guard: it pins the actual blockstor seed so a
// future refactor that flips it back to io-error fails loudly and the
// author is forced to confront the Bug 297 rationale + the delta row.
func TestCornerB_DefaultQuorumPairIsDeliberateDelta(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "cc-b-defaults"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201; body=%s", resp.StatusCode, readAll(t, resp))
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "cc-b-defaults")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	// First half MATCHES upstream verbatim: auto-quorum=majority.
	if got.Props[cbAutoQuorumKey] != "majority" {
		t.Errorf("B6: %s: got %q, want %q (upstream default, must match)",
			cbAutoQuorumKey, got.Props[cbAutoQuorumKey], "majority")
	}

	// Second half is the DELIBERATE DELTA: blockstor seeds suspend-io,
	// NOT the upstream-documented io-error. Pin suspend-io so a flip
	// back to io-error (reintroducing the Bug 297 data-loss) is caught.
	if got.Props[cbOnNoQuorumKey] != "suspend-io" {
		t.Errorf("B6 DELIBERATE DELTA: %s: got %q, want %q "+
			"(blockstor diverges from upstream io-error for Bug 297 data-safety; "+
			"see docs/cli-parity-known-deltas.md)",
			cbOnNoQuorumKey, got.Props[cbOnNoQuorumKey], "suspend-io")
	}
}
