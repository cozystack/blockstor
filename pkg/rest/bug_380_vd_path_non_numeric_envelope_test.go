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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 380 (P3, bughunt round 11 — 2026-06-02): the {vn} URL pathvar
// on every per-volume-definition REST entry point
// (`GET /v1/resource-definitions/{rd}/volume-definitions/{vn}`,
// PUT, DELETE, plus the per-resource `volumes/{vlmNr}` mirror)
// leaked a raw `strconv.ParseInt: parsing "abc": invalid syntax`
// when the caller passed a non-numeric segment. The stdlib func name
// in the wire body is internal Go plumbing — operators have no idea
// what the allowed range is, and the python CLI's XML decoder
// fallback can crash on the unexpected punctuation.
//
// Bug 365 already shipped a `volume_number must be in [0, 65535]`
// correction on the POST/DELETE *body* branch. Bug 380 brings the
// URL-path branch to the same shape via parseVolNum, so every wire
// edge in the VD CRUD surface returns the same operator-grade
// correction hint regardless of where the bad value arrived.

// TestBug380_VDGetNonNumericPathReturnsRangeHint pins the GET path.
// Pre-fix: body=`strconv.ParseInt: parsing "abc": invalid syntax`,
// status=400. Post-fix: body names the field, gives the [0, 65535]
// range, references the LINSTOR REST API documentation.
func TestBug380_VDGetNonNumericPathReturnsRangeHint(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t,
		base+"/v1/resource-definitions/some-rd/volume-definitions/abc")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	assertVolNumEnvelope(t, body, "abc")
}

// TestBug380_VDDeleteNonNumericPathReturnsRangeHint pins the DELETE
// path. Symmetric with the GET assertion above.
func TestBug380_VDDeleteNonNumericPathReturnsRangeHint(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t,
		base+"/v1/resource-definitions/some-rd/volume-definitions/abc")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	assertVolNumEnvelope(t, body, "abc")
}

// TestBug380_VolumeGetNonNumericPathReturnsRangeHint pins the
// per-resource volumes mirror that ships parseVolNum from
// volumes_per_resource.go. Pre-fix that handler also surfaced
// `strconv.ParseInt: ...` because it shared parseVolNum.
func TestBug380_VolumeGetNonNumericPathReturnsRangeHint(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(),
		&apiv1.ResourceDefinition{Name: "rd-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t,
		base+"/v1/resource-definitions/rd-1/resources/some-node/volumes/xyz")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	assertVolNumEnvelope(t, body, "xyz")
}

// assertVolNumEnvelope is the shared shape-assertion the Bug 380
// tests reuse: ApiCallRc array of one entry whose message names the
// offending segment, mentions `volume_number`, AND surfaces the
// canonical [0, 65535] range. Crucially the message must NOT leak
// the stdlib `strconv.ParseInt:` prefix.
func assertVolNumEnvelope(t *testing.T, body []byte, badSegment string) {
	t.Helper()

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(body, &rcs); err != nil {
		t.Fatalf("unmarshal envelope: %v; body=%s", err, body)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope length: got %d, want 1; body=%s", len(rcs), body)
	}

	msg := rcs[0].Message

	if strings.Contains(msg, "strconv.ParseInt") {
		t.Errorf("Bug 380: message still leaks raw strconv.ParseInt prefix: %q", msg)
	}

	if !strings.Contains(msg, badSegment) {
		t.Errorf("message must name the offending segment %q; got %q", badSegment, msg)
	}

	if !strings.Contains(msg, "volume_number") {
		t.Errorf("message must mention `volume_number` field; got %q", msg)
	}

	if !strings.Contains(msg, "[0, 65535]") {
		t.Errorf("message must surface the canonical [0, 65535] range; got %q", msg)
	}
}
