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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestInternalAnnotationsStrippedOnWireBoundary pins Bug-hunt v0.1.3
// Finding 2: the REST list / get handlers for Resource,
// ResourceDefinition, ResourceGroup, and Snapshot must NOT leak
// controller-internal `blockstor.io/*` and
// `bug<N>.blockstor.cozystack.io/*` annotations. The reconcilers stamp
// these onto the in-process structs for their own bookkeeping (the
// struct field must stay so reconcilers can keep reading), but the
// wire boundary scrubs them.
//
// The test seeds one CRD of each type, each carrying:
//   - one internal `blockstor.io/...` annotation,
//   - one per-bug `bugNNN.blockstor.cozystack.io/...` annotation,
//   - one third-party / operator-set annotation that MUST pass through.
//
// then exercises every list / get path mentioned in the bug-hunt
// report (view/resources, per-RD list, per-RD per-node get, RD list,
// RD get, RG list, RG get, snapshot view, per-RD snapshot list, per-
// RD per-snapshot get) and asserts that the wire payload carries only
// the third-party key.
func TestInternalAnnotationsStrippedOnWireBoundary(t *testing.T) {
	const (
		canonicalInternalKey = "blockstor.io/peer-changed"
		bugScopedInternalKey = "bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-0"
		thirdPartyKey        = "operator.example.com/owner"
		thirdPartyValue      = "cozystack-team"
	)

	internalAnnotations := map[string]string{
		canonicalInternalKey: "2026-05-30T16:57:22.019326537Z",
		bugScopedInternalKey: "4096",
		thirdPartyKey:        thirdPartyValue,
	}

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed: one RG, one RD, one Resource (single node), one Snapshot —
	// each carrying the same annotation bag so every wire path is
	// exercised uniformly.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:        "rg-leak",
		Annotations: cloneMap(internalAnnotations),
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd-leak",
		ResourceGroupName: "rg-leak",
		Annotations:       cloneMap(internalAnnotations),
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:        "rd-leak",
		NodeName:    "n1",
		Annotations: cloneMap(internalAnnotations),
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-leak",
		ResourceName: "rd-leak",
		Annotations:  cloneMap(internalAnnotations),
	}); err != nil {
		t.Fatalf("seed Snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Each row is one wire path + a closure that pulls the annotations
	// bag out of the decoded payload. The two assertions on every
	// path are identical, so we factor the wire check into a single
	// helper to keep the table dense.
	cases := []struct {
		name string
		path string
		// extractAnnotations is invoked with the response body and
		// returns the (possibly multiple) per-object annotation maps
		// the path emits.
		extractAnnotations func(t *testing.T, body []byte) []map[string]string
	}{
		{
			name: "view/resources",
			path: "/v1/view/resources",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got []apiv1.ResourceWithVolumes
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return resourceWithVolumesAnnotations(got)
			},
		},
		{
			name: "resource-definitions/{rd}/resources",
			path: "/v1/resource-definitions/rd-leak/resources",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got []apiv1.ResourceWithVolumes
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return resourceWithVolumesAnnotations(got)
			},
		},
		{
			name: "resource-definitions/{rd}/resources/{node}",
			path: "/v1/resource-definitions/rd-leak/resources/n1",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got apiv1.ResourceWithVolumes
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return []map[string]string{got.Annotations}
			},
		},
		{
			name: "resource-definitions list",
			path: "/v1/resource-definitions",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got []apiv1.ResourceDefinition
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				out := make([]map[string]string, 0, len(got))
				for i := range got {
					out = append(out, got[i].Annotations)
				}
				return out
			},
		},
		{
			name: "resource-definitions/{rd} get",
			path: "/v1/resource-definitions/rd-leak",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got apiv1.ResourceDefinition
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return []map[string]string{got.Annotations}
			},
		},
		{
			name: "resource-groups list",
			path: "/v1/resource-groups",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got []apiv1.ResourceGroup
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				out := make([]map[string]string, 0, len(got))
				for i := range got {
					out = append(out, got[i].Annotations)
				}
				return out
			},
		},
		{
			name: "resource-groups/{rg} get",
			path: "/v1/resource-groups/rg-leak",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got apiv1.ResourceGroup
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return []map[string]string{got.Annotations}
			},
		},
		{
			name: "view/snapshots",
			path: "/v1/view/snapshots",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got []apiv1.Snapshot
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return snapshotAnnotations(got)
			},
		},
		{
			name: "resource-definitions/{rd}/snapshots list",
			path: "/v1/resource-definitions/rd-leak/snapshots",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got []apiv1.Snapshot
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return snapshotAnnotations(got)
			},
		},
		{
			name: "resource-definitions/{rd}/snapshots/{snap} get",
			path: "/v1/resource-definitions/rd-leak/snapshots/snap-leak",
			extractAnnotations: func(t *testing.T, body []byte) []map[string]string {
				t.Helper()
				var got apiv1.Snapshot
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				return []map[string]string{got.Annotations}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := httpGet(t, base+tc.path)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200", resp.StatusCode)
			}

			body, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				t.Fatalf("read body: %v", readErr)
			}

			annoMaps := tc.extractAnnotations(t, body)

			if len(annoMaps) == 0 {
				t.Fatalf("no annotation maps decoded from %s — wire payload: %s",
					tc.path, body)
			}

			for i, anno := range annoMaps {
				if _, leaked := anno[canonicalInternalKey]; leaked {
					t.Errorf("path %s entry %d leaks internal annotation %q: %v",
						tc.path, i, canonicalInternalKey, anno)
				}

				if _, leaked := anno[bugScopedInternalKey]; leaked {
					t.Errorf("path %s entry %d leaks bug-scoped annotation %q: %v",
						tc.path, i, bugScopedInternalKey, anno)
				}

				if got := anno[thirdPartyKey]; got != thirdPartyValue {
					t.Errorf("path %s entry %d dropped operator annotation: got %q for key %q, want %q (full anno bag: %v)",
						tc.path, i, got, thirdPartyKey, thirdPartyValue, anno)
				}
			}
		})
	}

	// After the wire exercises, every in-store Annotations bag MUST
	// still carry the internal keys — the strip is on the wire copy,
	// not the source struct. Without this assertion a regression that
	// mutated the store-cached object (e.g. via in-place strip without
	// cloning) would slip through.
	rsc, err := st.Resources().Get(ctx, "rd-leak", "n1")
	if err != nil {
		t.Fatalf("post-test resource get: %v", err)
	}

	if _, kept := rsc.Annotations[canonicalInternalKey]; !kept {
		t.Errorf("stored Resource.Annotations lost internal key %q after wire reads: %v",
			canonicalInternalKey, rsc.Annotations)
	}

	if _, kept := rsc.Annotations[bugScopedInternalKey]; !kept {
		t.Errorf("stored Resource.Annotations lost bug-scoped key %q after wire reads: %v",
			bugScopedInternalKey, rsc.Annotations)
	}
}

// TestStripInternalAnnotationsHelper pins the prefix-match contract
// for the shared helper. The wire-boundary tests above already cover
// the integration path; this unit-level pin is the canary for any
// future caller that wants to scrub a hand-built map (e.g. a
// hypothetical new CRD type that grows an Annotations slot).
func TestStripInternalAnnotationsHelper(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		dropped bool
	}{
		{name: "canonical internal", key: "blockstor.io/volume-numbers", dropped: true},
		{name: "canonical internal nested", key: "blockstor.io/auto-tiebreaker-suppressed-until", dropped: true},
		{name: "bug-scoped internal", key: "bug136.blockstor.cozystack.io/resize-pending", dropped: true},
		{name: "bug-scoped variant", key: "bug342.blockstor.cozystack.io/uid", dropped: true},
		{name: "k8s last-applied passthrough", key: "kubectl.kubernetes.io/last-applied-configuration", dropped: false},
		{name: "operator passthrough", key: "operator.example.com/owner", dropped: false},
		// Empty key is non-sensitive — defensive guard, never appears
		// in practice.
		{name: "empty", key: "", dropped: false},
		// A key that contains the suffix substring but not anchored at
		// the `<ns>/<name>` boundary stays — defensive against
		// accidental over-match.
		{name: "suffix in value position", key: "foo/" + internalAnnotationSuffix[1:] + "bar", dropped: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := map[string]string{tc.key: "v"}
			out := stripInternalAnnotations(in)

			_, present := out[tc.key]
			if tc.dropped && present {
				t.Errorf("key %q should have been dropped but stayed: %v", tc.key, out)
			}

			if !tc.dropped && !present {
				t.Errorf("key %q should have passed through but was dropped: %v", tc.key, out)
			}
		})
	}

	// nil-in => nil-out preserves omitempty on the wire (no surprise
	// `"annotations":{}` on objects that never had any).
	if out := stripInternalAnnotations(nil); out != nil {
		t.Errorf("nil-in: got %v, want nil", out)
	}
}

// cloneMap returns a shallow copy of `in` so each seeded CRD owns its
// own annotations bag — the store is in-memory and would otherwise
// share references across rows.
func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}

	return out
}

// resourceWithVolumesAnnotations flattens a slice of
// ResourceWithVolumes into the embedded `Resource.Annotations` slot
// for the assertion loop.
func resourceWithVolumesAnnotations(in []apiv1.ResourceWithVolumes) []map[string]string {
	out := make([]map[string]string, 0, len(in))
	for i := range in {
		out = append(out, in[i].Annotations)
	}

	return out
}

// snapshotAnnotations is the Snapshot sibling.
func snapshotAnnotations(in []apiv1.Snapshot) []map[string]string {
	out := make([]map[string]string, 0, len(in))
	for i := range in {
		out = append(out, in[i].Annotations)
	}

	return out
}
