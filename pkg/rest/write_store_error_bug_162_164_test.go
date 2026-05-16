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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Static errors synthesised for the writeStoreError surface tests.
// These mirror the exact substrings the K8s-backed store would carry
// up from etcd and apimachinery before Bug 162's scrub guard.
//
// The apimachinery fixture preserves apimachinery's verbatim
// capitalised "Operation cannot be fulfilled on …" message
// (`fmt.Sprintf` in apierrors.NewConflict) — we need the exact bytes
// to verify the GroupResource fingerprint is scrubbed off the wire.
var (
	errEtcdTooLarge = errors.New("etcdserver: request is too large")
	//nolint:staticcheck // ST1005: fixture mirrors apimachinery's
	// literal "Operation cannot be fulfilled on …" message verbatim
	// so the scrub test sees the exact byte sequence emitted in prod.
	errApimachineryConflict = errors.New(
		`Operation cannot be fulfilled on controllerconfigs.blockstor.io.blockstor.io "default": stale`)
	errBug164ObjectModified = errors.New("the object has been modified")
)

// TestBug162WriteStoreErrorScrubsEtcdString pins Bug 162: an etcd-side
// failure ("etcdserver: request is too large") that bubbles up through
// the K8s-backed store reaches writeStoreError as a raw error string.
// Before the fix that string was passed straight through to the wire,
// leaking the persistence-backend identity (sibling to Bug 146, which
// only scrubbed the inbound JSON-decode path).
//
// The CSI driver doesn't care about the leak, but operators running
// curl against the apiserver see exactly which storage backend is in
// play. The fix routes every non-sentinel branch through
// scrubImplDetails before emitting.
func TestBug162WriteStoreErrorScrubsEtcdString(t *testing.T) {
	t.Parallel()

	rr := httptest.NewRecorder()
	writeStoreError(rr, errEtcdTooLarge)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status: got %d, want 500", rr.Code)
	}

	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if strings.Contains(strings.ToLower(string(body)), "etcd") {
		t.Errorf("body leaks etcd impl detail: %s", body)
	}

	// envelope must still be the LINSTOR `[]ApiCallRc` shape so
	// python-linstor / golinstor decode the failure correctly.
	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR set)", rc[0].RetCode)
	}

	// Generic message must still convey "store error" so operators
	// have something searchable in logs.
	if !strings.Contains(strings.ToLower(rc[0].Message), "store") {
		t.Errorf("scrubbed message should still mention 'store': %q", rc[0].Message)
	}
}

// TestBug162WriteStoreErrorScrubsApimachineryString pins the second
// Bug 162 flavor: apimachinery's NewConflict/NewAlreadyExists/etc.
// embed the GroupResource ("controllerconfigs.blockstor.io") in the
// error string. Without scrubbing, the wire response leaks both the
// CRD plural and the API group. We assert both substrings are gone.
func TestBug162WriteStoreErrorScrubsApimachineryString(t *testing.T) {
	t.Parallel()

	rr := httptest.NewRecorder()
	// errApimachineryConflict mirrors the exact
	// "controllerconfigs.blockstor.io.blockstor.io" fingerprint
	// apimachinery emits — qualifiedResource.String() is
	// `<resource>.<group>`, and the resource itself often carries the
	// group suffix, producing the double-suffix shape.
	writeStoreError(rr, errApimachineryConflict)

	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	for _, leak := range []string{
		"controllerconfigs.blockstor.io",
		"apimachinery",
		"k8s.io",
		"etcd",
	} {
		if strings.Contains(strings.ToLower(string(body)), strings.ToLower(leak)) {
			t.Errorf("body leaks impl detail %q: %s", leak, body)
		}
	}
}

// TestBug164IsConflictReturns409 pins Bug 164: an optimistic-lock
// conflict from the K8s store (apierrors.IsConflict-matching) MUST
// surface as HTTP 409, not 500.
//
// linstor-csi (and the upstream Reconciler retry loop) treat 5xx as
// fatal but 409 as retryable — a conflict bumped to 500 wedges every
// CSI operation against the same RD until the operator restarts the
// driver. The wire envelope must still be `[]ApiCallRc`.
func TestBug164IsConflictReturns409(t *testing.T) {
	t.Parallel()

	gr := schema.GroupResource{Group: "blockstor.io", Resource: "resourcedefinitions.blockstor.io"}
	conflictErr := apierrors.NewConflict(gr, "rd-test", errBug164ObjectModified)

	rr := httptest.NewRecorder()
	writeStoreError(rr, conflictErr)

	if rr.Code != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (csi treats 5xx as fatal, 409 as retryable)", rr.Code)
	}

	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR set)", rc[0].RetCode)
	}

	// The conflict path must also be scrubbed: apierrors.NewConflict
	// embeds the GroupResource string.
	for _, leak := range []string{
		"resourcedefinitions.blockstor.io",
		"apimachinery",
		"k8s.io",
		"etcd",
	} {
		if strings.Contains(strings.ToLower(string(body)), strings.ToLower(leak)) {
			t.Errorf("conflict body leaks impl detail %q: %s", leak, body)
		}
	}
}

// TestBug164IsAlreadyExistsReturns409 pins the sibling branch: a
// k8s-shaped AlreadyExists error (the K8s store may surface this when
// a Create races a parallel Create against the same RD name) must map
// to 409 even though it does NOT match the local `store.ErrAlreadyExists`
// sentinel. Before the fix the apimachinery error fell through to the
// default 500 branch, breaking idempotent create flows in linstor-csi.
func TestBug164IsAlreadyExistsReturns409(t *testing.T) {
	t.Parallel()

	gr := schema.GroupResource{Group: "blockstor.io", Resource: "resourcedefinitions.blockstor.io"}
	existsErr := apierrors.NewAlreadyExists(gr, "rd-test")

	rr := httptest.NewRecorder()
	writeStoreError(rr, existsErr)

	if rr.Code != http.StatusConflict {
		t.Errorf("status: got %d, want 409", rr.Code)
	}

	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative", rc[0].RetCode)
	}
}
