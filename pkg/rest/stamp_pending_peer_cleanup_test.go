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
	"context"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug342V12CStampPendingPeerCleanupWritesAnnotation pins the
// happy path: stampPendingPeerCleanup writes
// blockstor.io/pending-peer-cleanup-<peer> with an RFC3339Nano
// timestamp onto the parent RD's Annotations.
func TestBug342V12CStampPendingPeerCleanupWritesAnnotation(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rdA"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	srv := &Server{Store: st}

	before := time.Now().Add(-1 * time.Second)
	srv.stampPendingPeerCleanup(ctx, "rdA", "n3")

	got, err := st.ResourceDefinitions().Get(ctx, "rdA")
	if err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	val, ok := got.Annotations[apiv1.PendingPeerCleanupAnnotationPrefix+"n3"]
	if !ok {
		t.Fatalf("expected annotation %s%s; got map %v",
			apiv1.PendingPeerCleanupAnnotationPrefix, "n3", got.Annotations)
	}

	stamped, parseErr := time.Parse(time.RFC3339Nano, val)
	if parseErr != nil {
		t.Fatalf("annotation value %q is not RFC3339Nano: %v", val, parseErr)
	}

	if stamped.Before(before) {
		t.Errorf("stamp %v predates test start %v — clock skew or wrong source?", stamped, before)
	}
}

// TestBug342V12CStampPendingPeerCleanupNotFoundIsSilent pins the
// best-effort behaviour: a stamp against a non-existent RD MUST NOT
// panic or surface — a concurrent rd-delete cascade is the most
// common reason and the caller doesn't care.
func TestBug342V12CStampPendingPeerCleanupNotFoundIsSilent(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	srv := &Server{Store: st}

	// Just don't panic — there's no explicit "did the stamp fail"
	// surface (helper returns void) so a successful call here = pass.
	srv.stampPendingPeerCleanup(ctx, "rd-never-existed", "n3")
}

// TestBug342V12CStampPendingPeerCleanupPreservesOtherAnnotations
// pins co-existence: stamping a pending-peer-cleanup marker MUST
// NOT clobber other annotations (PeerChanged, TiebreakerSuppression,
// etc.) on the same RD.
func TestBug342V12CStampPendingPeerCleanupPreservesOtherAnnotations(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rdB",
		Annotations: map[string]string{
			apiv1.AutoTiebreakerSuppressedUntilAnnotation: "2099-01-01T00:00:00Z",
			"blockstor.io/other-marker":                   "preserve-me",
		},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	srv := &Server{Store: st}
	srv.stampPendingPeerCleanup(ctx, "rdB", "n2")

	got, err := st.ResourceDefinitions().Get(ctx, "rdB")
	if err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	for _, key := range []string{
		apiv1.AutoTiebreakerSuppressedUntilAnnotation,
		"blockstor.io/other-marker",
	} {
		if _, ok := got.Annotations[key]; !ok {
			t.Errorf("annotation %s clobbered after stampPendingPeerCleanup", key)
		}
	}

	if _, ok := got.Annotations[apiv1.PendingPeerCleanupAnnotationPrefix+"n2"]; !ok {
		t.Errorf("expected new marker %s%s; got %v",
			apiv1.PendingPeerCleanupAnnotationPrefix, "n2", got.Annotations)
	}

	// Sanity check key shape — the consumer (controller + satellite
	// reaper) parses by prefix.CutPrefix, so a regression that
	// changed the format would break the chain.
	for key := range got.Annotations {
		if strings.HasPrefix(key, apiv1.PendingPeerCleanupAnnotationPrefix) {
			peer := strings.TrimPrefix(key, apiv1.PendingPeerCleanupAnnotationPrefix)
			if peer != "n2" {
				t.Errorf("unexpected peer in marker key %q: got %q want n2", key, peer)
			}
		}
	}
}
