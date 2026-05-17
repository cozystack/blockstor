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

package drbd_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// Regression coverage for Bug 258 (renderer half): once the splitter
// routes DrbdOptions/Disk/*, /Handlers/*, /PeerDevice/* into per-
// section maps on drbd.Resource, the renderer must EMIT the matching
// `.res` blocks. drbd-9 rejects these keys inside `options{}` — they
// must live in their own `disk{}` / `handlers{}` blocks at the
// resource scope, and `disk{}` (peer-device flavour) inside each
// `connection{}` block.

// TestRenderEmitsDiskBlock: Resource.Disk options surface inside a
// dedicated `disk { ... }` block at the resource scope, NOT inside
// `options{}`. Pinning the canonical `on-io-error detach;` line
// drbd-9 expects.
func TestRenderEmitsDiskBlock(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Disk: map[string]string{"on-io-error": "detach"},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !strings.Contains(got, "disk {") {
		t.Errorf("missing disk{} block; got:\n%s", got)
	}

	// The on-io-error line must live inside disk{}, not options{}.
	diskStart, diskEnd := blockBounds(t, got, "disk {")
	if !strings.Contains(got[diskStart:diskEnd], "on-io-error detach;") {
		t.Errorf("on-io-error not inside disk{} block; block=%q\nfull=%s", got[diskStart:diskEnd], got)
	}
}

// TestRenderEmitsHandlersBlock: Resource.Handlers surface inside a
// dedicated `handlers { ... }` block. drbd-9 rejects handler keys
// (fence-peer, after-resync-target, …) at any other scope.
func TestRenderEmitsHandlersBlock(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name:     "pvc-1",
		Handlers: map[string]string{"fence-peer": "/usr/lib/drbd/crm-fence-peer.9.sh"},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !strings.Contains(got, "handlers {") {
		t.Errorf("missing handlers{} block; got:\n%s", got)
	}

	handlersStart, handlersEnd := blockBounds(t, got, "handlers {")
	if !strings.Contains(got[handlersStart:handlersEnd], "fence-peer /usr/lib/drbd/crm-fence-peer.9.sh;") {
		t.Errorf("fence-peer not inside handlers{} block; block=%q\nfull=%s", got[handlersStart:handlersEnd], got)
	}
}

// TestRenderEmitsPeerDeviceDiskBlock: Resource.PeerDevice options
// surface as a `disk { ... }` sub-block INSIDE each `connection {
// ... }` block (drbd-9's location for peer-device-scope options).
func TestRenderEmitsPeerDeviceDiskBlock(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name:       "pvc-1",
		PeerDevice: map[string]string{"c-fill-target": "1M"},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	connStart, connEnd := blockBounds(t, got, "connection {")
	connBlock := got[connStart:connEnd]

	if !strings.Contains(connBlock, "disk {") {
		t.Errorf("missing disk{} block inside connection{}; conn=%q\nfull=%s", connBlock, got)
	}

	if !strings.Contains(connBlock, "c-fill-target 1M;") {
		t.Errorf("c-fill-target not inside per-connection disk{}; conn=%q\nfull=%s", connBlock, got)
	}
}

// TestRenderRoundtripDoesNotPlaceDiskKeyInOptions: negative witness.
// After the fix, no `on-io-error` (or any disk-section key) must
// appear directly inside the resource-scope `options { ... }` block.
// Pre-fix, splitDRBDOptions stuffed it there — this assertion
// FAILS on baseline and PASSES post-fix.
func TestRenderRoundtripDoesNotPlaceDiskKeyInOptions(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name:    "pvc-1",
		Disk:    map[string]string{"on-io-error": "detach"},
		Options: map[string]string{"on-no-quorum": "io-error"}, // legitimate options-block key
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Walk just the `options { ... }` block contents — `on-io-error`
	// MUST NOT appear there. The legitimate `on-no-quorum` line
	// must, though, so we don't accidentally pin "options{} empty".
	optStart, optEnd := blockBounds(t, got, "options {")
	optBlock := got[optStart:optEnd]

	if strings.Contains(optBlock, "on-io-error") {
		t.Errorf("on-io-error leaked into options{} block (the Bug 258 misroute)\noptions=%q\nfull=%s", optBlock, got)
	}

	if !strings.Contains(optBlock, "on-no-quorum io-error;") {
		t.Errorf("legitimate resource-options key dropped; options=%q\nfull=%s", optBlock, got)
	}
}

// TestRenderHandlersResourceOptionsDiskAllCoexist: all three
// post-fix blocks must coexist with the existing net{}/options{}
// blocks in the same .res, in upstream-compatible order.
func TestRenderHandlersResourceOptionsDiskAllCoexist(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name:     "pvc-1",
		Net:      drbd.Net{Options: map[string]string{"max-buffers": "8000"}},
		Options:  map[string]string{"on-no-quorum": "io-error"},
		Disk:     map[string]string{"on-io-error": "detach"},
		Handlers: map[string]string{"fence-peer": "/usr/lib/drbd/crm-fence-peer.9.sh"},
		Hosts: []drbd.Host{
			{NodeName: "n1", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "n2", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, want := range []string{
		"net {",
		"options {",
		"disk {",
		"handlers {",
		"max-buffers 8000;",
		"on-no-quorum io-error;",
		"on-io-error detach;",
		"fence-peer /usr/lib/drbd/crm-fence-peer.9.sh;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in output:\n%s", want, got)
		}
	}
}

// blockBounds finds the index range of a `<name> {` block's body —
// returns (openIdx, closeIdx) such that got[openIdx:closeIdx] holds
// everything between the matching brace pair. Uses simple brace-
// depth tracking so nested blocks (path{}, on{} inside resource{})
// don't confuse the search.
func blockBounds(t *testing.T, s, header string) (int, int) {
	t.Helper()

	start := strings.Index(s, header)
	if start < 0 {
		t.Fatalf("block %q not found in:\n%s", header, s)
	}

	depth := 0
	i := start

	for ; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return start, i
			}
		}
	}

	t.Fatalf("block %q unterminated in:\n%s", header, s)

	return 0, 0
}
