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

package satellite

import (
	"strings"
	"testing"
)

// TestResolveAddr: whenever the controller-supplied address is empty
// or the 0.0.0.0 placeholder, resolveAddr substitutes the satellite's
// own IP. A non-empty / non-placeholder input passes through verbatim.
//
// The empty-fallback branch returns the placeholder unchanged — pinned
// here so unit tests of the reconciler that don't bother setting
// LocalAddress keep working without surprises.
func TestResolveAddr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name               string
		supplied, fallback string
		want               string
	}{
		{"placeholder + fallback", "0.0.0.0", "10.244.1.5", "10.244.1.5"},
		{"empty + fallback", "", "10.244.1.5", "10.244.1.5"},
		{"placeholder + empty fallback", "0.0.0.0", "", "0.0.0.0"},
		{"non-placeholder pass-through", "10.0.0.7", "10.244.1.5", "10.0.0.7"},
		{"non-placeholder + empty fallback", "10.0.0.7", "", "10.0.0.7"},
	}

	for _, c := range cases {
		got := resolveAddr(c.supplied, c.fallback)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// TestHelloErrorWraps pins the error-wrap on the registration RPC:
// when the controller's Hello returns an error (controller mid-
// restart, network partition, TLS handshake failure), the satellite's
// TestRunRequiresNodeName pins the early-validation branch of Run:
// an Agent constructed without NodeName must fail-fast with an
// explicit error, not crash later inside dial() with a confusing
// gRPC stack trace. Pinned because the satellite binary's main()
// trusts Run's err to surface bad config — a regression that
// silently let an empty NodeName through would propagate as
// "Hello: missing node_name" from the controller side, miles
// away from the actual misconfiguration.
func TestRunRequiresNodeName(t *testing.T) {
	t.Parallel()

	a := NewAgent(Config{}) // empty NodeName

	err := a.Run(t.Context())
	if err == nil {
		t.Fatalf("Run with empty NodeName: got nil, want error")
	}

	if !strings.Contains(err.Error(), "NodeName") {
		t.Errorf("error must name the missing field; got %q", err.Error())
	}
}
