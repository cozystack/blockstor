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

import "testing"

// TestHostFromEndpoint pins the trailing-port stripper Hello uses to
// derive the dial-back host from a SatelliteEndpoint prop. Same
// LastIndex shape as the dispatcher's peerAddress (IPv6-aware).
func TestHostFromEndpoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"10.244.1.5:7000", "10.244.1.5"},
		{"localhost:7001", "localhost"},
		{"no-colon-here", "no-colon-here"}, // returned verbatim when no port
		{"[fe80::1]:7000", "[fe80::1]"},    // IPv6 (LastIndex picks rightmost colon)
		{":7000", ":7000"},                 // empty-host edge case → leniency, return as-is
	}
	for _, c := range cases {
		got := hostFromEndpoint(c.in)
		if got != c.want {
			t.Errorf("hostFromEndpoint(%q): got %q, want %q", c.in, got, c.want)
		}
	}
}

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
