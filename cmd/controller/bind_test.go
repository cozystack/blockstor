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

package main

import (
	"net"
	"testing"
)

// With --enable-rest-api the controller serves the plain API too, with no
// authentication, so its default has to stay on loopback as well.
func TestPlainListenerDefaultsToLoopback(t *testing.T) {
	t.Parallel()

	host, _, err := net.SplitHostPort(defaultPlainBindAddress)
	if err != nil {
		t.Fatalf("%q is not host:port: %v", defaultPlainBindAddress, err)
	}

	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		t.Fatalf("plain listener defaults to %q, want a loopback address", defaultPlainBindAddress)
	}
}

// An empty address is refused, since net.Listen would bind every interface
// on a random port, and an address off loopback is flagged as exposed.
func TestCheckPlainBindAddress(t *testing.T) {
	t.Parallel()

	for addr, want := range map[string]struct{ exposed, fails bool }{
		"":               {fails: true},
		"3370":           {fails: true},
		"127.0.0.1:3370": {},
		"[::1]:3370":     {},
		"localhost:3370": {},
		":3370":          {exposed: true},
		"0.0.0.0:3370":   {exposed: true},
		"10.0.0.5:3370":  {exposed: true},
	} {
		exposed, err := checkPlainBindAddress(addr)
		if (err != nil) != want.fails || exposed != want.exposed {
			t.Errorf("checkPlainBindAddress(%q) = %v, %v; want exposed=%v fails=%v", addr, exposed, err, want.exposed, want.fails)
		}
	}
}
