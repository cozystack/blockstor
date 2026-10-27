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
	"os"
	"regexp"
	"testing"
)

// The plain listener has no authentication. A default that binds every
// interface hands the full API to any pod that can reach the pod IP.
func TestPlainListenerDefaultsToLoopback(t *testing.T) {
	t.Parallel()

	assertLoopback(t, "default", defaultPlainBindAddress)
}

// The manifest passes the flag explicitly, so it overrides the default and
// needs the same guard.
func TestStandManifestKeepsPlainListenerOnLoopback(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("../../stand/blockstor-apiserver-deploy.yaml")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	matches := regexp.MustCompile(`--rest-bind-address=(\S+)`).FindAllSubmatch(raw, -1)
	if len(matches) == 0 {
		t.Fatal("the manifest no longer sets --rest-bind-address; check what binds the plain listener")
	}

	for _, m := range matches {
		assertLoopback(t, "manifest", string(m[1]))
	}
}

func assertLoopback(t *testing.T, where, addr string) {
	t.Helper()

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("%s: %q is not host:port: %v", where, addr, err)
	}

	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		t.Fatalf("%s: plain listener binds %q, want a loopback address", where, addr)
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
