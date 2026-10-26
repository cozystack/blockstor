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
