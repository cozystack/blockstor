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
