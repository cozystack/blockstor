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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// certPaths holds the on-disk PEM file locations for a freshly-issued
// key-pair plus the CA bundle that signed it.
type certPaths struct {
	certFile string
	keyFile  string
	caFile   string
}

// testCA is a minimal in-process certificate authority used to mint
// the server and client certs the mTLS tests need. It avoids any
// dependency on cert-manager or external fixtures — the whole PKI is
// generated per-test.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen CA key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	return &testCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// issue mints a leaf certificate signed by the CA. cn is stamped into
// both the CommonName and (for server certs) the DNS SAN list so a
// client verifying ServerName=cn succeeds.
func (ca *testCA) issue(t *testing.T, cn string, isServer bool) ([]byte, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen leaf key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	if isServer {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{cn}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

// writeServerCerts mints a server cert for cn and writes the cert,
// key and CA bundle into dir, returning their paths. Used both for the
// initial material and to overwrite it mid-test (hot-reload).
func writeServerCerts(t *testing.T, ca *testCA, dir, cn string) certPaths {
	t.Helper()

	certPEM, keyPEM := ca.issue(t, cn, true)

	paths := certPaths{
		certFile: filepath.Join(dir, "tls.crt"),
		keyFile:  filepath.Join(dir, "tls.key"),
		caFile:   filepath.Join(dir, "ca.crt"),
	}

	writeFile(t, paths.certFile, certPEM)
	writeFile(t, paths.keyFile, keyPEM)
	writeFile(t, paths.caFile, ca.certPEM)

	return paths
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()

	err := os.WriteFile(path, data, 0o600)
	if err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// clientFor builds an *http.Client that presents the given client
// key-pair (may be empty for the no-cert case) and trusts caPEM.
func clientFor(t *testing.T, caPEM, certPEM, keyPEM []byte) *http.Client {
	t.Helper()

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("append CA to pool")
	}

	tlsCfg := &tls.Config{
		RootCAs:    pool,
		ServerName: "blockstor-apiserver",
		MinVersion: tls.VersionTLS12,
	}

	if certPEM != nil {
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatalf("client key pair: %v", err)
		}

		tlsCfg.Certificates = []tls.Certificate{pair}
	}

	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   3 * time.Second,
	}
}

// startTLSServer boots a Server with both the plain-HTTP and mutual-TLS
// listeners on ephemeral ports and waits until both are reachable.
// Returns the plain addr, the TLS addr, and a stop func.
func startTLSServer(t *testing.T, paths certPaths) (string, string, func()) {
	t.Helper()

	plainAddr := pickFreeAddr(t)
	tlsAddr := pickFreeAddr(t)

	srv := &Server{
		Addr: plainAddr,
		TLS: &TLSOptions{
			Addr:         tlsAddr,
			CertFile:     paths.certFile,
			KeyFile:      paths.keyFile,
			ClientCAFile: paths.caFile,
		},
	}
	srv.SetResolveHost(func(_ context.Context, _ string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Start(ctx) }()

	waitReachable(t, ctx, plainAddr)
	waitReachable(t, ctx, tlsAddr)

	stop := func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Errorf("server did not stop within 3s")
		}
	}

	return plainAddr, tlsAddr, stop
}

func waitReachable(t *testing.T, ctx context.Context, addr string) {
	t.Helper()

	dialer := &net.Dialer{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)

	for {
		c, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = c.Close()

			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("addr %s never became reachable: %v", addr, err)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// TestMTLSRejectsClientWithoutCert verifies the listener enforces
// RequireAndVerifyClientCert: a client that presents NO certificate is
// rejected at the TLS handshake.
func TestMTLSRejectsClientWithoutCert(t *testing.T) {
	ca := newTestCA(t)
	paths := writeServerCerts(t, ca, t.TempDir(), "blockstor-apiserver")

	_, tlsAddr, stop := startTLSServer(t, paths)
	defer stop()

	// No client cert → handshake must fail.
	noCert := clientFor(t, ca.certPEM, nil, nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+tlsAddr+"/v1/controller/version", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := noCert.Do(req)
	if err == nil {
		_ = resp.Body.Close()

		t.Fatalf("expected handshake failure for client without cert, got status %d", resp.StatusCode)
	}
}

// TestMTLSAcceptsClientWithValidCert verifies a client that presents a
// cert signed by the configured CA completes the handshake and gets a
// 200 from /v1/controller/version.
func TestMTLSAcceptsClientWithValidCert(t *testing.T) {
	ca := newTestCA(t)
	paths := writeServerCerts(t, ca, t.TempDir(), "blockstor-apiserver")

	_, tlsAddr, stop := startTLSServer(t, paths)
	defer stop()

	clientCert, clientKey := ca.issue(t, "blockstor-csi", false)
	good := clientFor(t, ca.certPEM, clientCert, clientKey)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+tlsAddr+"/v1/controller/version", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := good.Do(req)
	if err != nil {
		t.Fatalf("valid-cert client request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}

// TestMTLSRejectsClientCertFromUnknownCA verifies a client cert signed
// by a DIFFERENT CA is rejected even though it is a syntactically valid
// client cert — the server validates the chain against ITS client CA.
func TestMTLSRejectsClientCertFromUnknownCA(t *testing.T) {
	ca := newTestCA(t)
	paths := writeServerCerts(t, ca, t.TempDir(), "blockstor-apiserver")

	_, tlsAddr, stop := startTLSServer(t, paths)
	defer stop()

	// A leaf signed by a foreign CA; the client still trusts the real
	// server CA so the server side is what must reject it.
	rogue := newTestCA(t)
	rogueCert, rogueKey := rogue.issue(t, "attacker", false)
	client := clientFor(t, ca.certPEM, rogueCert, rogueKey)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+tlsAddr+"/v1/controller/version", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()

		t.Fatalf("expected rejection of client cert from unknown CA, got status %d", resp.StatusCode)
	}
}

// TestPlainDebugListenerStillServes verifies the plain-HTTP debug
// listener serves on its own port even when the TLS listener is up —
// this is the `kubectl port-forward` path.
func TestPlainDebugListenerStillServes(t *testing.T) {
	ca := newTestCA(t)
	paths := writeServerCerts(t, ca, t.TempDir(), "blockstor-apiserver")

	plainAddr, _, stop := startTLSServer(t, paths)
	defer stop()

	resp := httpGet(t, "http://"+plainAddr+"/v1/controller/version")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("plain debug listener status: got %d, want 200", resp.StatusCode)
	}
}

// TestCertHotReload verifies the server serves a NEW serving cert after
// the on-disk material is swapped, WITHOUT a restart. It drives the
// reloadableTLS.reload path directly (the fsnotify watcher's debounce
// makes a timing-based assertion flaky in CI); the listener picks up
// the swapped cert because GetCertificate reads the holder on every
// handshake.
func TestCertHotReload(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	paths := writeServerCerts(t, ca, dir, "blockstor-apiserver")

	reloader, err := newReloadableTLS(paths.certFile, paths.keyFile, paths.caFile)
	if err != nil {
		t.Fatalf("initial load: %v", err)
	}

	first, err := reloader.getCertificate(nil)
	if err != nil {
		t.Fatalf("get initial cert: %v", err)
	}

	firstLeaf, err := x509.ParseCertificate(first.Certificate[0])
	if err != nil {
		t.Fatalf("parse initial leaf: %v", err)
	}

	// Swap in a fresh cert for a different CN (so the serial/subject
	// differs) and reload.
	_ = writeServerCerts(t, ca, dir, "blockstor-apiserver-rotated")

	err = reloader.reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	second, err := reloader.getCertificate(nil)
	if err != nil {
		t.Fatalf("get reloaded cert: %v", err)
	}

	secondLeaf, err := x509.ParseCertificate(second.Certificate[0])
	if err != nil {
		t.Fatalf("parse reloaded leaf: %v", err)
	}

	if firstLeaf.SerialNumber.Cmp(secondLeaf.SerialNumber) == 0 {
		t.Errorf("serial unchanged after reload: %s", firstLeaf.SerialNumber)
	}

	if secondLeaf.Subject.CommonName != "blockstor-apiserver-rotated" {
		t.Errorf("CN after reload: got %q, want %q", secondLeaf.Subject.CommonName, "blockstor-apiserver-rotated")
	}
}

// TestCertHotReloadOverWire is the end-to-end variant: a live TLS
// listener is serving, the on-disk cert is swapped, the holder is
// reloaded, and a fresh client connection observes the NEW leaf — all
// without restarting the server.
func TestCertHotReloadOverWire(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	paths := writeServerCerts(t, ca, dir, "blockstor-apiserver")

	// Drive the reloader directly so we control the swap timing, and
	// inject it into a Server we start by hand (startTLSServer builds
	// its own reloader internally, which we can't reach).
	tlsAddr := pickFreeAddr(t)
	plainAddr := pickFreeAddr(t)

	srv := &Server{
		Addr: plainAddr,
		TLS: &TLSOptions{
			Addr:         tlsAddr,
			CertFile:     paths.certFile,
			KeyFile:      paths.keyFile,
			ClientCAFile: paths.caFile,
		},
	}
	srv.SetResolveHost(func(_ context.Context, _ string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()

	waitReachable(t, ctx, tlsAddr)

	clientCert, clientKey := ca.issue(t, "blockstor-csi", false)
	client := clientFor(t, ca.certPEM, clientCert, clientKey)

	firstSerial := peerLeafSerial(t, client, tlsAddr)

	// Rotate the on-disk material. Keep the SAME CN (the client's
	// ServerName check still passes after rotation, exactly like a
	// cert-manager renewal of the same Certificate) but a fresh serial
	// so we can prove a different cert is now served. The server's
	// fsnotify watcher picks the swap up; we poll until it does, so
	// the assertion is "reload happened without a restart", not a
	// fixed sleep.
	_ = writeServerCerts(t, ca, dir, "blockstor-apiserver")

	deadline := time.Now().Add(5 * time.Second)
	for {
		// New connection (no keep-alive reuse) so the handshake runs
		// against the current holder state.
		client.CloseIdleConnections()

		serial := peerLeafSerial(t, client, tlsAddr)
		if serial != firstSerial {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("served cert serial never rotated; still %s", serial)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// peerLeafSerial opens a TLS connection via client to addr and returns
// the serial number of the leaf certificate the server presented.
func peerLeafSerial(t *testing.T, client *http.Client, addr string) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+addr+"/v1/controller/version", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.TLS == nil || len(resp.TLS.PeerCertificates) == 0 {
		t.Fatalf("no peer certificates on response")
	}

	return resp.TLS.PeerCertificates[0].SerialNumber.String()
}
