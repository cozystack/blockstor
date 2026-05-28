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

package clienttls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSelfSignedPair mints a self-signed cert+key and a CA bundle (the
// same cert serving as its own CA) into dir, returning a Config that
// points at them. Enough to exercise the loader paths without a real
// PKI.
func writeSelfSignedPair(t *testing.T, dir string) Config {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "client"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	cfg := Config{
		CertFile: filepath.Join(dir, "tls.crt"),
		KeyFile:  filepath.Join(dir, "tls.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}

	mustWrite(t, cfg.CertFile, certPEM)
	mustWrite(t, cfg.KeyFile, keyPEM)
	mustWrite(t, cfg.CAFile, certPEM)

	return cfg
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()

	err := os.WriteFile(path, data, 0o600)
	if err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestTLSConfigLoadsMaterial verifies the happy path: a valid cert/key/
// CA produces a *tls.Config with the client cert and a non-empty root
// pool, and the requested ServerName.
func TestTLSConfigLoadsMaterial(t *testing.T) {
	cfg := writeSelfSignedPair(t, t.TempDir())

	tlsCfg, err := cfg.TLSConfig("blockstor-apiserver.blockstor-system.svc")
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}

	if len(tlsCfg.Certificates) != 1 {
		t.Errorf("Certificates: got %d, want 1", len(tlsCfg.Certificates))
	}

	if tlsCfg.RootCAs == nil {
		t.Errorf("RootCAs is nil")
	}

	if tlsCfg.ServerName != "blockstor-apiserver.blockstor-system.svc" {
		t.Errorf("ServerName: got %q", tlsCfg.ServerName)
	}

	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion: got %x, want TLS1.2", tlsCfg.MinVersion)
	}
}

// TestTLSConfigEmptyCARejected verifies an empty/garbage CA bundle
// surfaces ErrEmptyCABundle rather than a silently-empty pool.
func TestTLSConfigEmptyCARejected(t *testing.T) {
	cfg := writeSelfSignedPair(t, t.TempDir())
	mustWrite(t, cfg.CAFile, []byte("not a certificate"))

	_, err := cfg.TLSConfig("")
	if !errors.Is(err, ErrEmptyCABundle) {
		t.Fatalf("error: got %v, want ErrEmptyCABundle", err)
	}
}

// TestHTTPClientBuilds verifies HTTPClient returns a client with the
// TLS transport wired and a request timeout set.
func TestHTTPClientBuilds(t *testing.T) {
	cfg := writeSelfSignedPair(t, t.TempDir())

	client, err := cfg.HTTPClient("")
	if err != nil {
		t.Fatalf("HTTPClient: %v", err)
	}

	if client.Timeout == 0 {
		t.Errorf("expected a non-zero request timeout")
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport: got %T, want *http.Transport", client.Transport)
	}

	if transport.TLSClientConfig == nil {
		t.Errorf("transport.TLSClientConfig is nil; client cert not wired")
	}

	if len(transport.TLSClientConfig.Certificates) != 1 {
		t.Errorf("client cert count: got %d, want 1", len(transport.TLSClientConfig.Certificates))
	}
}

// TestFromEnv verifies the LINSTOR-standard env var contract: all three
// set ⇒ ok=true; any missing ⇒ ok=false (plain-HTTP fallback).
func TestFromEnv(t *testing.T) {
	t.Setenv("LS_USER_CERTIFICATE", "/etc/tls/tls.crt")
	t.Setenv("LS_USER_KEY", "/etc/tls/tls.key")
	t.Setenv("LS_ROOT_CA", "/etc/tls/ca.crt")

	cfg, ok := FromEnv()
	if !ok {
		t.Fatalf("FromEnv ok: got false, want true")
	}

	if cfg.CertFile != "/etc/tls/tls.crt" || cfg.KeyFile != "/etc/tls/tls.key" || cfg.CAFile != "/etc/tls/ca.crt" {
		t.Errorf("FromEnv cfg mismatch: %+v", cfg)
	}

	t.Setenv("LS_ROOT_CA", "")

	_, ok = FromEnv()
	if ok {
		t.Errorf("FromEnv ok: got true with LS_ROOT_CA unset, want false")
	}
}
