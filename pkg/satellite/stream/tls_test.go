package stream

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/storage"
)

// The stream returns raw volume data, and it was served on every node
// address with no authentication: any pod that could reach a node read any
// volume with a snapshot there. It now runs over mutual TLS only.

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	return testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue writes a satellite certificate for dnsName signed by ca, with the CA
// bundle next to it, and returns the files.
func (ca testCA) issue(t *testing.T, dnsName string) TLSFiles {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	files := TLSFiles{
		CertFile: filepath.Join(dir, "tls.crt"),
		KeyFile:  filepath.Join(dir, "tls.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}

	writeFile(t, files.CertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeFile(t, files.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	writeFile(t, files.CAFile, ca.pem)

	return files
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// noSnapshots answers every lookup with "not on this node", so a request
// that gets past the handshake comes back 404.
type noSnapshots struct{}

func (noSnapshots) ProviderForResource(string) (storage.Provider, error) {
	return nil, storage.ErrNotFound
}

// serve starts the stream server with files on a free loopback port.
func serve(t *testing.T, files TLSFiles) string {
	t.Helper()

	var lc net.ListenConfig

	probe, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe port: %v", err)
	}

	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = ListenAndServe(ctx, addr, NewServer(noSnapshots{}), files) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var dialer net.Dialer

		conn, dialErr := dialer.DialContext(t.Context(), "tcp", addr)
		if dialErr == nil {
			_ = conn.Close()

			return addr
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("stream server did not come up on %s", addr)

	return ""
}

func TestStreamFetchSucceedsBetweenSatellitesOfOneCA(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	addr := serve(t, ca.issue(t, ServerName))

	_, err := NewClient(ca.issue(t, ServerName)).Fetch(t.Context(), addr, "rd", "snap", 0)
	if !errors.Is(err, storage.ErrNotFound) || !strings.Contains(err.Error(), "has no snapshot") {
		t.Fatalf("fetch between two satellites of one CA = %v, want the server's 404", err)
	}
}

func TestStreamRefusesAPeerWithoutACertificate(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	addr := serve(t, ca.issue(t, ServerName))

	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: ServerName,
	}}}

	resp, err := client.Do(newGet(t, "https://"+addr+PathPrefix+"/rd/snap/"+strconv.Itoa(0)))
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("a client with no certificate got %s", resp.Status)
	}

	plain, err := http.DefaultClient.Do(newGet(t, "http://"+addr+PathPrefix+"/rd/snap/0"))
	if err == nil {
		defer func() { _ = plain.Body.Close() }()

		if plain.StatusCode == http.StatusNotFound || plain.StatusCode == http.StatusOK {
			t.Fatalf("plain HTTP reached the handler: %s", plain.Status)
		}
	}
}

func TestStreamRefusesAPeerFromAnotherCA(t *testing.T) {
	t.Parallel()

	addr := serve(t, newTestCA(t).issue(t, ServerName))

	_, err := NewClient(newTestCA(t).issue(t, ServerName)).Fetch(t.Context(), addr, "rd", "snap", 0)
	if err == nil || strings.Contains(err.Error(), "has no snapshot") {
		t.Fatalf("a peer from another CA reached the handler: %v", err)
	}
}

func TestStreamRefusesAServerCertificateForAnotherName(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	addr := serve(t, ca.issue(t, "not-a-satellite"))

	_, err := NewClient(ca.issue(t, ServerName)).Fetch(t.Context(), addr, "rd", "snap", 0)
	if err == nil || strings.Contains(err.Error(), "has no snapshot") {
		t.Fatalf("a server certificate for another name was accepted: %v", err)
	}
}

// Without TLS configured the stream is off, not open.
func TestStreamWithoutTLSIsOff(t *testing.T) {
	t.Parallel()

	_, err := NewClient(TLSFiles{}).Fetch(t.Context(), "127.0.0.1:1", "rd", "snap", 0)
	if !errors.Is(err, ErrStreamDisabled) {
		t.Fatalf("fetch with no TLS = %v, want ErrStreamDisabled", err)
	}

	if err := ListenAndServe(t.Context(), "127.0.0.1:0", NewServer(noSnapshots{}), TLSFiles{}); !errors.Is(err, ErrTLSIncomplete) {
		t.Fatalf("server with no TLS = %v, want ErrTLSIncomplete", err)
	}

	if err := (TLSFiles{CertFile: "c", KeyFile: "k"}).Validate(); !errors.Is(err, ErrTLSIncomplete) {
		t.Fatalf("partial TLS flags = %v, want ErrTLSIncomplete", err)
	}
}

func newGet(t *testing.T, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	return req
}
