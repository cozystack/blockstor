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

// Package clienttls builds mutual-TLS http.Clients for the blockstor
// in-cluster components that dial the apiserver's HTTPS endpoint.
//
// The contract mirrors upstream LINSTOR's HTTPS story so the same
// cert-manager-issued material works for both blockstor's own Go
// clients and the external golinstor-based consumers (linstor-csi,
// piraeus-operator): a client presents a key-pair signed by the API
// CA and verifies the server against that same CA. The apiserver's
// TLS listener runs RequireAndVerifyClientCert, so a client WITHOUT a
// valid cert is rejected at the handshake.
//
// Certs are read once at client-construction time. Unlike the server
// side (pkg/rest reloads its serving cert/CA on rotation without a
// restart), clients re-read on the next process start — cert-manager
// rotation of a client key-pair is rare and the consuming Deployments
// already roll on their own cadence; baking a watcher into every
// client would add surface for little gain. Hot-reload on the SERVER
// is the load-bearing requirement (issue: rolling the serving cert
// must not drop in-flight clients).
package clienttls

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"time"

	"github.com/cockroachdb/errors"
)

// clientTimeout caps a single mutual-TLS request. The apiserver REST
// calls are all short control-plane operations; 30s leaves headroom
// for a slow cold cache list without letting a wedged peer hang a
// client goroutine forever.
const clientTimeout = 30 * time.Second

// ErrEmptyCABundle is returned when the configured CA file parses to
// zero certificates — almost always a wrong path or an empty mounted
// Secret key. Surfaced as a sentinel so callers can branch on it.
var ErrEmptyCABundle = errors.New("no certificates parsed from CA bundle")

// Config points at the three PEM files a mutual-TLS client needs.
// All three are mounted from a cert-manager-issued Secret (tls.crt /
// tls.key / ca.crt by the cert-manager convention).
type Config struct {
	// CertFile is the client certificate (PEM). Presented to the
	// server during the handshake.
	CertFile string
	// KeyFile is the private key (PEM) for CertFile.
	KeyFile string
	// CAFile is the CA bundle (PEM) used to verify the server's
	// serving certificate.
	CAFile string
}

// FromEnv reads the standard LINSTOR client-TLS environment variables
// (LS_USER_CERTIFICATE / LS_USER_KEY / LS_ROOT_CA) and returns a
// Config plus whether all three were set. blockstor follows the same
// env contract golinstor uses so a single mounted Secret + three env
// vars wires both blockstor's own clients and golinstor-based ones.
//
// ok is false when none or only some of the variables are present;
// callers treat that as "plain HTTP, no client TLS" so local
// port-forward debugging keeps working.
func FromEnv() (Config, bool) {
	cfg := Config{
		CertFile: os.Getenv("LS_USER_CERTIFICATE"),
		KeyFile:  os.Getenv("LS_USER_KEY"),
		CAFile:   os.Getenv("LS_ROOT_CA"),
	}

	ok := cfg.CertFile != "" && cfg.KeyFile != "" && cfg.CAFile != ""

	return cfg, ok
}

// TLSConfig builds a *tls.Config that presents the client key-pair and
// verifies the server against CAFile. ServerName, when non-empty, is
// the DNS name the client expects in the server certificate (the
// in-cluster Service FQDN); leave it empty to let the dialer derive it
// from the request URL host.
func (c Config) TLSConfig(serverName string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "load client key pair")
	}

	caPEM, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, errors.Wrap(err, "read client CA bundle")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.Wrapf(ErrEmptyCABundle, "CA bundle %q", c.CAFile)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// HTTPClient returns an *http.Client whose Transport presents the
// client cert and verifies the server against the configured CA. The
// returned client carries no request timeout — callers that need one
// (most do) set http.Client.Timeout on the result. The transport is a
// clone of http.DefaultTransport so connection pooling / proxy / dial
// defaults are preserved.
func (c Config) HTTPClient(serverName string) (*http.Client, error) {
	tlsCfg, err := c.TLSConfig(serverName)
	if err != nil {
		return nil, err
	}

	// Clone DefaultTransport to preserve pooling/proxy/dial defaults, but
	// fall back to a fresh transport if something (e.g. an OTel/APM tracing
	// library) has replaced DefaultTransport with a non-*http.Transport
	// RoundTripper — a direct type assertion would panic/fail there.
	var cloned *http.Transport
	if transport, ok := http.DefaultTransport.(*http.Transport); ok {
		cloned = transport.Clone()
	} else {
		cloned = &http.Transport{}
	}

	cloned.TLSClientConfig = tlsCfg

	return &http.Client{
		Transport: cloned,
		Timeout:   clientTimeout,
	}, nil
}
