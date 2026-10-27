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

package stream

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/cockroachdb/errors"
)

// ServerName is the name every satellite's stream certificate carries and
// every fetch verifies. Satellites dial each other by node address, which
// is not on any certificate, so the identity checked is "a satellite of this
// cluster": a certificate for this name signed by the cluster's CA.
const ServerName = "blockstor-satellite"

// ErrTLSIncomplete refuses a partial TLS configuration: a listener with a
// certificate but no CA would serve without checking who asks.
var ErrTLSIncomplete = errors.New("stream TLS needs a certificate, a key and a CA bundle")

// ErrNoCACertificate is a CA bundle with no certificate in it.
var ErrNoCACertificate = errors.New("no certificate in the stream CA bundle")

// TLSFiles names the PEM files the stream's mutual TLS is built from: one
// certificate per satellite, usable for both server and client auth, and the
// CA that signed every satellite's certificate. cert-manager writes all three
// into one Secret.
//
// The files are read on every handshake rather than once. A stream transfer
// is one handshake for minutes of data, so this costs nothing, and a
// rotated certificate is picked up with no watcher and no restart.
type TLSFiles struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// Configured reports whether all three files are set.
func (f TLSFiles) Configured() bool {
	return f.CertFile != "" && f.KeyFile != "" && f.CAFile != ""
}

// Validate refuses a configuration with some of the files set and not all,
// and a full set whose files do not load: an unreadable file or a key that
// does not match the certificate would otherwise start fine and fail every
// cross-node restore on the node. The files are still read again on every
// handshake; this is a check at startup only.
func (f TLSFiles) Validate() error {
	if f.CertFile == "" && f.KeyFile == "" && f.CAFile == "" {
		return nil
	}

	if !f.Configured() {
		return ErrTLSIncomplete
	}

	_, _, err := f.load()

	return err
}

func (f TLSFiles) load() (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, errors.Wrap(err, "load the stream certificate")
	}

	caPEM, err := os.ReadFile(f.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, errors.Wrap(err, "read the stream CA bundle")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, errors.Wrapf(ErrNoCACertificate, "%s", f.CAFile)
	}

	return cert, pool, nil
}

// serverConfig answers every handshake with the files as they are now, and
// requires a client certificate signed by the CA.
func (f TLSFiles) serverConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cert, pool, err := f.load()
			if err != nil {
				return nil, err
			}

			return &tls.Config{
				MinVersion:   tls.VersionTLS12,
				Certificates: []tls.Certificate{cert},
				ClientCAs:    pool,
				ClientAuth:   tls.RequireAndVerifyClientCert,
			}, nil
		},
	}
}

// clientConfig presents this satellite's certificate and verifies the peer's
// against the CA and ServerName.
func (f TLSFiles) clientConfig() (*tls.Config, error) {
	cert, pool, err := f.load()
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   ServerName,
	}, nil
}
