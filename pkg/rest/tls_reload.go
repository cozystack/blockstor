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
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/fsnotify/fsnotify"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// TLSOptions configures the apiserver's mutual-TLS listener. All file
// paths point into a cert-manager-issued Secret mounted into the pod
// (tls.crt / tls.key / ca.crt by cert-manager convention). The
// listener hot-reloads all three from disk on rotation without a
// restart (see reloadableTLS) — no reloader.stakater.com needed.
type TLSOptions struct {
	// Addr is the mutual-TLS bind address, e.g. ":3371" (upstream
	// LINSTOR's HTTPS REST port). This is the ONLY port the in-cluster
	// Service exposes.
	Addr string
	// CertFile is the serving certificate (PEM), issued for the
	// in-cluster Service DNS SANs.
	CertFile string
	// KeyFile is the serving private key (PEM) for CertFile.
	KeyFile string
	// ClientCAFile is the CA bundle (PEM) every client certificate is
	// verified against (tls.RequireAndVerifyClientCert).
	ClientCAFile string
}

// tlsReloadDebounce coalesces the burst of fsnotify events cert-manager
// emits when it rotates a Secret. The kubelet swaps the mounted Secret
// by atomically re-pointing the `..data` symlink, which fires several
// CREATE/RENAME/REMOVE events in quick succession; re-reading on the
// first one would race a half-written set of files. A short settle
// window lets the swap finish before we re-load.
const tlsReloadDebounce = 500 * time.Millisecond

// tlsReloadPoll is the periodic fallback re-read interval. fsnotify on
// a kubelet atomic-symlink-swap mount is reliable in practice, but a
// missed event (watch re-arm race after the dir is recreated) would
// otherwise leave a stale cert in memory until the next rotation. A
// low-frequency poll bounds the staleness without meaningful cost.
const tlsReloadPoll = 60 * time.Second

// reloadableTLS holds the apiserver's serving certificate and the
// client-CA pool, both re-readable from disk WITHOUT a process
// restart. cert-manager rotates the mounted Secret in place; this
// type re-loads the new material so existing pods keep serving across
// a rotation (no rolling restart, no reloader.stakater.com).
//
// Race-safety: the hot path (GetCertificate / GetConfigForClient,
// invoked by crypto/tls on every handshake) only ever takes an
// RLock and reads two pointers. The reload path takes the write lock
// just long enough to swap those two pointers to freshly-parsed
// values; parsing happens OUTSIDE the lock so a slow disk read never
// blocks in-flight handshakes. A failed reload (truncated file mid-
// rotation) logs and keeps the previous good material — the server
// never serves a nil cert.
type reloadableTLS struct {
	certFile     string
	keyFile      string
	clientCAFile string

	mu       sync.RWMutex
	cert     *tls.Certificate
	clientCA *x509.CertPool
}

// newReloadableTLS loads the initial material and returns the holder.
// An error here is fatal at boot (the operator mounted a bad Secret);
// after boot, reloads that fail are non-fatal and keep the last good
// material.
func newReloadableTLS(certFile, keyFile, clientCAFile string) (*reloadableTLS, error) {
	r := &reloadableTLS{
		certFile:     certFile,
		keyFile:      keyFile,
		clientCAFile: clientCAFile,
	}

	err := r.reload()
	if err != nil {
		return nil, errors.Wrap(err, "initial TLS material load")
	}

	return r, nil
}

// reload re-reads the serving key-pair and the client-CA bundle from
// disk and atomically swaps them in. Parsing is done before the lock
// so handshakes are never blocked on I/O.
func (r *reloadableTLS) reload() error {
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return errors.Wrap(err, "load serving key pair")
	}

	caPEM, err := os.ReadFile(r.clientCAFile)
	if err != nil {
		return errors.Wrap(err, "read client CA bundle")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return errors.Wrapf(ErrEmptyClientCA, "client CA bundle %q", r.clientCAFile)
	}

	r.mu.Lock()
	r.cert = &cert
	r.clientCA = pool
	r.mu.Unlock()

	return nil
}

// ErrEmptyClientCA is returned when the configured client-CA file
// parses to zero certificates. Sentinel so the reload path can log it
// distinctly from a file-not-found.
var ErrEmptyClientCA = errors.New("no certificates parsed from client CA bundle")

// getCertificate is wired into tls.Config.GetCertificate so every
// handshake picks up the current serving cert. Reads under RLock.
func (r *reloadableTLS) getCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.cert, nil
}

// getConfigForClient is wired into tls.Config.GetConfigForClient so
// every handshake validates the client cert against the CURRENT
// client-CA pool. Returning a fresh *tls.Config per handshake is the
// canonical way to hot-reload ClientCAs — the base config's ClientCAs
// is captured at listen time and would otherwise never update.
func (r *reloadableTLS) getConfigForClient(_ *tls.ClientHelloInfo) (*tls.Config, error) {
	r.mu.RLock()
	pool := r.clientCA
	r.mu.RUnlock()

	return &tls.Config{
		GetCertificate: r.getCertificate,
		ClientCAs:      pool,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		MinVersion:     tls.VersionTLS12,
	}, nil
}

// serverTLSConfig returns the base *tls.Config the listener is built
// with. GetConfigForClient supersedes the static fields on every
// handshake (so ClientCAs hot-reloads), but the static fields are set
// too for clients/tools that introspect the base config.
func (r *reloadableTLS) serverTLSConfig() *tls.Config {
	r.mu.RLock()
	pool := r.clientCA
	r.mu.RUnlock()

	return &tls.Config{
		GetCertificate:     r.getCertificate,
		GetConfigForClient: r.getConfigForClient,
		ClientCAs:          pool,
		ClientAuth:         tls.RequireAndVerifyClientCert,
		MinVersion:         tls.VersionTLS12,
	}
}

// watch runs the hot-reload loop until ctx is cancelled. It combines
// an fsnotify watch on the directories backing the cert/key/CA files
// (kubelet swaps the whole mount, so we watch dirs, not the symlinked
// files) with a periodic poll fallback. Both paths funnel through a
// debounced reload so the kubelet's multi-event atomic swap settles
// before we re-read. Watch failures degrade to poll-only — the server
// keeps serving the last good material regardless.
func (r *reloadableTLS) watch(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("tls-reload")

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error(err, "fsnotify watcher unavailable; falling back to periodic poll")

		r.pollOnly(ctx, logger)

		return
	}
	defer func() { _ = watcher.Close() }()

	for _, dir := range r.watchDirs() {
		addErr := watcher.Add(dir)
		if addErr != nil {
			logger.Error(addErr, "watch dir add failed", "dir", dir)
		}
	}

	r.watchLoop(ctx, watcher, logger)
}

// watchDirs is the de-duplicated set of directories holding the
// cert/key/CA files. Watching the directory (not the file) is required
// because a kubelet Secret mount swaps the entire `..data` dir; the
// individual files are symlinks whose target changes, which an inotify
// watch on the file itself does not always observe.
func (r *reloadableTLS) watchDirs() []string {
	seen := map[string]struct{}{}
	dirs := []string{}

	for _, f := range []string{r.certFile, r.keyFile, r.clientCAFile} {
		dir := filepath.Dir(f)
		if _, ok := seen[dir]; ok {
			continue
		}

		seen[dir] = struct{}{}

		dirs = append(dirs, dir)
	}

	return dirs
}

// watchLoop is the select loop pulled out of watch to keep it under
// the funlen budget. Debounces fsnotify bursts and re-reads on a poll
// ticker.
func (r *reloadableTLS) watchLoop(ctx context.Context, watcher *fsnotify.Watcher, logger logr) {
	poll := time.NewTicker(tlsReloadPoll)
	defer poll.Stop()

	var debounce *time.Timer

	debounceC := make(<-chan time.Time)

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-watcher.Events:
			if !ok {
				return
			}

			if debounce == nil {
				debounce = time.NewTimer(tlsReloadDebounce)
				debounceC = debounce.C
			} else {
				// Stop+drain before Reset: a timer that already fired left a
				// value in its channel; without draining it the next select
				// would trip <-debounceC immediately and skip the debounce,
				// reloading certs before the kubelet finished writing them.
				if !debounce.Stop() {
					select {
					case <-debounce.C:
					default:
					}
				}

				debounce.Reset(tlsReloadDebounce)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}

			logger.Error(err, "fsnotify error")
		case <-debounceC:
			debounce = nil
			debounceC = make(<-chan time.Time)

			r.reloadAndLog(logger, "cert dir change")
		case <-poll.C:
			r.reloadAndLog(logger, "periodic poll")
		}
	}
}

// pollOnly is the degraded path used when fsnotify is unavailable.
func (r *reloadableTLS) pollOnly(ctx context.Context, logger logr) {
	poll := time.NewTicker(tlsReloadPoll)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			r.reloadAndLog(logger, "periodic poll")
		}
	}
}

// reloadAndLog re-reads the material and logs the outcome. A failed
// reload is non-fatal: the previous good cert/CA stays in memory so
// the server never starts rejecting handshakes because of a transient
// mid-rotation read.
func (r *reloadableTLS) reloadAndLog(logger logr, reason string) {
	err := r.reload()
	if err != nil {
		logger.Error(err, "TLS reload failed; keeping previous material", "reason", reason)

		return
	}

	logger.Info("TLS material reloaded", "reason", reason)
}

// logr is the minimal logging surface watchLoop / pollOnly need. It
// matches the methods of logr.Logger used here so tests can inject a
// no-op without pulling the whole logging dep into the test.
type logr interface {
	Error(err error, msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
}
