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
	"net/http"
	"net/url"
	"sort"
	"sync"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// linstorRemoteRegistry holds Linstor remote definitions in memory.
// Scenario 4.17 (P2 spec-pin) exposes ONLY the REST surface — the
// satellite-side cluster-to-cluster ship path is intentionally not
// implemented (see handleLinstorRemoteShipNotImplemented). Persisting
// the entries on a CRD would imply functionality we don't actually
// deliver, so we keep the storage process-local: a fresh apiserver
// pod starts with an empty remote list. That matches the operational
// reality — nothing in Cozystack reads or acts on these entries today.
//
// Callers can POST a remote, list it, and DELETE it. POSTing a
// backup-ship against it returns 501 with an actionable message
// pointing at the in-cluster alternative.
type linstorRemoteRegistry struct {
	mu      sync.RWMutex
	entries map[string]linstorRemoteEntry
}

// linstorRemoteEntry is the wire shape upstream LINSTOR uses for
// `POST /v1/remotes/linstor` request and `GET /v1/remotes/linstor`
// response items. Fields match golinstor's `client.LinstorRemote`.
type linstorRemoteEntry struct {
	RemoteName string `json:"remote_name"`
	URL        string `json:"url"`
	Passphrase string `json:"passphrase,omitempty"`
	ClusterID  string `json:"cluster_id,omitempty"`
}

func newLinstorRemoteRegistry() *linstorRemoteRegistry {
	return &linstorRemoteRegistry{entries: map[string]linstorRemoteEntry{}}
}

// registerRemotes wires the LINSTOR `remotes` endpoints. Cozystack
// doesn't actually ship cluster-to-cluster, but the REST surface
// must accept CRUD so golinstor-based tooling can drive a remote
// list without erroring. The typed-array vs envelope distinction on
// the GET shape stays the same — golinstor's `client.RemoteList`
// decodes an object with three named arrays, but `GetAllLinstor` /
// `GetAllS3` / `GetAllEbs` decode a bare array of that type. Either
// mismatch produces a JSON decode error on every snapshot-list call.
func (s *Server) registerRemotes(mux *http.ServeMux) {
	// Lazy-init the in-memory registry on first call. buildMux runs
	// once during server start, so a pointer field added here is
	// safe to populate without a constructor change.
	if s.linstorRemotes == nil {
		s.linstorRemotes = newLinstorRemoteRegistry()
	}

	// Envelope shape — golinstor's `RemoteService.GetAll()` decodes
	// into `client.RemoteList` (object with three typed arrays).
	mux.HandleFunc("GET /v1/remotes", s.handleRemotesEnvelope)
	// Typed-array shape — golinstor's GetAllLinstor / GetAllS3 /
	// GetAllEbs decode `[]LinstorRemote` / `[]S3Remote` / `[]EbsRemote`.
	mux.HandleFunc("GET /v1/remotes/linstor", s.handleListLinstorRemotes)
	mux.HandleFunc("GET /v1/remotes/s3", handleEmptyRemoteArray)
	mux.HandleFunc("GET /v1/remotes/ebs", handleEmptyRemoteArray)
	// Create / delete Linstor remotes. S3 / EBS stay stubbed — no
	// Cozystack code-path drives them today.
	mux.HandleFunc("POST /v1/remotes/linstor", s.handleCreateLinstorRemote)
	mux.HandleFunc("DELETE /v1/remotes", s.handleDeleteRemote)
	// Backup-ship endpoint — the upstream LINSTOR path is
	// `POST /v1/remotes/{remote_name}/backups/ship`. We pin it as
	// 501 because the satellite-side cross-cluster shipper isn't
	// implemented (see pkg/satellite/cross_cluster_ship_test.go).
	mux.HandleFunc("POST /v1/remotes/{remote_name}/backups/ship",
		s.handleLinstorRemoteShipNotImplemented)
}

// remoteListEnvelope is upstream LINSTOR's `RemoteList`: an object
// with three named arrays. golinstor decodes the GET /v1/remotes
// body into this shape unconditionally. We populate `linstor_remotes`
// from the in-memory registry; s3 / ebs stay empty because no
// Cozystack workflow drives them.
type remoteListEnvelope struct {
	S3Remotes      []map[string]string  `json:"s3_remotes"`
	LinstorRemotes []linstorRemoteEntry `json:"linstor_remotes"`
	EbsRemotes     []map[string]string  `json:"ebs_remotes"`
}

// emptyRemoteList is the type alias the pre-CRUD test
// (TestRemotesEnvelopeShape) decodes into. We keep the name so
// that file continues to compile without a rewrite — the envelope
// is the same shape, only the LinstorRemotes element type differs
// (the legacy test asserts the field is non-nil, which still holds).
type emptyRemoteList = remoteListEnvelope

func (s *Server) handleRemotesEnvelope(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, remoteListEnvelope{
		S3Remotes:      []map[string]string{},
		LinstorRemotes: redactLinstorRemotes(s.linstorRemotes.snapshot()),
		EbsRemotes:     []map[string]string{},
	})
}

func (s *Server) handleListLinstorRemotes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, redactLinstorRemotes(s.linstorRemotes.snapshot()))
}

// redactLinstorRemotes scrubs the `passphrase` field on every Linstor
// remote before the wire emit. The in-memory registry stores the
// operator-supplied value verbatim so the satellite-side ship path
// can use it for the cross-cluster handshake; the REST read surface
// must NOT echo it back. Bug 187 (P0): pre-fix, `GET /v1/remotes` and
// `GET /v1/remotes/linstor` rendered `passphrase` verbatim — any
// read-only operator could grep the cross-cluster shared secret.
//
// The empty-string sentinel collapses to the `passphrase,omitempty`
// JSON tag, which would DROP the key entirely and hide that
// encryption IS configured. We stamp `<redacted>` instead so the
// key survives the encode and the operator-facing wire shape still
// surfaces "encryption is set". Mirrors the Bug 115 pattern used by
// `redactSensitiveProps` for the prop-bag deny list.
//
// snapshot() already returns a deep copy of the registry, so the
// in-place mutation here is local to the response.
func redactLinstorRemotes(in []linstorRemoteEntry) []linstorRemoteEntry {
	for i := range in {
		if in[i].Passphrase != "" {
			in[i].Passphrase = redactedPropValue
		}
	}

	return in
}

// handleEmptyRemoteArray returns `[]` for stubbed remote types
// (s3 / ebs). Cozystack doesn't wire either, so an empty array
// keeps golinstor's typed decoder happy without us pretending to
// support a feature we don't.
func handleEmptyRemoteArray(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []map[string]string{})
}

// handleCreateLinstorRemote stores the envelope in the in-memory
// registry. Treats `remote_name` and `url` as required (LINSTOR's
// own validator rejects either-empty) and writes 400 with a hint
// when the body is malformed — without that, golinstor surfaces a
// generic decode error which is harder for operators to act on.
//
// URL validation (Bug 119): the pre-fix handler stored whatever
// `url` field the body carried, so `linstor remote c linstor t1
// not-a-url` landed a remote nothing could reach. The fix parses
// with `net/url.Parse` and rejects bodies whose URL has no scheme
// or no host — `not-a-url`, `http://`, `//host`, `garbage:` all
// trip the gate. golinstor's `client.LinstorRemote` ships only an
// absolute http(s) URL in practice, so a strict scheme/host check
// is the right shape: false-rejects in normal usage are zero.
//
// Wire shape (Bug 119): the pre-fix handler returned 201 with the
// bare `linstorRemoteEntry` object as the body. python-linstor
// 1.27.1 decodes the body as `[ApiCallResponse]` unconditionally
// (responses.py:124 `data[0]["ret_code"]`), so a bare object
// crashed the CLI with `TypeError: string indices must be integers,
// not 'str'`. We now return the standard `[]APICallRc` envelope on
// success — same shape every other write-side handler in this
// package uses (see Bug 101 / aa5134fcf for the node-connection
// precedent). The 201 status is preserved so HTTP-level callers
// can still distinguish "newly created" from "list".
//
// Idempotent on duplicate `remote_name`: upstream's behaviour is
// 201 with the previous entry overwritten — we mirror that. A
// future CRD-backed implementation should surface 409 on conflict;
// pin the current shape so the change is intentional.
func (s *Server) handleCreateLinstorRemote(w http.ResponseWriter, r *http.Request) {
	var entry linstorRemoteEntry

	// Bug 158/161: typed-envelope decode (oversized → 413, empty → 400
	// "request body is empty", wrong shape → 400 with no Go type leak,
	// unknown top-level field → 400 + envelope).
	if !decodeJSON(w, r, &entry) {
		return
	}

	if entry.RemoteName == "" {
		writeError(w, http.StatusBadRequest, "remote_name is required")

		return
	}

	if entry.URL == "" {
		writeError(w, http.StatusBadRequest, "url is required")

		return
	}

	parsed, parseErr := url.Parse(entry.URL)
	if parseErr != nil || parsed.Scheme == "" || parsed.Host == "" {
		writeError(w, http.StatusBadRequest,
			"url is not a valid absolute URL (need scheme and host): "+entry.URL)

		return
	}

	s.linstorRemotes.put(entry)
	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "remote created: " + entry.RemoteName,
	}})
}

// handleDeleteRemote removes a remote by `remote_name` query
// parameter. Upstream uses a query string (not a path segment)
// because the same handler removes S3 / EBS / Linstor remotes —
// the type is inferred from the registry's contents.
//
// Idempotent on unknown remote (Bug 66): the remote registry is
// process-local (in-memory; see linstorRemoteRegistry's doc), so a
// controller restart wipes every entry — `linstor remote d` retries
// then crashed the python CLI's XML decoder fallback on the bare 404.
// Folding the "already absent" path into 200 + warn-mask keeps the
// CLI exit-0 and stays consistent with the rest of the delete-of-
// missing handlers in this package.
func (s *Server) handleDeleteRemote(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("remote_name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "remote_name query parameter is required")

		return
	}

	if !s.linstorRemotes.delete(name) {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnRemoteNotFound,
			Message: "remote already absent: " + name,
		}})

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "remote deleted: " + name,
	}})
}

// handleLinstorRemoteShipNotImplemented surfaces 501 + an
// operator-actionable message on POST /v1/remotes/{remote}/backups/ship.
//
// Cluster-to-cluster snapshot ship via a LINSTOR remote is NOT
// implemented in blockstor — the satellite-side wire shape today
// only knows in-cluster cross-node ship (the
// CrossNodeFetcher → SnapshotShipper pipeline; see
// pkg/satellite/reconciler.go crossNodeClone). The supported
// alternative is `snapshot-restore-resource` against the source RD
// in the same cluster, which fans the snapshot's contents into a
// new RD via the in-cluster ship path.
//
// 501 (Not Implemented) is the correct shape here over 405 (Method
// Not Allowed): the URL IS handled; the operation is just not
// available. golinstor turns 501 into ErrServer with the body text
// preserved, so the operator-facing message lands intact.
func (s *Server) handleLinstorRemoteShipNotImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented,
		"linstor-remote ship not implemented; "+
			"use snapshot-restore-resource for in-cluster ship "+
			"(POST /v1/resource-definitions/{rd}/snapshot-restore-resource)")
}

// --- linstorRemoteRegistry helpers -------------------------------------

func (r *linstorRemoteRegistry) put(e linstorRemoteEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.entries[e.RemoteName] = e
}

func (r *linstorRemoteRegistry) delete(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.entries[name]; !ok {
		return false
	}

	delete(r.entries, name)

	return true
}

// snapshot returns a deterministic-order copy of the registry so
// the GET response is stable for assertion. Order: ascending by
// remote_name; upstream LINSTOR's order is implementation-defined
// (DB row order), so locking to a sort here is a Cozystack choice
// rather than a contract match — pin it so the tests are stable.
func (r *linstorRemoteRegistry) snapshot() []linstorRemoteEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]linstorRemoteEntry, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].RemoteName < out[j].RemoteName
	})

	return out
}
