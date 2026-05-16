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

import "net/http"

// registerSchedules wires the upstream LINSTOR `linstor schedule`
// surface. blockstor does not yet implement scheduled snapshots /
// scheduled backups, but every CLI verb must answer with a typed
// envelope rather than fall through to the generic 404/405 catch-all
// — otherwise operators see two unrelated error stories ("method not
// allowed" / "endpoint not implemented") for the same half-implemented
// feature.
//
// Bug 100 wired the LIST verb to return `{"data": []}` so
// python-linstor's ScheduleListResponse decodes cleanly. Bug 141
// completes the contract: POST / PUT / DELETE all return a canned
// 501 envelope that names the verb ("schedule create not yet
// implemented" etc.) — same pattern as Bug 127's sos-report create
// + download. Once schedules are modelled, each of these handlers
// is replaced with the real implementation.
func (s *Server) registerSchedules(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/schedules", s.handleScheduleList)
	mux.HandleFunc("POST /v1/schedules", handleScheduleCreate)
	mux.HandleFunc("PUT /v1/schedules/{name}", handleScheduleModify)
	mux.HandleFunc("DELETE /v1/schedules/{name}", handleScheduleDelete)
}

// scheduleListResponse is the wire shape ScheduleListResponse expects
// (see linstor/responses.py:2717). The `data` field carries an array
// of Schedule objects; an empty list is the canonical "no schedules
// defined" reply.
type scheduleListResponse struct {
	Data []scheduleEntry `json:"data"`
}

// scheduleEntry mirrors the upstream `Schedule` REST type
// (linstor/responses.py:2684). All optional fields are pointer-typed
// so the empty-list path doesn't have to populate placeholder zero
// values.
type scheduleEntry struct {
	ScheduleName string  `json:"schedule_name"`
	FullCron     string  `json:"full_cron"`
	IncCron      *string `json:"inc_cron,omitempty"`
	KeepLocal    *int32  `json:"keep_local,omitempty"`
	KeepRemote   *int32  `json:"keep_remote,omitempty"`
	OnFailure    *string `json:"on_failure,omitempty"`
	MaxRetries   *int32  `json:"max_retries,omitempty"`
}

// handleScheduleList returns the empty schedule list. The endpoint is
// store-agnostic — we don't probe persistence because there's nothing
// to read; the body shape is what matters.
func (s *Server) handleScheduleList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, scheduleListResponse{Data: []scheduleEntry{}})
}

// handleScheduleCreate returns the canned "not yet implemented"
// envelope for `linstor schedule create`. Mirrors the Bug 127
// sos-report-create shape — same 501 status, same `[]ApiCallRc` body
// with a verb-naming message so the CLI's ERROR line tells the
// operator exactly which feature is missing.
func handleScheduleCreate(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "schedule create not yet implemented")
}

// handleScheduleModify is the PUT/modify side of the canned envelope
// triplet. `linstor schedule modify <name>` posts a PUT against
// /v1/schedules/{name}.
func handleScheduleModify(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "schedule modify not yet implemented")
}

// handleScheduleDelete is the DELETE side. `linstor schedule delete
// <name>` posts a DELETE against /v1/schedules/{name}. Without this
// route the CLI hit the Bug 103 404 catch-all envelope, which is
// structurally correct but carries a generic "endpoint not
// implemented" message that doesn't name the schedule verb.
func handleScheduleDelete(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "schedule delete not yet implemented")
}
