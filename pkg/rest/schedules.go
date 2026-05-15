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

// registerSchedules wires the upstream LINSTOR `linstor schedule list`
// surface. blockstor does not yet implement scheduled snapshots /
// scheduled backups, but the endpoint must answer with a parseable
// `{"data": []}` envelope — python-linstor (1.27.1) decodes the body
// into responses.ScheduleListResponse which reads `data["data"]`,
// and a bare 404 page crashes the client with
// xml.etree.ElementTree.ParseError (Bug 100). The wire shape matches
// upstream's empty-list reply for a controller with no schedules
// defined.
//
// The list endpoint is intentionally the only schedule route we wire
// — create / modify / delete are write paths that need real storage
// support before they can return anything but "501 not implemented".
// Once schedules are modelled, this file grows the full CRUD set.
func (s *Server) registerSchedules(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/schedules", s.handleScheduleList)
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
