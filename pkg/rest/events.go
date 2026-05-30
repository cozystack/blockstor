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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/events"
)

// SSE event-stream wiring for `/v1/events/drbd/promotion` and
// `/v1/events/nodes` (bug-hunt v0.1.3 Finding 12). Upstream LINSTOR
// emits these as Server-Sent Events (`Accept: text/event-stream`) —
// golinstor's `EventService.DRBDPromotion` decodes the
// `may-promote-change` event name into a typed channel. We match that
// wire shape so the canonical Go client (used by ha-controller and
// piraeus-affinity-controller) works without translation.
//
// Why SSE and not newline-delimited JSON (NDJSON):
//
//   - The upstream consumer is golinstor's EventService, which is hard-
//     coded to `text/event-stream` + the SSE framing (`event:`, `data:`,
//     `id:` lines). NDJSON would require a forked client.
//   - SSE has built-in `Last-Event-ID` on reconnect (the eventsource
//     library sets the header automatically), giving us a free resume
//     story for the long-poll consumer side.
//
// Endpoint behaviour:
//
//   - `eventId` query parameter OR `Last-Event-ID` request header act as
//     the resume token. The query form wins when both are present (the
//     query form is the one the bug-hunt report calls out explicitly).
//     Token 0 / empty / unparseable = "no resume; live tail only".
//   - On Subscribe, the broker's backlog (events with id > resume token)
//     is flushed first as SSE frames; future events follow on the same
//     connection.
//   - Connection management: the handler returns when `r.Context()` is
//     done (client disconnect, server shutdown). The broker's per-
//     subscriber goroutine is bound to the same context, so no
//     goroutines leak — see pkg/events/broker.go.
//
// Stub-friendly: the brokers are lazily initialised on the first
// Subscribe (so a zero-value Server keeps working in unit tests), and
// nothing in this package publishes to them yet. The controller-side
// observers that emit role-transition / node-change events are not
// part of this PR's scope — see Finding 12's "stub status" call-out in
// the PR body. Until those observers are wired, the endpoints serve as
// a no-event long-poll stream that's still useful to validate the wire
// shape end-to-end (and avoids the `404 endpoint not implemented`
// breakage that downstream consumers crash on).

// sseEventNameMayPromoteChange is the SSE `event:` line value matching
// upstream LINSTOR. golinstor's subscribe loop filters on this string
// before unmarshalling — keep them in sync.
const sseEventNameMayPromoteChange = "may-promote-change"

// sseEventNameNodeChange is the SSE `event:` line value we emit on
// `/v1/events/nodes`. Upstream LINSTOR ships a slightly different name
// per minor version; we picked the value that piraeus-affinity-
// controller's existing watcher logs (`node-change`).
const sseEventNameNodeChange = "node-change"

// sseHeartbeatInterval is the cadence of the `:keep-alive` SSE comment
// line. SSE permits a comment line (starts with `:`) as a no-op the
// client ignores; without periodic traffic, an idle SSE connection
// through an HTTP proxy may be dropped after the proxy's idle timeout
// (default 60s in NGINX). 25s keeps comfortably within every commonly-
// deployed proxy's keepalive window.
const sseHeartbeatInterval = 25 * time.Second

// DRBDPromotionBroker returns the controller's broker for DRBD
// promotion events. Reconcilers / observers that detect a role
// transition (Secondary → Primary or vice versa) call
// `srv.DRBDPromotionBroker().Publish(...)` to fan the event out to
// every SSE subscriber.
//
// Lazy-initialised so a zero-value Server (the shape every unit test
// builds) keeps working without a constructor step.
func (s *Server) DRBDPromotionBroker() *events.Broker[apiv1.EventMayPromoteChange] {
	s.eventBrokersInit.Do(s.initEventBrokers)

	return s.drbdPromotionBroker
}

// NodeChangeBroker returns the controller's broker for node
// connection-state changes (ONLINE/OFFLINE/etc.). Observers that watch
// the satellite-connection state call
// `srv.NodeChangeBroker().Publish(...)` to fan events out.
func (s *Server) NodeChangeBroker() *events.Broker[apiv1.EventNodeChange] {
	s.eventBrokersInit.Do(s.initEventBrokers)

	return s.nodeChangeBroker
}

func (s *Server) initEventBrokers() {
	s.drbdPromotionBroker = events.NewBroker[apiv1.EventMayPromoteChange]()
	s.nodeChangeBroker = events.NewBroker[apiv1.EventNodeChange]()
}

// registerEvents wires the SSE event-stream endpoints. Finding 12 from
// the v0.1.3 bug-hunt report.
func (s *Server) registerEvents(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/events/drbd/promotion", s.handleDRBDPromotionEvents)
	mux.HandleFunc("GET /v1/events/nodes", s.handleNodeChangeEvents)
}

// handleDRBDPromotionEvents serves SSE-framed `may-promote-change`
// events to the caller. See the file-level docstring for the wire
// contract.
func (s *Server) handleDRBDPromotionEvents(w http.ResponseWriter, r *http.Request) {
	streamSSE(w, r, sseEventNameMayPromoteChange, s.DRBDPromotionBroker())
}

// handleNodeChangeEvents serves SSE-framed `node-change` events to the
// caller. See the file-level docstring for the wire contract.
func (s *Server) handleNodeChangeEvents(w http.ResponseWriter, r *http.Request) {
	streamSSE(w, r, sseEventNameNodeChange, s.NodeChangeBroker())
}

// streamSSE is the generic SSE serving loop shared by every event
// endpoint. The handler:
//
//  1. Resolves the resume token (`?eventId=` query param wins over the
//     `Last-Event-ID` header; both default to 0).
//  2. Subscribes to the broker, flushes the replay backlog as SSE
//     frames, then streams live events until ctx is cancelled.
//  3. Writes a periodic `:keep-alive` comment line so idle proxies
//     don't drop the connection.
//
// The handler is generic over the payload type so DRBD and node
// streams share one body; the per-endpoint `event:` line name is
// passed in.
func streamSSE[T any](w http.ResponseWriter, r *http.Request, eventName string, broker *events.Broker[T]) {
	resumeID := resolveResumeID(r)

	flusher, ok := w.(http.Flusher)
	if !ok {
		// No flusher → we cannot deliver an unbounded stream. Surface
		// the same 500 + LINSTOR envelope shape the rest of the
		// apiserver uses on infrastructure failures. In practice
		// every net/http response writer implements Flusher, so this
		// branch is defensive.
		writeError(w, http.StatusInternalServerError,
			"this connection does not support streaming responses; "+
				"the apiserver requires an HTTP/1.1 or HTTP/2 transport")

		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// X-Accel-Buffering disables NGINX's response buffering. Some
	// in-cluster proxies (ingress-nginx with the default config) buffer
	// the whole response before flushing, which defeats SSE. The header
	// is a no-op against other proxies.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Flush the headers immediately so the client's eventsource
	// library sees the open stream without waiting for the first
	// data frame.
	flusher.Flush()

	sub := broker.Subscribe(r.Context(), resumeID)

	// Replay backlog first so the client sees a contiguous id
	// sequence (the resume contract).
	for _, ev := range sub.Replay {
		err := writeSSEFrame(w, eventName, ev.ID, ev.Payload)
		if err != nil {
			return
		}

		flusher.Flush()
	}

	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-sub.Events:
			if !ok {
				// Broker dropped us (slow subscriber) or context
				// teardown raced ahead. Either way the stream is
				// over; the client will reconnect with Last-Event-ID.
				return
			}

			err := writeSSEFrame(w, eventName, ev.ID, ev.Payload)
			if err != nil {
				return
			}

			flusher.Flush()
		case <-heartbeat.C:
			_, err := fmt.Fprint(w, ":keep-alive\n\n")
			if err != nil {
				return
			}

			flusher.Flush()
		}
	}
}

// resolveResumeID extracts the SSE resume token from the request. The
// `?eventId=N` query parameter takes precedence over the
// `Last-Event-ID` header so an explicit URL-encoded resume always wins
// over an eventsource library's automatic reconnect header. Unparseable
// / negative / absent tokens collapse to 0 ("live tail only").
func resolveResumeID(r *http.Request) uint64 {
	raw := strings.TrimSpace(r.URL.Query().Get("eventId"))
	if raw != "" {
		id, err := strconv.ParseUint(raw, 10, 64)
		if err == nil {
			return id
		}
	}

	raw = strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if raw != "" {
		id, err := strconv.ParseUint(raw, 10, 64)
		if err == nil {
			return id
		}
	}

	return 0
}

// writeSSEFrame writes one SSE event frame in the canonical
// `id:`/`event:`/`data:`/blank-line shape. payload is JSON-encoded
// inline; an encoding failure aborts the stream (we cannot recover from
// an event the client can't decode anyway).
func writeSSEFrame(w http.ResponseWriter, eventName string, eventID uint64, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal SSE payload: %w", err)
	}

	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", eventID, eventName, data)
	if err != nil {
		return fmt.Errorf("write SSE frame: %w", err)
	}

	return nil
}
