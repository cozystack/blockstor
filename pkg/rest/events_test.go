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
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestDRBDPromotionStreamWireShape covers the canonical happy path for
// `/v1/events/drbd/promotion`: subscribe, inject a `may-promote-change`
// event via the broker, read one SSE frame off the wire, close the
// connection cleanly.
//
// The test pins the SSE framing (`id:`/`event:`/`data:` lines, blank-
// line terminator) AND the JSON payload shape (matches golinstor's
// `client.EventMayPromoteChange`) so a regression in either layer is
// caught at the wire edge.
func TestDRBDPromotionStreamWireShape(t *testing.T) {
	t.Parallel()

	srv := &Server{Addr: pickFreeAddr(t)}
	base, stop := startServerCustom(t, srv)
	defer stop()

	// Pre-build the subscriber connection BEFORE publishing, so the
	// test exercises the live-tail path (Replay is empty).
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/v1/events/drbd/promotion", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type: got %q want text/event-stream", ct)
	}

	// Wait for the broker to register the subscriber before the
	// Publish (otherwise the event lands in the backlog but our
	// connection — opened mid-air — may miss the channel send).
	broker := srv.DRBDPromotionBroker()
	waitForSubscribers(t, broker.SubscriberCount, 1)

	broker.Publish(apiv1.EventMayPromoteChange{
		ResourceName: "rd-a",
		NodeName:     "worker-1",
		MayPromote:   true,
	})

	frame := readSSEFrame(t, resp.Body)

	if frame.id != "1" {
		t.Errorf("frame id: got %q want %q", frame.id, "1")
	}

	if frame.event != "may-promote-change" {
		t.Errorf("frame event: got %q want %q", frame.event, "may-promote-change")
	}

	var payload apiv1.EventMayPromoteChange

	err = json.Unmarshal([]byte(frame.data), &payload)
	if err != nil {
		t.Fatalf("unmarshal data: %v (raw=%q)", err, frame.data)
	}

	if payload.ResourceName != "rd-a" || payload.NodeName != "worker-1" || !payload.MayPromote {
		t.Errorf("payload: got %+v", payload)
	}
}

// TestNodeChangeStreamWireShape mirrors the DRBD promotion test for the
// node-state endpoint. Same SSE framing, different broker + event
// name + payload type.
func TestNodeChangeStreamWireShape(t *testing.T) {
	t.Parallel()

	srv := &Server{Addr: pickFreeAddr(t)}
	base, stop := startServerCustom(t, srv)
	defer stop()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/v1/events/nodes", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}

	broker := srv.NodeChangeBroker()
	waitForSubscribers(t, broker.SubscriberCount, 1)

	broker.Publish(apiv1.EventNodeChange{
		NodeName:         "worker-2",
		ConnectionStatus: "ONLINE",
	})

	frame := readSSEFrame(t, resp.Body)

	if frame.event != "node-change" {
		t.Errorf("frame event: got %q want %q", frame.event, "node-change")
	}

	var payload apiv1.EventNodeChange

	err = json.Unmarshal([]byte(frame.data), &payload)
	if err != nil {
		t.Fatalf("unmarshal data: %v (raw=%q)", err, frame.data)
	}

	if payload.NodeName != "worker-2" || payload.ConnectionStatus != "ONLINE" {
		t.Errorf("payload: got %+v", payload)
	}
}

// TestEventStreamResumeFromEventIDQuery verifies the `?eventId=N` query
// param resume path: an event published before subscribe lands in the
// backlog; a fresh subscriber with `?eventId=0` gets the full replay.
func TestEventStreamResumeFromEventIDQuery(t *testing.T) {
	t.Parallel()

	srv := &Server{Addr: pickFreeAddr(t)}
	base, stop := startServerCustom(t, srv)
	defer stop()

	// Force broker init and publish BEFORE any client subscribes —
	// the event lands in the broker's backlog.
	broker := srv.DRBDPromotionBroker()
	broker.Publish(apiv1.EventMayPromoteChange{ResourceName: "rd-x", NodeName: "n-1", MayPromote: false})
	broker.Publish(apiv1.EventMayPromoteChange{ResourceName: "rd-x", NodeName: "n-2", MayPromote: true})

	// Resume from id=0 → the resolveResumeID code path collapses 0
	// to "no resume / live tail only". Use id=0 via empty query
	// instead: omit the query param so resolveResumeID returns 0,
	// then subscribe with eventId=1 to get the SECOND event back as
	// replay.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/v1/events/drbd/promotion?eventId=1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	frame := readSSEFrame(t, resp.Body)
	if frame.id != "2" {
		t.Errorf("replay frame id: got %q want %q (full=%+v)", frame.id, "2", frame)
	}

	var payload apiv1.EventMayPromoteChange

	err = json.Unmarshal([]byte(frame.data), &payload)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if payload.NodeName != "n-2" || !payload.MayPromote {
		t.Errorf("replay payload: got %+v", payload)
	}
}

// TestEventStreamCleanDisconnect verifies that closing the HTTP body
// (the SSE-client disconnect) propagates through r.Context() and the
// broker drops the subscription — no goroutine leak.
func TestEventStreamCleanDisconnect(t *testing.T) {
	t.Parallel()

	srv := &Server{Addr: pickFreeAddr(t)}
	base, stop := startServerCustom(t, srv)
	defer stop()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/v1/events/nodes", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	broker := srv.NodeChangeBroker()
	waitForSubscribers(t, broker.SubscriberCount, 1)

	// Close the body — this triggers the server-side r.Context()
	// teardown chain (net/http detects the dropped connection on
	// the next read attempt). The broker's per-subscription
	// goroutine watches ctx.Done() and tears the entry down.
	_ = resp.Body.Close()

	// Poll until SubscriberCount drops to 0, with a generous deadline
	// — net/http's connection-loss detection on the server side is
	// async (it depends on the read loop noticing the closed conn).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if broker.SubscriberCount() == 0 {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("subscriber never dropped after client disconnect (still %d)",
		broker.SubscriberCount())
}

// sseFrame is the parsed shape of a single SSE event-stream frame —
// only the fields our tests assert on. Comments / heartbeats are
// skipped by the parser.
type sseFrame struct {
	id    string
	event string
	data  string
}

// readSSEFrame reads up to the next full SSE event (terminated by a
// blank line) from r and returns the parsed id/event/data fields. The
// parser tolerates the `:keep-alive` comment heartbeat by skipping
// comment-only frames.
func readSSEFrame(t *testing.T, body io.Reader) sseFrame {
	t.Helper()

	scanner := bufio.NewScanner(body)
	// SSE frames are arbitrary length; bump the scanner buffer to
	// avoid bufio.ErrTooLong on a fat data: line.
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)

	deadline := time.Now().Add(3 * time.Second)

	for {
		if time.Now().After(deadline) {
			t.Fatal("timeout reading SSE frame")
		}

		var frame sseFrame

		hasData := false

		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				// End of frame.
				if hasData {
					return frame
				}
				// Comment / heartbeat only — read the next frame.
				break
			}

			switch {
			case strings.HasPrefix(line, ":"):
				// SSE comment line — heartbeat. Ignore.
			case strings.HasPrefix(line, "id:"):
				frame.id = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			case strings.HasPrefix(line, "event:"):
				frame.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				frame.data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				hasData = true
			}
		}

		if err := scanner.Err(); err != nil {
			t.Fatalf("scanner: %v", err)
		}

		// EOF before a complete frame — fail.
		if !hasData {
			t.Fatal("EOF before a complete SSE frame")
		}
	}
}

// waitForSubscribers polls the broker until SubscriberCount() reaches
// want, or fails the test after a reasonable deadline. Used to
// synchronise tests that publish after the HTTP request is issued: the
// subscriber registration happens on the server-side goroutine, so the
// publisher has to wait for it before sending events.
func waitForSubscribers(t *testing.T, count func() int, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count() >= want {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("subscriber count never reached %d (got %d)", want, count())
}
