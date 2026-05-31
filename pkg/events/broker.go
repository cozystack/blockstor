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

// Package events implements a tiny in-process pub-sub broker used by the
// REST event-stream endpoints (`/v1/events/drbd/promotion`,
// `/v1/events/nodes`). It exists because blockstor has no controller-
// side observer that already exposes typed event channels for DRBD role
// transitions / node connection-state changes, and the REST layer needs
// SOMETHING to fan out to long-lived SSE subscribers.
//
// The Broker is deliberately minimal:
//
//   - generic over the payload type so the DRBD-promotion broker and the
//     node-state broker share one implementation;
//   - assigns a strictly-increasing eventId on Publish so SSE clients
//     can resume from `Last-Event-ID`;
//   - keeps a bounded backlog of the most-recent events (so a resume
//     within the backlog window replays missed events; outside the
//     window the client gets the live tail with a one-line gap);
//   - subscribers receive on a per-subscriber buffered channel; a slow
//     subscriber that fills its buffer is dropped (a fresh, lossy SSE
//     stream is the standard recovery — the client reconnects);
//   - Subscribe is canceled by ctx (the REST handler passes
//     `r.Context()`); on cancel the goroutine exits cleanly and the
//     subscription is removed from the broker.
//
// The package is intentionally free of REST / DRBD-specific knowledge —
// it is a generic fan-out primitive. The wire-shape decisions
// (event names, JSON tags) live in `pkg/rest/events.go`.
package events

import (
	"context"
	"sync"
)

// backlogCap is the per-broker history depth. A long-poll client
// reconnecting within this window can resume cleanly via
// Last-Event-ID; a longer gap means the client sees the live tail with
// a one-line gap and is expected to reconcile via the snapshot APIs
// (the same fallback golinstor's reference EventService uses).
//
// 256 is enough to ride out a 30-second client retry on a busy cluster
// (a few thousand promotion / node-state events per minute is well
// beyond what blockstor can realistically generate — DRBD role
// transitions are operator-scale events, not microsecond-scale events).
const backlogCap = 256

// subscriberBuf is the per-subscriber channel depth. A subscriber that
// can't drain this many events between Publishes is dropped — a fresh,
// lossy SSE stream is the contract: the client reconnects with the last
// id it saw and the backlog replay covers the gap (when in-window).
const subscriberBuf = 64

// Event wraps a payload with its monotonically-increasing broker-side
// id. The id is the SSE `id:` field on the wire and the resume token a
// client sends back as `Last-Event-ID` (or `?eventId=`). Brokers reset
// their id sequence to 0 on construction, so ids are unique within a
// broker instance — across a controller restart the client sees a
// renumbering, which is the same behaviour upstream LINSTOR ships.
type Event[T any] struct {
	ID      uint64
	Payload T
}

// Broker is a single-broker pub-sub fan-out for events of type T. The
// zero value is NOT ready to use; call NewBroker.
type Broker[T any] struct {
	mu     sync.Mutex
	nextID uint64
	// ring is a fixed-size backlog of the most-recent events. ring[i]
	// holds the (i mod backlogCap)-th slot; we look up by id by
	// scanning the slice (length capped at backlogCap, so a linear
	// scan is fine).
	ring []Event[T]
	// subs is the active subscription set. Map key is a per-broker
	// monotonic handle; value is the per-subscriber channel.
	subs   map[uint64]chan Event[T]
	nextHS uint64
}

// NewBroker constructs an empty Broker ready for Publish / Subscribe.
func NewBroker[T any]() *Broker[T] {
	return &Broker[T]{
		ring: make([]Event[T], 0, backlogCap),
		subs: map[uint64]chan Event[T]{},
	}
}

// Publish fans the payload out to every live subscriber and appends it
// to the backlog ring. Returns the assigned event id so callers that
// want to log "published event 42" can correlate with what subscribers
// see.
//
// Slow-subscriber semantics: if a subscriber's channel is full at the
// moment of publish, the broker DROPS that subscriber — it closes the
// channel and removes it from the active set. The client will see the
// stream close at the SSE wire and reconnect; resume is best-effort
// via Last-Event-ID.
func (b *Broker[T]) Publish(payload T) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	ev := Event[T]{ID: b.nextID, Payload: payload}

	// Append to backlog (bounded).
	if len(b.ring) < backlogCap {
		b.ring = append(b.ring, ev)
	} else {
		// Drop oldest, append newest.
		copy(b.ring, b.ring[1:])
		b.ring[len(b.ring)-1] = ev
	}

	// Fan out. A blocked send means the subscriber is too slow to
	// keep up with the live tail; drop them.
	for handle, ch := range b.subs {
		select {
		case ch <- ev:
		default:
			close(ch)
			delete(b.subs, handle)
		}
	}

	return ev.ID
}

// Subscription is the live read-side of a Subscribe call.
type Subscription[T any] struct {
	// Events is closed when the subscription ends (context cancel or
	// slow-subscriber drop). Receivers should treat a closed channel
	// as "stream ended; reconnect".
	Events <-chan Event[T]
	// Replay holds events with id > lastSeen that were in the broker's
	// backlog at Subscribe time. The handler should flush these to
	// the wire BEFORE draining Events so the client sees a contiguous
	// stream. May be empty.
	Replay []Event[T]
}

// Subscribe registers a new subscriber and returns the immediately-
// available backlog (events with id > lastSeen) plus a channel for
// future events. The subscription is bound to ctx — when ctx is
// cancelled (e.g. the SSE client disconnects) the goroutine exits and
// the channel is closed.
//
// lastSeen == 0 means "no resume requested" — the subscriber gets only
// future events with no backlog. A non-zero lastSeen returns every
// backlog entry with id strictly greater than lastSeen; if the backlog
// no longer covers lastSeen (the client was disconnected too long), the
// returned Replay starts at the oldest still-buffered event and the
// caller is expected to surface the gap to the client.
func (b *Broker[T]) Subscribe(ctx context.Context, lastSeen uint64) Subscription[T] {
	b.mu.Lock()

	handle := b.nextHS
	b.nextHS++

	ch := make(chan Event[T], subscriberBuf)
	b.subs[handle] = ch

	var replay []Event[T]

	for i := range b.ring {
		if b.ring[i].ID > lastSeen {
			replay = append(replay, b.ring[i])
		}
	}

	b.mu.Unlock()

	// Tear the subscription down when ctx is cancelled. The goroutine
	// is the only place that removes the entry from b.subs after the
	// Subscribe call (Publish's slow-subscriber drop is the other,
	// orthogonal path).
	go func() {
		<-ctx.Done()

		b.mu.Lock()
		defer b.mu.Unlock()

		// If Publish already dropped us as a slow subscriber, the
		// channel is closed and the map entry is gone. Guard against
		// the double-close / double-delete by re-checking the map.
		if existing, ok := b.subs[handle]; ok && existing == ch {
			close(ch)
			delete(b.subs, handle)
		}
	}()

	return Subscription[T]{
		Events: ch,
		Replay: replay,
	}
}

// SubscriberCount returns the number of currently-registered
// subscribers. Exposed for tests and for a future /metrics gauge.
func (b *Broker[T]) SubscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.subs)
}

// LatestID returns the id of the most-recently-published event, or 0
// when the broker has never published. Useful for tests that need to
// sync on Publish completion without polling SubscriberCount.
func (b *Broker[T]) LatestID() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.nextID
}
