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

package events

import (
	"context"
	"testing"
	"time"
)

// TestBrokerPublishSubscribeFanOut covers the simplest contract: every
// subscriber sees every event published after Subscribe with a
// monotonically-increasing id.
func TestBrokerPublishSubscribeFanOut(t *testing.T) {
	t.Parallel()

	b := NewBroker[string]()

	ctxA, cancelA := context.WithCancel(t.Context())
	defer cancelA()

	ctxB, cancelB := context.WithCancel(t.Context())
	defer cancelB()

	subA := b.Subscribe(ctxA, 0)
	subB := b.Subscribe(ctxB, 0)

	idOne := b.Publish("first")
	idTwo := b.Publish("second")

	if idOne != 1 || idTwo != 2 {
		t.Fatalf("ids: got %d,%d want 1,2", idOne, idTwo)
	}

	for name, sub := range map[string]Subscription[string]{"A": subA, "B": subB} {
		ev := mustReceive(t, sub.Events, "first/"+name)

		if ev.ID != 1 || ev.Payload != "first" {
			t.Errorf("%s first: got %+v", name, ev)
		}

		ev = mustReceive(t, sub.Events, "second/"+name)
		if ev.ID != 2 || ev.Payload != "second" {
			t.Errorf("%s second: got %+v", name, ev)
		}
	}
}

// TestBrokerReplayFromLastSeen verifies that Subscribe with a non-zero
// lastSeen returns the backlog entries with id > lastSeen and continues
// to deliver future events on the channel.
func TestBrokerReplayFromLastSeen(t *testing.T) {
	t.Parallel()

	b := NewBroker[int]()
	_ = b.Publish(10)
	_ = b.Publish(20)
	_ = b.Publish(30)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Resume from id 1 → should replay events 2 and 3 (payloads 20, 30).
	sub := b.Subscribe(ctx, 1)
	if len(sub.Replay) != 2 {
		t.Fatalf("replay len: got %d want 2 (events: %+v)", len(sub.Replay), sub.Replay)
	}

	if sub.Replay[0].Payload != 20 || sub.Replay[1].Payload != 30 {
		t.Errorf("replay payloads: got %+v", sub.Replay)
	}

	// Live tail still works.
	_ = b.Publish(40)

	ev := mustReceive(t, sub.Events, "live")
	if ev.Payload != 40 || ev.ID != 4 {
		t.Errorf("live tail: got %+v", ev)
	}
}

// TestBrokerContextCancelClosesChannel ensures the SSE-disconnect path
// works: cancelling ctx must close the subscriber's Events channel and
// drop it from the active set so the broker doesn't leak goroutines.
func TestBrokerContextCancelClosesChannel(t *testing.T) {
	t.Parallel()

	b := NewBroker[string]()

	ctx, cancel := context.WithCancel(t.Context())
	sub := b.Subscribe(ctx, 0)

	if got := b.SubscriberCount(); got != 1 {
		t.Fatalf("pre-cancel subs: got %d want 1", got)
	}

	cancel()

	// Wait for the goroutine to close the channel. The teardown is
	// async (a goroutine waits on ctx.Done()) so we poll briefly.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case _, ok := <-sub.Events:
			if !ok {
				if got := b.SubscriberCount(); got != 0 {
					t.Errorf("post-cancel subs: got %d want 0", got)
				}

				return
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	t.Fatal("channel never closed after cancel")
}

// TestBrokerBacklogCap pins the bounded-history behaviour: once more
// than backlogCap events have been published, the oldest entries are
// evicted and a Subscribe(lastSeen=0) caller's Replay window starts
// from the oldest still-buffered event.
func TestBrokerBacklogCap(t *testing.T) {
	t.Parallel()

	b := NewBroker[int]()
	for i := range backlogCap + 50 {
		_ = b.Publish(i)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A fresh Subscribe with no resume token gets future events only —
	// no Replay (we documented Replay as "events with id > lastSeen
	// that were in the broker's backlog"; lastSeen=0 means "no
	// resume", so Replay should be empty even though the backlog is
	// full).
	//
	// Subtlety: our implementation treats lastSeen=0 the same as any
	// other id — it returns every backlog entry with id > 0, which is
	// the entire backlog. That's the right answer for a fresh client
	// that wants the full available history; an SSE handler that
	// wants the "live tail only" behaviour passes lastSeen = LatestID
	// at Subscribe time.
	sub := b.Subscribe(ctx, 0)
	if len(sub.Replay) != backlogCap {
		t.Errorf("replay len after %d publishes: got %d want %d",
			backlogCap+50, len(sub.Replay), backlogCap)
	}

	// The oldest still-buffered event should be event id 51 (we
	// published ids 1..backlogCap+50; the oldest 50 were evicted).
	if got := sub.Replay[0].ID; got != 51 {
		t.Errorf("oldest replay id: got %d want 51", got)
	}
}

// TestBrokerLiveTailOnly demonstrates the SSE-handler-friendly way to
// get "from now on, no backlog": pass Broker.LatestID as lastSeen.
func TestBrokerLiveTailOnly(t *testing.T) {
	t.Parallel()

	b := NewBroker[int]()
	_ = b.Publish(1)
	_ = b.Publish(2)
	_ = b.Publish(3)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sub := b.Subscribe(ctx, b.LatestID())
	if len(sub.Replay) != 0 {
		t.Errorf("live-tail replay: got %+v want empty", sub.Replay)
	}

	_ = b.Publish(99)

	ev := mustReceive(t, sub.Events, "live")
	if ev.Payload != 99 {
		t.Errorf("live: got %+v", ev)
	}
}

// mustReceive blocks up to one second waiting for an event. Failing the
// test on timeout keeps a goroutine leak from hanging the suite.
func mustReceive[T any](t *testing.T, ch <-chan Event[T], label string) Event[T] {
	t.Helper()

	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatalf("%s: channel closed unexpectedly", label)
		}

		return ev
	case <-time.After(time.Second):
		t.Fatalf("%s: timeout waiting for event", label)

		return Event[T]{}
	}
}
