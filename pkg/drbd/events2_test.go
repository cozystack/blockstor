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

package drbd_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestParseEventResourceLine: a basic `exists resource …` line parses
// to action=exists, kind=resource, fields keyed by their k:v split.
func TestParseEventResourceLine(t *testing.T) {
	line := "exists resource name:pvc-1 role:Secondary suspended:no write-ordering:flush"

	ev, err := drbd.ParseEvent(line)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}

	if ev.Action != "exists" {
		t.Errorf("Action: got %q, want exists", ev.Action)
	}

	if ev.Kind != "resource" {
		t.Errorf("Kind: got %q, want resource", ev.Kind)
	}

	if got := ev.Fields["name"]; got != "pvc-1" {
		t.Errorf("name: got %q, want pvc-1", got)
	}

	if got := ev.Fields["role"]; got != "Secondary" {
		t.Errorf("role: got %q, want Secondary", got)
	}
}

// TestParseEventChangeDeviceLine: `change device …` → action=change,
// kind=device, fields preserved.
func TestParseEventChangeDeviceLine(t *testing.T) {
	line := "change device name:pvc-1 volume:0 disk:UpToDate"

	ev, err := drbd.ParseEvent(line)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}

	if ev.Action != "change" {
		t.Errorf("Action: got %q", ev.Action)
	}

	if ev.Kind != "device" {
		t.Errorf("Kind: got %q", ev.Kind)
	}

	if got := ev.Fields["disk"]; got != "UpToDate" {
		t.Errorf("disk: got %q", got)
	}
}

// TestParseEventInitialSyncMarker: `exists -` is the marker drbdsetup
// emits once it has flushed the initial state. We surface it as
// kind=marker so the consumer can flip "ready" without special-casing.
func TestParseEventInitialSyncMarker(t *testing.T) {
	ev, err := drbd.ParseEvent("exists -")
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}

	if ev.Action != "exists" {
		t.Errorf("Action: got %q", ev.Action)
	}

	if ev.Kind != "-" {
		t.Errorf("Kind: got %q, want -", ev.Kind)
	}

	if len(ev.Fields) != 0 {
		t.Errorf("Fields: got %v, want empty", ev.Fields)
	}
}

// TestParseEventInvalidLine: malformed lines surface as errors so the
// listener can log + skip rather than corrupt downstream state.
func TestParseEventInvalidLine(t *testing.T) {
	_, err := drbd.ParseEvent("")
	if err == nil {
		t.Errorf("ParseEvent(\"\"): expected error, got nil")
	}

	_, err = drbd.ParseEvent("only-one-token")
	if err == nil {
		t.Errorf("ParseEvent(short): expected error, got nil")
	}
}

// TestParseEventIgnoresExtraWhitespace: drbdsetup pads with multiple
// spaces in some kernel builds; tolerate it.
func TestParseEventIgnoresExtraWhitespace(t *testing.T) {
	ev, err := drbd.ParseEvent("exists  resource   name:pvc-1   role:Primary")
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}

	if ev.Fields["role"] != "Primary" {
		t.Errorf("role: got %q", ev.Fields["role"])
	}
}

// TestParseEventValueWithColon: peer-disk:UpToDate has a single ":";
// disk paths or addresses might carry more. Our split must use only the
// first ":".
func TestParseEventValueWithColon(t *testing.T) {
	line := "exists peer-device name:pvc-1 peer-node-id:1 address:ipv4:10.0.0.1:7000"

	ev, err := drbd.ParseEvent(line)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}

	if got := ev.Fields["address"]; got != "ipv4:10.0.0.1:7000" {
		t.Errorf("address: got %q, want ipv4:10.0.0.1:7000", got)
	}
}

// TestEventStreamReplay feeds a captured trace through the parser and
// checks the event count + selected fields. Smoke test for the typical
// "two-peer resource comes up" sequence.
func TestEventStreamReplay(t *testing.T) {
	trace := strings.Join([]string{
		"exists resource name:pvc-1 role:Secondary suspended:no",
		"exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connected",
		"exists device name:pvc-1 volume:0 minor:1000 disk:UpToDate quorum:yes",
		"exists -",
		"change device name:pvc-1 volume:0 disk:Outdated",
	}, "\n")

	lines := strings.Split(trace, "\n")
	got := make([]drbd.Event, 0, len(lines))

	for _, line := range lines {
		ev, err := drbd.ParseEvent(line)
		if err != nil {
			t.Fatalf("line %q: %v", line, err)
		}

		got = append(got, ev)
	}

	if len(got) != 5 {
		t.Errorf("len: got %d, want 5", len(got))
	}

	if got[3].Kind != "-" {
		t.Errorf("got[3].Kind: got %q, want -", got[3].Kind)
	}

	if got[4].Action != "change" || got[4].Fields["disk"] != "Outdated" {
		t.Errorf("got[4]: %+v", got[4])
	}
}

// TestWatcherStreamsEvents pins the Watcher's parse-and-emit pipeline
// against an in-memory reader. drbdsetup events2 sends one event per
// line; the Watcher must surface each one as a parsed Event on the
// channel. Pinned because the satellite's runObserveLoop wires this
// straight into its observation pipeline — a regression in line
// boundary handling would silently drop kernel events and stale the
// controller's view of replication state.
func TestWatcherStreamsEvents(t *testing.T) {
	t.Parallel()

	src := strings.NewReader(strings.Join([]string{
		"exists resource name:pvc-1 role:Primary",
		"change device name:pvc-1 minor:1000 disk:UpToDate",
		"", // blank line — must be skipped, not abort the pipeline
		"exists -",
	}, "\n") + "\n")

	w := drbd.NewWatcher(src)
	ch := make(chan drbd.Event, 8)

	if err := w.Watch(t.Context(), ch); err != nil {
		t.Fatalf("Watch: %v", err)
	}

	got := make([]drbd.Event, 0, 4)
	for ev := range ch {
		got = append(got, ev)
	}

	if len(got) != 3 {
		t.Fatalf("event count: got %d, want 3 (blank line must be skipped); %+v", len(got), got)
	}

	if got[0].Action != "exists" || got[0].Kind != "resource" || got[0].Fields["name"] != "pvc-1" {
		t.Errorf("event[0]: got %+v", got[0])
	}

	if got[1].Action != "change" || got[1].Fields["disk"] != "UpToDate" {
		t.Errorf("event[1]: got %+v", got[1])
	}

	if got[2].Action != "exists" || got[2].Kind != "-" {
		t.Errorf("event[2] (initial-sync marker): got %+v", got[2])
	}
}

// TestWatcherClosesChannelOnEOF: the channel must be closed when the
// source EOFs (e.g. drbdsetup exits). Without this the consumer
// goroutine in runObserveLoop hangs forever.
func TestWatcherClosesChannelOnEOF(t *testing.T) {
	t.Parallel()

	src := strings.NewReader("") // immediate EOF

	w := drbd.NewWatcher(src)
	ch := make(chan drbd.Event, 1)

	if err := w.Watch(t.Context(), ch); err != nil {
		t.Fatalf("Watch on empty source: got %v, want nil", err)
	}

	// Reading from a closed channel returns immediately with zero-value.
	select {
	case ev, ok := <-ch:
		if ok {
			t.Errorf("channel got an event from empty source: %+v", ev)
		}
	default:
		t.Errorf("channel not closed after EOF — runObserveLoop would hang")
	}
}
