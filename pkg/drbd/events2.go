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

package drbd

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"strings"

	"github.com/cockroachdb/errors"
)

// Event is a parsed line from `drbdsetup events2 --statistics`. Fields
// are key:value pairs lifted verbatim from the line — interpretation is
// the consumer's responsibility (we don't enumerate roles / disk states
// here because new kernels add values without bumping the format).
type Event struct {
	// Action is the verb: typically "exists" (initial flush), "change"
	// (delta), "create" / "destroy".
	Action string

	// Kind is the object kind: "resource", "connection", "device",
	// "peer-device", or "-" (the marker between the initial state and
	// the live stream).
	Kind string

	// Fields are the key:value attributes that follow the verb+kind.
	// Empty for the marker event.
	Fields map[string]string
}

// ParseEvent parses one drbdsetup events2 line. Returns an error for
// blank or single-token lines so the caller can log and continue.
func ParseEvent(line string) (Event, error) {
	parts := strings.Fields(line)
	if len(parts) < eventMinTokens {
		return Event{}, errors.Errorf("drbd: malformed events2 line %q", line)
	}

	ev := Event{
		Action: parts[0],
		Kind:   parts[1],
		Fields: map[string]string{},
	}

	for _, kv := range parts[2:] {
		key, value, ok := strings.Cut(kv, ":")
		if !ok {
			continue
		}

		ev.Fields[key] = value
	}

	return ev, nil
}

// Watcher consumes drbdsetup events2 lines from a Reader and pushes
// parsed Events into a channel. It is a thin glue layer — the heavy
// lifting (state machine, status writebacks) lives in the satellite
// reconciler.
type Watcher struct {
	src io.Reader
}

// NewWatcher wraps src (typically the stdout pipe of `drbdsetup events2
// --statistics`) into a Watcher.
func NewWatcher(src io.Reader) *Watcher {
	return &Watcher{src: src}
}

// Watch streams events into ch until ctx is cancelled or the source
// EOFs. Malformed lines are skipped (they would otherwise stall the
// pipeline on a single kernel quirk). The channel is closed on return.
func (w *Watcher) Watch(ctx context.Context, ch chan<- Event) error {
	defer close(ch)

	scanner := bufio.NewScanner(w.src)
	scanner.Buffer(make([]byte, scannerInitial), scannerMax)

	for scanner.Scan() {
		err := ctx.Err()
		if err != nil {
			return errors.Wrap(err, "events2: context cancelled")
		}

		ev, err := ParseEvent(scanner.Text())
		if err != nil {
			continue
		}

		select {
		case ch <- ev:
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "events2: context cancelled")
		}
	}

	err := scanner.Err()
	if err != nil {
		return errors.Wrap(err, "events2: scanner")
	}

	return nil
}

// StartDrbdsetupEvents2 launches `drbdsetup events2 --statistics` and
// returns a Watcher hooked to its stdout, plus a cleanup func that
// kills the child process. Production wiring; not used in unit tests
// (those feed Watcher a fake io.Reader instead).
func StartDrbdsetupEvents2(ctx context.Context) (*Watcher, func(), error) {
	cmd := exec.CommandContext(ctx, "drbdsetup", "events2", "--statistics")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, errors.Wrap(err, "events2: stdout pipe")
	}

	err = cmd.Start()
	if err != nil {
		return nil, nil, errors.Wrap(err, "events2: start")
	}

	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	return NewWatcher(stdout), cleanup, nil
}

const (
	eventMinTokens = 2
	scannerInitial = 4 * 1024
	scannerMax     = 1024 * 1024
)
