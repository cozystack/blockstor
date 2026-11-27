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

package main

import (
	"errors"
	"flag"
	"io"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/satellite/stream"
)

func parseTestFlags(args ...string) (options, error) {
	fs := flag.NewFlagSet("satellite", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	return parseFlags(fs, args)
}

// A partial set of stream TLS flags refuses to start. Let through, it would
// quietly turn the stream off and every cross-node restore on the node would
// fall back to a resync.
func TestSatelliteRefusesAPartialStreamTLSConfiguration(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"--stream-tls-cert-file=/tls/tls.crt"},
		{"--stream-tls-cert-file=/tls/tls.crt", "--stream-tls-key-file=/tls/tls.key"},
		{"--stream-tls-ca-file=/tls/ca.crt"},
	} {
		_, err := parseTestFlags(args...)
		if !errors.Is(err, stream.ErrTLSIncomplete) {
			t.Errorf("flags %v = %v, want stream.ErrTLSIncomplete", args, err)
		}
	}
}

func TestSatelliteStartsWithTheStreamOff(t *testing.T) {
	t.Parallel()

	opts, err := parseTestFlags("--node-name=n1")
	if err != nil {
		t.Fatalf("no stream TLS flags = %v, want the stream off and no error", err)
	}

	if opts.nodeName != "n1" || opts.streamTLS.Configured() {
		t.Fatalf("options = %+v, want node n1 and the stream off", opts)
	}
}

// Without the flag the satellite applies several resources at once, so a
// rebooted node does not bring its replicas up one by one.
func TestSatelliteAppliesResourcesInParallelByDefault(t *testing.T) {
	t.Parallel()

	opts, err := parseTestFlags("--node-name=n1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if opts.resourceWorkers != controllers.DefaultResourceWorkers {
		t.Errorf("resource workers = %d, want the default %d", opts.resourceWorkers, controllers.DefaultResourceWorkers)
	}

	opts, err = parseTestFlags("--node-name=n1", "--resource-workers=3")
	if err != nil || opts.resourceWorkers != 3 {
		t.Errorf("--resource-workers=3 = %d, %v", opts.resourceWorkers, err)
	}
}

// No worker at all would leave every resource on the node unreconciled.
func TestSatelliteRefusesNoResourceWorkers(t *testing.T) {
	t.Parallel()

	for _, arg := range []string{"--resource-workers=0", "--resource-workers=-1"} {
		_, err := parseTestFlags("--node-name=n1", arg)
		if err == nil {
			t.Errorf("%s accepted, want refused", arg)
		}
	}
}

// The DRBD workarounds the LINSTOR-mode plunger runs are all on by
// default, at the plunger's cadence.
func TestSatelliteRunsTheDRBDWorkaroundsByDefault(t *testing.T) {
	t.Parallel()

	opts, err := parseTestFlags("--node-name=n1")
	if err != nil {
		t.Fatal(err)
	}

	if opts.workarounds != controllers.DefaultWorkaroundsConfig() {
		t.Errorf("workarounds = %+v, want the defaults %+v", opts.workarounds, controllers.DefaultWorkaroundsConfig())
	}

	w := opts.workarounds
	if w.Interval != 30*time.Second || w.StalledResyncPasses != 4 || w.StalledResyncMinRate != 200 ||
		!w.StalledResync || !w.IdlePrimary || !w.ForcedIOFailures || !w.StaleLoopDevices {
		t.Errorf("defaults = %+v, want every check on, 30s, 4 passes, 200 KiB/s", w)
	}

	// A stalled-resync reconnect left a replica diverged from the
	// Primary on the test cluster; it runs only when asked for.
	if w.StalledResyncAct {
		t.Errorf("defaults = %+v, want the stalled-resync check to report only", w)
	}

	opts, err = parseTestFlags("--node-name=n1", "--drbd-workaround-stalled-resync-act")
	if err != nil || !opts.workarounds.StalledResyncAct {
		t.Errorf("--drbd-workaround-stalled-resync-act = %+v, %v, want the action on", opts.workarounds, err)
	}
}

// Each workaround has a flag of its own to turn it off.
func TestSatelliteTurnsEachDRBDWorkaroundOff(t *testing.T) {
	t.Parallel()

	opts, err := parseTestFlags("--node-name=n1",
		"--drbd-workaround-stalled-resync=false",
		"--drbd-workaround-idle-primary=false",
		"--drbd-workaround-forced-io-failures=false",
		"--drbd-workaround-stale-loop-devices=false",
		"--drbd-workaround-stalled-resync-passes=6",
		"--drbd-workaround-stalled-resync-min-rate=50",
		"--drbd-workarounds-interval=1m")
	if err != nil {
		t.Fatal(err)
	}

	want := controllers.WorkaroundsConfig{Interval: time.Minute, StalledResyncPasses: 6, StalledResyncMinRate: 50}
	if opts.workarounds != want {
		t.Errorf("workarounds = %+v, want %+v", opts.workarounds, want)
	}
}

func TestSatelliteRefusesBadWorkaroundFlags(t *testing.T) {
	t.Parallel()

	for _, arg := range []string{
		"--drbd-workarounds-interval=-1s",
		"--drbd-workaround-stalled-resync-passes=0",
		"--drbd-workaround-stalled-resync-min-rate=0",
	} {
		_, err := parseTestFlags("--node-name=n1", arg)
		if !errors.Is(err, errBadWorkaroundFlags) {
			t.Errorf("%s = %v, want errBadWorkaroundFlags", arg, err)
		}
	}
}
