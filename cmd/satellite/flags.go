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
	"flag"
	"os"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/satellite/stream"
)

// errNoResourceWorkers refuses a worker count that would leave every
// resource on the node unreconciled.
var errNoResourceWorkers = errors.New("the satellite needs at least one resource worker")

// options are the satellite's command-line flags.
type options struct {
	nodeName            string
	stateDir            string
	probeAddr           string
	controllerNamespace string
	streamTLS           stream.TLSFiles
	resourceWorkers     int
	workarounds         controllers.WorkaroundsConfig
}

// parseFlags registers the satellite's flags on flagSet, parses args and
// refuses a stream TLS configuration the satellite must not start with:
// some of the three files set and not all would quietly turn the stream
// off.
func parseFlags(flagSet *flag.FlagSet, args []string) (options, error) {
	var opts options

	flagSet.StringVar(&opts.nodeName, "node-name", os.Getenv("NODE_NAME"),
		"name this satellite registers under (defaults to NODE_NAME env)")
	flagSet.StringVar(&opts.stateDir, "state-dir", "/var/lib/blockstor-satellite",
		"directory the satellite uses to persist DRBD .res files and per-resource state")
	flagSet.StringVar(&opts.probeAddr, "health-probe-bind-address", ":8081",
		"The address the /healthz and /readyz probe endpoints bind to.")
	flagSet.StringVar(&opts.controllerNamespace, "controller-namespace", "",
		"Namespace where the controller's Secrets (cluster master passphrase) live (default: $POD_NAMESPACE, then 'blockstor-system').")

	flagSet.StringVar(&opts.streamTLS.CertFile, "stream-tls-cert-file", "",
		"PEM certificate this satellite presents on the snapshot stream, for both server and client auth (cert-manager tls.crt).")
	flagSet.StringVar(&opts.streamTLS.KeyFile, "stream-tls-key-file", "",
		"PEM key for --stream-tls-cert-file (cert-manager tls.key).")
	flagSet.StringVar(&opts.streamTLS.CAFile, "stream-tls-ca-file", "",
		"PEM CA bundle every peer satellite's stream certificate is verified against (cert-manager ca.crt). "+
			"With none of the three set the snapshot stream is off and cross-node clone and restore fall back to a DRBD resync.")

	flagSet.IntVar(&opts.resourceWorkers, "resource-workers", controllers.DefaultResourceWorkers,
		"How many local resources the satellite applies at once, for instance when it brings every replica of the node up after a reboot.")

	registerWorkaroundFlags(flagSet, &opts.workarounds)

	err := flagSet.Parse(args)
	if err != nil {
		return opts, errors.Wrap(err, "parse flags")
	}

	if opts.resourceWorkers < 1 {
		return opts, errors.Wrapf(errNoResourceWorkers, "--resource-workers=%d", opts.resourceWorkers)
	}

	if opts.workarounds.Interval < 0 || opts.workarounds.StalledResyncPasses < 1 {
		return opts, errors.Wrapf(errBadWorkaroundFlags, "interval %s, stalled resync passes %d",
			opts.workarounds.Interval, opts.workarounds.StalledResyncPasses)
	}

	err = opts.streamTLS.Validate()
	if err != nil {
		return opts, errors.Wrap(err, "snapshot stream TLS flags")
	}

	return opts, nil
}

// errBadWorkaroundFlags refuses a negative workaround interval or a
// stalled-resync check that would act without seeing a stall.
var errBadWorkaroundFlags = errors.New("--drbd-workarounds-interval must not be negative " +
	"and --drbd-workaround-stalled-resync-passes must be at least 1")

// registerWorkaroundFlags registers the flags of the satellite's DRBD
// workarounds (pkg/satellite/controllers/drbd_workarounds.go): all on by
// default, each one can be turned off, and a zero interval turns off all.
func registerWorkaroundFlags(flagSet *flag.FlagSet, cfg *controllers.WorkaroundsConfig) {
	def := controllers.DefaultWorkaroundsConfig()

	flagSet.DurationVar(&cfg.Interval, "drbd-workarounds-interval", def.Interval,
		"Time between passes of the DRBD workarounds; 0 turns them all off.")
	flagSet.BoolVar(&cfg.StalledResync, "drbd-workaround-stalled-resync", def.StalledResync,
		"Disconnect and connect the peer a resync stalled on while another peer waits on it (LINBIT/drbd#149).")
	flagSet.IntVar(&cfg.StalledResyncPasses, "drbd-workaround-stalled-resync-passes", def.StalledResyncPasses,
		"Passes in a row without resync progress before the stalled-resync workaround acts.")
	flagSet.BoolVar(&cfg.IdlePrimary, "drbd-workaround-idle-primary", def.IdlePrimary,
		"Demote a Primary with no volume open (DRBD auto-demote not firing, piraeus-operator#942).")
	flagSet.BoolVar(&cfg.ForcedIOFailures, "drbd-workaround-forced-io-failures", def.ForcedIOFailures,
		"Take down and up a Secondary with no volume open that still forces I/O failures (LINBIT/drbd#153).")
	flagSet.BoolVar(&cfg.StaleLoopDevices, "drbd-workaround-stale-loop-devices", def.StaleLoopDevices,
		"Detach loop devices whose backing file reads \"/\" or \"/ (deleted)\" (kubernetes#102408).")
}
