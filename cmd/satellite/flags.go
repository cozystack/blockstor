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

// options are the satellite's command-line flags.
type options struct {
	nodeName            string
	stateDir            string
	probeAddr           string
	controllerNamespace string
	streamTLS           stream.TLSFiles
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

	registerWorkaroundFlags(flagSet, &opts.workarounds)

	err := flagSet.Parse(args)
	if err != nil {
		return opts, errors.Wrap(err, "parse flags")
	}

	if opts.workarounds.Interval < 0 || opts.workarounds.StalledResyncPasses < 1 ||
		opts.workarounds.StalledResyncMinRate < 1 {
		return opts, errors.Wrapf(errBadWorkaroundFlags, "interval %s, stalled resync passes %d, min rate %d",
			opts.workarounds.Interval, opts.workarounds.StalledResyncPasses, opts.workarounds.StalledResyncMinRate)
	}

	err = opts.streamTLS.Validate()
	if err != nil {
		return opts, errors.Wrap(err, "snapshot stream TLS flags")
	}

	return opts, nil
}

// errBadWorkaroundFlags refuses a negative workaround interval or a
// stalled-resync check that would act without seeing a stall, or never
// see one.
var errBadWorkaroundFlags = errors.New("--drbd-workarounds-interval must not be negative, " +
	"--drbd-workaround-stalled-resync-passes and --drbd-workaround-stalled-resync-min-rate must be at least 1")

// registerWorkaroundFlags registers the flags of the satellite's DRBD
// workarounds (pkg/satellite/controllers/drbd_workarounds.go): all on by
// default, each one can be turned off, and a zero interval turns off all.
// The stalled-resync check only reports unless its act flag is set.
func registerWorkaroundFlags(flagSet *flag.FlagSet, cfg *controllers.WorkaroundsConfig) {
	def := controllers.DefaultWorkaroundsConfig()

	flagSet.DurationVar(&cfg.Interval, "drbd-workarounds-interval", def.Interval,
		"Time between passes of the DRBD workarounds; 0 turns them all off.")
	flagSet.BoolVar(&cfg.StalledResync, "drbd-workaround-stalled-resync", def.StalledResync,
		"Report a stalled resync (LINBIT/drbd#149) as a Warning Event with the drbdadm commands that would recover it.")
	flagSet.IntVar(&cfg.StalledResyncPasses, "drbd-workaround-stalled-resync-passes", def.StalledResyncPasses,
		"Passes in a row with too little resync progress before the stalled-resync workaround reports, or acts.")
	flagSet.IntVar(&cfg.StalledResyncMinRate, "drbd-workaround-stalled-resync-min-rate", def.StalledResyncMinRate,
		"Resync progress in KiB/s below which a pass counts as stalled; keep it under the resource's "+
			"c-min-rate, or its resync-rate when the rate is fixed.")
	flagSet.BoolVar(&cfg.StalledResyncAct, "drbd-workaround-stalled-resync-act", def.StalledResyncAct,
		"RISKY: also run the commands --drbd-workaround-stalled-resync reports, unless the resync is an initial sync: "+
			"disconnect and connect again the peer whose resync waits on the stalled one, or failing that "+
			"reconnect the peer the stalled resync pulls from. With a Primary writing, such a reconnect made a "+
			"Secondary the SyncSource of another Secondary and left the two with different data while DRBD "+
			"reported both UpToDate (LINBIT/drbd#153, LINBIT/drbd#143), so with a Primary in sight it only "+
			"disconnects the waiting peer, from a node whose volumes are all Inconsistent, and connects it again "+
			"once every other link is Connected to a Secondary. With a running guest that is when the guest stops. "+
			"Meanwhile the two Secondaries stay apart: if the Primary's node fails, its link is no longer Connected, "+
			"the connect stays owed, and with three replicas neither Secondary has quorum to be promoted until an "+
			"operator runs the `drbdadm connect` the Warning Event names.")
	flagSet.BoolVar(&cfg.IdlePrimary, "drbd-workaround-idle-primary", def.IdlePrimary,
		"Demote a Primary with no volume open (DRBD auto-demote not firing, piraeus-operator#942).")
	flagSet.BoolVar(&cfg.ForcedIOFailures, "drbd-workaround-forced-io-failures", def.ForcedIOFailures,
		"Take down and up a Secondary with no volume open that still forces I/O failures (LINBIT/drbd#153).")
	flagSet.BoolVar(&cfg.StaleLoopDevices, "drbd-workaround-stale-loop-devices", def.StaleLoopDevices,
		"Detach loop devices whose backing file reads \"/\" or \"/ (deleted)\" (kubernetes#102408).")
}
