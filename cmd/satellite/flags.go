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

	"github.com/cozystack/blockstor/pkg/satellite/stream"
)

// options are the satellite's command-line flags.
type options struct {
	nodeName            string
	stateDir            string
	probeAddr           string
	controllerNamespace string
	streamTLS           stream.TLSFiles
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

	err := flagSet.Parse(args)
	if err != nil {
		return opts, errors.Wrap(err, "parse flags")
	}

	err = opts.streamTLS.Validate()
	if err != nil {
		return opts, errors.Wrap(err, "snapshot stream TLS flags")
	}

	return opts, nil
}
