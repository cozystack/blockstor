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
	"fmt"
	"net"
)

// errEmptyPlainBindAddress refuses an empty --rest-bind-address. Unlike the
// TLS flag, where empty turns the listener off, an empty address here would
// reach net.Listen and bind every interface on a random port.
var errEmptyPlainBindAddress = errors.New(
	"--rest-bind-address is empty, which would bind every interface on a random port; " +
		"give a loopback address such as " + defaultPlainBindAddress)

// defaultPlainBindAddress is where the plain-HTTP listener binds unless a
// flag says otherwise. That listener serves the whole API without a client
// certificate, so it stays on loopback: `kubectl port-forward` reaches it
// through the pod's own network namespace, and no other pod can.
const defaultPlainBindAddress = "127.0.0.1:3370"

// checkPlainBindAddress refuses an empty or unparsable address for the
// plain listener, and reports whether a valid one is off loopback, where it
// serves the whole API without a certificate to every pod that can reach
// it. A deployment applied from an older manifest still passes `:3370`.
func checkPlainBindAddress(addr string) (bool, error) {
	if addr == "" {
		return false, errEmptyPlainBindAddress
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("--rest-bind-address %q: %w", addr, err)
	}

	if host == "localhost" {
		return false, nil
	}

	ip := net.ParseIP(host)

	return ip == nil || !ip.IsLoopback(), nil
}
