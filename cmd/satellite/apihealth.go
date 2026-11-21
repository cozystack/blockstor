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
	"os"

	"github.com/cockroachdb/errors"
)

// client-go puts an HTTP/2 health check on every API connection and
// reads its two timeouts from these variables when it builds a
// transport. Its defaults, 30 s without a frame and then 15 s for the
// ping answer, keep a connection to an API server that died without
// a FIN or RST, a node that was hard reset, in use for 45 s. Every
// watch and status write of the satellite that sat on it stalls that
// long. 10 s and 5 s drop such a connection within 15 s, at the cost
// of one ping per idle connection every 10 s.
const (
	apiReadIdleTimeoutEnv = "HTTP2_READ_IDLE_TIMEOUT_SECONDS"
	apiPingTimeoutEnv     = "HTTP2_PING_TIMEOUT_SECONDS"

	apiReadIdleTimeoutSeconds = "10"
	apiPingTimeoutSeconds     = "5"
)

// tuneAPIHealthCheck shortens the health check for every API client
// the process builds after it. A value the operator set on the pod
// wins.
func tuneAPIHealthCheck() error {
	for _, setting := range [][2]string{
		{apiReadIdleTimeoutEnv, apiReadIdleTimeoutSeconds},
		{apiPingTimeoutEnv, apiPingTimeoutSeconds},
	} {
		if _, set := os.LookupEnv(setting[0]); set {
			continue
		}

		err := os.Setenv(setting[0], setting[1])
		if err != nil {
			return errors.Wrapf(err, "set %s", setting[0])
		}
	}

	return nil
}
