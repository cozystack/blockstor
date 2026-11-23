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

// Package apihealth loads the Kubernetes API config every blockstor
// daemon talks through, with a health check short enough that a
// connection to an API server that died silently is dropped well
// inside the node monitor grace period.
package apihealth

import (
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

// client-go puts an HTTP/2 health check on every API connection and
// reads its two timeouts from these variables when it builds a
// transport. Its defaults, 30 s without a frame and then 15 s for the
// ping answer, keep a connection to an API server that died without
// a FIN or RST, a node that was hard reset, in use for 45 s. Every
// watch, cached read and status write that sat on it stalls that
// long: the satellite's heartbeat does not get through, and the
// controller's cache keeps showing heartbeats that are long gone.
// 10 s and 5 s drop such a connection within 15 s, at the cost of
// one ping per idle connection every 10 s.
const (
	ReadIdleTimeoutEnv = "HTTP2_READ_IDLE_TIMEOUT_SECONDS"
	PingTimeoutEnv     = "HTTP2_PING_TIMEOUT_SECONDS"

	ReadIdleTimeout = 10 * time.Second
	PingTimeout     = 5 * time.Second
)

// Tune shortens the health check for every API client the process
// builds after it. A value the operator set on the pod wins.
func Tune() error {
	for _, setting := range []struct {
		env   string
		value time.Duration
	}{
		{ReadIdleTimeoutEnv, ReadIdleTimeout},
		{PingTimeoutEnv, PingTimeout},
	} {
		if _, set := os.LookupEnv(setting.env); set {
			continue
		}

		err := os.Setenv(setting.env, strconv.Itoa(int(setting.value/time.Second)))
		if err != nil {
			return errors.Wrapf(err, "set %s", setting.env)
		}
	}

	return nil
}

// GetConfig tunes the health check and then loads the API config the
// way controller-runtime does. Every long-running blockstor process
// loads its config through here, so none of them is left on
// client-go's 45 s.
func GetConfig() (*rest.Config, error) {
	err := Tune()
	if err != nil {
		return nil, err
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, errors.Wrap(err, "load Kubernetes config")
	}

	return cfg, nil
}
