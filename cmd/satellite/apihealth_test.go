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
	"strconv"
	"testing"
	"time"

	"github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
)

// unsetEnv clears key for the test and restores it afterwards.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")

	err := os.Unsetenv(key)
	if err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
}

func envSeconds(t *testing.T, key string) time.Duration {
	t.Helper()

	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		t.Fatalf("%s=%q: %v", key, os.Getenv(key), err)
	}

	return time.Duration(n) * time.Second
}

// TestTuneAPIHealthCheckOutrunsTheGracePeriod: when a node is hard
// reset, the satellites whose API connections sat on its API server
// get nothing through until the HTTP/2 health check drops those
// connections. That, plus one heartbeat period to get the next stamp
// in, has to stay inside the controller's grace period, or a node that
// is running fine goes OFFLINE. client-go's own 30 s + 15 s does not.
func TestTuneAPIHealthCheckOutrunsTheGracePeriod(t *testing.T) {
	unsetEnv(t, apiReadIdleTimeoutEnv)
	unsetEnv(t, apiPingTimeoutEnv)

	err := tuneAPIHealthCheck()
	if err != nil {
		t.Fatalf("tuneAPIHealthCheck: %v", err)
	}

	dead := envSeconds(t, apiReadIdleTimeoutEnv) + envSeconds(t, apiPingTimeoutEnv)
	if dead <= 0 {
		t.Fatalf("health check disabled (%s); a dead connection would never be dropped", dead)
	}

	if dead+controllers.HeartbeatPeriod >= controller.NodeMonitorGracePeriod {
		t.Errorf("a dead API connection is dropped after %s, plus a %s heartbeat period that is not inside the %s grace period",
			dead, controllers.HeartbeatPeriod, controller.NodeMonitorGracePeriod)
	}
}

// TestTuneAPIHealthCheckKeepsTheOperatorsValue: a timeout set on the
// pod is the operator's call and stays.
func TestTuneAPIHealthCheckKeepsTheOperatorsValue(t *testing.T) {
	t.Setenv(apiReadIdleTimeoutEnv, "20")
	unsetEnv(t, apiPingTimeoutEnv)

	err := tuneAPIHealthCheck()
	if err != nil {
		t.Fatalf("tuneAPIHealthCheck: %v", err)
	}

	if got := os.Getenv(apiReadIdleTimeoutEnv); got != "20" {
		t.Errorf("%s = %q, want the operator's 20", apiReadIdleTimeoutEnv, got)
	}

	if got := os.Getenv(apiPingTimeoutEnv); got != apiPingTimeoutSeconds {
		t.Errorf("%s = %q, want %s", apiPingTimeoutEnv, got, apiPingTimeoutSeconds)
	}
}
