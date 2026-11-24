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

package apihealth_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/apihealth"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
)

// relistSlack is what a reflector takes, once its watch connection is
// dropped, to back off and list again on a new one: client-go waits
// about 800 ms before the first retry, and the list itself is quick.
const relistSlack = 5 * time.Second

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

// tunedDeadConnection runs Tune on a clean environment and returns how
// long a connection to an API server that died silently stays in use.
func tunedDeadConnection(t *testing.T) time.Duration {
	t.Helper()
	unsetEnv(t, apihealth.ReadIdleTimeoutEnv)
	unsetEnv(t, apihealth.PingTimeoutEnv)

	err := apihealth.Tune()
	if err != nil {
		t.Fatalf("Tune: %v", err)
	}

	dead := envSeconds(t, apihealth.ReadIdleTimeoutEnv) + envSeconds(t, apihealth.PingTimeoutEnv)
	if dead <= 0 {
		t.Fatalf("health check disabled (%s); a dead connection would never be dropped", dead)
	}

	return dead
}

// TestTuneOutrunsTheGracePeriodOnTheSatellite: when a node is hard
// reset, the satellites whose API connections sat on its API server
// get nothing through until the HTTP/2 health check drops those
// connections. That, plus one heartbeat period to get the next stamp
// in, has to stay inside the controller's grace period, or a node that
// is running fine goes OFFLINE. client-go's own 30 s + 15 s does not.
func TestTuneOutrunsTheGracePeriodOnTheSatellite(t *testing.T) {
	dead := tunedDeadConnection(t)

	if dead+controllers.HeartbeatPeriod >= controller.NodeMonitorGracePeriod {
		t.Errorf("a dead API connection is dropped after %s, plus a %s heartbeat period that is not inside the %s grace period",
			dead, controllers.HeartbeatPeriod, controller.NodeMonitorGracePeriod)
	}
}

// TestTuneOutrunsTheGracePeriodOnTheController: when the controller's
// own connection sits on the API server that died, its cache stops
// seeing the stamps the satellites keep writing through live API
// servers. The newest stamp it holds can already be a heartbeat
// period old when the connection dies; the cache moves again only
// after the health check drops the connection and the reflector lists
// again. All of that has to stay inside the grace period, or the
// watchdog calls every node OFFLINE off its own frozen cache.
func TestTuneOutrunsTheGracePeriodOnTheController(t *testing.T) {
	dead := tunedDeadConnection(t)

	frozen := controllers.HeartbeatPeriod + dead + relistSlack
	if frozen >= controller.NodeMonitorGracePeriod {
		t.Errorf("the controller's cache can hold a heartbeat %s old (%s period + %s dead connection + %s relist), not inside the %s grace period",
			frozen, controllers.HeartbeatPeriod, dead, relistSlack, controller.NodeMonitorGracePeriod)
	}
}

// TestTuneKeepsTheOperatorsValue: a timeout set on the pod is the
// operator's call and stays.
func TestTuneKeepsTheOperatorsValue(t *testing.T) {
	t.Setenv(apihealth.ReadIdleTimeoutEnv, "20")
	unsetEnv(t, apihealth.PingTimeoutEnv)

	err := apihealth.Tune()
	if err != nil {
		t.Fatalf("Tune: %v", err)
	}

	if got := os.Getenv(apihealth.ReadIdleTimeoutEnv); got != "20" {
		t.Errorf("%s = %q, want the operator's 20", apihealth.ReadIdleTimeoutEnv, got)
	}

	if got := envSeconds(t, apihealth.PingTimeoutEnv); got != apihealth.PingTimeout {
		t.Errorf("%s = %s, want %s", apihealth.PingTimeoutEnv, got, apihealth.PingTimeout)
	}
}

// TestGetConfigTunesBeforeLoading: the config a daemon gets back is
// only safe if the health check was tuned before any client is built
// from it.
func TestGetConfigTunesBeforeLoading(t *testing.T) {
	unsetEnv(t, apihealth.ReadIdleTimeoutEnv)
	unsetEnv(t, apihealth.PingTimeoutEnv)

	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")

	err := os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: c
  context:
    cluster: c
    user: u
current-context: c
users:
- name: u
  user:
    token: t
`), 0o600)
	if err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	t.Setenv("KUBECONFIG", kubeconfig)

	cfg, err := apihealth.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}

	if cfg.Host != "https://127.0.0.1:6443" {
		t.Errorf("Host = %q, want the kubeconfig's server", cfg.Host)
	}

	if got := envSeconds(t, apihealth.ReadIdleTimeoutEnv); got != apihealth.ReadIdleTimeout {
		t.Errorf("%s = %s after GetConfig, want %s", apihealth.ReadIdleTimeoutEnv, got, apihealth.ReadIdleTimeout)
	}

	if got := envSeconds(t, apihealth.PingTimeoutEnv); got != apihealth.PingTimeout {
		t.Errorf("%s = %s after GetConfig, want %s", apihealth.PingTimeoutEnv, got, apihealth.PingTimeout)
	}
}
