// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"testing"
	"time"
)

// The satellite runs on host networking, so any listener its manager opens is
// a port of the node. Nothing scrapes its metrics, and controller-runtime's
// default would open one on every address.
func TestTheSatelliteManagerOpensNoMetricsListener(t *testing.T) {
	t.Parallel()

	grace := time.Second

	opts := managerOptions(&Config{}, &grace)
	if got := opts.Metrics.BindAddress; got != "0" {
		t.Errorf("metrics bind address = %q, want %q (disabled)", got, "0")
	}
}
