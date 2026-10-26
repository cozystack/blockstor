// SPDX-License-Identifier: Apache-2.0

package stream_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite/stream"
)

// The stream listens on the node's own network, beside DRBD, LINSTOR's
// satellite and controller ports, node-exporter and the satellite's health
// probe. A collision does not stop the satellite, only the stream, so it is
// caught here rather than on a cluster.
func TestStreamPortIsFreeOnTheNode(t *testing.T) {
	t.Parallel()

	taken := map[int]string{
		3366: "LINSTOR satellite", 3367: "LINSTOR satellite (TLS)",
		3370: "LINSTOR controller", 3371: "LINSTOR controller (TLS)",
		9100: "node-exporter", 9101: "blockstor-satellite health probe",
	}

	if owner, ok := taken[stream.Port]; ok {
		t.Errorf("stream port %d is %s's", stream.Port, owner)
	}

	if stream.Port >= 7000 && stream.Port <= 7999 {
		t.Errorf("stream port %d is in DRBD's default range", stream.Port)
	}
}
