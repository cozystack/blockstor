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

package rest

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// DefaultResourceReadyWait is how long LINSTOR waits for a placed
// resource to become ready before it answers the create with a
// warning (CtrlRscCrtApiHelper.DFLT_RSC_READY_WAIT_TIME_IN_MS).
const DefaultResourceReadyWait = 15 * time.Second

// resourceReadyPollInterval is how often the wait re-reads the cached
// replicas. A variable so tests can shorten it.
//
//nolint:gochecknoglobals // tunable
var resourceReadyPollInterval = 250 * time.Millisecond

// waitPlacedReplicasReady holds an autoplace answer until every
// diskful replica of rdName on a reachable node reports a device path
// for every volume of the definition, or until s.ResourceReadyWait
// runs out.
//
// LINSTOR answers an autoplace only after its satellites applied the
// new resources and, within fifteen seconds, reported them ready.
// linstor-csi relies on that: its ControllerPublishVolume reads the
// volume's device path right after CreateVolume returned, and fails
// the attach when there is none. Answering before the satellites
// built the replicas turned that into a ControllerPublishVolume error
// whose exponential retry roughly doubled the time to attach.
//
// Replicas that never report a path within the bound get the warning
// LINSTOR gives in that case; the placement itself stands either way.
func (s *Server) waitPlacedReplicasReady(ctx context.Context, rdName string) []apiv1.APICallRc {
	if s.ResourceReadyWait <= 0 {
		return nil
	}

	volumes := s.definitionVolumeSizes(ctx, rdName)
	if len(volumes) == 0 {
		// No volume, nothing for a satellite to build: LINSTOR does
		// not wait either.
		return nil
	}

	deadline := time.Now().Add(s.ResourceReadyWait)

	for {
		pending, err := s.replicasWithoutDevicePath(ctx, rdName, volumes)
		if err != nil || len(pending) == 0 {
			return nil
		}

		if !time.Now().Before(deadline) {
			return notReadyWarnings(pending)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(resourceReadyPollInterval):
		}
	}
}

// replicasWithoutDevicePath returns the nodes of rdName's diskful,
// live replicas on reachable nodes that miss a device path for at
// least one of the definition's volumes.
func (s *Server) replicasWithoutDevicePath(ctx context.Context, rdName string, volumes map[int32]int64) ([]string, error) {
	replicas, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return nil, errors.Wrap(err, "list replicas")
	}

	var pending []string

	for i := range replicas {
		rsc := &replicas[i]

		if isDisklessReplica(rsc) ||
			slices.Contains(rsc.Flags, apiv1.ResourceFlagDeleting) ||
			slices.Contains(rsc.Flags, apiv1.ResourceFlagDelete) {
			continue
		}

		if !isNodeOnline(ctx, s.Store.Nodes(), rsc.NodeName) {
			continue
		}

		if !hasDevicePathForEveryVolume(rsc.Volumes, volumes) {
			pending = append(pending, rsc.NodeName)
		}
	}

	return pending, nil
}

// hasDevicePathForEveryVolume reports whether every volume number in
// volumes has a reported entry with a device path.
func hasDevicePathForEveryVolume(reported []apiv1.Volume, volumes map[int32]int64) bool {
	for vn := range volumes {
		built := slices.ContainsFunc(reported, func(v apiv1.Volume) bool {
			return v.VolumeNumber == vn && v.DevicePath != ""
		})
		if !built {
			return false
		}
	}

	return true
}

// notReadyWarnings renders LINSTOR's per-node warning for a resource
// that did not become ready in time.
func notReadyWarnings(nodes []string) []apiv1.APICallRc {
	out := make([]apiv1.APICallRc, 0, len(nodes))

	for _, node := range nodes {
		out = append(out, apiv1.APICallRc{
			RetCode: maskWarn,
			Message: "Resource did not become ready on node '" + node +
				"' within reasonable time, check Satellite for errors.",
			ObjRefs: map[string]string{"Node": node},
		})
	}

	return out
}
