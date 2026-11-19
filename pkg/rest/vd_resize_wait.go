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
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// resizeWaitPollInterval is how often a resize request re-reads the
// replicas while it waits for them to grow.
//
//nolint:gochecknoglobals // tunable; tests shorten it
var resizeWaitPollInterval = 250 * time.Millisecond

// warnNotConnected is LINSTOR's WARN_NOT_CONNECTED (1002 in the warning
// band), the entry its controller adds for every satellite an update
// could not reach.
const warnNotConnected = maskWarn | int64(1002)

// resizeReplicas sorts the replicas of a definition by what a grow of
// one volume has to wait for.
type resizeReplicas struct {
	// growing are the replicas whose satellite has not yet finished
	// applying the target size.
	growing []string
	// offline are the diskful replicas on a node whose satellite is
	// not connected. They catch up when it reconnects, so a resize does
	// not wait for them, as in LINSTOR.
	offline []string
	// appliedKib is the smallest size an awaited replica reports as
	// applied, 0 when none reports one.
	appliedKib int64
}

// awaitsResize reports whether a grow has to wait for this replica: a
// diskful, active replica that is not being deleted and whose satellite
// has reported an applied size for the volume. A replica with none is a
// satellite that has not finished its first pass, which builds the
// volume at the definition's size anyway, or one that predates the
// report.
func awaitsResize(rsc *apiv1.Resource, vn int32) (int64, bool) {
	if isDisklessReplica(rsc) ||
		slices.Contains(rsc.Flags, "INACTIVE") ||
		slices.Contains(rsc.Flags, apiv1.ResourceFlagDelete) {
		return 0, false
	}

	for i := range rsc.Volumes {
		if rsc.Volumes[i].VolumeNumber == vn && rsc.Volumes[i].AppliedSizeKib > 0 {
			return rsc.Volumes[i].AppliedSizeKib, true
		}
	}

	return 0, false
}

// classifyResizeReplicas reads where a grow of volume vn to targetKib
// stands on each replica of rd.
func (s *Server) classifyResizeReplicas(ctx context.Context, rd string, vn int32, targetKib int64) (resizeReplicas, error) {
	var out resizeReplicas

	resources, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return out, err //nolint:wrapcheck // the store error is the answer
	}

	for i := range resources {
		rsc := &resources[i]

		applied, ok := awaitsResize(rsc, vn)
		if !ok {
			continue
		}

		if !isNodeOnline(ctx, s.Store.Nodes(), rsc.NodeName) {
			out.offline = append(out.offline, rsc.NodeName)

			continue
		}

		if out.appliedKib == 0 || applied < out.appliedKib {
			out.appliedKib = applied
		}

		if applied < targetKib {
			out.growing = append(out.growing, rsc.NodeName)
		}
	}

	sort.Strings(out.growing)
	sort.Strings(out.offline)

	return out, nil
}

// waitForVolumeGrown blocks a resize request until every awaited
// replica has applied targetKib, the way LINSTOR answers
// `vd set-size` only after its satellites resized the backing volume
// and then DRBD (CtrlVlmDfnModifyApiCallHandler.updateSatellitesInScope,
// collected by ApiCallRcRestUtils.mapToMonoResponse). linstor-csi's
// ControllerExpandVolume relies on exactly that: it returns as soon as
// the PUT does, and kubelet runs resize2fs next.
//
// There is no deadline of its own, as in LINSTOR: the wait ends when the
// replicas have grown or when the caller gives up. It returns false in
// the second case, and the handler then writes nothing. The caller's
// retry finds the old size on a GET (see reportAppliedSizes) and asks
// again, so a resize slower than the CSI timeout is not acknowledged
// early either.
//
// The returned entries are LINSTOR's not-connected warnings for the
// diskful replicas the wait skipped.
func (s *Server) waitForVolumeGrown(ctx context.Context, rd string, vn int32, targetKib int64) ([]apiv1.APICallRc, bool) {
	logged := false

	for {
		state, err := s.classifyResizeReplicas(ctx, rd, vn, targetKib)
		if err == nil && len(state.growing) == 0 {
			return notConnectedWarnings(state.offline, rd, vn), true
		}

		if err == nil && !logged {
			slog.Default().Info("resize waits for replicas to grow",
				"rd", rd, "volume", vn, "sizeKib", targetKib, "nodes", state.growing)

			logged = true
		}

		select {
		case <-ctx.Done():
			slog.Default().Info("resize caller gave up before the replicas grew; the resize goes on",
				"rd", rd, "volume", vn, "sizeKib", targetKib)

			return nil, false
		case <-time.After(resizeWaitPollInterval):
		}
	}
}

// waitForVDPatchGrown is the wait a volume definition modify takes: a
// grow is answered once the replicas have grown, as LINSTOR does. The
// same size again is waited for too, since it is linstor-csi's retry of
// a grow whose first request it stopped waiting for. A modify without a
// size, or a forced shrink, does not wait.
func (s *Server) waitForVDPatchGrown(
	ctx context.Context, rd string, vn int32, sizeKib *int64, previousSizeKib int64,
) ([]apiv1.APICallRc, bool) {
	if sizeKib == nil || *sizeKib < previousSizeKib {
		return nil, true
	}

	return s.waitForVolumeGrown(ctx, rd, vn, *sizeKib)
}

// notConnectedWarnings builds one WARN_NOT_CONNECTED entry per node.
func notConnectedWarnings(nodes []string, rd string, vn int32) []apiv1.APICallRc {
	out := make([]apiv1.APICallRc, 0, len(nodes))

	for _, node := range nodes {
		out = append(out, apiv1.APICallRc{
			RetCode: warnNotConnected,
			Message: fmt.Sprintf("No active connection to satellite '%s'", node),
			Details: "The controller stored the change; the satellite receives it once it is connected.",
			ObjRefs: map[string]string{
				objRefNode:   node,
				objRefRscDfn: rd,
				objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
			},
		})
	}

	return out
}

// reportAppliedSizes lowers each volume definition's size to what its
// replicas have finished applying, while a grow is still in progress.
//
// LINSTOR shows the new size at once, with a RESIZE flag, and answers
// the PUT only when the resize is done. That leaves one gap linstor-csi
// falls into: when the PUT outlasts the external resizer's timeout, the
// retry reads the definition, finds the requested size, skips the PUT
// and reports success, and resize2fs runs on the old device. Reporting
// the applied size makes the retry PUT again, and that PUT waits.
// Steady state is unchanged: every replica has applied the definition
// size, so nothing is lowered.
func (s *Server) reportAppliedSizes(ctx context.Context, rd string, vds []apiv1.VolumeDefinition) {
	for i := range vds {
		vd := &vds[i]

		state, err := s.classifyResizeReplicas(ctx, rd, vd.VolumeNumber, vd.SizeKib)
		if err != nil {
			continue
		}

		if state.appliedKib > 0 && state.appliedKib < vd.SizeKib {
			vd.SizeKib = state.appliedKib
		}
	}
}
