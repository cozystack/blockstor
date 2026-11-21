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

	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// forgetOfflineReplicas answers the replicas of an OFFLINE node with no
// satellite state, the way LINSTOR does (apiv1.ForgetSatelliteState).
// Without it a dead node's replicas kept their last report, UpToDate
// and connected, so neither `r l` nor `r l --faulty` showed them while
// the node was down.
//
// A node list that fails leaves the replicas as stored: the read still
// answers, with the state last reported, rather than failing outright.
func (s *Server) forgetOfflineReplicas(ctx context.Context, resources []apiv1.Resource) {
	if len(resources) == 0 {
		return
	}

	nodes, err := s.Store.Nodes().List(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "list nodes to find offline satellites; answering the stored replica state")

		return
	}

	apiv1.ForgetStateOnOfflineNodes(resources, nodes)
}

// answeredReplica is forgetOfflineReplicas for a read that answers a
// single replica.
func (s *Server) answeredReplica(ctx context.Context, res apiv1.Resource) apiv1.Resource {
	answered := []apiv1.Resource{res}
	s.forgetOfflineReplicas(ctx, answered)

	return answered[0]
}
