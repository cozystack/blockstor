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
	"time"

	"github.com/cozystack/blockstor/pkg/store"
)

// rdReplicaDrainBudget bounds how long a definition delete waits for the
// replicas on reachable nodes to finish tearing down. It sits well inside
// the CSI sidecars' call timeout, which linstor-csi's DeleteVolume, and the
// rollback inside a failed CreateVolume, both run under.
//
//nolint:gochecknoglobals // tunable
var rdReplicaDrainBudget = 20 * time.Second

// rdReplicaDrainPoll is the cadence of the drain wait's reads.
//
//nolint:gochecknoglobals // tunable
var rdReplicaDrainPoll = 250 * time.Millisecond

// waitForOnlineReplicasDrained holds a definition delete until no replica of
// it is left on a node that is reachable, or rdReplicaDrainBudget runs out.
//
// LINSTOR answers `rd d` once the online satellites confirm their replicas
// gone, and keeps the definition, flagged DELETE, only for an offline node.
// Here the definition object goes at once, and a create of the same name is
// refused while any replica of the old one is still draining
// (store.RefuseOverDrainingReplicas). Without this wait a delete followed by a
// create of the same name, with every node up, failed whenever the teardown
// took longer than the reply. Replicas on an offline node are not waited for:
// they drain when the node is back, and the create is refused until then, as
// LINSTOR refuses it.
//
// Read past the cache: a cache trailing the teardown would hold the reply for
// a replica already gone. The budget running out, or a read failing, answers
// the delete anyway: it has been committed, and the create that follows says
// which nodes are still draining.
func (s *Server) waitForOnlineReplicasDrained(ctx context.Context, name string) {
	if s == nil || s.Store == nil {
		return
	}

	deadline := time.Now().Add(rdReplicaDrainBudget)

	for {
		if !s.replicasLeftOnOnlineNodes(ctx, name) || time.Now().After(deadline) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(rdReplicaDrainPoll):
		}
	}
}

// replicasLeftOnOnlineNodes reports whether a replica under name is still
// there on a node that is not offline.
func (s *Server) replicasLeftOnOnlineNodes(ctx context.Context, name string) bool {
	replicas, err := store.LiveReplicas(ctx, s.Store, name)
	if err != nil {
		return false
	}

	for i := range replicas {
		if isNodeOnline(ctx, s.Store.Nodes(), replicas[i].NodeName) {
			return true
		}
	}

	return false
}
