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

package satellite

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/log"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// applyResultKey carries, in the context of one resource's apply, the
// result that apply returns.
type applyResultKey struct{}

func withApplyResult(ctx context.Context, res *intent.ResourceApplyResult) context.Context {
	return context.WithValue(ctx, applyResultKey{}, res)
}

// reportReconnectedPeers records that the adjust of resource just set
// up again the StandAlone connections to peers, which the kernel had
// dropped over a peer configuration the resource no longer has. The
// replica looked healthy from its own side all along, so the apply
// result carries the peers for a Warning Event on the Resource, and the
// log says it too.
func reportReconnectedPeers(ctx context.Context, resource string, peers []string) {
	log.FromContext(ctx).Info("reconnected StandAlone peers whose kernel configuration was stale",
		"resource", resource, "peers", peers)

	res, ok := ctx.Value(applyResultKey{}).(*intent.ResourceApplyResult)
	if !ok || res == nil {
		return
	}

	res.ReconnectedPeers = append(res.ReconnectedPeers, peers...)
}
