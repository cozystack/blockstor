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

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// defaultStorPoolName is LINSTOR's InternalApiConsts.DEFAULT_STOR_POOL_NAME,
// the last pool resolveNeighbourDiskfulStorPool offers.
const defaultStorPoolName = "DfltStorPool"

// viewPoolResolver names the pool a placeholder volume lists for a
// diskful replica whose spec carries none. LINSTOR cannot hold such a
// replica, because it creates every Volume with a resolved pool, so
// the name is the one LINSTOR v1.33.1 would have resolved
// (CtrlStorPoolResolveHelper.resolveStorPool):
//
//  1. StorPoolName from the replica, then the volume definition, the
//     resource definition, the resource group and the node, first set
//     wins and is the only candidate;
//  2. otherwise the pools of the diskful replicas on other nodes,
//     then DfltStorPool;
//
// and the first candidate that exists on the replica's own node is
// taken. A name that does not exist there is not listed: the CLI's
// `n describe` crashes on it exactly as on an empty one, and `r lv`
// would show a pool the replica cannot have.
//
// The view only reads, so every lookup is best effort: a failure is
// logged and leaves the pool empty rather than failing the whole
// /v1/view/resources answer over one definition.
type viewPoolResolver struct {
	s   *Server
	rsc *apiv1.Resource

	loaded    bool
	vdProps   map[int32]map[string]string
	chain     []map[string]string // RD, RG, node
	neighbour []string
}

func (v *viewPoolResolver) poolFor(ctx context.Context, vn int32) string {
	if !v.loaded {
		v.load(ctx)
		v.loaded = true
	}

	candidates := v.neighbour

	if name := firstStorPoolName(v.rsc.Props, v.vdProps[vn], v.chain); name != "" {
		candidates = []string{name}
	}

	for _, name := range candidates {
		sp, err := v.s.Store.StoragePools().Get(ctx, v.rsc.NodeName, name)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				v.logSkip(ctx, err, "look up a candidate pool on the replica's node")
			}

			continue
		}

		if sp.ProviderKind != apiv1.StoragePoolKindDiskless {
			return sp.StoragePoolName
		}
	}

	return ""
}

func firstStorPoolName(rscProps, vdProps map[string]string, chain []map[string]string) string {
	if name := rscProps[storPoolPropKey]; name != "" {
		return name
	}

	if name := vdProps[storPoolPropKey]; name != "" {
		return name
	}

	for _, props := range chain {
		if name := props[storPoolPropKey]; name != "" {
			return name
		}
	}

	return ""
}

func (v *viewPoolResolver) load(ctx context.Context) {
	st := v.s.Store

	vds, err := st.VolumeDefinitions().List(ctx, v.rsc.Name)
	if err != nil {
		v.logSkip(ctx, err, "list volume definitions")
	}

	v.vdProps = make(map[int32]map[string]string, len(vds))
	for i := range vds {
		v.vdProps[vds[i].VolumeNumber] = vds[i].Props
	}

	rd, err := st.ResourceDefinitions().Get(ctx, v.rsc.Name)

	switch {
	case err == nil:
		v.chain = append(v.chain, rd.Props)

		if rd.ResourceGroupName != "" {
			rg, rgErr := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
			if rgErr == nil {
				v.chain = append(v.chain, rg.Props)
			} else if !errors.Is(rgErr, store.ErrNotFound) {
				v.logSkip(ctx, rgErr, "get resource group")
			}
		}
	case !errors.Is(err, store.ErrNotFound):
		v.logSkip(ctx, err, "get resource definition")
	}

	node, err := st.Nodes().Get(ctx, v.rsc.NodeName)
	if err == nil {
		v.chain = append(v.chain, node.Props)
	} else if !errors.Is(err, store.ErrNotFound) {
		v.logSkip(ctx, err, "get node")
	}

	v.neighbour = append(v.neighbourPools(ctx), defaultStorPoolName)
}

// neighbourPools mirrors resolveNeighbourDiskfulStorPool: the pools
// of every diskful replica on another node. LINSTOR keeps only pools
// with a backing device; poolFor drops a diskless kind on the
// replica's own node, which is where the name has to resolve.
func (v *viewPoolResolver) neighbourPools(ctx context.Context) []string {
	siblings, err := v.s.Store.Resources().ListByDefinition(ctx, v.rsc.Name)
	if err != nil {
		v.logSkip(ctx, err, "list sibling replicas")

		return nil
	}

	var out []string

	for i := range siblings {
		peer := &siblings[i]
		if naming.Same(peer.NodeName, v.rsc.NodeName) || isDisklessReplica(peer) {
			continue
		}

		names := []string{peer.Props[storPoolPropKey]}
		for j := range peer.Volumes {
			names = append(names, peer.Volumes[j].StoragePool)
		}

		for _, name := range names {
			if name != "" {
				out = append(out, name)
			}
		}
	}

	return out
}

func (v *viewPoolResolver) logSkip(ctx context.Context, err error, what string) {
	log.FromContext(ctx).WithName("rest").Info("view lists a placeholder volume without a pool: could not "+what,
		"resource", v.rsc.Name, "node", v.rsc.NodeName, "error", err.Error())
}
