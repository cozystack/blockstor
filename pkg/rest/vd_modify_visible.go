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
	"maps"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// patchVolumeDefinition merges patch into the volume definition (rd, vn)
// and then holds the answer, within cacheConvergeBudget, until this
// replica's cache shows what the patch wrote: the props it set, deleted
// or cleared by namespace, and the size when it set one.
//
// The write goes to the API server and reads come from the informer
// cache, so a GET right after the modify could return the props from
// before it. LINSTOR answers a modify once its own state has the change,
// and the next read returns it; linstor-csi and the CLI read back after
// a modify. As with the delete waits (cache_invalidation.go), the writer
// pays the wait instead of every reader paying an uncached read, and a
// budget that runs out still answers the write, which did commit.
func (s *Server) patchVolumeDefinition(ctx context.Context, rd string, vn int32, patch *volumeDefinitionModifyBody) error {
	var written apiv1.VolumeDefinition

	err := s.Store.VolumeDefinitions().PatchVolumeDefinitionSpec(ctx, rd, vn, func(vd *apiv1.VolumeDefinition) error {
		mergeVolumeDefinitionPatch(vd, patch)

		// Path-derived VolumeNumber wins: never trust the body's
		// vol_num.
		vd.VolumeNumber = vn

		written = *vd
		written.Props = maps.Clone(vd.Props)

		return nil
	})
	if err != nil {
		return err //nolint:wrapcheck // the handler answers the store error as it is
	}

	s.waitForVDPatchVisible(ctx, rd, vn, patch, &written)

	return nil
}

// waitForVDPatchVisible polls the cached volume definition until
// vdPatchVisible holds, the budget runs out, the read fails or ctx ends.
func (s *Server) waitForVDPatchVisible(
	ctx context.Context, rd string, vn int32, patch *volumeDefinitionModifyBody, written *apiv1.VolumeDefinition,
) {
	deadline := time.Now().Add(cacheConvergeBudget)

	for {
		cached, err := s.Store.VolumeDefinitions().Get(ctx, rd, vn)
		if err != nil || vdPatchVisible(&cached, patch, written) {
			return
		}

		if time.Now().After(deadline) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(cacheConvergePollInterval):
		}
	}
}

// vdPatchVisible reports whether cached agrees with written on what patch
// touched. Props the patch did not name are not compared, so a concurrent
// modify of other props does not hold the answer.
func vdPatchVisible(cached *apiv1.VolumeDefinition, patch *volumeDefinitionModifyBody, written *apiv1.VolumeDefinition) bool {
	if patch.SizeKib != nil && cached.SizeKib != written.SizeKib {
		return false
	}

	same := func(key string) bool {
		got, inCache := cached.Props[key]
		want, inWrite := written.Props[key]

		return inCache == inWrite && got == want
	}

	for _, keys := range []map[string]string{patch.OverrideProps, patch.Props} {
		for key := range keys {
			if !same(key) {
				return false
			}
		}
	}

	for _, key := range patch.DeleteProps {
		if !same(key) {
			return false
		}
	}

	for _, ns := range patch.DeleteNamespaces {
		for key := range cached.Props {
			if store.NamespaceCovers(ns, key) && !same(key) {
				return false
			}
		}
	}

	return true
}
