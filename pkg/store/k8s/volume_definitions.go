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

package k8s

import (
	"context"
	"sort"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// volumeDefinitions stores VolumeDefinition objects inline on the parent
// ResourceDefinition CRD's spec.volumeDefinitions array. There is no
// separate VolumeDefinition CRD: upstream LINSTOR addresses VDs through the
// RD anyway, and a single CRD makes ownership/reclamation trivially correct.
type volumeDefinitions struct {
	c ctrlclient.Client

	// apiReader is a direct, UNCACHED reader (mgr.GetAPIReader()) used
	// only by CreateAutoNumbered's retry loop. The cached client's Get
	// trails a just-committed write by the informer round-trip, so
	// retrying an optimistic-lock conflict against the cache re-reads
	// the stale RD, re-derives the SAME smallest-free VolumeNumber, and
	// the loop never converges — the BUG-048 lost-update where the
	// second of two concurrent `vd c` is dropped. Reading live on each
	// attempt makes the allocation see the winner's committed write.
	// nil in non-production stores (in-memory / unit) → fall back to the
	// cached client.
	apiReader ctrlclient.Reader
}

func (s *volumeDefinitions) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	rd, err := s.fetchRD(ctx, rdName)
	if err != nil {
		return nil, err
	}

	out := make([]apiv1.VolumeDefinition, 0, len(rd.Spec.VolumeDefinitions))
	for i := range rd.Spec.VolumeDefinitions {
		out = append(out, crdToWireVD(&rd.Spec.VolumeDefinitions[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VolumeNumber < out[j].VolumeNumber })

	return out, nil
}

func (s *volumeDefinitions) Get(ctx context.Context, rdName string, volumeNumber int32) (apiv1.VolumeDefinition, error) {
	rd, err := s.fetchRD(ctx, rdName)
	if err != nil {
		return apiv1.VolumeDefinition{}, err
	}

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == volumeNumber {
			return crdToWireVD(&rd.Spec.VolumeDefinitions[i]), nil
		}
	}

	return apiv1.VolumeDefinition{}, errors.Wrapf(store.ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
}

func (s *volumeDefinitions) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if vd == nil {
		return errors.New("nil VolumeDefinition")
	}

	// VolumeDefinitions live inline on the parent RD's spec, which
	// the RD reconciler also writes (annotations, layer-stack
	// defaulting, derived flags). A bare Get-modify-Update races
	// those writes — we observed "the object has been modified"
	// 409s on `linstor rg spawn-resources` right after RD create.
	// We also retry on NotFound: the informer cache may not have
	// observed the just-created RD yet on a write that arrives
	// milliseconds after the POST /v1/resource-definitions response.
	return errors.Wrapf(retry.OnError(retry.DefaultRetry, isConflictOrNotFound, func() error {
		rd, err := s.fetchRD(ctx, rdName)
		if err != nil {
			return err
		}

		for i := range rd.Spec.VolumeDefinitions {
			if rd.Spec.VolumeDefinitions[i].VolumeNumber == vd.VolumeNumber {
				return errors.Wrapf(store.ErrAlreadyExists, "volume %d on resource definition %q", vd.VolumeNumber, rdName)
			}
		}

		rd.Spec.VolumeDefinitions = append(rd.Spec.VolumeDefinitions, wireToCRDVD(vd))

		return s.c.Update(ctx, rd)
	}), "update RD %q to add volume %d", rdName, vd.VolumeNumber)
}

// CreateAutoNumbered allocates the smallest free non-negative
// VolumeNumber INSIDE the conflict-retry loop and persists `vd` under
// it, returning the assigned number. BUG-048 (P1, availability): the
// REST handler used to pick the number in a separate "List the RD →
// smallest hole" step BEFORE this Create, so two concurrent
// `linstor vd c <rd>` calls both read `[vol-0]`, both decided VlmNr=1,
// and the loser was rejected with FAIL_EXISTS_VLM_DFN — the operator's
// second intended volume silently vanished (only one VD landed, no
// usable error: the message claimed vol-1 "already exists" though the
// operator never named a number). Re-deriving the hole on every retry
// attempt makes the read-of-existing-set and the append atomic with
// respect to a racing CreateAutoNumbered: the loser's retry re-fetches
// the RD now carrying `[vol-0, vol-1]` and lands at vol-2.
//
// On the k8s backend the optimistic-concurrency guarantee comes from
// the apiserver's resourceVersion check on Update: if a racing write
// landed between our fetch and Update, the Update 409s, isConflictOr
// NotFound retries, and the whole allocate+append re-runs against the
// fresh RD. (Mirrors the existing explicit-number Create's retry; the
// only difference is the number is computed here rather than supplied.)
func (s *volumeDefinitions) CreateAutoNumbered(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) (int32, error) {
	if vd == nil {
		return 0, errors.New("nil VolumeDefinition")
	}

	var assigned int32

	// Generous retry budget: each attempt does a LIVE (uncached) RD read
	// so a conflict-retry sees the racing winner's committed VD and lands
	// at the next free number. retry.DefaultRetry (≈5 steps) is enough
	// for the common 2-way race, but widen it so a burst of concurrent
	// `vd c` against one RD still converges rather than dropping the
	// straggler.
	backoff := retry.DefaultRetry
	backoff.Steps = 12

	err := retry.OnError(backoff, isConflictOrNotFound, func() error {
		rd, fetchErr := s.fetchRDLive(ctx, rdName)
		if fetchErr != nil {
			return fetchErr
		}

		assigned = smallestFreeVolumeNumber(rd.Spec.VolumeDefinitions)

		entry := wireToCRDVD(vd)
		entry.VolumeNumber = assigned

		rd.Spec.VolumeDefinitions = append(rd.Spec.VolumeDefinitions, entry)

		updErr := s.c.Update(ctx, rd)
		if updErr != nil {
			return updErr //nolint:wrapcheck // outer errors.Wrapf adds context; the bare error preserves the apierrors type isConflictOrNotFound matches on
		}

		// Post-commit verification (BUG-048 defence-in-depth). The
		// optimistic-lock Update normally guarantees no racing writer
		// clobbered us — a stale resourceVersion 409s and we retry. But
		// under heavy apiserver/etcd load a follower read can hand back a
		// resourceVersion that is already superseded yet still accepted on
		// Update, so two concurrent creates could both "succeed" while
		// only one VolumeDefinition lands (the silent lost-update the
		// operator hit). Re-read live and confirm our assigned number is
		// actually present; if it vanished (a racer's clobber won), force
		// a retry by surfacing a synthetic Conflict so the whole
		// allocate+append re-runs against the now-correct state. This
		// makes the silent drop impossible: the create either persists or
		// retries — it never returns success having lost the volume.
		//
		// Crucially this only holds when the verifying read is LIVE. With
		// no uncached reader wired (apiReader == nil) the re-read falls
		// back to the informer cache, which trails the Update we just
		// committed — so it routinely fails to observe our own freshly
		// written volume and surfaces a FALSE synthetic Conflict. The
		// retry then re-derives the next free number off the same lagging
		// cache and appends a SECOND volume, so a single auto-numbered
		// create can leave several phantom VolumeDefinitions behind
		// (BUG-048 de-regress). A successful optimistic-locked Update is
		// already the apiserver's authoritative confirmation; without a
		// live reader there is nothing trustworthy to verify against, so
		// skip the check rather than second-guess a committed write
		// against a stale cache. Production wires mgr.GetAPIReader(), so
		// the live verification path is preserved where it actually
		// guards the lost-update.
		if s.apiReader == nil {
			return nil
		}

		return s.verifyVolumeLanded(ctx, rdName, assigned)
	})
	if err != nil {
		return 0, errors.Wrapf(err, "auto-numbered create on RD %q", rdName)
	}

	vd.VolumeNumber = assigned

	return assigned, nil
}

// smallestFreeVolumeNumber returns the lowest non-negative VolumeNumber
// not present in vds. Mirrors upstream LINSTOR's smallest-hole rule
// (VDs 0 and 2 present → 1, not 3).
func smallestFreeVolumeNumber(vds []crdv1alpha1.ResourceDefinitionVolume) int32 {
	used := make(map[int32]bool, len(vds))
	for i := range vds {
		used[vds[i].VolumeNumber] = true
	}

	for candidate := int32(0); candidate >= 0; candidate++ {
		if !used[candidate] {
			return candidate
		}
	}

	return 0
}

func (s *volumeDefinitions) Update(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if vd == nil {
		return errors.New("nil VolumeDefinition")
	}

	return errors.Wrapf(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rd, err := s.fetchRD(ctx, rdName)
		if err != nil {
			return err
		}

		idx := -1

		for i := range rd.Spec.VolumeDefinitions {
			if rd.Spec.VolumeDefinitions[i].VolumeNumber == vd.VolumeNumber {
				idx = i

				break
			}
		}

		if idx == -1 {
			return errors.Wrapf(store.ErrNotFound, "volume %d on resource definition %q", vd.VolumeNumber, rdName)
		}

		rd.Spec.VolumeDefinitions[idx] = wireToCRDVD(vd)

		return s.c.Update(ctx, rd)
	}), "update RD %q for volume %d", rdName, vd.VolumeNumber)
}

// PatchVolumeDefinitionSpec runs `mutate` against the freshly-fetched
// VolumeDefinition (resolved out of the parent RD's
// spec.volumeDefinitions[] by volumeNumber) and persists the mutated
// value via a typed-Patch (JSON-merge-patch) on the parent RD under
// `RetryOnConflict` with the Bug 201 backoff. On 409 the entire fetch+
// mutate+patch cycle re-runs against the live RD — so concurrent
// disjoint VD prop edits (vd set-property under linstor-csi load,
// satellite-side reconciler bumps) converge instead of being lost to
// the wholesale `Update`'s stale-wire-snapshot replay (Bug 204b).
func (s *volumeDefinitions) PatchVolumeDefinitionSpec(ctx context.Context, rdName string, volumeNumber int32, mutate func(*apiv1.VolumeDefinition) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrapf(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		rd, err := s.fetchRD(ctx, rdName)
		if err != nil {
			return err
		}

		idx := -1

		for i := range rd.Spec.VolumeDefinitions {
			if rd.Spec.VolumeDefinitions[i].VolumeNumber == volumeNumber {
				idx = i

				break
			}
		}

		if idx == -1 {
			return errors.Wrapf(store.ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
		}

		base := rd.DeepCopy()

		wire := crdToWireVD(&rd.Spec.VolumeDefinitions[idx])

		err = mutate(&wire)
		if err != nil {
			return err
		}

		// Re-derive the inline CRD entry from the mutated wire value
		// and write it back into the parent RD's slice.
		rd.Spec.VolumeDefinitions[idx] = wireToCRDVD(&wire)

		return s.c.Patch(ctx, rd, ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}))
	}), "patch RD %q for volume %d", rdName, volumeNumber)
}

func (s *volumeDefinitions) Delete(ctx context.Context, rdName string, volumeNumber int32) error {
	return errors.Wrapf(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rd, err := s.fetchRD(ctx, rdName)
		if err != nil {
			return err
		}

		idx := -1

		for i := range rd.Spec.VolumeDefinitions {
			if rd.Spec.VolumeDefinitions[i].VolumeNumber == volumeNumber {
				idx = i

				break
			}
		}

		if idx == -1 {
			return errors.Wrapf(store.ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
		}

		rd.Spec.VolumeDefinitions = append(rd.Spec.VolumeDefinitions[:idx], rd.Spec.VolumeDefinitions[idx+1:]...)

		return s.c.Update(ctx, rd)
	}), "update RD %q to remove volume %d", rdName, volumeNumber)
}

// isConflictOrNotFound flags errors that justify a retry on the
// VolumeDefinitions Create path. Conflict = RD spec mutated between
// Get and Update (RD reconciler races our write). NotFound = the
// informer cache hasn't yet observed the just-created RD; the
// caller's POST handler returns before the watch event lands, so a
// follow-up VD Create on the same connection sees the stale cache.
func isConflictOrNotFound(err error) bool {
	if err == nil {
		return false
	}

	return apierrors.IsConflict(err) ||
		apierrors.IsNotFound(err) ||
		errors.Is(err, store.ErrNotFound)
}

func (s *volumeDefinitions) fetchRD(ctx context.Context, rdName string) (*crdv1alpha1.ResourceDefinition, error) {
	var rd crdv1alpha1.ResourceDefinition

	err := s.c.Get(ctx, types.NamespacedName{Name: Name(rdName)}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(store.ErrNotFound, "resource definition %q", rdName)
		}

		return nil, errors.Wrapf(err, "get ResourceDefinition %q", rdName)
	}

	return &rd, nil
}

// fetchRDLive reads the RD through the direct, UNCACHED API reader when
// one is wired (production), so CreateAutoNumbered's retry sees the
// latest committed VolumeDefinitions rather than a stale informer-cache
// revision (BUG-048). Falls back to the cached client when no apiReader
// is configured (in-memory / unit harnesses). The returned object is
// still safe to mutate + write back through s.c (the resourceVersion the
// live read carries is what the subsequent optimistic-locked Update
// checks against).
func (s *volumeDefinitions) fetchRDLive(ctx context.Context, rdName string) (*crdv1alpha1.ResourceDefinition, error) {
	if s.apiReader == nil {
		return s.fetchRD(ctx, rdName)
	}

	var rd crdv1alpha1.ResourceDefinition

	err := s.apiReader.Get(ctx, types.NamespacedName{Name: Name(rdName)}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(store.ErrNotFound, "resource definition %q", rdName)
		}

		return nil, errors.Wrapf(err, "get ResourceDefinition %q (live)", rdName)
	}

	return &rd, nil
}

// verifyVolumeLanded re-reads the RD live and returns a synthetic
// Conflict error (so the CreateAutoNumbered retry loop re-runs) when the
// just-written VolumeNumber is NOT present — the BUG-048 stale-read
// clobber. Returns nil when the volume is durably present. A NotFound
// (RD vanished) is surfaced as-is so the retry loop's isConflictOrNotFound
// also catches it.
func (s *volumeDefinitions) verifyVolumeLanded(ctx context.Context, rdName string, assigned int32) error {
	rd, err := s.fetchRDLive(ctx, rdName)
	if err != nil {
		return err
	}

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == assigned {
			return nil
		}
	}

	return apierrors.NewConflict(
		schema.GroupResource{Group: crdv1alpha1.GroupVersion.Group, Resource: "resourcedefinitions"},
		Name(rdName),
		errors.Newf("volume %d did not persist (concurrent clobber); retrying allocation", assigned),
	)
}

func crdToWireVD(vd *crdv1alpha1.ResourceDefinitionVolume) apiv1.VolumeDefinition {
	return apiv1.VolumeDefinition{
		VolumeNumber: vd.VolumeNumber,
		SizeKib:      vd.SizeKib,
		Props:        vd.Props,
		Flags:        vd.Flags,
	}
}

func wireToCRDVD(vd *apiv1.VolumeDefinition) crdv1alpha1.ResourceDefinitionVolume {
	return crdv1alpha1.ResourceDefinitionVolume{
		VolumeNumber: vd.VolumeNumber,
		SizeKib:      vd.SizeKib,
		Props:        vd.Props,
		Flags:        vd.Flags,
	}
}
