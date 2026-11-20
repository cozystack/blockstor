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
	"os"
	"slices"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/naming"
)

// defaultAdoptionPeerWait bounds how long the first render of an
// adopted resource waits for the Resources of the peers its kernel
// slot is connected to. A bulk apply delivers every Resource of a
// definition within seconds; the bound only matters when a peer's
// Resource never comes, and then the resource must not stay unapplied
// for good.
const defaultAdoptionPeerWait = 5 * time.Minute

// adoptionPeerWaitEnv overrides defaultAdoptionPeerWait (seconds,
// integer). Invalid, zero or negative values fall back to the default.
const adoptionPeerWaitEnv = "BSTOR_ADOPTION_PEER_WAIT_S"

// AdoptionHold is the answer to "may the first render of this adopted
// resource go ahead?". Missing lists the kernel peers that have no
// observed Resource; an empty list means nothing holds the render.
type AdoptionHold struct {
	// Missing are the kernel peer names with no observed Resource,
	// sorted.
	Missing []string

	// Since is when this satellite first saw the render held.
	Since time.Time

	// Expired reports that the hold has lasted defaultAdoptionPeerWait
	// (or its override) and the render should go ahead without the
	// missing peers.
	Expired bool
}

// HoldsRender reports whether the caller must not render yet.
func (h AdoptionHold) HoldsRender() bool {
	return len(h.Missing) > 0 && !h.Expired
}

// AdoptedPeersWithoutResource checks a kernel slot blockstor has not
// rendered yet (no `.owned` marker) for peers whose Resource has not
// been observed. The first render writes a .res from the observed
// Resources only, and `drbdadm adjust` then deletes every kernel
// connection the .res does not name: a peer whose Resource arrives a
// moment later would be disconnected and resynced. LINSTOR never
// renders from a partial view, because its satellite receives the
// whole resource definition at once, so the render waits instead.
//
// peerNames are the observed peer node names, without the local node.
// An owned slot, an unloaded slot and an unreadable status all return
// an empty hold: there is no adopted connection to protect, or no way
// to see one, and the render proceeds as before.
func (r *Reconciler) AdoptedPeersWithoutResource(
	ctx context.Context,
	rdName string,
	peerNames []string,
	now time.Time,
) AdoptionHold {
	if r.cfg.Adm == nil || r.ownsKernelSlot(rdName) {
		r.clearAdoptionHold(rdName)

		return AdoptionHold{}
	}

	slots, err := r.cfg.Adm.Show(ctx, rdName)
	if err != nil {
		log.FromContext(ctx).Info("adoption hold: kernel status unreadable, not holding the render",
			"rd", rdName, "err", err.Error())

		return AdoptionHold{}
	}

	observed := make(map[string]struct{}, len(peerNames))
	for _, n := range peerNames {
		observed[naming.Key(n)] = struct{}{}
	}

	var missing []string

	for name := range slots {
		if _, ok := observed[naming.Key(name)]; !ok {
			missing = append(missing, name)
		}
	}

	if len(missing) == 0 {
		r.clearAdoptionHold(rdName)

		return AdoptionHold{}
	}

	slices.Sort(missing)

	since := r.observeAdoptionHold(rdName, now)

	return AdoptionHold{
		Missing: missing,
		Since:   since,
		Expired: now.Sub(since) >= adoptionPeerWait(),
	}
}

// observeAdoptionHold returns when the render of rdName was first held,
// recording now on the first call.
func (r *Reconciler) observeAdoptionHold(rdName string, now time.Time) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	since, ok := r.adoptionHeldSince[rdName]
	if !ok {
		r.adoptionHeldSince[rdName] = now

		return now
	}

	return since
}

// clearAdoptionHold forgets the hold on rdName, so a later one starts a
// fresh wait.
func (r *Reconciler) clearAdoptionHold(rdName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.adoptionHeldSince, rdName)
}

// adoptionPeerWait reads BSTOR_ADOPTION_PEER_WAIT_S and falls back to
// defaultAdoptionPeerWait on a missing or unusable value.
func adoptionPeerWait() time.Duration {
	n, err := strconv.Atoi(os.Getenv(adoptionPeerWaitEnv))
	if err != nil || n <= 0 {
		return defaultAdoptionPeerWait
	}

	return time.Duration(n) * time.Second
}
