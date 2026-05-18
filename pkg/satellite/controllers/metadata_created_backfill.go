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

package controllers

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// MetadataCreatedBackfillRunnable closes the Phase 11.3 Stage 1
// upgrade gap: clusters running a pre-11.3 satellite have on-disk
// `.md-created` markers but no `MetadataCreated` Status Condition
// stamped. On satellite startup this runnable walks every Resource
// CRD placed on this node, and for each one whose Condition is
// absent BUT whose kernel-side `drbdmeta dump-md` reports valid
// metadata (`Adm.HasMD` returns true — authoritative probe),
// stamps the Condition.
//
// Best-effort: failures are logged and skipped. Subsequent
// reconciles will re-attempt the stamp via `ensureMetadata`'s
// post-create-md path (idempotent SSA patch), so a transient
// apiserver hiccup here doesn't break the migration — it just
// defers the backfill to the first apply pass.
//
// Single-shot: runs once at startup, then exits. The c-r manager
// considers `Start(ctx)` returning nil a normal completion and
// keeps the rest of the controllers running.
type MetadataCreatedBackfillRunnable struct {
	// Client is the controller-runtime cached client. The runnable
	// waits for the cache to sync via mgr.GetCache() before
	// listing, so the List sees the full Resource set this
	// satellite is meant to own.
	Client client.Client

	// Adm is the drbdadm wrapper. Used for the authoritative
	// `HasMD` probe — drbdmeta dump-md on the lower disk reports
	// true iff valid DRBD-9 metadata is present.
	Adm *drbd.Adm

	// Stamper is the MetadataCreatedStamper used to SSA-patch the
	// Condition. Same instance the satellite reconciler uses for
	// post-create-md stamps, so the apiserver sees a single
	// field-owner across both paths.
	Stamper *MetadataCreatedStamper

	// NodeName is this satellite's own node identifier. The
	// runnable filters the Resource list to those placed on this
	// node via `Spec.NodeName == NodeName`.
	NodeName string
}

// Start implements manager.Runnable. Single-shot startup pass.
// Exits cleanly after one walk of the Resource list (or on ctx
// cancel). Failures are logged at Info level — the path is
// best-effort and a deferred backfill via `ensureMetadata` will
// catch any Resource the startup pass missed.
func (b *MetadataCreatedBackfillRunnable) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("metadata-created-backfill")

	if b.Adm == nil || b.Stamper == nil {
		logger.Info("skip backfill: Adm or Stamper not wired")

		return nil
	}

	var resList blockstoriov1alpha1.ResourceList

	err := b.Client.List(ctx, &resList)
	if err != nil {
		logger.Info("list Resources for backfill (best-effort)", "err", err.Error())

		return nil
	}

	stamped := 0

	for i := range resList.Items {
		if b.backfillOne(ctx, &resList.Items[i], logger.WithValues("resource", resList.Items[i].Name)) {
			stamped++
		}
	}

	if stamped > 0 {
		logger.Info("backfilled MetadataCreated Conditions",
			"count", stamped,
			"node", b.NodeName)
	}

	return nil
}

// NeedLeaderElection returns false: every satellite pod runs the
// backfill on its own node-local Resource set. There's no
// global-leader role to coordinate.
func (b *MetadataCreatedBackfillRunnable) NeedLeaderElection() bool {
	return false
}

// RegisterWithManager adds the runnable to the c-r manager. Mirrors
// the other satellite runnables' wiring style. The manager waits for
// the cache to sync before invoking Start, so the List inside Start
// sees the full Resource set.
func (b *MetadataCreatedBackfillRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(b)
	if err != nil {
		return errors.Wrap(err, "add MetadataCreatedBackfillRunnable")
	}

	return nil
}

// backfillOne handles the per-Resource branch of Start: filter to
// this node, skip if Condition already True, kernel-probe via
// HasMD, stamp on True. Returns true iff a Condition was stamped
// (caller bumps the counter). Pulled out so Start stays under the
// funlen budget.
func (b *MetadataCreatedBackfillRunnable) backfillOne(ctx context.Context, res *blockstoriov1alpha1.Resource, logger logr.Logger) bool {
	if res.Spec.NodeName != b.NodeName {
		return false
	}

	if meta.IsStatusConditionTrue(res.Status.Conditions, blockstoriov1alpha1.ConditionMetadataCreated) {
		return false
	}

	// Authoritative kernel-side probe. The Resource may be
	// pre-create-md (newly-placed diskless) or post-tear-down;
	// in both cases HasMD returns false and we skip — no
	// metadata to advertise.
	hasMD, probeErr := b.Adm.HasMD(ctx, res.Name)
	if probeErr != nil {
		logger.Info("dump-md probe failed (best-effort)", "err", probeErr.Error())

		return false
	}

	if !hasMD {
		return false
	}

	stampErr := b.Stamper.StampMetadataCreated(ctx, res.Name)
	if stampErr != nil {
		logger.Info("stamp MetadataCreated (best-effort)", "err", stampErr.Error())

		return false
	}

	return true
}
