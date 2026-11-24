// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
)

// ownedRetry paces the cold-start read of the owned list while the API
// server cannot be reached.
type ownedRetry struct {
	// attempt bounds one read, so a dial that hangs is retried rather
	// than waited out.
	attempt time.Duration
	// first is the pause after the first failed read; it doubles up to
	// longest.
	first, longest time.Duration
}

// defaultOwnedRetry suits a node that has just booted: the service network
// usually comes up within a minute of the satellite.
func defaultOwnedRetry() ownedRetry {
	const (
		attempt = 10 * time.Second
		longest = 15 * time.Second
	)

	return ownedRetry{attempt: attempt, first: time.Second, longest: longest}
}

// ownedKernelResources names the DRBD resources blockstor has a replica of on
// nodeName: the definitions of the Resource objects scheduled there, read from
// the API server rather than a cache, since the cache is not up yet. It keeps
// trying until the API server answers or ctx ends.
func ownedKernelResources(ctx context.Context, logger *slog.Logger, cfg *rest.Config, nodeName string) (map[string]struct{}, error) {
	scheme := runtime.NewScheme()

	err := blockstoriov1alpha1.AddToScheme(scheme)
	if err != nil {
		return nil, errors.Wrap(err, "register the blockstor types")
	}

	c, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, errors.Wrap(err, "build an API client")
	}

	return waitOwnedOnNode(ctx, logger, c, nodeName, defaultOwnedRetry())
}

// waitOwnedOnNode reads the owned list until a read succeeds, the API server
// refuses it for good, or ctx ends.
//
// A satellite that starts with the node often runs before the node can reach
// the API server's service address. Giving up on the first failure left the
// kernel's orphans in place without a word above INFO, and on a restart of
// the container alone, with the kernel still full, that is every orphan the
// previous run left. Nothing the satellite does next works without the API
// server either, so waiting for it costs nothing.
func waitOwnedOnNode(
	ctx context.Context, logger *slog.Logger, c ctrlclient.Client, nodeName string, retry ownedRetry,
) (map[string]struct{}, error) {
	pause := retry.first

	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, retry.attempt)
		owned, err := ownedOnNode(attemptCtx, c, nodeName)

		cancel()

		if err == nil {
			if attempt > 1 {
				logger.Info("cold-start: read which DRBD resources are blockstor's", "attempts", attempt)
			}

			return owned, nil
		}

		if permanentAPIError(err) {
			return nil, errors.Wrap(err, "read the owned list")
		}

		logger.Warn("cold-start: cannot read which DRBD resources are blockstor's yet; retrying",
			"attempt", attempt, "retry_in", pause.String(), "err", err.Error())

		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "wait for the API server")
		case <-time.After(pause):
		}

		pause = min(2*pause, retry.longest)
	}
}

// permanentAPIError reports an answer from the API server that waiting will
// not change: the satellite is not allowed to list Resources, which an RBAC
// rule behind an upgrade causes, or the Resource kind is not installed. The
// satellite then starts without the owned list, as it did before it waited,
// rather than never starting.
func permanentAPIError(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsNotFound(err) || meta.IsNoMatchError(err)
}

// ownedOnNode is ownedKernelResources over a given client.
func ownedOnNode(ctx context.Context, c ctrlclient.Client, nodeName string) (map[string]struct{}, error) {
	replicas, err := k8sstore.New(c).Resources().ListByNode(ctx, nodeName)
	if err != nil {
		return nil, errors.Wrapf(err, "list the replicas on %q", nodeName)
	}

	owned := make(map[string]struct{}, len(replicas))
	for i := range replicas {
		owned[naming.Key(replicas[i].Name)] = struct{}{}
	}

	return owned, nil
}

// selectOrphans splits the kernel's resources that are not healthy into the
// ones blockstor takes down, which it has a replica of here, and foreign ones
// it leaves alone: another DRBD user's, or a resource whose Resource object is
// already gone, which is not blockstor's to judge.
func selectOrphans(names []string, healthy, owned map[string]struct{}) ([]string, []string) {
	var orphans, foreign []string

	for _, name := range names {
		if _, ok := healthy[name]; ok {
			continue
		}

		if _, ok := owned[naming.Key(name)]; !ok {
			foreign = append(foreign, name)

			continue
		}

		orphans = append(orphans, name)
	}

	return orphans, foreign
}
