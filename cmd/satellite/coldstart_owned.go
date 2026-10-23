// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
)

// ownedKernelResources names the DRBD resources blockstor has a replica of on
// nodeName: the definitions of the Resource objects scheduled there, read from
// the API server rather than a cache, since the cache is not up yet.
func ownedKernelResources(ctx context.Context, cfg *rest.Config, nodeName string) (map[string]struct{}, error) {
	scheme := runtime.NewScheme()

	err := blockstoriov1alpha1.AddToScheme(scheme)
	if err != nil {
		return nil, errors.Wrap(err, "register the blockstor types")
	}

	c, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, errors.Wrap(err, "build an API client")
	}

	return ownedOnNode(ctx, c, nodeName)
}

// ownedOnNode is ownedKernelResources over a given client.
func ownedOnNode(ctx context.Context, c ctrlclient.Client, nodeName string) (map[string]struct{}, error) {
	replicas, err := k8sstore.New(c).Resources().ListByNode(ctx, nodeName)
	if err != nil {
		return nil, errors.Wrapf(err, "list the replicas on %q", nodeName)
	}

	owned := make(map[string]struct{}, len(replicas))
	for i := range replicas {
		owned[replicas[i].Name] = struct{}{}
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

		if _, ok := owned[name]; !ok {
			foreign = append(foreign, name)

			continue
		}

		orphans = append(orphans, name)
	}

	return orphans, foreign
}
