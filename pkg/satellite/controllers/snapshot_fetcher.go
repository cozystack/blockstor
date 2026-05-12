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
	"io"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/stream"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// SnapshotFetcher implements satellite.CrossNodeFetcher. Looks up
// the Snapshot CRD to discover which nodes host the snapshot locally,
// resolves their addresses via the Node CRD, and pulls the byte
// stream from the first peer that answers OK.
//
// One instance per satellite — the agent wires this in after the
// controller-runtime manager is built (cached client lives there).
type SnapshotFetcher struct {
	// Client is the controller-runtime cached client. Reads through
	// the informer cache so per-snapshot lookups don't hammer the
	// apiserver during the cross-node clone path.
	Client client.Client

	// NodeName is this satellite's node identifier. Used to exclude
	// self from the peer-iteration in Fetch — the local clone has
	// already failed by the time Fetch is called.
	NodeName string

	// Stream is the HTTP client that talks to peer satellites'
	// snapshot-stream endpoints. Nil → NewClient(nil) is used at
	// first Fetch call.
	Stream *stream.Client
}

// Fetch implements satellite.CrossNodeFetcher.
//
// Resolution order:
//  1. Get Snapshot CRD by composite name (k8s.Name pattern).
//  2. Walk snap.Spec.Nodes; skip this satellite's own NodeName.
//  3. For each remaining peer, resolve its host address (Node CRD's
//     NetInterfaces, first IsActive or first entry as fallback).
//  4. HTTP GET via stream.Client.Fetch. First non-404 success wins.
//  5. All peers exhausted → storage.ErrNotFound so the caller falls
//     through to a blank CreateVolume + DRBD resync.
func (f *SnapshotFetcher) Fetch(ctx context.Context, srcRD, snapName string, vol int32) (io.ReadCloser, string, error) {
	if f.Stream == nil {
		f.Stream = stream.NewClient(nil)
	}

	var snap blockstoriov1alpha1.Snapshot

	err := f.Client.Get(ctx, client.ObjectKey{Name: k8s.Name(srcRD + "." + snapName)}, &snap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "", errors.Wrapf(storage.ErrNotFound, "snapshot CRD %s/%s", srcRD, snapName)
		}

		return nil, "", errors.Wrapf(err, "get snapshot %s/%s", srcRD, snapName)
	}

	if len(snap.Spec.Nodes) == 0 {
		return nil, "", errors.Wrapf(storage.ErrNotFound, "snapshot %s/%s has empty Spec.Nodes", srcRD, snapName)
	}

	var lastErr error

	for _, peer := range snap.Spec.Nodes {
		if peer == f.NodeName {
			continue
		}

		addr, err := f.resolvePeerAddr(ctx, peer)
		if err != nil {
			lastErr = err

			continue
		}

		body, err := f.Stream.Fetch(ctx, addr, srcRD, snapName, vol)
		if err == nil {
			return body, peer, nil
		}

		if !errors.Is(err, storage.ErrNotFound) {
			lastErr = err
		}
	}

	if lastErr != nil {
		return nil, "", lastErr
	}

	return nil, "", errors.Wrapf(storage.ErrNotFound, "no peer hosts %s/%s", srcRD, snapName)
}

// resolvePeerAddr looks up the satellite's snapshot-stream address
// on the peer node. Picks the "default" NetInterface when present,
// else falls back to the first non-empty Address. The DaemonSet
// runs on hostNetwork so the stream port lives on each Node CRD's
// advertised address — no separate Service / Endpoints lookup
// needed.
func (f *SnapshotFetcher) resolvePeerAddr(ctx context.Context, peerNode string) (string, error) {
	var node blockstoriov1alpha1.Node

	err := f.Client.Get(ctx, client.ObjectKey{Name: peerNode}, &node)
	if err != nil {
		return "", errors.Wrapf(err, "get node CRD %s", peerNode)
	}

	if len(node.Spec.NetInterfaces) == 0 {
		return "", errors.Errorf("node %s has no NetInterfaces", peerNode)
	}

	addr := ""

	for i := range node.Spec.NetInterfaces {
		if node.Spec.NetInterfaces[i].Name == "default" && node.Spec.NetInterfaces[i].Address != "" {
			addr = node.Spec.NetInterfaces[i].Address

			break
		}
	}

	if addr == "" {
		for i := range node.Spec.NetInterfaces {
			if node.Spec.NetInterfaces[i].Address != "" {
				addr = node.Spec.NetInterfaces[i].Address

				break
			}
		}
	}

	if addr == "" {
		return "", errors.Errorf("node %s NetInterfaces have no usable Address", peerNode)
	}

	return stream.PeerAddr(addr), nil
}
