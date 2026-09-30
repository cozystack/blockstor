// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// staleNodeCache is a store whose cached Get trails the API server. Get
// answers the status the node carried before the satellite stopped
// reporting; GetUncached answers what the API server actually holds. That is
// the shape a manager's informer produces under load, and the integration
// harness reproduced it: the satellite mock had stamped OFFLINE and the API
// server had it, while the REST server's cached client still read ONLINE and
// refused the cleanup.
type staleNodeCache struct {
	store.NodeStore

	stale string
}

func (s staleNodeCache) Get(ctx context.Context, name string) (apiv1.Node, error) {
	node, err := s.NodeStore.Get(ctx, name)
	if err != nil {
		return node, errors.Wrap(err, "stale-cache node read")
	}

	node.ConnectionStatus = s.stale

	return node, nil
}

type staleNodeCacheStore struct {
	store.Store

	stale string
}

func (s staleNodeCacheStore) Nodes() store.NodeStore {
	return staleNodeCache{NodeStore: s.Store.Nodes(), stale: s.stale}
}

// The gate decides whether to unregister a node and cascade away its replicas
// on one field, and then acts. Read from a cache, a status one beat behind is
// not a slow answer but a wrong one: here the satellite is gone and the API
// server says so, while the cache still holds the last ONLINE heartbeat, and
// the cleanup the operator needs is refused with a reason that is no longer
// true. Nothing converges behind the refusal — the operator retries by hand.
func TestNodeLostReadsTheStatusFromTheAPIServerNotTheCache(t *testing.T) {
	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := backend.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("seed connection status: %v", err)
	}

	st := staleNodeCacheStore{Store: backend, stale: apiv1.NodeTypeOnline}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		var rcs []apiv1.APICallRc
		_ = json.NewDecoder(resp.Body).Decode(&rcs)

		t.Fatalf("status: got %d, want 200 — the gate refused on a cached ONLINE the "+
			"API server had already moved past; envelope=%+v", resp.StatusCode, rcs)
	}

	if _, err := backend.Nodes().Get(ctx, "n1"); err == nil {
		t.Errorf("node still present after a legitimate `n lost`")
	}
}

// The other direction, and the dangerous one: the cache is behind on a node
// that has come back. Answering from it would unregister a satellite that is
// reporting and orphan the DRBD state on a live host, which is the whole
// reason the gate exists.
func TestNodeLostStillRefusesWhenOnlyTheCacheSaysOffline(t *testing.T) {
	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := backend.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOnline); err != nil {
		t.Fatalf("seed connection status: %v", err)
	}

	st := staleNodeCacheStore{Store: backend, stale: apiv1.NodeTypeOffline}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 — a stale cached OFFLINE let `n lost` "+
			"through against a satellite that is reporting", resp.StatusCode)
	}

	if _, err := backend.Nodes().Get(ctx, "n1"); err != nil {
		t.Errorf("live node was removed anyway: %v", err)
	}
}
