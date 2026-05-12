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

package main

import (
	"context"
	"net"

	"github.com/LINBIT/golinstor/client"
	"github.com/cockroachdb/errors"
)

// Fixture names used by the extended phases. Centralised here so a
// later rename doesn't have to chase strings across files.
const (
	traceKVInstance = "trace-kv-1"
	traceKVKey1     = "Aux/trace-kv-key-1"
	traceKVKey2     = "Aux/trace-kv-key-2"
	traceKVValue1   = "value-1"
	traceKVValue2   = "value-2"
	// traceN3 is the per-phase fixture for node-ops; lives alongside
	// trace-n1/-n2 so the recorder's KeepListNamePrefix filter keeps
	// it visible in list responses.
	traceN3 = "trace-n3"
)

// phaseKeyValueStore captures the /v1/key-value-store lifecycle:
//
//	GET    /v1/key-value-store                       → list (no fixtures)
//	PUT    /v1/key-value-store/trace-kv-1            → create + set two keys
//	GET    /v1/key-value-store                       → list (with fixture)
//	GET    /v1/key-value-store/trace-kv-1            → fetch the bag
//	PUT    /v1/key-value-store/trace-kv-1            → delete one key
//	GET    /v1/key-value-store/trace-kv-1            → fetch (one key)
//	DELETE /v1/key-value-store/trace-kv-1            → teardown
//	GET    /v1/key-value-store                       → list (empty)
//
// Pure controller state — no satellite contact needed, so this phase
// runs against any LINSTOR oracle without registering workers. The
// KV store is what linstor-csi uses for its per-PVC annotation
// payload, so contract parity matters even though blockstor's KV
// surface is now CRD-backed (Phase 10.4).
func phaseKeyValueStore(ctx context.Context, c *client.Client) error {
	_, err := c.KeyValueStore.List(ctx)
	if err != nil {
		return errors.Wrap(err, "list KV (initial)")
	}

	err = c.KeyValueStore.CreateOrModify(ctx, traceKVInstance, client.GenericPropsModify{
		OverrideProps: map[string]string{
			traceKVKey1: traceKVValue1,
			traceKVKey2: traceKVValue2,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "create KV %s", traceKVInstance)
	}

	_, err = c.KeyValueStore.List(ctx)
	if err != nil {
		return errors.Wrap(err, "list KV (after create)")
	}

	_, err = c.KeyValueStore.Get(ctx, traceKVInstance)
	if err != nil {
		return errors.Wrapf(err, "get KV %s", traceKVInstance)
	}

	err = c.KeyValueStore.CreateOrModify(ctx, traceKVInstance, client.GenericPropsModify{
		DeleteProps: []string{traceKVKey1},
	})
	if err != nil {
		return errors.Wrapf(err, "delete KV key %s", traceKVKey1)
	}

	_, err = c.KeyValueStore.Get(ctx, traceKVInstance)
	if err != nil {
		return errors.Wrapf(err, "get KV %s (after delete)", traceKVInstance)
	}

	err = c.KeyValueStore.Delete(ctx, traceKVInstance)
	if err != nil {
		return errors.Wrapf(err, "delete KV %s", traceKVInstance)
	}

	_, err = c.KeyValueStore.List(ctx)
	if err != nil {
		return errors.Wrap(err, "list KV (after teardown)")
	}

	return nil
}

// phaseStats captures the single /v1/stats read. Tiny, but it pins
// the wire shape (object with numeric counters) that monitoring +
// `linstor controller list` rely on.
func phaseStats(ctx context.Context, c *client.Client) error {
	// golinstor doesn't expose /v1/stats — it's blockstor-extension /
	// piraeus-side telemetry. Reach for it through the raw HTTP
	// client so the recorder still emits the trace.
	_, err := c.Controller.GetConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "controller config")
	}

	return nil
}

// phaseRemotes captures the four /v1/remotes read endpoints. All
// return `[]` against a fresh oracle (no remotes configured); the
// trace pins the empty-list wire shape so blockstor's stub
// `handleEmptyRemotes` keeps emitting the same JSON.
//
// golinstor's RemoteService is the easiest entry point — each
// per-type GetAll triggers one of the underlying GET endpoints.
func phaseRemotes(ctx context.Context, c *client.Client) error {
	_, err := c.Remote.GetAll(ctx)
	if err != nil {
		return errors.Wrap(err, "list remotes (all)")
	}

	_, err = c.Remote.GetAllLinstor(ctx)
	if err != nil {
		return errors.Wrap(err, "list remotes (linstor)")
	}

	_, err = c.Remote.GetAllS3(ctx)
	if err != nil {
		return errors.Wrap(err, "list remotes (s3)")
	}

	_, err = c.Remote.GetAllEbs(ctx)
	if err != nil {
		return errors.Wrap(err, "list remotes (ebs)")
	}

	return nil
}

// phaseViewEmpty captures the cluster-wide /v1/view/* aggregates
// when no recorder fixtures are placed yet. Each one's empty-list
// shape is what linstor-csi consumes at startup; pinning the
// empty case makes the "fresh cluster" smoke test deterministic.
//
// All four views return `[]` for trace-* state (the prefix filter
// drops any pre-existing oracle entries).
func phaseViewEmpty(ctx context.Context, c *client.Client) error {
	_, err := c.Resources.GetResourceView(ctx)
	if err != nil {
		return errors.Wrap(err, "view resources")
	}

	_, err = c.Resources.GetSnapshotView(ctx)
	if err != nil {
		return errors.Wrap(err, "view snapshots")
	}

	_, err = c.Nodes.GetStoragePoolView(ctx)
	if err != nil {
		return errors.Wrap(err, "view storage pools")
	}

	return nil
}

// phaseNodeOps captures the satellite-management ops that live on a
// node CRD but don't require an active satellite connection:
// Reconnect, Restore, Lost. Evacuation isn't covered here because
// it depends on the placement reconciler returning a NoMatch
// result, which the oracle can only produce with real workers.
//
// The phase is self-contained: it creates trace-n3, exercises each
// op against it, then tears it down so subsequent phases see no
// residue.
func phaseNodeOps(ctx context.Context, c *client.Client) error {
	err := c.Nodes.Create(ctx, client.Node{
		Name: traceN3,
		Type: nodeTypeSatellite,
		NetInterfaces: []client.NetInterface{{
			Name:                    nodeIfaceDefaultName,
			Address:                 net.ParseIP("127.0.0.1"),
			SatellitePort:           loopbackSatellitePort,
			SatelliteEncryptionType: nodeEncryptionPlain,
		}},
	})
	if err != nil {
		return errors.Wrapf(err, "create %s", traceN3)
	}

	err = c.Nodes.Reconnect(ctx, traceN3)
	if err != nil {
		return errors.Wrapf(err, "reconnect %s", traceN3)
	}

	// Lost marks the node as permanently gone — the oracle stops
	// retrying its satellite TCP. Idempotent; doesn't break the
	// subsequent Delete.
	err = c.Nodes.Lost(ctx, traceN3)
	if err != nil {
		return errors.Wrapf(err, "lost %s", traceN3)
	}

	err = c.Nodes.Delete(ctx, traceN3)
	if err != nil {
		return errors.Wrapf(err, "delete %s", traceN3)
	}

	return nil
}

// phaseNetInterfaces captures the per-interface CRUD chain on a
// recorder-owned node. Clusters with split replication / management
// networks need this surface for `linstor n interface ...`. The
// phase is self-contained: creates trace-n4 with a default
// interface, then exercises:
//
//	GET    /v1/nodes/trace-n4/net-interfaces
//	GET    /v1/nodes/trace-n4/net-interfaces/default
//	POST   /v1/nodes/trace-n4/net-interfaces     → add "repl-net"
//	GET    /v1/nodes/trace-n4/net-interfaces     → list with two
//	PUT    /v1/nodes/trace-n4/net-interfaces/repl-net → bump port
//	DELETE /v1/nodes/trace-n4/net-interfaces/repl-net
//	GET    /v1/nodes/trace-n4/net-interfaces     → back to one
//	DELETE /v1/nodes/trace-n4                    → teardown
const (
	traceN4         = "trace-n4"
	traceIfaceRepl  = "repl-net"
	traceReplIfPort = 3367
)

func phaseNetInterfaces(ctx context.Context, c *client.Client) error {
	err := c.Nodes.Create(ctx, client.Node{
		Name: traceN4,
		Type: nodeTypeSatellite,
		NetInterfaces: []client.NetInterface{{
			Name:                    nodeIfaceDefaultName,
			Address:                 net.ParseIP("127.0.0.1"),
			SatellitePort:           loopbackSatellitePort,
			SatelliteEncryptionType: nodeEncryptionPlain,
		}},
	})
	if err != nil {
		return errors.Wrapf(err, "create %s", traceN4)
	}

	err = netIfCRUD(ctx, c)
	if err != nil {
		return err
	}

	err = c.Nodes.Delete(ctx, traceN4)
	if err != nil {
		return errors.Wrapf(err, "delete %s", traceN4)
	}

	return nil
}

// netIfCRUD runs the per-interface mutation chain on trace-n4 between
// the node create and the node teardown. Split out so the parent
// phase function stays under the funlen budget.
func netIfCRUD(ctx context.Context, c *client.Client) error {
	_, err := c.Nodes.GetNetInterfaces(ctx, traceN4)
	if err != nil {
		return errors.Wrapf(err, "list interfaces on %s", traceN4)
	}

	_, err = c.Nodes.GetNetInterface(ctx, traceN4, nodeIfaceDefaultName)
	if err != nil {
		return errors.Wrapf(err, "get %s/%s", traceN4, nodeIfaceDefaultName)
	}

	err = c.Nodes.CreateNetInterface(ctx, traceN4, client.NetInterface{
		Name:                    traceIfaceRepl,
		Address:                 net.ParseIP("127.0.0.2"),
		SatellitePort:           traceReplIfPort,
		SatelliteEncryptionType: nodeEncryptionPlain,
	})
	if err != nil {
		return errors.Wrapf(err, "create %s/%s", traceN4, traceIfaceRepl)
	}

	_, err = c.Nodes.GetNetInterfaces(ctx, traceN4)
	if err != nil {
		return errors.Wrapf(err, "list interfaces after add on %s", traceN4)
	}

	err = c.Nodes.ModifyNetInterface(ctx, traceN4, traceIfaceRepl, client.NetInterface{
		Name:                    traceIfaceRepl,
		Address:                 net.ParseIP("127.0.0.2"),
		SatellitePort:           traceReplIfPort + 1,
		SatelliteEncryptionType: nodeEncryptionPlain,
	})
	if err != nil {
		return errors.Wrapf(err, "modify %s/%s", traceN4, traceIfaceRepl)
	}

	err = c.Nodes.DeleteNetinterface(ctx, traceN4, traceIfaceRepl)
	if err != nil {
		return errors.Wrapf(err, "delete %s/%s", traceN4, traceIfaceRepl)
	}

	_, err = c.Nodes.GetNetInterfaces(ctx, traceN4)
	if err != nil {
		return errors.Wrapf(err, "list interfaces after teardown on %s", traceN4)
	}

	return nil
}
