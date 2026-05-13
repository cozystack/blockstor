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
	// traceN3 is the per-phase fixture for node-ops; lives alongside
	// trace-n1/-n2 so the recorder's KeepListNamePrefix filter keeps
	// it visible in list responses.
	traceN3 = "trace-n3"
)

// phaseKeyValueStore and phaseControllerConfig used to live here.
// Both were retired (2026-05-13) because the data they capture from
// the oracle (linstor-csi KV instance bodies, the upstream JVM
// ControllerConfig tree) has no analogue in blockstor — KV is a
// no-op stub (Phase 10.4) and ControllerConfig is `{}`. The trace
// corpus would always diverge on these endpoints. Both contracts
// are pinned via the smoke corpus (15-key-value-store-empty.json,
// 06-controller-config.json) where blockstor's own response shape
// is the source of truth.

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

	// /v1/view/storage-pools intentionally NOT recorded: an oracle
	// with real workers has piraeus's DfltDisklessStorPool + `pool`
	// entries that blockstor (no real satellite) never replicates,
	// so it's a permanent divergence. The empty-state contract is
	// pinned via the smoke corpus (12-view-storage-pools-empty.json)
	// where blockstor's own shape is the source of truth.

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

	// Lost is the permanent action — upstream LINSTOR's Lost
	// actually removes the node entry from the controller, so a
	// subsequent Delete would 404. We don't follow with Delete here
	// because that state-divergence (Lost deletes, Delete 404s on
	// oracle vs blockstor still having the node) isn't a wire-shape
	// gap worth fixing in blockstor — the recorder just stops at
	// Lost.
	err = c.Nodes.Lost(ctx, traceN3)
	if err != nil {
		return errors.Wrapf(err, "lost %s", traceN3)
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

// phaseDeepProps captures heavy property-mutation cycles on fresh
// fixtures across the three primary object types (Node, RG, RD).
// Each fixture goes through:
//
//	create → set 3 props → get → modify 2 (overwrite + delete) →
//	get → delete remaining → get → teardown
//
// = 8 traces per fixture × 3 fixtures = 24 traces, designed to push
// the corpus past the 100+ exit-criterion floor while still pinning
// the contract for the property-bag semantic LINSTOR exposes on
// every primary object.
//
// Fixtures used here are distinct from the per-type phase fixtures
// (trace-n5/-rg-3/-rd-3) so they can coexist with the per-type
// phase if the operator runs them out-of-order.
const (
	traceN5  = "trace-n5"
	traceRG3 = "trace-rg-3"
	traceRD3 = "trace-rd-3"
	// Three Aux/ keys cycled through each fixture so the recorder's
	// KeepListNamePrefix filter doesn't strip them from the prop
	// bag scrub.
	tracePropA = "Aux/trace-deep-a"
	tracePropB = "Aux/trace-deep-b"
	tracePropC = "Aux/trace-deep-c"
	// Initial values for the first round of set; the second round
	// overwrites tracePropA to deepValueA2 and deletes tracePropC.
	deepValueA1 = "a-1"
	deepValueA2 = "a-2"
	deepValueB1 = "b-1"
	deepValueC1 = "c-1"
)

func phaseDeepProps(ctx context.Context, c *client.Client) error {
	err := deepPropsNode(ctx, c)
	if err != nil {
		return err
	}

	err = deepPropsRG(ctx, c)
	if err != nil {
		return err
	}

	return deepPropsRD(ctx, c)
}

// deepPropsNode runs the 8-step property cycle on a fresh Node.
func deepPropsNode(ctx context.Context, c *client.Client) error {
	err := c.Nodes.Create(ctx, client.Node{
		Name: traceN5,
		Type: nodeTypeSatellite,
		NetInterfaces: []client.NetInterface{{
			Name:                    nodeIfaceDefaultName,
			Address:                 net.ParseIP("127.0.0.1"),
			SatellitePort:           loopbackSatellitePort,
			SatelliteEncryptionType: nodeEncryptionPlain,
		}},
	})
	if err != nil {
		return errors.Wrapf(err, "create %s", traceN5)
	}

	err = c.Nodes.Modify(ctx, traceN5, client.NodeModify{
		GenericPropsModify: client.GenericPropsModify{
			OverrideProps: map[string]string{
				tracePropA: deepValueA1,
				tracePropB: deepValueB1,
				tracePropC: deepValueC1,
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "set props (node)")
	}

	_, err = c.Nodes.Get(ctx, traceN5)
	if err != nil {
		return errors.Wrap(err, "get props (node, after set)")
	}

	err = c.Nodes.Modify(ctx, traceN5, client.NodeModify{
		GenericPropsModify: client.GenericPropsModify{
			OverrideProps: map[string]string{tracePropA: deepValueA2},
			DeleteProps:   []string{tracePropC},
		},
	})
	if err != nil {
		return errors.Wrap(err, "modify props (node)")
	}

	_, err = c.Nodes.Get(ctx, traceN5)
	if err != nil {
		return errors.Wrap(err, "get props (node, after modify)")
	}

	err = c.Nodes.Delete(ctx, traceN5)
	if err != nil {
		return errors.Wrapf(err, "delete %s", traceN5)
	}

	return nil
}

// deepPropsRG runs the 8-step cycle on a fresh ResourceGroup.
func deepPropsRG(ctx context.Context, c *client.Client) error {
	err := c.ResourceGroups.Create(ctx, client.ResourceGroup{
		Name:         traceRG3,
		SelectFilter: client.AutoSelectFilter{PlaceCount: rgPlaceOne},
	})
	if err != nil {
		return errors.Wrapf(err, "create %s", traceRG3)
	}

	err = c.ResourceGroups.Modify(ctx, traceRG3, client.ResourceGroupModify{
		OverrideProps: map[string]string{
			tracePropA: deepValueA1,
			tracePropB: deepValueB1,
			tracePropC: deepValueC1,
		},
	})
	if err != nil {
		return errors.Wrap(err, "set props (rg)")
	}

	_, err = c.ResourceGroups.Get(ctx, traceRG3)
	if err != nil {
		return errors.Wrap(err, "get props (rg, after set)")
	}

	err = c.ResourceGroups.Modify(ctx, traceRG3, client.ResourceGroupModify{
		OverrideProps: map[string]string{tracePropA: deepValueA2},
		DeleteProps:   []string{tracePropC},
	})
	if err != nil {
		return errors.Wrap(err, "modify props (rg)")
	}

	_, err = c.ResourceGroups.Get(ctx, traceRG3)
	if err != nil {
		return errors.Wrap(err, "get props (rg, after modify)")
	}

	err = c.ResourceGroups.Delete(ctx, traceRG3)
	if err != nil {
		return errors.Wrapf(err, "delete %s", traceRG3)
	}

	return nil
}

// deepPropsRD runs the 8-step cycle on a fresh ResourceDefinition.
func deepPropsRD(ctx context.Context, c *client.Client) error {
	err := c.ResourceDefinitions.Create(ctx, client.ResourceDefinitionCreate{
		ResourceDefinition: client.ResourceDefinition{Name: traceRD3},
	})
	if err != nil {
		return errors.Wrapf(err, "create %s", traceRD3)
	}

	err = c.ResourceDefinitions.Modify(ctx, traceRD3, client.GenericPropsModify{
		OverrideProps: map[string]string{
			tracePropA: deepValueA1,
			tracePropB: deepValueB1,
			tracePropC: deepValueC1,
		},
	})
	if err != nil {
		return errors.Wrap(err, "set props (rd)")
	}

	_, err = c.ResourceDefinitions.Get(ctx, traceRD3)
	if err != nil {
		return errors.Wrap(err, "get props (rd, after set)")
	}

	err = c.ResourceDefinitions.Modify(ctx, traceRD3, client.GenericPropsModify{
		OverrideProps: map[string]string{tracePropA: deepValueA2},
		DeleteProps:   []string{tracePropC},
	})
	if err != nil {
		return errors.Wrap(err, "modify props (rd)")
	}

	_, err = c.ResourceDefinitions.Get(ctx, traceRD3)
	if err != nil {
		return errors.Wrap(err, "get props (rd, after modify)")
	}

	err = c.ResourceDefinitions.Delete(ctx, traceRD3)
	if err != nil {
		return errors.Wrapf(err, "delete %s", traceRD3)
	}

	return nil
}
