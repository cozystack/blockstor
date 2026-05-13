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
	"fmt"
	"net"
	"os"

	"github.com/LINBIT/golinstor/client"
	"github.com/cockroachdb/errors"
)

// phase is one self-contained section of the recording. Each phase
// drives the golinstor client through a fixed sequence of
// operations against the oracle and is responsible for leaving the
// oracle in the same state it started (so subsequent phases see a
// known baseline). The recorder writes one Trace per HTTP call
// transparently; phases just emit the calls.
type phase struct {
	name string
	run  func(ctx context.Context, c *client.Client) error
}

// Test fixture names used across phases. Centralised so the
// goconst linter doesn't flag every repetition and so a rename
// doesn't have to chase strings.
const (
	traceNode1 = "trace-n1"
	traceNode2 = "trace-n2"
	// loopbackSatellitePort matches the dummy port the recorder
	// announces for its fake nodes. Real oracle never opens this;
	// the test just needs a structurally-valid NodeCreate body.
	loopbackSatellitePort = 3366
	// Shared constants for NodeCreate payloads — lifted so goconst
	// stops flagging every NodeCreate site and a future LINSTOR
	// rename (e.g. SATELLITE → AGENT) lands in one place.
	nodeTypeSatellite    = "SATELLITE"
	nodeIfaceDefaultName = "default"
	nodeEncryptionPlain  = "PLAIN"
)

func selectPhases(name string) []phase {
	all := []phase{
		{"bootstrap", phaseBootstrap},
		{"controller-props", phaseControllerProps},
		{"error-reports", phaseErrorReports},
		{"remotes", phaseRemotes},
		{"nodes", phaseNodes},
		{"node-ops", phaseNodeOps},
		{"net-interfaces", phaseNetInterfaces},
		{"resource-groups", phaseResourceGroups},
		{"resource-definitions", phaseResourceDefinitions},
		{"deep-props", phaseDeepProps},
		{"multi-vd", phaseMultiVD},
		{"more-nodes", phaseMoreNodes},
		// view-empty must run AFTER all teardown phases so the oracle's
		// /v1/view/* responses are genuinely empty for trace-* state.
		{"view-empty", phaseViewEmpty},
		// key-value-store and controller-config are intentionally
		// skipped from "all": blockstor's KV surface is a no-op stub
		// (Phase 10.4 retired the CRD-backed KV) so persisted-data
		// traces against the oracle would never match. ControllerConfig
		// is similar — oracle returns a heavy JVM-config bag, blockstor
		// returns {}. Both are pinned via smoke traces instead.
		// Invoke directly via --phase key-value-store / controller-config
		// for diagnostic purposes.
	}

	if name == "all" {
		return all
	}

	for _, p := range all {
		if p.name == name {
			return []phase{p}
		}
	}

	names := make([]string, 0, len(all)+1)
	for _, p := range all {
		names = append(names, p.name)
	}

	names = append(names, "all")

	fmt.Fprintf(os.Stderr, "unknown phase %q; available: %v\n", name, names)
	os.Exit(2)

	return nil
}

// phaseBootstrap captures the cheap idempotent endpoints every
// golinstor session opens with: controller version + health
// probes. These traces verify the basic JSON envelope shape
// (`{"version":"…"}`) the linstor CLI's about/version path
// depends on.
func phaseBootstrap(ctx context.Context, c *client.Client) error {
	_, err := c.Controller.GetVersion(ctx)
	if err != nil {
		return errors.Wrap(err, "controller version")
	}

	return nil
}

// phaseNodes captures the upstream-LINSTOR node lifecycle:
//
//	GET    /v1/nodes                  → empty list
//	POST   /v1/nodes                  → create n1
//	POST   /v1/nodes                  → create n2
//	GET    /v1/nodes                  → list of two
//	GET    /v1/nodes/n1               → single fetch
//	PUT    /v1/nodes/n1               → modify (add prop)
//	DELETE /v1/nodes/n1               → tear-down
//	DELETE /v1/nodes/n2               → tear-down
//	GET    /v1/nodes                  → empty list (idempotency check)
//
// Each call lands as its own JSON trace. The replay harness then
// asserts blockstor emits the same status + body for each.
func phaseNodes(ctx context.Context, c *client.Client) error {
	_, err := c.Nodes.GetAll(ctx)
	if err != nil {
		return errors.Wrap(err, "list nodes (initial)")
	}

	for _, name := range []string{traceNode1, traceNode2} {
		err := c.Nodes.Create(ctx, client.Node{
			Name: name,
			Type: nodeTypeSatellite,
			NetInterfaces: []client.NetInterface{{
				Name:                    nodeIfaceDefaultName,
				Address:                 net.ParseIP("127.0.0.1"),
				SatellitePort:           loopbackSatellitePort,
				SatelliteEncryptionType: nodeEncryptionPlain,
			}},
		})
		if err != nil {
			return errors.Wrapf(err, "create node %s", name)
		}
	}

	_, err = c.Nodes.GetAll(ctx)
	if err != nil {
		return errors.Wrap(err, "list nodes (after create)")
	}

	_, err = c.Nodes.Get(ctx, traceNode1)
	if err != nil {
		return errors.Wrapf(err, "get %s", traceNode1)
	}

	err = c.Nodes.Modify(ctx, traceNode1, client.NodeModify{
		GenericPropsModify: client.GenericPropsModify{
			OverrideProps: map[string]string{
				"Aux/recorder-stamp": traceStampYes,
			},
		},
	})
	if err != nil {
		return errors.Wrapf(err, "modify %s", traceNode1)
	}

	for _, name := range []string{traceNode1, traceNode2} {
		err := c.Nodes.Delete(ctx, name)
		if err != nil {
			return errors.Wrapf(err, "delete node %s", name)
		}
	}

	_, err = c.Nodes.GetAll(ctx)
	if err != nil {
		return errors.Wrap(err, "list nodes (after teardown)")
	}

	return nil
}
