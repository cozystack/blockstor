//go:build integration

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

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

const (
	lmRD          = "pvc-live-migrate"
	lmSource      = harness.NodeWorker1
	lmTarget      = harness.NodeWorker2
	lmThird       = harness.NodeWorker3
	lmWaitTimeout = 60 * time.Second

	// LINSTOR's FAIL_IN_USE, ORed with MASK_ERROR.
	lmFailInUse = -0x4000_0000_0000_0000 | 997
	// blockstor's warning band.
	lmMaskWarn = 0x0002_0000_0000
	// MASK_INFO | RC_RSC_DELETED, the answer of a replica delete.
	lmRscDeleted = 0x0040_0000_0000_0000 | 0x4200
)

// TestLiveMigrationDualPrimaryRendersOnTheMigrationConnection drives the
// calls linstor-csi v1.13 makes for a KubeVirt live migration of an RWX
// block volume against the real store, reconcilers and REST server: attach
// on the source, attach on the target with auto_manage_dual_primary while
// the source runs, detach the source, detach the target. After each step it
// renders the .res file every node's satellite would write from the CRDs
// and checks the DRBD options in it.
//
// The definition runs protocol A, so the migration needs both
// allow-two-primaries and protocol C, and on the source-target connection
// only: the third replica keeps replicating asynchronously throughout.
func TestLiveMigrationDualPrimaryRendersOnTheMigrationConnection(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	ctx := context.Background()

	// The target has no storage of its own, so its attachment stays
	// diskless: blockstor promotes an in-use diskless replica on a node
	// with a pool to diskful, which would keep it on the detach.
	lmRemovePools(ctx, t, stack, lmTarget)
	base := stack.RestURL + "/v1/resource-definitions/"

	lmCall(t, http.MethodPost, stack.RestURL+"/v1/resource-definitions", http.StatusCreated, map[string]any{
		"resource_definition": map[string]any{
			"name":  lmRD,
			"props": map[string]string{"DrbdOptions/Net/protocol": "A"},
		},
	})
	lmCall(t, http.MethodPost, base+lmRD+"/volume-definitions", http.StatusOK, map[string]any{
		"volume_definition": map[string]any{"size_kib": 1024 * 1024},
	})

	for _, node := range []string{lmSource, lmThird} {
		lmCall(t, http.MethodPost, base+lmRD+"/resources", http.StatusCreated, []map[string]any{{
			"resource": map[string]any{
				"node_name": node,
				"props":     map[string]string{"StorPoolName": "lvm-thin"},
			},
		}})
	}

	// Attach on the source: a plain attach, the volume is in use nowhere.
	lmCall(t, http.MethodPost, base+lmRD+"/resources/"+lmSource+"/make-available", http.StatusOK,
		map[string]any{"auto_manage_dual_primary": true})
	lmAssertNoMigrationProps(t, stack)
	lmSetInUse(ctx, t, stack, lmSource, true)

	// Attach on the target while the VM runs on the source. It answers
	// once the satellites of both ends reported they applied the props,
	// so the answer carries no warning about them.
	for _, rc := range lmCall(t, http.MethodPost, base+lmRD+"/resources/"+lmTarget+"/make-available", http.StatusOK,
		map[string]any{"diskful": false, "auto_manage_dual_primary": true}) {
		if rc.RetCode&lmMaskWarn != 0 {
			t.Errorf("target attach warned: %+v", rc)
		}
	}

	props := lmRDProps(ctx, t, stack)
	for key, want := range map[string]string{
		"Internal/Drbd/LiveMigrate/SourceNode":                                                     lmSource,
		"Internal/Drbd/LiveMigrate/TargetNode":                                                     lmTarget,
		"Internal/Drbd/LiveMigrate/ProtocolSetOn":                                                  "RSC_CONN",
		apiv1.ResourceConnectionPropKey(lmSource, lmTarget, "DrbdOptions/Net/allow-two-primaries"): "yes",
		apiv1.ResourceConnectionPropKey(lmSource, lmTarget, "DrbdOptions/Net/protocol"):            "C",
	} {
		if props[key] != want {
			t.Errorf("after the target attach %s = %q, want %q", key, props[key], want)
		}
	}

	migrating := "    net {\n      allow-two-primaries yes;\n      protocol C;\n    }\n"

	for _, node := range []string{lmSource, lmTarget} {
		res := lmRender(ctx, t, stack, node)

		block := lmConnectionBlock(t, res, lmSource, lmTarget)
		if !strings.Contains(block, migrating) {
			t.Errorf("%s: the source-target connection does not allow two Primaries over protocol C:\n%s", node, res)
		}
	}

	for _, node := range []string{lmSource, lmThird} {
		res := lmRender(ctx, t, stack, node)
		if block := lmConnectionBlock(t, res, lmSource, lmThird); strings.Contains(block, "net {") {
			t.Errorf("%s: the connection to the third replica changed:\n%s", node, res)
		}

		if !strings.Contains(res, "protocol A;") {
			t.Errorf("%s: the resource no longer runs protocol A:\n%s", node, res)
		}
	}

	// The target opens the device; the source still holds it, so its
	// detach is refused and leaves the migration open.
	lmSetInUse(ctx, t, stack, lmTarget, true)

	rcs := lmCall(t, http.MethodPost, base+lmRD+"/resources/"+lmSource+"/unmake-available", http.StatusConflict, nil)
	if len(rcs) == 0 || rcs[0].RetCode != lmFailInUse {
		t.Fatalf("detach of an in-use source: %+v, want FAIL_IN_USE", rcs)
	}

	if !strings.Contains(lmRender(ctx, t, stack, lmTarget), "allow-two-primaries") {
		t.Fatal("a refused detach closed the migration")
	}

	// The source lets go: its detach closes the migration and keeps the
	// diskful replica there.
	lmSetInUse(ctx, t, stack, lmSource, false)
	lmCall(t, http.MethodPost, base+lmRD+"/resources/"+lmSource+"/unmake-available", http.StatusOK, nil)
	lmAssertNoMigrationProps(t, stack)

	for _, node := range []string{lmSource, lmTarget, lmThird} {
		if res := lmRender(ctx, t, stack, node); strings.Contains(res, "allow-two-primaries") {
			t.Errorf("%s still allows two Primaries after the source detach:\n%s", node, res)
		}
	}

	var kept blockstoriov1alpha1.Resource
	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: lmRD + "." + lmSource}, &kept); err != nil {
		t.Fatalf("the diskful source replica is gone: %v", err)
	}

	// The VM stops on the target. Its diskless attachment is the third
	// voter of the two diskful replicas, so it turns into their
	// tiebreaker in place, as LINSTOR's unmake-available does, instead
	// of being deleted and leaving them without one until the reconciler
	// places a new one.
	lmSetInUse(ctx, t, stack, lmTarget, false)

	rcs = lmCall(t, http.MethodPost, base+lmRD+"/resources/"+lmTarget+"/unmake-available", http.StatusOK, nil)
	if slices.ContainsFunc(rcs, func(rc apiv1.APICallRc) bool { return rc.RetCode == lmRscDeleted }) {
		t.Fatalf("detach of the target: %+v, want the attachment kept as the tiebreaker", rcs)
	}

	var witness blockstoriov1alpha1.Resource
	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: lmRD + "." + lmTarget}, &witness); err != nil ||
		witness.DeletionTimestamp != nil || !slices.Contains(witness.Spec.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Fatalf("the attachment on the target: %+v, %v; want it kept as the tiebreaker", witness.Spec, err)
	}
}

func lmCall(t *testing.T, method, url string, want int, payload any) []apiv1.APICallRc {
	t.Helper()

	var (
		status int
		body   []byte
	)

	if payload == nil {
		status, body = b434HTTP(t, method, url, struct{}{})
	} else {
		status, body = b434HTTP(t, method, url, payload)
	}

	if status != want {
		t.Fatalf("%s %s: %d %s, want %d", method, url, status, body, want)
	}

	var rcs []apiv1.APICallRc

	_ = json.Unmarshal(body, &rcs)

	return rcs
}

func lmSetInUse(ctx context.Context, t *testing.T, stack *harness.Stack, node string, using bool) {
	t.Helper()

	err := retryStatusPatch(ctx, stack, lmRD+"."+node, func(r *blockstoriov1alpha1.Resource) {
		r.Status.InUse = using
	})
	if err != nil {
		t.Fatalf("set in_use=%v on %s: %v", using, node, err)
	}

	// The REST server reads replicas through the manager's cache.
	harness.Eventually(t, lmWaitTimeout, func() bool {
		res, ok := lmGetReplica(ctx, stack.RestURL+"/v1/resource-definitions/"+lmRD+"/resources/"+node)

		return ok && res.State.InUse != nil && *res.State.InUse == using
	}, "the REST server to see in_use="+map[bool]string{true: "true", false: "false"}[using]+" on "+node)
}

func lmRDProps(ctx context.Context, t *testing.T, stack *harness.Stack) map[string]string {
	t.Helper()

	var rd blockstoriov1alpha1.ResourceDefinition
	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: lmRD}, &rd); err != nil {
		t.Fatalf("get RD: %v", err)
	}

	return rd.Spec.Props
}

func lmAssertNoMigrationProps(t *testing.T, stack *harness.Stack) {
	t.Helper()

	for key := range lmRDProps(context.Background(), t, stack) {
		if strings.HasPrefix(key, "Internal/Drbd/LiveMigrate/") || strings.HasPrefix(key, apiv1.ResourceConnectionPropsPrefix) {
			t.Errorf("live-migration prop %s is set", key)
		}
	}
}

// lmRender renders the .res file the satellite on node writes, from the
// CRDs as the satellite reads them, once every replica has its DRBD node
// id (a replica without one is left out of its peers' files).
func lmRender(ctx context.Context, t *testing.T, stack *harness.Stack, node string) string {
	t.Helper()

	cli := stack.Env.Client

	var (
		replicas blockstoriov1alpha1.ResourceList
		target   *blockstoriov1alpha1.Resource
	)

	harness.Eventually(t, lmWaitTimeout, func() bool {
		target = nil

		if cli.List(ctx, &replicas) != nil {
			return false
		}

		allocated := true

		for i := range replicas.Items {
			res := &replicas.Items[i]
			if res.Spec.ResourceDefinitionName != lmRD || res.DeletionTimestamp != nil {
				continue
			}

			allocated = allocated && res.Spec.DRBDNodeID != nil

			if naming.Same(res.Spec.NodeName, node) {
				target = res
			}
		}

		return allocated && target != nil
	}, "every replica of "+lmRD+" to get a DRBD node id")

	var (
		rd    blockstoriov1alpha1.ResourceDefinition
		nodes blockstoriov1alpha1.NodeList
		pools blockstoriov1alpha1.StoragePoolList
	)

	for _, read := range []func() error{
		func() error { return cli.Get(ctx, types.NamespacedName{Name: lmRD}, &rd) },
		func() error { return cli.List(ctx, &nodes) },
		func() error { return cli.List(ctx, &pools) },
	} {
		if err := read(); err != nil {
			t.Fatalf("read CRDs: %v", err)
		}
	}

	var peers []blockstoriov1alpha1.Resource

	for i := range replicas.Items {
		if replicas.Items[i].Spec.ResourceDefinitionName == lmRD && replicas.Items[i].DeletionTimestamp == nil {
			peers = append(peers, replicas.Items[i])
		}
	}

	eff, err := effectiveprops.Resolve(ctx, client.Reader(cli), target, &rd)
	if err != nil {
		t.Fatalf("resolve effective props: %v", err)
	}

	desired := dispatcher.BuildDesired(target, peers, nodes.Items, pools.Items, &rd, eff)

	body, err := satellite.BuildResFile(desired, node, "10.0.0.1")
	if err != nil {
		t.Fatalf("render .res for %s: %v", node, err)
	}

	return body
}

// lmConnectionBlock cuts the connection block between nodes a and b out
// of a rendered .res file.
func lmConnectionBlock(t *testing.T, res, a, b string) string {
	t.Helper()

	for _, block := range strings.Split(res, "  connection {\n")[1:] {
		block, _, _ = strings.Cut(block, "\n  }\n")
		if strings.Contains(block, "host "+a+" ") && strings.Contains(block, "host "+b+" ") {
			return block + "\n"
		}
	}

	t.Fatalf("no connection between %s and %s in:\n%s", a, b, res)

	return ""
}

func lmRemovePools(ctx context.Context, t *testing.T, stack *harness.Stack, node string) {
	t.Helper()

	var pools blockstoriov1alpha1.StoragePoolList
	if err := stack.Env.Client.List(ctx, &pools); err != nil {
		t.Fatalf("list pools: %v", err)
	}

	for i := range pools.Items {
		if naming.Same(pools.Items[i].Spec.NodeName, node) {
			if err := stack.Env.Client.Delete(ctx, &pools.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				t.Fatalf("delete pool %s: %v", pools.Items[i].Name, err)
			}
		}
	}

	harness.Eventually(t, lmWaitTimeout, func() bool {
		var left blockstoriov1alpha1.StoragePoolList
		if stack.Env.Client.List(ctx, &left) != nil {
			return false
		}

		for i := range left.Items {
			if naming.Same(left.Items[i].Spec.NodeName, node) {
				return false
			}
		}

		return true
	}, "the pools of "+node+" to be gone")
}

// lmGetReplica reads one replica through the REST server; ok is false on
// any failure, for a caller that polls.
func lmGetReplica(ctx context.Context, url string) (apiv1.Resource, bool) {
	var res apiv1.Resource

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return res, false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return res, false
	}

	defer func() { _ = resp.Body.Close() }()

	return res, resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&res) == nil
}
