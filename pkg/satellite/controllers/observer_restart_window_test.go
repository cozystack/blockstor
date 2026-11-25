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

package controllers

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

const (
	// restartReplicas is about what the node that showed the problem
	// held.
	restartReplicas = 110

	// restartWindowLimit bounds how long the replicas of a restarted
	// satellite may read Unknown. LINSTOR shows them Unknown for the
	// few seconds its controller takes to get their state back from
	// the reconnected satellite.
	restartWindowLimit = 5 * time.Second

	// restartAPILatency and restartExecLatency stand in for what an
	// apiserver write and a `drbdadm get-gi` cost on a real node; the
	// envtest apiserver and the fake exec answer at once.
	restartAPILatency  = 20 * time.Millisecond
	restartExecLatency = 30 * time.Millisecond
)

// After a satellite restart reads hide every replica of the node until
// the new process has written its state, so how long the node's
// replicas read Unknown is how long the observer takes to write the
// initial events2 state. Frame by frame it took 30 to 40 seconds on a
// node with ~110 replicas; LINSTOR gets them back in a few seconds.
// Measured here against a real apiserver, through the read the REST
// and CLI answer from, from the moment the new satellite's observer
// starts until every replica shows its state again.
func TestRestartedSatelliteShowsItsReplicasWithinSeconds(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := startRestartEnv(t)
	cfg.QPS = -1 // what ctrl.GetConfig gives the satellite

	direct, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	// The heartbeat of the new process has already named it on the
	// Node, so every report the previous one left behind is hidden.
	seedRestartNode(ctx, t, direct, "after")

	previous := &ObserverRunnable{Client: direct, NodeName: "n1", Incarnation: "before"}

	for i := range restartReplicas {
		rd := restartRD(i)

		err = direct.Create(ctx, &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName(rd, "n1")},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: "n1"},
		})
		if err != nil {
			t.Fatalf("create %s: %v", rd, err)
		}

		err = previous.writeStatus(ctx, &observation{ResourceName: rd, DrbdState: "UpToDate", Role: "Secondary"})
		if err != nil {
			t.Fatalf("seed %s: %v", rd, err)
		}
	}

	st := k8s.New(direct)
	if shown := shownReplicas(ctx, t, st); shown != 0 {
		t.Fatalf("%d replicas shown before the new satellite reported any; want none", shown)
	}

	cli := restartObserverClient(ctx, t, cfg)

	stream, feed := io.Pipe()
	defer func() { _ = feed.Close() }()

	go func() {
		_, _ = io.WriteString(feed, restartInitialState())
	}()

	exec := slowExec{delay: restartExecLatency}
	o := &ObserverRunnable{
		Client:      cli,
		Exec:        exec,
		NodeName:    "n1",
		Incarnation: "after",
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			return drbd.NewWatcher(stream), func() {}, nil
		},
	}

	started := time.Now()

	go func() { _ = o.Start(ctx) }()

	deadline := started.Add(4 * restartWindowLimit)

	for shownReplicas(ctx, t, st) < restartReplicas {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d replicas shown %s after the satellite started",
				shownReplicas(ctx, t, st), restartReplicas, time.Since(started))
		}

		time.Sleep(20 * time.Millisecond)
	}

	window := time.Since(started)
	t.Logf("all %d replicas shown %s after the satellite started", restartReplicas, window)

	if window > restartWindowLimit {
		t.Errorf("the replicas of a restarted satellite read Unknown for %s; want under %s", window, restartWindowLimit)
	}
}

func startRestartEnv(t *testing.T) *rest.Config {
	t.Helper()

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}

	t.Cleanup(func() {
		stopErr := env.Stop()
		if stopErr != nil {
			t.Logf("stop envtest: %v", stopErr)
		}
	})

	return cfg
}

func seedRestartNode(ctx context.Context, t *testing.T, cli client.Client, incarnation string) {
	t.Helper()

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	err := cli.Create(ctx, node)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.Status.ConnectionStatus = apiv1.NodeTypeOnline
	node.Status.SatelliteIncarnation = incarnation

	err = cli.Status().Update(ctx, node)
	if err != nil {
		t.Fatalf("stamp node: %v", err)
	}
}

// restartObserverClient reads through an informer cache, as the
// manager's client does, and writes with restartAPILatency added.
func restartObserverClient(ctx context.Context, t *testing.T, cfg *rest.Config) client.Client {
	t.Helper()

	informers, err := cache.New(cfg, cache.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}

	go func() { _ = informers.Start(ctx) }()

	// A read before Start has run fails with "the cache is not
	// started", which a loaded machine hits.
	if !informers.WaitForCacheSync(ctx) {
		t.Fatal("cache did not start")
	}

	cached, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme, Cache: &client.CacheOptions{Reader: informers}})
	if err != nil {
		t.Fatalf("cached client: %v", err)
	}

	// Sync the informers the observer reads before the clock starts;
	// the manager has them synced before it starts the observer.
	for _, list := range []client.ObjectList{
		&blockstoriov1alpha1.ResourceList{},
		&blockstoriov1alpha1.ResourceDefinitionList{},
		&blockstoriov1alpha1.NodeList{},
	} {
		err = cached.List(ctx, list)
		if err != nil {
			t.Fatalf("prime cache: %v", err)
		}
	}

	return interceptor.NewClient(cached, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			time.Sleep(restartAPILatency)

			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// shownReplicas counts the node's replicas a read shows state for.
func shownReplicas(ctx context.Context, t *testing.T, st *k8s.Store) int {
	t.Helper()

	resources, err := st.Resources().List(ctx)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	nodes, err := st.Nodes().List(ctx)
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}

	apiv1.ForgetStaleSatelliteState(resources, nodes)

	shown := 0

	for i := range resources {
		if resources[i].State.DrbdState != "" {
			shown++
		}
	}

	return shown
}

func restartRD(i int) string { return fmt.Sprintf("pvc-%03d", i) }

// restartInitialState is what `drbdsetup events2 --full` prints for a
// node holding restartReplicas replicas, each with two peers.
func restartInitialState() string {
	var b strings.Builder

	for i := range restartReplicas {
		rd := restartRD(i)

		fmt.Fprintf(&b, "exists resource name:%s role:Secondary suspended:no\n", rd)

		for peer, node := range []string{"n2", "n3"} {
			fmt.Fprintf(&b, "exists connection name:%s peer-node-id:%d conn-name:%s connection:Connected role:Secondary\n",
				rd, peer+1, node)
		}

		fmt.Fprintf(&b, "exists device name:%s volume:0 minor:%d disk:UpToDate client:no quorum:yes backing_dev:/dev/zvol/data/%s_00000\n",
			rd, 1000+i, rd)

		for peer, node := range []string{"n2", "n3"} {
			fmt.Fprintf(&b, "exists peer-device name:%s peer-node-id:%d conn-name:%s volume:0 replication:Established peer-disk:UpToDate\n",
				rd, peer+1, node)
		}
	}

	b.WriteString("exists -\n")

	return b.String()
}

// slowExec answers every command after delay, as a real `drbdadm`
// would, with output the caller does not use.
type slowExec struct{ delay time.Duration }

func (e slowExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return e.RunWithStdin(ctx, nil, name, args...)
}

func (e slowExec) RunWithStdin(context.Context, io.Reader, string, ...string) ([]byte, error) {
	time.Sleep(e.delay)

	return nil, nil
}
