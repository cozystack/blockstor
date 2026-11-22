// SPDX-License-Identifier: Apache-2.0

//go:build integration

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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/rest"
	"github.com/cozystack/blockstor/pkg/satellite"
	satcontrollers "github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

const (
	burstPool    = "zfs"
	burstRG      = "burst"
	burstVolKib  = 1 << 20
	burstWorkers = 10 // csi-provisioner --worker-threads
)

// burstProvider is a storage pool whose every call takes a while, so a
// satellite pass costs about what a real one does on a busy node.
type burstProvider struct {
	delay time.Duration
}

func (burstProvider) Kind() string { return "ZFS_THIN" }

func (burstProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{TotalCapacityKib: 1 << 40, FreeCapacityKib: 1 << 40}, nil
}

func (p burstProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	time.Sleep(p.delay)

	return nil
}

func (burstProvider) DeleteVolume(_ context.Context, _ storage.Volume) error { return nil }

func (burstProvider) ResizeVolume(_ context.Context, _ storage.Volume) error { return nil }

func (p burstProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	time.Sleep(p.delay)

	return storage.VolumeStatus{DevicePath: "/dev/zvol/" + vol.ResourceName, UsableKib: vol.SizeKib}, nil
}

func (burstProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (burstProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (burstProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}

// errBurstCall is a REST call of the benchmark the server refused.
var errBurstCall = errors.New("REST call refused")

// quietExec answers every command with empty output, as FakeExec does,
// without keeping a log that grows with every pass.
type quietExec struct{}

func (quietExec) Run(_ context.Context, _ string, _ ...string) ([]byte, error) { return nil, nil }

func (quietExec) RunWithStdin(_ context.Context, _ io.Reader, _ string, _ ...string) ([]byte, error) {
	return nil, nil
}

// slowExec makes every drbdadm call take delay: drbdadm parses every
// .res on the node, so on a node with a hundred replicas each call
// takes about that long.
type slowExec struct {
	storage.Exec

	delay time.Duration
}

func (e *slowExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if name == "drbdadm" {
		time.Sleep(e.delay)
	}

	return e.Exec.Run(ctx, name, args...) //nolint:wrapcheck // test double
}

func (e *slowExec) RunWithStdin(ctx context.Context, stdin io.Reader, name string, args ...string) ([]byte, error) {
	if name == "drbdadm" {
		time.Sleep(e.delay)
	}

	return e.Exec.RunWithStdin(ctx, stdin, name, args...) //nolint:wrapcheck // test double
}

// burstStack is the controller, the REST server and one real satellite
// resource reconciler per node, each with its own cache, on envtest.
type burstStack struct {
	cli     client.Client
	watch   client.WithWatch
	restURL string
	nodes   []string
}

func startBurstStack(t *testing.T, nodes []string, passDelay time.Duration) *burstStack {
	t.Helper()

	return startBurstStackWith(t, nodes, burstLatency{pass: passDelay})
}

// burstLatency is what the stack's API round trips cost on a real
// cluster, where envtest answers in a fraction of it: a drbdadm call
// on the satellites, a replica create from the REST server, and a
// controller read of every Resource in the cluster. Zero is envtest's.
type burstLatency struct {
	pass, create, wholeClusterRead time.Duration
}

func startBurstStackWith(t *testing.T, nodes []string, latency burstLatency) *burstStack {
	t.Helper()

	env := harness.Start(t)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	skip := true
	opts := ctrl.Options{
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: &skip},
	}

	mgr, st, err := storek8s.NewManager(env.Cfg, opts)
	if err != nil {
		t.Fatalf("controller manager: %v", err)
	}

	wireBurstController(t, mgr, st, slowWholeClusterReader{Reader: mgr.GetAPIReader(), delay: latency.wholeClusterRead})

	addr := freeAddr(t)

	err = mgr.Add(&rest.Server{
		Addr:         addr,
		Store:        slowCreateStore{Store: st, delay: latency.create},
		Client:       mgr.GetClient(),
		Namespace:    "default",
		SecretReader: mgr.GetAPIReader(),
	})
	if err != nil {
		t.Fatalf("rest: %v", err)
	}

	runManager(ctx, t, &wg, mgr)

	for _, node := range nodes {
		ex := &slowExec{Exec: &quietExec{}, delay: latency.pass}

		satMgr, err := ctrl.NewManager(env.Cfg, opts)
		if err != nil {
			t.Fatalf("satellite manager %s: %v", node, err)
		}

		rec := &satcontrollers.ResourceReconciler{
			Client: satMgr.GetClient(),
			Config: satcontrollers.Config{
				NodeName: node,
				Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
					Providers: map[string]storage.Provider{burstPool: burstProvider{}},
					NodeName:  node,
					Adm:       drbd.NewAdm(ex),
					Exec:      ex,
					StateDir:  t.TempDir(),
				}),
				Exec:      ex,
				APIReader: satMgr.GetAPIReader(),
			},
		}

		err = rec.SetupWithManager(satMgr)
		if err != nil {
			t.Fatalf("satellite %s: %v", node, err)
		}

		runManager(ctx, t, &wg, satMgr)
	}

	watch, err := client.NewWithWatch(env.Cfg, client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		t.Fatalf("watch client: %v", err)
	}

	s := &burstStack{cli: env.Client, watch: watch, restURL: "http://" + addr, nodes: nodes}
	s.seed(ctx, t)

	wg.Go(func() { s.kernel(ctx) })

	return s
}

func wireBurstController(t *testing.T, mgr manager.Manager, st *storek8s.Store, apiReader client.Reader) {
	t.Helper()

	steps := []func(ctrl.Manager) error{
		(&controller.NodeReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Store: st}).SetupWithManager,
		(&controller.StoragePoolReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager,
		(&controller.ResourceGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Store: st}).SetupWithManager,
		(&controller.ResourceDefinitionReconciler{
			Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Store: st, APIReader: apiReader,
		}).SetupWithManager,
		(&controller.ResourceReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Store: st}).SetupWithManager,
		(&controller.ResourceMigrationReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager,
	}

	for _, step := range steps {
		err := step(mgr)
		if err != nil {
			t.Fatalf("wire controller: %v", err)
		}
	}
}

func runManager(ctx context.Context, t *testing.T, wg *sync.WaitGroup, mgr manager.Manager) {
	t.Helper()

	wg.Go(func() {
		err := mgr.Start(ctx)
		if err != nil {
			t.Errorf("manager: %v", err)
		}
	})
}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := listener.Addr().String()
	_ = listener.Close()

	return addr
}

// seed creates the nodes, one pool on each and a group placing three
// replicas, and keeps the nodes ONLINE the way their satellites do.
func (s *burstStack) seed(ctx context.Context, t *testing.T) {
	t.Helper()

	for i, node := range s.nodes {
		mustCreateBurst(ctx, t, s.cli, &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: node},
			Spec: blockstoriov1alpha1.NodeSpec{
				Type: "SATELLITE",
				NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{{
					Name: "default", Address: fmt.Sprintf("10.0.0.%d", i+1),
					SatellitePort: 3366, SatelliteEncryptionType: "PLAIN",
				}},
			},
		})

		mustCreateBurst(ctx, t, s.cli, &blockstoriov1alpha1.StoragePool{
			ObjectMeta: metav1.ObjectMeta{Name: burstPool + "." + node},
			Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: node, PoolName: burstPool, ProviderKind: "ZFS_THIN"},
		})
	}

	mustCreateBurst(ctx, t, s.cli, &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: burstRG},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				PlaceCount: int32(len(s.nodes)), StoragePoolList: []string{burstPool},
			},
		},
	})

	s.stampNodesAndPools(ctx)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.stampNodesAndPools(ctx)
			}
		}
	}()
}

func (s *burstStack) stampNodesAndPools(ctx context.Context) {
	for _, name := range s.nodes {
		var node blockstoriov1alpha1.Node
		if s.cli.Get(ctx, client.ObjectKey{Name: name}, &node) != nil {
			continue
		}

		node.Status.ConnectionStatus = blockstoriov1alpha1.NodeConnectionStatusOnline
		now := metav1.Now()
		node.Status.LastHeartbeatTime = &now
		node.Status.Conditions = []metav1.Condition{{
			Type: blockstoriov1alpha1.NodeConditionReady, Status: metav1.ConditionTrue,
			Reason: "Burst", Message: "burst", LastTransitionTime: now,
		}}
		_ = s.cli.Status().Update(ctx, &node)

		var pool blockstoriov1alpha1.StoragePool
		if s.cli.Get(ctx, client.ObjectKey{Name: burstPool + "." + name}, &pool) != nil || pool.Status.FreeCapacity > 0 {
			continue
		}

		pool.Status.FreeCapacity = 1 << 40
		pool.Status.TotalCapacity = 1 << 40
		pool.Status.SupportsSnapshots = true
		pool.Status.StaticTraits = map[string]string{"kind": "ZFS_THIN"}
		_ = s.cli.Status().Update(ctx, &pool)
	}
}

// kernel plays the part of DRBD and the observer: a replica whose
// device the satellite reported comes up UpToDate and connected to
// every peer whose device is reported too, a moment later.
func (s *burstStack) kernel(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		var list blockstoriov1alpha1.ResourceList
		if s.cli.List(ctx, &list) != nil {
			continue
		}

		built := map[string][]string{}

		for i := range list.Items {
			res := &list.Items[i]
			if deviceReported(res) {
				built[res.Spec.ResourceDefinitionName] = append(built[res.Spec.ResourceDefinitionName], res.Spec.NodeName)
			}
		}

		for i := range list.Items {
			res := &list.Items[i]
			if !deviceReported(res) {
				continue
			}

			s.bringUp(ctx, res, built[res.Spec.ResourceDefinitionName])
		}
	}
}

func deviceReported(res *blockstoriov1alpha1.Resource) bool {
	if len(res.Status.Volumes) == 0 {
		return false
	}

	for i := range res.Status.Volumes {
		if res.Status.Volumes[i].DevicePath == "" {
			return false
		}
	}

	return true
}

func (s *burstStack) bringUp(ctx context.Context, res *blockstoriov1alpha1.Resource, peers []string) {
	want := []blockstoriov1alpha1.ResourceConnectionStatus{}

	for _, peer := range peers {
		if peer != res.Spec.NodeName {
			want = append(want, blockstoriov1alpha1.ResourceConnectionStatus{PeerNodeName: peer, Connected: true})
		}
	}

	sort.Slice(want, func(i, j int) bool { return want[i].PeerNodeName < want[j].PeerNodeName })

	upToDate := true

	for i := range res.Status.Volumes {
		if res.Status.Volumes[i].DiskState != "UpToDate" {
			upToDate = false
		}
	}

	have := slices.Clone(res.Status.Connections)
	sort.Slice(have, func(i, j int) bool { return have[i].PeerNodeName < have[j].PeerNodeName })

	if upToDate && slices.EqualFunc(have, want, func(a, b blockstoriov1alpha1.ResourceConnectionStatus) bool {
		return a.PeerNodeName == b.PeerNodeName && a.Connected == b.Connected
	}) {
		return
	}

	patched := res.DeepCopy()
	for i := range patched.Status.Volumes {
		patched.Status.Volumes[i].DiskState = "UpToDate"
	}

	patched.Status.Connections = want
	_ = s.cli.Status().Update(ctx, patched)
}

func envInt(name string, def int) int {
	v, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return def
	}

	return v
}

func mustCreateBurst(ctx context.Context, t *testing.T, cli client.Client, obj client.Object) {
	t.Helper()

	err := cli.Create(ctx, obj)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create %s: %v", obj.GetName(), err)
	}
}

func (s *burstStack) post(ctx context.Context, path string, body any) ([]apiv1.APICallRc, time.Duration, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal %s: %w", path, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.restURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, fmt.Errorf("request %s: %w", path, err)
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("post %s: %w", path, err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	took := time.Since(start)

	var rcs []apiv1.APICallRc

	_ = json.Unmarshal(raw, &rcs)

	if resp.StatusCode >= 300 {
		return rcs, took, fmt.Errorf("%w: %s: %d %s", errBurstCall, path, resp.StatusCode, raw)
	}

	return rcs, took, nil
}

// waitReplicas waits until the definition has want replicas, each with
// its device reported, and, for more than one, every one UpToDate and
// connected to the others.
func (s *burstStack) waitReplicas(ctx context.Context, rdName string, want int) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	for {
		var list blockstoriov1alpha1.ResourceList

		err := s.cli.List(ctx, &list)
		if err == nil && replicasDone(&list, rdName, want) {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("replicas of %s not done: %w", rdName, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func replicasDone(list *blockstoriov1alpha1.ResourceList, rdName string, want int) bool {
	var replicas []*blockstoriov1alpha1.Resource

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName == rdName {
			replicas = append(replicas, &list.Items[i])
		}
	}

	if len(replicas) < want {
		return false
	}

	for _, res := range replicas {
		if !deviceReported(res) {
			return false
		}

		if want == 1 {
			continue
		}

		if len(res.Status.Connections) < len(replicas)-1 {
			return false
		}

		for i := range res.Status.Volumes {
			if res.Status.Volumes[i].DiskState != "UpToDate" {
				return false
			}
		}
	}

	return true
}

// createVolume is linstor-csi's CreateVolume: the definition, its
// volume, the replica on the preferred node, then the rest.
func (s *burstStack) createVolume(ctx context.Context, name, preferred string) (time.Duration, []time.Duration, error) {
	start := time.Now()

	_, _, err := s.post(ctx, "/v1/resource-definitions", apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: name, ResourceGroupName: burstRG},
	})
	if err != nil {
		return 0, nil, err
	}

	_, _, err = s.post(ctx, "/v1/resource-definitions/"+name+"/volume-definitions", apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{SizeKib: burstVolKib},
	})
	if err != nil {
		return 0, nil, err
	}

	calls := make([]time.Duration, 0, 2)

	for _, filter := range []apiv1.AutoSelectFilter{
		{PlaceCount: 1, NodeNameList: []string{preferred}, StoragePool: burstPool},
		{PlaceCount: apiv1.LaxInt32(len(s.nodes)), StoragePool: burstPool},
	} {
		callStart := time.Now()

		_, _, err := s.post(ctx, "/v1/resource-definitions/"+name+"/autoplace", apiv1.AutoPlaceRequest{SelectFilter: filter})
		if err != nil {
			return 0, nil, err
		}

		// What LINSTOR answers the call on: the replicas built, and
		// once all are placed, ready. A REST server that waits for it
		// itself has already, and this returns at once.
		err = s.waitReplicas(ctx, name, int(filter.PlaceCount))
		if err != nil {
			return 0, nil, err
		}

		calls = append(calls, time.Since(callStart))
	}

	return time.Since(start), calls, nil
}

// TestBurstProvisioningLatency reproduces a burst of new replicated
// volumes through the REST API linstor-csi uses, with the controller
// and one real satellite resource reconciler per node: sixteen volumes
// at once behind csi-provisioner's ten workers, on nodes that already
// carry volumes of their own. It reports how long each CreateVolume
// took to have its replicas built and ready, which is when LINSTOR
// answers it, and fails when one took longer than the budget.
//
// It is a benchmark and takes minutes, so it runs only when
// BURST_BENCH is set. BURST_EXISTING sets how many volumes the nodes
// carry before the burst, BURST_DRBDADM_MS how long each drbdadm call
// takes, and BURST_BUDGET_MS the budget. A field cluster with about
// four hundred replicas is BURST_EXISTING=137; GOMAXPROCS stands in for
// the CPU limits the pods run under.
func TestBurstProvisioningLatency(t *testing.T) {
	if os.Getenv("BURST_BENCH") == "" {
		t.Skip("benchmark; set BURST_BENCH=1 to run it")
	}

	const burst = 16

	existing := envInt("BURST_EXISTING", 20)
	passDelay := time.Duration(envInt("BURST_DRBDADM_MS", 100)) * time.Millisecond
	budget := time.Duration(envInt("BURST_BUDGET_MS", 20000)) * time.Millisecond

	s := startBurstStack(t, []string{"n1", "n2", "n3"}, passDelay)
	ctx := t.Context()

	for i := range existing {
		took, _, err := s.createVolume(ctx, fmt.Sprintf("pvc-idle-%02d", i), s.nodes[i%len(s.nodes)])
		if err != nil {
			t.Fatalf("seed volume %d: %v", i, err)
		}

		t.Logf("seed volume %d: %v", i, took)
	}

	type result struct {
		name  string
		took  time.Duration
		calls []time.Duration
		err   error
	}

	jobs := make(chan int)
	results := make(chan result, burst)

	var wg sync.WaitGroup

	for range burstWorkers {
		wg.Go(func() {
			for i := range jobs {
				name := fmt.Sprintf("pvc-burst-%02d", i)
				took, calls, err := s.createVolume(ctx, name, s.nodes[i%len(s.nodes)])
				results <- result{name: name, took: took, calls: calls, err: err}
			}
		})
	}

	start := time.Now()

	for i := range burst {
		jobs <- i
	}

	close(jobs)
	wg.Wait()
	close(results)

	total := time.Since(start)

	var slow []string

	for r := range results {
		if r.err != nil {
			t.Errorf("%s: %v", r.name, r.err)

			continue
		}

		t.Logf("%s: CreateVolume %v (autoplace %v)", r.name, r.took.Round(time.Millisecond), r.calls)

		if r.took > budget {
			slow = append(slow, fmt.Sprintf("%s %v", r.name, r.took.Round(time.Millisecond)))
		}
	}

	t.Logf("burst of %d: %v", burst, total.Round(time.Millisecond))

	if len(slow) > 0 {
		sort.Strings(slow)
		t.Errorf("%d of %d CreateVolume calls took longer than %v: %v", len(slow), burst, budget, slow)
	}
}
