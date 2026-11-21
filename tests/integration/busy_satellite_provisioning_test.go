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
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
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
	"github.com/cozystack/blockstor/pkg/uevent"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// The satellite pod on the field cluster runs under a 500m CPU limit
// and spends it on the external tools it starts. What each tool costs
// was read off the satellite's cgroup there (CPU time of the child, not
// wall time, averaged over repeated runs); everything else costs
// busyCostDefault.
const (
	busyPool  = "zfs"
	busyRG    = "busy"
	busyVolKi = 1 << 20

	busyCPULimit = 0.5

	busyCostLsblk   = 370 * time.Millisecond
	busyCostLVM     = 90 * time.Millisecond
	busyCostDrbdadm = 32 * time.Millisecond
	busyCostZFS     = 13 * time.Millisecond
	busyCostDefault = 4 * time.Millisecond

	// busyCandidateDisks is how many empty disks lsblk lists on each
	// node, as on the field nodes: each costs a pvs in every scan.
	busyCandidateDisks = 8
)

// busyCPU is one satellite pod's CPU limit: the tools running at the
// same time share busyCPULimit cores evenly, the way the CFS quota
// throttles a container that wants more than its limit.
type busyCPU struct {
	mu     sync.Mutex
	active map[*busyJob]struct{}
	used   time.Duration
	byTool map[string]time.Duration

	// limited is off while the benchmark seeds the nodes, which is
	// not what it measures.
	limited atomic.Bool
}

// busyJob is one tool run: the CPU time it still needs.
type busyJob struct {
	left time.Duration
	done chan struct{}
}

// busyTick is how often busyCPU hands out its share.
const busyTick = 5 * time.Millisecond

func newBusyCPU() *busyCPU {
	return &busyCPU{active: map[*busyJob]struct{}{}, byTool: map[string]time.Duration{}}
}

func (c *busyCPU) spend(ctx context.Context, tool string, cost time.Duration) {
	if !c.limited.Load() {
		return
	}

	job := &busyJob{left: cost, done: make(chan struct{})}

	c.mu.Lock()
	c.active[job] = struct{}{}
	c.used += cost
	c.byTool[tool] += cost
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.active, job)
		c.mu.Unlock()
	case <-job.done:
	}
}

// run hands out busyCPULimit of every tick to the runs in flight.
func (c *busyCPU) run(ctx context.Context) {
	ticker := time.NewTicker(busyTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		c.mu.Lock()

		if n := len(c.active); n > 0 {
			share := time.Duration(float64(busyTick) * busyCPULimit / float64(n))

			for job := range c.active {
				job.left -= share
				if job.left <= 0 {
					close(job.done)
					delete(c.active, job)
				}
			}
		}

		c.mu.Unlock()
	}
}

func (c *busyCPU) usedSoFar() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.used
}

// tools returns the CPU each tool took since the last call, the
// largest first, and starts the count over.
func (c *busyCPU) tools() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := make([]string, 0, len(c.byTool))
	for name := range c.byTool {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool { return c.byTool[names[i]] > c.byTool[names[j]] })

	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s %v", name, c.byTool[name].Round(10*time.Millisecond)))
	}

	clear(c.byTool)

	return strings.Join(parts, ", ")
}

func busyCost(name string, args []string) time.Duration {
	switch name {
	case "lsblk":
		if slices.Contains(args, "-Pb") {
			return busyCostLsblk
		}
	case "pvs", "vgs":
		return busyCostLVM
	case "drbdadm":
		return busyCostDrbdadm
	case "zfs", "zpool":
		return busyCostZFS
	}

	return busyCostDefault
}

// busyTool names a tool run for the per-tool CPU report: the tool and
// its subcommand.
func busyTool(name string, args []string) string {
	for _, arg := range args {
		if arg != "" && !strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "/") && !strings.Contains(arg, "pvc-") {
			return name + " " + arg
		}
	}

	return name
}

// errBusyTool is a tool run that fails on the field nodes: udevadm is
// not in the satellite image, and drbdmeta finds no metadata on an
// empty disk.
var errBusyTool = errors.New("exit status 1")

// busyExec runs a satellite's tools against busyCPU. A drbdmeta run on
// a zvol opens it read-write, and udev answers the close with a change
// uevent for it, which reaches the satellite's discovery loop.
type busyExec struct {
	cpu     *busyCPU
	uevents chan uevent.Event
	lsblk   []byte
}

func (e *busyExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	e.cpu.spend(ctx, busyTool(name, args), busyCost(name, args))

	switch name {
	case "udevadm":
		return nil, errBusyTool
	case "lsblk":
		if slices.Contains(args, "-Pb") {
			return e.lsblk, nil
		}
	case "drbdmeta":
		return e.drbdmeta(args)
	}

	return nil, nil
}

func (e *busyExec) RunWithStdin(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	return e.Run(ctx, name, args...)
}

func (e *busyExec) drbdmeta(args []string) ([]byte, error) {
	for _, arg := range args {
		if !strings.HasPrefix(arg, "/dev/zvol/") {
			continue
		}

		select {
		case e.uevents <- uevent.Event{Action: "change", Subsystem: "block", Kernel: "zd0", Devname: "zd0"}:
		default:
		}
	}

	switch {
	case slices.Contains(args, "get-gi"):
		return []byte("9F1D3C5A7E2B4D60:0000000000000000:0000000000000000:0000000000000000\n"), nil
	case slices.Contains(args, "dump-md"):
		return nil, errBusyTool
	}

	return nil, nil
}

// busyUevents is the uevent listener of one node.
type busyUevents struct{ ch chan uevent.Event }

func (u busyUevents) Events() <-chan uevent.Event { return u.ch }

// busyProvider is a ZFS pool whose calls cost what a zfs run does.
type busyProvider struct{ cpu *busyCPU }

func (busyProvider) Kind() string { return "ZFS_THIN" }

func (busyProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{TotalCapacityKib: 1 << 40, FreeCapacityKib: 1 << 40}, nil
}

func (p busyProvider) CreateVolume(ctx context.Context, _ storage.Volume) error {
	p.cpu.spend(ctx, "zfs create", busyCostZFS)

	return nil
}

func (busyProvider) DeleteVolume(_ context.Context, _ storage.Volume) error { return nil }

func (busyProvider) ResizeVolume(_ context.Context, _ storage.Volume) error { return nil }

func (p busyProvider) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	p.cpu.spend(ctx, "zfs list", busyCostZFS)

	return storage.VolumeStatus{DevicePath: busyZvol(vol.ResourceName), UsableKib: vol.SizeKib}, nil
}

func (busyProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (busyProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (busyProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}

func busyZvol(rdName string) string { return "/dev/zvol/" + busyPool + "/" + rdName + "_00000" }

// errBusyCall is a REST call of the benchmark the server refused.
var errBusyCall = errors.New("REST call refused")

// busyStack is the controller, the REST server and, on every node, the
// satellite's resource reconciler, events2 observer and disk discovery,
// all sharing the node's busyCPU.
type busyStack struct {
	cli     client.Client
	restURL string
	nodes   []string
	cpus    map[string]*busyCPU
	execs   map[string]*busyExec
	idle    atomic.Bool
}

func startBusyStack(t *testing.T, nodes []string) *busyStack {
	t.Helper()

	env := harness.Start(t)

	// The satellites write their state until they stop, so their
	// directories go only after them.
	stateDirs := map[string]string{}
	for _, node := range nodes {
		stateDirs[node] = t.TempDir()
	}

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

	wireBusyController(t, mgr, st)

	addr := busyFreeAddr(t)

	err = mgr.Add(&rest.Server{
		Addr: addr, Store: st, Client: mgr.GetClient(), Namespace: "default", SecretReader: mgr.GetAPIReader(),
	})
	if err != nil {
		t.Fatalf("rest: %v", err)
	}

	busyRun(ctx, t, &wg, mgr)

	s := &busyStack{
		cli: env.Client, restURL: "http://" + addr, nodes: nodes,
		cpus: map[string]*busyCPU{}, execs: map[string]*busyExec{},
	}

	for _, node := range nodes {
		cpu := newBusyCPU()
		wg.Go(func() { cpu.run(ctx) })
		ex := &busyExec{cpu: cpu, uevents: make(chan uevent.Event, 1024), lsblk: busyLsblk()}
		s.cpus[node], s.execs[node] = cpu, ex

		satMgr, err := ctrl.NewManager(env.Cfg, opts)
		if err != nil {
			t.Fatalf("satellite manager %s: %v", node, err)
		}

		rec := &satcontrollers.ResourceReconciler{
			Client: satMgr.GetClient(),
			Config: satcontrollers.Config{
				NodeName: node,
				Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
					Providers: map[string]storage.Provider{busyPool: busyProvider{cpu: cpu}},
					NodeName:  node,
					Adm:       drbd.NewAdm(ex),
					Exec:      ex,
					StateDir:  stateDirs[node],
				}),
				Exec:      ex,
				APIReader: satMgr.GetAPIReader(),
			},
		}

		err = rec.SetupWithManager(satMgr)
		if err != nil {
			t.Fatalf("satellite %s: %v", node, err)
		}

		busyRun(ctx, t, &wg, satMgr)
	}

	s.seed(ctx, t)

	wg.Go(func() { s.kernel(ctx) })

	return s
}

func wireBusyController(t *testing.T, mgr manager.Manager, st *storek8s.Store) {
	t.Helper()

	steps := []func(ctrl.Manager) error{
		(&controller.NodeReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Store: st}).SetupWithManager,
		(&controller.StoragePoolReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager,
		(&controller.ResourceGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Store: st}).SetupWithManager,
		(&controller.ResourceDefinitionReconciler{
			Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Store: st, APIReader: mgr.GetAPIReader(),
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

func busyRun(ctx context.Context, t *testing.T, wg *sync.WaitGroup, run manager.Runnable) {
	t.Helper()

	wg.Go(func() {
		err := run.Start(ctx)
		if err != nil && ctx.Err() == nil {
			t.Errorf("runnable: %v", err)
		}
	})
}

func busyFreeAddr(t *testing.T) string {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := listener.Addr().String()
	_ = listener.Close()

	return addr
}

// busyLsblk is what lsblk lists on a field node that matters to the
// discovery loop: empty disks that pass lsblk's own checks.
func busyLsblk() []byte {
	var b strings.Builder

	for i := range busyCandidateDisks {
		name := "sd" + string(rune('b'+i))
		fmt.Fprintf(&b, "NAME=%q KNAME=%q PKNAME=\"\" MAJ:MIN=\"8:%d\" SIZE=\"1800000000000\" FSTYPE=\"\" "+
			"TYPE=\"disk\" MOUNTPOINT=\"\" WWN=\"\" MODEL=\"disk\" SERIAL=\"s%d\" ROTA=\"0\" TRAN=\"sas\"\n",
			name, name, 16*(i+1), i)
	}

	return []byte(b.String())
}

func (s *busyStack) seed(ctx context.Context, t *testing.T) {
	t.Helper()

	for i, node := range s.nodes {
		busyMustCreate(ctx, t, s.cli, &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: node},
			Spec: blockstoriov1alpha1.NodeSpec{
				Type: "SATELLITE",
				NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{{
					Name: "default", Address: fmt.Sprintf("10.0.0.%d", i+1),
					SatellitePort: 3366, SatelliteEncryptionType: "PLAIN",
				}},
			},
		})

		busyMustCreate(ctx, t, s.cli, &blockstoriov1alpha1.StoragePool{
			ObjectMeta: metav1.ObjectMeta{Name: busyPool + "." + node},
			Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: node, PoolName: busyPool, ProviderKind: "ZFS_THIN"},
		})
	}

	busyMustCreate(ctx, t, s.cli, &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: busyRG},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				PlaceCount: int32(len(s.nodes)), StoragePoolList: []string{busyPool},
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

func (s *busyStack) stampNodesAndPools(ctx context.Context) {
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
			Reason: "Busy", Message: "busy", LastTransitionTime: now,
		}}
		_ = s.cli.Status().Update(ctx, &node)

		var pool blockstoriov1alpha1.StoragePool
		if s.cli.Get(ctx, client.ObjectKey{Name: busyPool + "." + name}, &pool) != nil || pool.Status.FreeCapacity > 0 {
			continue
		}

		pool.Status.FreeCapacity = 1 << 40
		pool.Status.TotalCapacity = 1 << 40
		pool.Status.SupportsSnapshots = true
		pool.Status.StaticTraits = map[string]string{"kind": "ZFS_THIN"}
		_ = s.cli.Status().Update(ctx, &pool)
	}
}

// kernel plays DRBD for the volumes the benchmark creates: a replica
// whose device the satellite reported comes up UpToDate and connected
// to every peer whose device is reported too. The idle volumes are left
// to the observers once they run.
func (s *busyStack) kernel(ctx context.Context) {
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
			if busyDeviceReported(res) {
				built[res.Spec.ResourceDefinitionName] = append(built[res.Spec.ResourceDefinitionName], res.Spec.NodeName)
			}
		}

		for i := range list.Items {
			res := &list.Items[i]
			if !busyDeviceReported(res) || (s.idle.Load() && strings.HasPrefix(res.Spec.ResourceDefinitionName, "pvc-idle-")) {
				continue
			}

			s.bringUp(ctx, res, built[res.Spec.ResourceDefinitionName])
		}
	}
}

func busyDeviceReported(res *blockstoriov1alpha1.Resource) bool {
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

func (s *busyStack) bringUp(ctx context.Context, res *blockstoriov1alpha1.Resource, peers []string) {
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

func busyMustCreate(ctx context.Context, t *testing.T, cli client.Client, obj client.Object) {
	t.Helper()

	err := cli.Create(ctx, obj)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create %s: %v", obj.GetName(), err)
	}
}

func (s *busyStack) post(ctx context.Context, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.restURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("request %s: %w", path, err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", path, err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%w: %s: %d %s", errBusyCall, path, resp.StatusCode, raw)
	}

	return nil
}

// waitReplicas waits until the definition has want replicas with their
// devices reported and, for more than one, all UpToDate and connected.
func (s *busyStack) waitReplicas(ctx context.Context, rdName string, want int) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	for {
		var list blockstoriov1alpha1.ResourceList

		err := s.cli.List(ctx, &list)
		if err == nil && busyReplicasDone(&list, rdName, want) {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("replicas of %s not done: %w", rdName, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func busyReplicasDone(list *blockstoriov1alpha1.ResourceList, rdName string, want int) bool {
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
		if !busyDeviceReported(res) {
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
// volume, the replica on the preferred node, then the rest, each
// placement answered once its replicas are built and ready.
func (s *busyStack) createVolume(ctx context.Context, name, preferred string) (time.Duration, error) {
	start := time.Now()

	err := s.post(ctx, "/v1/resource-definitions", apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: name, ResourceGroupName: busyRG},
	})
	if err != nil {
		return 0, err
	}

	err = s.post(ctx, "/v1/resource-definitions/"+name+"/volume-definitions", apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{SizeKib: busyVolKi},
	})
	if err != nil {
		return 0, err
	}

	for _, filter := range []apiv1.AutoSelectFilter{
		{PlaceCount: 1, NodeNameList: []string{preferred}, StoragePool: busyPool},
		{PlaceCount: apiv1.LaxInt32(len(s.nodes)), StoragePool: busyPool},
	} {
		err := s.post(ctx, "/v1/resource-definitions/"+name+"/autoplace", apiv1.AutoPlaceRequest{SelectFilter: filter})
		if err != nil {
			return 0, err
		}

		err = s.waitReplicas(ctx, name, int(filter.PlaceCount))
		if err != nil {
			return 0, err
		}
	}

	return time.Since(start), nil
}

// startNodeAgents starts, on every node, the satellite's events2
// observer and its disk discovery, the way the satellite binary runs
// them. drbdsetup on PATH stands in for the kernel: it lists the idle
// volumes UpToDate and connected, the way events2 opens on a node
// that holds them, and stays quiet afterwards.
func (s *busyStack) startNodeAgents(ctx context.Context, t *testing.T, idle []string) {
	t.Helper()

	dir := t.TempDir()

	for _, node := range s.nodes {
		var b strings.Builder

		for minor, rd := range idle {
			fmt.Fprintf(&b, "exists resource name:%s role:Secondary suspended:no force-io-failures:no may_promote:no\n", rd)

			for id, peer := range s.nodes {
				if peer == node {
					continue
				}

				fmt.Fprintf(&b, "exists connection name:%s peer-node-id:%d conn-name:%s connection:Connected role:Secondary\n",
					rd, id, peer)
			}

			fmt.Fprintf(&b, "exists device name:%s volume:0 minor:%d disk:UpToDate client:no backing_dev:%s quorum:yes\n",
				rd, 1000+minor, busyZvol(rd))

			for id, peer := range s.nodes {
				if peer == node {
					continue
				}

				fmt.Fprintf(&b, "exists peer-device name:%s peer-node-id:%d conn-name:%s volume:0 "+
					"replication:Established peer-disk:UpToDate peer-client:no\n", rd, id, peer)
			}
		}

		b.WriteString("exists -\n")

		err := os.WriteFile(filepath.Join(dir, "events-"+node), []byte(b.String()), 0o600)
		if err != nil {
			t.Fatalf("events2 dump: %v", err)
		}
	}

	script := "#!/bin/sh\n[ \"$1\" = events2 ] || exit 1\n" +
		"for n in " + strings.Join(s.nodes, " ") + "; do\n" +
		"  if mkdir \"" + dir + "/claim-$n\" 2>/dev/null; then cat \"" + dir + "/events-$n\"; exec sleep 3600; fi\n" +
		"done\nexec sleep 3600\n"

	err := os.WriteFile(filepath.Join(dir, "drbdsetup"), []byte(script), 0o700)
	if err != nil {
		t.Fatalf("drbdsetup stand-in: %v", err)
	}

	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	s.idle.Store(true)

	var wg sync.WaitGroup

	t.Cleanup(wg.Wait)

	for _, node := range s.nodes {
		ex := s.execs[node]

		observer := &satcontrollers.ObserverRunnable{Client: s.cli, Exec: ex, NodeName: node}
		busyRun(ctx, t, &wg, observer)

		// One stream claims one node's dump; start the next observer
		// only once this one has its own.
		for {
			_, err := os.Stat(filepath.Join(dir, "claim-"+node))
			if err == nil {
				break
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}

		discovery := &satcontrollers.PhysicalDeviceDiscoveryRunnable{
			Client: s.cli, Exec: ex, NodeName: node, Uevent: busyUevents{ch: ex.uevents},
		}
		busyRun(ctx, t, &wg, discovery)
	}
}

// TestSingleVolumeOnBusySatellites reproduces the field's time to
// provision a single replicated volume: three nodes that already hold
// about a hundred replicas each, each satellite limited to the 500m CPU
// it runs under in the field, with its events2 observer and disk
// discovery running beside the resource reconciler. It creates volumes
// one at a time the way linstor-csi does and fails when one took longer
// than the budget; LINSTOR answers the same CreateVolume in a few
// seconds.
//
// It is a benchmark and takes minutes, so it runs only when BURST_BENCH
// is set. BUSY_EXISTING sets how many volumes each node holds before
// (the field nodes hold 115), BUSY_VOLUMES how many are measured and
// BUSY_BUDGET_MS the budget for each.
func TestSingleVolumeOnBusySatellites(t *testing.T) {
	if os.Getenv("BURST_BENCH") == "" {
		t.Skip("benchmark; set BURST_BENCH=1 to run it")
	}

	existing := busyEnvInt("BUSY_EXISTING", 115)
	volumes := busyEnvInt("BUSY_VOLUMES", 3)
	budget := time.Duration(busyEnvInt("BUSY_BUDGET_MS", 8000)) * time.Millisecond

	if os.Getenv("BUSY_LOG") != "" {
		ctrl.SetLogger(zap.New(zap.WriteTo(os.Stdout)))
	}

	s := startBusyStack(t, []string{"n1", "n2", "n3"})
	ctx := t.Context()

	idle := make([]string, existing)
	seeded := time.Now()

	var seeders sync.WaitGroup

	jobs := make(chan int)

	for range 10 {
		seeders.Go(func() {
			for i := range jobs {
				idle[i] = fmt.Sprintf("pvc-idle-%03d", i)

				_, err := s.createVolume(ctx, idle[i], s.nodes[i%len(s.nodes)])
				if err != nil {
					t.Errorf("seed volume %d: %v", i, err)
				}
			}
		})
	}

	for i := range existing {
		jobs <- i
	}

	close(jobs)
	seeders.Wait()

	if t.Failed() {
		t.FailNow()
	}

	t.Logf("seeded %d volumes in %v", existing, time.Since(seeded).Round(time.Second))

	for _, cpu := range s.cpus {
		cpu.limited.Store(true)
	}

	s.startNodeAgents(ctx, t, idle)

	// Let the agents settle into their steady state first: the dump,
	// its first writes and a few resync rounds.
	time.Sleep(30 * time.Second)

	before := map[string]time.Duration{}
	for node, cpu := range s.cpus {
		before[node] = cpu.usedSoFar()
		cpu.tools()
	}

	quiet := time.Now()

	time.Sleep(10 * time.Second)

	for _, node := range s.nodes {
		used := s.cpus[node].usedSoFar() - before[node]
		t.Logf("%s idle: %.0f%% of its CPU limit (%s)", node,
			100*used.Seconds()/(time.Since(quiet).Seconds()*busyCPULimit), s.cpus[node].tools())
	}

	var slow []string

	for i := range volumes {
		name := fmt.Sprintf("pvc-new-%02d", i)

		took, err := s.createVolume(ctx, name, s.nodes[i%len(s.nodes)])
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		t.Logf("%s: CreateVolume %v", name, took.Round(time.Millisecond))

		if took > budget {
			slow = append(slow, name+" "+took.Round(time.Millisecond).String())
		}
	}

	if len(slow) > 0 {
		t.Errorf("%d of %d CreateVolume calls took longer than %v: %v", len(slow), volumes, budget, slow)
	}
}

func busyEnvInt(name string, def int) int {
	v, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return def
	}

	return v
}
