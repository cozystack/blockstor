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

package controller_test

import (
	"context"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// blockingReader holds the uncached read of one Resource until the
// test lets it go, keeping that Resource's reconcile in flight.
type blockingReader struct {
	client.Reader

	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*blockstoriov1alpha1.Resource); ok && key.Name == b.name {
		b.once.Do(func() { close(b.entered) })

		select {
		case <-b.release:
		case <-ctx.Done():
		}
	}

	return b.Reader.Get(ctx, key, obj, opts...)
}

// TestNewReplicaAllocatedWhileAnotherReconciles runs the resource
// controller as the binary does and keeps one replica's reconcile in
// flight. A replica created meanwhile must still get its DRBD
// identities: the satellites build nothing before those exist, and
// with a single worker every new replica of a burst waited behind the
// reconciles of all the others, long enough for the CSI provisioner to
// give up on the volume.
func TestNewReplicaAllocatedWhileAnotherReconciles(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cli, mgr := startWorkersEnvtest(t)

	reader := &blockingReader{
		Reader:  mgr.GetAPIReader(),
		name:    "pvc-held.w1",
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	rec := &controllerpkg.ResourceReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), APIReader: reader}
	if err := rec.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup: %v", err)
	}

	var wg sync.WaitGroup

	defer func() {
		cancel()
		close(reader.release)
		wg.Wait()
	}()

	wg.Go(func() {
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	createReplica(ctx, t, cli, "pvc-held", "w1")

	select {
	case <-reader.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the held replica was never reconciled")
	}

	createReplica(ctx, t, cli, "pvc-new", "w1")

	deadline := time.Now().Add(15 * time.Second)

	for {
		var res blockstoriov1alpha1.Resource
		if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-new.w1"}, &res); err != nil {
			t.Fatalf("get: %v", err)
		}

		if res.Spec.DRBDNodeID != nil && res.Spec.DRBDPort != nil {
			return
		}

		if time.Now().After(deadline) {
			t.Fatal("new replica got no DRBD identity while another replica's reconcile was in flight")
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func TestResourceWorkersDefault(t *testing.T) {
	t.Parallel()

	if controllerpkg.DefaultResourceWorkers < 2 {
		t.Errorf("default of %d workers queues a new replica's allocation behind every status report",
			controllerpkg.DefaultResourceWorkers)
	}
}

func createReplica(ctx context.Context, t *testing.T, cli client.Client, rdName, node string) {
	t.Helper()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1 << 20}},
		},
	}
	if err := cli.Create(ctx, rd); err != nil {
		t.Fatalf("create definition: %v", err)
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rdName, NodeName: node},
	}
	if err := cli.Create(ctx, res); err != nil {
		t.Fatalf("create replica: %v", err)
	}
}

func startWorkersEnvtest(t *testing.T) (client.Client, ctrl.Manager) {
	t.Helper()

	_, thisFile, _, _ := goruntime.Caller(0) //nolint:dogsled // only the file path is useful
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest: %v", err)
	}

	t.Cleanup(func() { _ = env.Stop() })

	scheme := newScheme(t)

	cli, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	// Controller names are registered process-wide for their metrics.
	skipNameValidation := true

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	return cli, mgr
}
