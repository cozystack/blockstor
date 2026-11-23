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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

// Every pass of every replica on the node asks the cache which spellings
// of its definition the cached replicas carry. The satellite's manager is
// not built by storek8s.NewManager, so it registers the index that answers
// that itself; without it every pass copies every Resource in the cache.
//
// The informer refuses a second indexer under a name it already holds, so
// registering the index again fails exactly when the manager did register
// it.
func TestManagerIndexesCachedReplicasByDefinitionKey(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; envtest binaries are needed to build a manager")
	}

	_, thisFile, _, _ := runtime.Caller(0) //nolint:dogsled // only the file path is useful
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}

	t.Cleanup(func() { _ = env.Stop() })

	mgr, err := NewManager(cfg, &Config{
		NodeName: "node-a",
		Apply:    satellite.NewReconciler(satellite.ReconcilerConfig{StateDir: t.TempDir()}),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = mgr.GetFieldIndexer().IndexField(t.Context(), &blockstoriov1alpha1.Resource{},
		storek8s.FieldResourceDefinitionKey, func(client.Object) []string { return nil })
	if err == nil {
		t.Errorf("the satellite's cache carries no %s index", storek8s.FieldResourceDefinitionKey)
	}
}
