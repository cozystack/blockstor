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

package linstormigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	lm "github.com/cozystack/blockstor/pkg/linstormigrate"
)

// TestConvertedManifestsPassCRDValidation boots a real kube-apiserver
// (envtest), installs blockstor's actual CRDs, and server-side-applies
// EVERY converted object. This proves the migrator's output satisfies
// the real OpenAPI schema + every CEL XValidation rule (composite
// <rd>.<node> / <pool>.<node> / <rd>.<snap> names, settable-once
// drbdPort/drbdNodeID/initialized, enum constraints) — a plain Go
// struct build cannot catch a CEL violation, only the apiserver can.
//
// The synthetic fixture (testdata/dump) always runs. The two
// production dumps are validated too WHEN PRESENT at /tmp/infra and
// /tmp/hidora (or $LINSTOR_DUMP_DIRS, colon-separated) — they carry
// real secrets and never enter the repo, so their absence skips that
// leg instead of failing.
//
// Requires envtest assets: run under `make test` or with
// KUBEBUILDER_ASSETS set (the Makefile's setup-envtest target).
func TestConvertedManifestsPassCRDValidation(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
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

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	t.Run("synthetic-fixture", func(t *testing.T) {
		res := loadAndConvert(t, filepath.Join("testdata", "dump"))
		applyAll(t, k8s, res)
	})

	for _, dir := range dumpDirs() {
		if _, statErr := os.Stat(dir); statErr != nil {
			t.Logf("skipping production dump %s (absent)", dir)

			continue
		}

		t.Run("production-dump:"+filepath.Base(dir), func(t *testing.T) {
			res := loadAndConvert(t, dir)
			applyAll(t, k8s, res)
		})
	}
}

func dumpDirs() []string {
	if env := os.Getenv("LINSTOR_DUMP_DIRS"); env != "" {
		return filepath.SplitList(env)
	}

	return []string{"/tmp/infra", "/tmp/hidora"}
}

func loadAndConvert(t *testing.T, dir string) *lm.Result {
	t.Helper()

	dump, err := lm.LoadDump(dir)
	if err != nil {
		t.Fatalf("LoadDump(%s): %v", dir, err)
	}

	res, err := lm.Convert(dump)
	if err != nil {
		t.Fatalf("Convert(%s): %v", dir, err)
	}

	return res
}

// applyAll server-side-applies every converted object into a fresh
// apiserver and fails on the first rejection (schema or CEL). Uses a
// unique field-manager so SSA owns the whole object.
func applyAll(t *testing.T, k8s client.Client, res *lm.Result) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	objs := make([]client.Object, 0)

	if res.ControllerConfig != nil {
		objs = append(objs, res.ControllerConfig)
	}

	for i := range res.Nodes {
		objs = append(objs, &res.Nodes[i])
	}

	for i := range res.StoragePools {
		objs = append(objs, &res.StoragePools[i])
	}

	for i := range res.ResourceGroups {
		objs = append(objs, &res.ResourceGroups[i])
	}

	for i := range res.ResourceDefinitions {
		objs = append(objs, &res.ResourceDefinitions[i])
	}

	for i := range res.Resources {
		objs = append(objs, &res.Resources[i])
	}

	for i := range res.Snapshots {
		objs = append(objs, &res.Snapshots[i])
	}

	applied := 0

	for _, obj := range objs {
		// Deep-copy so the SSA patch doesn't mutate the shared object
		// (envtest stamps managedFields / resourceVersion back onto it).
		patch := obj.DeepCopyObject().(client.Object)

		err := k8s.Patch(ctx, patch,
			client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
			client.FieldOwner("linstor-migrate-validation"), client.ForceOwnership)
		if err != nil {
			t.Errorf("CRD validation rejected %T %q: %v",
				obj, obj.GetName(), err)

			continue
		}

		applied++
	}

	t.Logf("applied %d/%d objects with zero validation errors", applied, len(objs))

	if applied != len(objs) {
		t.Fatalf("%d object(s) failed CRD validation", len(objs)-applied)
	}
}

// ensure metav1 import is used (SSA objects carry TypeMeta the
// converter already sets; this keeps the import explicit for readers).
var _ = metav1.TypeMeta{}
