// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Both server binaries exit when NewManager fails, so whatever it needs from
// the API server at construction turns a server that is briefly unreachable at
// pod start into a CrashLoopBackOff. ctrl.NewManager asks it for nothing, and a
// pod waits on cache sync instead. Registering the field indexes resolved the
// REST mapping of each indexed kind, which is a discovery call, before Start.
//
// So construction is held against an address that refuses every connection:
// it has to succeed, and promptly.
func TestNewManagerDoesNotNeedTheAPIServer(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	unreachable := &rest.Config{Host: "https://127.0.0.1:1"}

	type built struct {
		err error
		ok  bool
	}

	done := make(chan built, 1)

	go func() {
		mgr, st, err := k8s.NewManager(unreachable, ctrl.Options{
			Scheme:                 scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
		})
		done <- built{err: err, ok: mgr != nil && st != nil}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("NewManager against an unreachable API server: %v; the binaries exit on this "+
				"and a brief outage at pod start becomes a crash loop", got.err)
		}

		if !got.ok {
			t.Fatal("NewManager returned no error and no manager or store")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("NewManager is still waiting on an unreachable API server after 10s")
	}
}

// NewManager maps blockstor's own kinds in process so that registering the
// indexes needs no discovery. A mapping that disagrees with the CRD the API
// server actually serves would send every request for that kind to the wrong
// path, so each CRD under config/crd/bases is resolved through the manager's
// mapper, against a server that refuses connections, and compared on plural
// and scope. A kind the in-process mapping lacks falls through to discovery
// and fails here.
func TestNewManagerMapsEveryCRDWithoutDiscovery(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	mgr, _, err := k8s.NewManager(&rest.Config{Host: "https://127.0.0.1:1"}, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	files, err := filepath.Glob(filepath.Join("..", "..", "..", "config", "crd", "bases", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("find the CRDs: %v (%d files)", err, len(files))
	}

	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}

		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal(raw, &crd); err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}

		wantScope := meta.RESTScopeNameRoot
		if crd.Spec.Scope == apiextensionsv1.NamespaceScoped {
			wantScope = meta.RESTScopeNameNamespace
		}

		for _, version := range crd.Spec.Versions {
			gk := schema.GroupKind{Group: crd.Spec.Group, Kind: crd.Spec.Names.Kind}

			mapping, err := mgr.GetRESTMapper().RESTMapping(gk, version.Name)
			if err != nil {
				t.Errorf("%s %s: not mapped without the API server: %v", gk, version.Name, err)

				continue
			}

			if mapping.Resource.Resource != crd.Spec.Names.Plural {
				t.Errorf("%s %s: mapped to resource %q, the CRD serves %q",
					gk, version.Name, mapping.Resource.Resource, crd.Spec.Names.Plural)
			}

			if mapping.Scope.Name() != wantScope {
				t.Errorf("%s %s: mapped with scope %q, the CRD is %q",
					gk, version.Name, mapping.Scope.Name(), wantScope)
			}
		}
	}
}
