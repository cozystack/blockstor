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

// Package manifests holds pure-Go tests that parse the YAML in
// stand/ and assert deployment-shape invariants that are easy to
// regress by hand-editing a manifest. They run as plain `go test
// ./tests/manifests/...` — no envtest, no cluster, no build tags.
package manifests

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"sigs.k8s.io/yaml"
)

// probe captures the subset of corev1.Probe we care about — the
// readiness/liveness wire-up. Using a hand-rolled struct keeps the
// test free of a transitive corev1 dependency and avoids the
// JSON-vs-YAML field-name quirks of sigs.k8s.io/yaml when only a
// couple of fields matter.
type probe struct {
	HTTPGet *struct {
		Path string `json:"path"`
		Port any    `json:"port"`
	} `json:"httpGet,omitempty"`
	TCPSocket *struct {
		Port any `json:"port"`
	} `json:"tcpSocket,omitempty"`
}

type container struct {
	Name           string `json:"name"`
	ReadinessProbe *probe `json:"readinessProbe,omitempty"`
	LivenessProbe  *probe `json:"livenessProbe,omitempty"`
}

type podSpec struct {
	Containers []container `json:"containers"`
}

type podTemplate struct {
	Spec podSpec `json:"spec"`
}

type deploymentSpec struct {
	Template podTemplate `json:"template"`
}

type k8sObject struct {
	Kind     string         `json:"kind"`
	Metadata map[string]any `json:"metadata"`
	Spec     deploymentSpec `json:"spec"`
}

// repoRoot resolves the repository root from the test source file
// path so the test runs from any cwd (`go test ./...`, IDE per-test,
// CI matrix). runtime.Caller(0) returns this file's path; walk up
// two levels (tests/manifests -> tests -> repo root).
func repoRoot(t *testing.T) string {
	t.Helper()

	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed to resolve test source path")
	}

	return filepath.Clean(filepath.Join(filepath.Dir(here), "..", ".."))
}

// loadDeployment parses the named manifest from stand/, splits on
// `---`, and returns the first object with kind=Deployment. The test
// suite only inspects Deployment shape; ServiceAccount/Role/Service
// docs in the same file are skipped.
func loadDeployment(t *testing.T, relPath string) k8sObject {
	t.Helper()

	full := filepath.Join(repoRoot(t), relPath)

	raw, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read %s: %v", full, err)
	}

	for _, doc := range bytes.Split(raw, []byte("\n---")) {
		doc = bytes.TrimSpace(doc)
		if len(doc) == 0 {
			continue
		}

		var obj k8sObject

		err := yaml.Unmarshal(doc, &obj)
		if err != nil {
			t.Fatalf("yaml.Unmarshal %s: %v\n---\n%s", full, err, doc)
		}

		if obj.Kind == "Deployment" {
			return obj
		}
	}

	t.Fatalf("no Deployment object found in %s", full)

	return k8sObject{}
}

// TestApiserverReadinessProbeIsReadyzHTTPGet is the issue-218 witness.
//
// Pre-fix: stand/blockstor-apiserver-deploy.yaml has
//
//	readinessProbe:
//	  tcpSocket: {port: 3370}
//
// The /readyz gate from issue 213 (cmd/apiserver) is in place but
// the manifest never asks for it — kube-proxy sees the pod Ready as
// soon as the TCP listener binds, regardless of whether the manager
// cache has synced. The gate is inert at runtime.
//
// Post-fix: readinessProbe must use httpGet on /readyz, port=health
// (8081), matching the canonical pattern in
// stand/blockstor-satellite-daemonset.yaml.
func TestApiserverReadinessProbeIsReadyzHTTPGet(t *testing.T) {
	t.Parallel()

	dep := loadDeployment(t, "stand/blockstor-apiserver-deploy.yaml")

	if len(dep.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("apiserver Deployment has no containers")
	}

	c := dep.Spec.Template.Spec.Containers[0]

	if c.ReadinessProbe == nil {
		t.Fatal("apiserver container has no readinessProbe")
	}

	if c.ReadinessProbe.TCPSocket != nil {
		t.Fatalf("apiserver readinessProbe still uses tcpSocket (issue 218): %+v",
			c.ReadinessProbe.TCPSocket)
	}

	if c.ReadinessProbe.HTTPGet == nil {
		t.Fatal("apiserver readinessProbe must use httpGet on /readyz (issue 218)")
	}

	if c.ReadinessProbe.HTTPGet.Path != "/readyz" {
		t.Fatalf("apiserver readinessProbe.httpGet.path = %q, want /readyz",
			c.ReadinessProbe.HTTPGet.Path)
	}

	// The probe must target the healthz container port (named
	// `health`, container port 8081) — NOT the REST port 3370.
	// /readyz is mounted by the controller-runtime manager on its
	// health-probe listener, not by the REST server. Accept either
	// the port name or the numeric form.
	got := stringifyPort(c.ReadinessProbe.HTTPGet.Port)
	if got != "health" && got != "8081" {
		t.Fatalf("apiserver readinessProbe.httpGet.port = %q, want health or 8081", got)
	}
}

// TestApiserverLivenessProbeIsHealthzHTTPGet is the second half of
// issue 218. kubelet uses livenessProbe to *restart* a wedged pod —
// if the apiserver's manager deadlocks but the TCP listener stays
// bound, a tcpSocket liveness would never trip and the pod would
// stay broken forever. /healthz on the manager surface is the
// canonical signal (matches satellite + controller deployments).
func TestApiserverLivenessProbeIsHealthzHTTPGet(t *testing.T) {
	t.Parallel()

	dep := loadDeployment(t, "stand/blockstor-apiserver-deploy.yaml")

	if len(dep.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("apiserver Deployment has no containers")
	}

	c := dep.Spec.Template.Spec.Containers[0]

	if c.LivenessProbe == nil {
		t.Fatal("apiserver container has no livenessProbe")
	}

	if c.LivenessProbe.TCPSocket != nil {
		t.Fatalf("apiserver livenessProbe must not use tcpSocket: %+v",
			c.LivenessProbe.TCPSocket)
	}

	if c.LivenessProbe.HTTPGet == nil {
		t.Fatal("apiserver livenessProbe must use httpGet on /healthz")
	}

	if c.LivenessProbe.HTTPGet.Path != "/healthz" {
		t.Fatalf("apiserver livenessProbe.httpGet.path = %q, want /healthz",
			c.LivenessProbe.HTTPGet.Path)
	}

	got := stringifyPort(c.LivenessProbe.HTTPGet.Port)
	if got != "health" && got != "8081" {
		t.Fatalf("apiserver livenessProbe.httpGet.port = %q, want health or 8081", got)
	}
}

// stringifyPort accepts either a string (named port) or a number
// (numeric port) from the decoded YAML and renders the canonical
// string form. sigs.k8s.io/yaml decodes numbers as float64 by
// default, hence the float branch.
func stringifyPort(p any) string {
	switch v := p.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return ""
	}
}
