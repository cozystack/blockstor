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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cozystack/blockstor/internal/cli"
)

const twoClusters = `apiVersion: v1
kind: Config
clusters:
- name: prod
  cluster: {server: "https://prod.invalid:6443"}
- name: staging
  cluster: {server: "https://staging.invalid:6443"}
users:
- name: u
  user: {token: "t"}
contexts:
- name: prod
  context: {cluster: prod, user: u}
- name: staging
  context: {cluster: staging, user: u}
current-context: prod
`

// --kubeconfig and --context pick the file and the context, as kubectl's
// flags do, instead of the current context of the ambient kubeconfig.
func TestRestConfigHonoursKubeconfigAndContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")

	err := os.WriteFile(path, []byte(twoClusters), 0o600)
	if err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "absent"))

	for want, target := range map[string]cli.KubeTarget{
		"https://prod.invalid:6443":    {Kubeconfig: path},
		"https://staging.invalid:6443": {Kubeconfig: path, Context: "staging"},
	} {
		cfg, err := restConfig(target)
		if err != nil {
			t.Fatalf("%+v: %v", target, err)
		}

		if cfg.Host != want {
			t.Errorf("%+v: host %q, want %q", target, cfg.Host, want)
		}
	}
}
