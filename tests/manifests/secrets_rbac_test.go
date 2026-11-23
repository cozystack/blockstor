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

package manifests

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// blockstorSecrets are the Secrets blockstor reads by name: the keyring,
// the generated unlock Secret, and the passphrase Secret under piraeus's
// name.
func blockstorSecrets() []string {
	return []string{"blockstor-master-key", "blockstor-unlock", "linstor-passphrase"}
}

func secretsManifests(t *testing.T, path string) ([]rbacv1.Role, []rbacv1.ClusterRole, []rbacv1.RoleBinding) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(repoRoot240(t), path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var (
		roles    []rbacv1.Role
		cluster  []rbacv1.ClusterRole
		bindings []rbacv1.RoleBinding
	)

	for doc := range strings.SplitSeq(string(raw), "\n---") {
		var meta struct{ Kind string }

		if yaml.Unmarshal([]byte(doc), &meta) != nil {
			continue
		}

		switch meta.Kind {
		case "Role":
			var role rbacv1.Role
			if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
				t.Fatalf("%s: %v", path, err)
			}

			roles = append(roles, role)
		case "ClusterRole":
			var role rbacv1.ClusterRole
			if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
				t.Fatalf("%s: %v", path, err)
			}

			cluster = append(cluster, role)
		case "RoleBinding":
			var binding rbacv1.RoleBinding
			if err := yaml.Unmarshal([]byte(doc), &binding); err != nil {
				t.Fatalf("%s: %v", path, err)
			}

			bindings = append(bindings, binding)
		}
	}

	return roles, cluster, bindings
}

// Secrets are readable only by name, only the ones blockstor uses, and
// only in its namespace: under Cozystack that namespace is shared with
// piraeus, whose TLS keys must stay out of reach. Every such Role is bound.
func TestSecretAccessIsLimitedToBlockstorsSecrets(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"stand/blockstor-satellite-daemonset.yaml",
		"stand/blockstor-deploy.yaml",
		"stand/blockstor-apiserver-deploy.yaml",
		"config/rbac/satellite_role.yaml",
		"config/rbac/secrets_role.yaml",
	} {
		roles, cluster, bindings := secretsManifests(t, path)

		for _, role := range cluster {
			for _, rule := range role.Rules {
				if slices.Contains(rule.Resources, "secrets") {
					t.Errorf("%s: ClusterRole %s grants Secrets cluster-wide", path, role.Name)
				}
			}
		}

		secretRoles := 0

		for _, role := range roles {
			grants := false

			for _, rule := range role.Rules {
				if !slices.Contains(rule.Resources, "secrets") {
					continue
				}

				grants = true

				readsOrWrites := slices.ContainsFunc(rule.Verbs, func(v string) bool { return v != "create" })
				if readsOrWrites && !slices.Equal(rule.ResourceNames, blockstorSecrets()) {
					t.Errorf("%s: Role %s verbs %v on Secrets %v, want only %v",
						path, role.Name, rule.Verbs, rule.ResourceNames, blockstorSecrets())
				}

				if slices.Contains(rule.Verbs, "list") || slices.Contains(rule.Verbs, "watch") || slices.Contains(rule.Verbs, "*") {
					t.Errorf("%s: Role %s lists or watches Secrets", path, role.Name)
				}
			}

			if !grants {
				continue
			}

			secretRoles++

			bound := slices.ContainsFunc(bindings, func(b rbacv1.RoleBinding) bool {
				return b.RoleRef.Kind == "Role" && b.RoleRef.Name == role.Name && len(b.Subjects) > 0
			})
			if !bound {
				t.Errorf("%s: Role %s is not bound", path, role.Name)
			}
		}

		if secretRoles == 0 {
			t.Errorf("%s: no Role grants the Secrets blockstor reads", path)
		}
	}
}
