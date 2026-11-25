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
	"strings"
	"testing"
)

// The satellite logs version.Version at start-up, and a released image
// that logs `0.0.0-dev` cannot tell which release a node runs. Every go
// build in the Dockerfile must stamp it from the VERSION build-arg, and the
// release workflow must pass that build-arg.
func TestReleasedImagesStampTheirVersion(t *testing.T) {
	t.Parallel()

	root := repoRoot240(t)

	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}

	const stamp = "-X github.com/cozystack/blockstor/pkg/version.Version=${VERSION}"

	src := string(dockerfile)

	if !strings.Contains(src, "ARG VERSION") {
		t.Errorf("Dockerfile declares no VERSION build-arg")
	}

	if got, want := strings.Count(src, stamp), strings.Count(src, "pkg/version.GitCommit="); got != want {
		t.Errorf("Dockerfile stamps version.Version in %d of its %d ldflags sets", got, want)
	}

	workflow, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}

	if !strings.Contains(string(workflow), "\n            VERSION=") {
		t.Errorf("release workflow does not pass the VERSION build-arg to the image build")
	}
}
