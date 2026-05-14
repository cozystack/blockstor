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

package harness

import (
	"net/url"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"

	csidriver "github.com/cozystack/blockstor/pkg/csi-driver"
)

// CSI exposes blockstor's CSI shim wired to the in-process REST
// server. The project does NOT ship a gRPC CSI server today —
// `pkg/csi-driver.Driver` is a behaviour-bearing adapter that the
// real linstor-csi sidecar wraps in `csi.ControllerServer`. The
// docs/test-strategy.md scaffold table calls for in-process CSI
// gRPC; until the project actually grows that binary, Phase 0
// exposes the Driver directly. Phase 1 Group J builds on this
// surface.
//
// The TODO is tracked in the PR body's "caveats" section.
type CSI struct {
	// Driver is the same Driver linstor-csi uses in production —
	// see pkg/csi-driver/driver.go.
	Driver *csidriver.Driver

	// Client is the underlying golinstor REST client, exposed for
	// tests that need direct lapi-level operations not yet
	// proxied through Driver (e.g. ListSnapshots envelope checks
	// for Bug 201 in Phase 1 Group J).
	Client *lapi.Client
}

// NewCSI builds the CSI surface against `stack.RestURL`. Cheap;
// no goroutines spawned. The Driver re-uses one lapi.Client across
// the test, matching the linstor-csi sidecar's lifecycle.
func NewCSI(t *testing.T, stack *Stack) *CSI {
	t.Helper()

	u, err := url.Parse(stack.RestURL)
	if err != nil {
		t.Fatalf("parse REST URL %q: %v", stack.RestURL, err)
	}

	c, err := lapi.NewClient(lapi.BaseURL(u))
	if err != nil {
		t.Fatalf("lapi.NewClient: %v", err)
	}

	return &CSI{
		Driver: &csidriver.Driver{Client: c},
		Client: c,
	}
}
