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

package cli_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

// With several clusters configured, the operator names the one a
// command is for, as with kubectl; the store is opened for that one.
func TestKubeconfigAndContextFlagsReachTheStore(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want cli.KubeTarget
	}{
		"both":    {[]string{"r", "l", "--kubeconfig", "/tmp/k", "--context=staging"}, cli.KubeTarget{Kubeconfig: "/tmp/k", Context: "staging"}},
		"context": {[]string{"r", "l", "--context", "staging"}, cli.KubeTarget{Context: "staging"}},
		"none":    {[]string{"r", "l"}, cli.KubeTarget{}},
	} {
		t.Run(name, func(t *testing.T) {
			var got cli.KubeTarget

			var out, errBuf bytes.Buffer

			app := &cli.App{
				Out: &out,
				Err: &errBuf,
				StoreFor: func(ctx context.Context) (store.Store, error) {
					got = cli.KubeTargetFrom(ctx)

					return store.NewInMemory(), nil
				},
			}

			if code := app.Run(t.Context(), tc.args); code != 0 {
				t.Fatalf("exit %d: %s", code, errBuf.String())
			}

			if got != tc.want {
				t.Fatalf("store opened for %+v, want %+v", got, tc.want)
			}
		})
	}
}
