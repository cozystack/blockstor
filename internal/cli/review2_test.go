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
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

var errNoKubeconfig = errors.New("no kubeconfig")

// An empty passphrase must never be stored. `passphrase.Read` cannot
// tell an empty Secret from a missing one, so an empty master key
// leaves create reporting "already set" and enter reporting "none set"
// — contradictory, with no way out through this CLI — while encrypting
// every volume with nothing.
func TestEmptyPassphraseIsRejected(t *testing.T) {
	t.Parallel()

	for _, argv := range [][]string{
		{"encryption", "create-passphrase", "--passphrase", ""},
		{"encryption", "create-passphrase", ""},
		{"encryption", "enter-passphrase", ""},
	} {
		app, client, _ := newKubeApp(t)

		if got := app.Run(t.Context(), argv); got != 2 {
			t.Errorf("%v exit = %d, want 2", argv, got)
		}

		var secret corev1.Secret

		key := types.NamespacedName{Namespace: testNamespace, Name: "blockstor-cluster-passphrase"}
		if err := client.Get(t.Context(), key, &secret); err == nil {
			t.Errorf("%v wrote a passphrase secret", argv)
		}
	}
}

// A bad size must not leave the definition created: the corrected
// retry would then fail on "already exists", so a typo would cost a
// manual delete before the operator could try again.
func TestSpawnValidatesBeforeWriting(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp"})
	})

	if got := app.Run(t.Context(), []string{"rg", "spawn", "grp", "pvc-x", "32M", "bogus"}); got == 0 {
		t.Fatal("spawn accepted a malformed size")
	}

	if _, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x"); err == nil {
		t.Error("the refused spawn left an orphan definition behind")
	}

	// The corrected retry now works, because nothing was written.
	if got := app.Run(t.Context(), []string{"rg", "spawn", "grp", "pvc-x", "32M"}); got != 0 {
		t.Errorf("the corrected retry exit = %d, want 0", got)
	}
}

// A malformed --limit must not fail open: silently returning the full
// list is how a script that meant to page processes everything.
func TestLimitIsValidated(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, seedResource)
	if got := app.Run(t.Context(), []string{"r", "l", "--limit", "banana"}); got != 2 {
		t.Errorf("--limit banana exit = %d, want 2", got)
	}

	negative, _, _ := newApp(t, seedResource)
	if got := negative.Run(t.Context(), []string{"r", "l", "--limit=-1"}); got != 2 {
		t.Errorf("--limit=-1 exit = %d, want 2", got)
	}

	// Zero means unlimited, the convention the flag's name implies.
	zero, out, _ := newApp(t, seedResource)
	if got := zero.Run(t.Context(), []string{"r", "l", "--limit", "0"}); got != 0 {
		t.Fatalf("--limit 0 exit = %d", got)
	}

	if !strings.Contains(out.String(), "pvc-x") {
		t.Errorf("--limit 0 returned no rows:\n%s", out.String())
	}
}

// `blockstor r l --help` asks about that command, not for a flag.
func TestPerCommandHelp(t *testing.T) {
	t.Parallel()

	app, out, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"r", "l", "--help"}); got != 0 {
		t.Fatalf("per-command --help exit = %d, want 0", got)
	}

	if !strings.Contains(out.String(), "usage:") {
		t.Errorf("--help printed no usage:\n%s", out.String())
	}
}

// A client-side mistake is classified the same way whether or not the
// cluster happens to be reachable, and does not open a connection.
func TestColourIsValidatedBeforeTheClusterIsOpened(t *testing.T) {
	t.Parallel()

	opened := false

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			opened = true

			return nil, errNoKubeconfig
		},
	}

	if got := app.Run(t.Context(), []string{"r", "l", "--color=bogus"}); got != 2 {
		t.Errorf("exit = %d, want 2 even with an unreachable cluster", got)
	}

	if opened {
		t.Error("a known-invalid invocation still opened a cluster connection")
	}
}

// Setting and unsetting the same knob in one invocation resolves by
// map iteration order, so it is refused rather than left to chance.
func TestDrbdOptionsRefusesContradiction(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, seedDefinition)

	argv := []string{"rd", "drbd-options", "--max-buffers=8000", "--unset-max-buffers", "pvc-x"}
	if got := app.Run(t.Context(), argv); got != 2 {
		t.Errorf("contradictory set/unset exit = %d, want 2", got)
	}
}

// The size query's headline number must reach `-m` consumers.
func TestQuerySizeInfoMachineCarriesTheMaxSize(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "grp",
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "data"},
		})
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: "node-1", StoragePoolName: "data",
			ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 50 << 20,
		})
	})

	if got := app.Run(t.Context(), []string{"rg", "query-size-info", "grp", "-m"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !strings.Contains(out.String(), "max_volume_size_kib") {
		t.Errorf("-m dropped the computed size:\n%s", out.String())
	}
}

// `vg list -m` rows must name their parent group, or they are
// ambiguous across two groups.
func TestVolumeGroupListMachineNamesTheGroup(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name: "grp-a", VolumeGroups: []apiv1.VolumeGroup{{VolumeNumber: 0}},
		})
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name: "grp-b", VolumeGroups: []apiv1.VolumeGroup{{VolumeNumber: 0}},
		})
	})

	if got := app.Run(t.Context(), []string{"vg", "l", "-m"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	for _, want := range []string{"grp-a", "grp-b"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("-m rows do not name %s:\n%s", want, out.String())
		}
	}
}

// A failed batch must not leave a group with fewer members than its
// declared GroupSize: the controller opens the suspend-io barrier only
// once the whole group has landed, so a short group strands it.
func TestCreateMultipleDoesNotLeaveAShortGroup(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-a"})
	})

	// pvc-b does not exist, so the batch cannot complete.
	if got := app.Run(t.Context(), []string{"s", "create-multiple", "pvc-a:snap", "pvc-b:snap"}); got == 0 {
		t.Fatal("a batch naming an unknown definition succeeded")
	}

	snaps, err := appStore(t, app).Snapshots().List(t.Context())
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}

	if len(snaps) != 0 {
		t.Errorf("the failed batch left %d snapshot(s) behind: %+v", len(snaps), snaps)
	}
}

// The exit codes for the semantic refusals are pinned, so a script
// branching on them cannot silently reclassify one.
func TestSemanticRefusalExitCodes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		argv []string
		want int
		seed func(context.Context, store.Store)
	}{
		{
			name: "shrink without force",
			argv: []string{"vd", "s", "pvc-x", "0", "8M"},
			want: 10,
			seed: func(ctx context.Context, backend store.Store) {
				seedDefinition(ctx, backend)
				_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{SizeKib: 2 << 20})
			},
		},
		{
			name: "size outside the bounds",
			argv: []string{"vd", "c", "pvc-x", "1M"},
			want: 10,
			seed: seedDefinition,
		},
		{
			name: "rollback is refused",
			argv: []string{"s", "rollback", "pvc-x", "snap"},
			want: 10,
			seed: seedDefinition,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, tc.seed)
			if got := app.Run(t.Context(), tc.argv); got != tc.want {
				t.Errorf("exit = %d, want %d (stderr: %s)", got, tc.want, errBuf.String())
			}
		})
	}
}
