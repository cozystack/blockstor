// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

func set(names ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		out[n] = struct{}{}
	}

	return out
}

// An unhealthy resource blockstor has no replica of on the node is another
// DRBD user's, LINSTOR's during a migration, and is left alone; only an
// unhealthy one of its own is taken down.
func TestSelectOrphansTakesDownOnlyBlockstorsOwn(t *testing.T) {
	t.Parallel()

	kernel := []string{"pvc-healthy", "pvc-ours", "pvc-linstor"}

	orphans, foreign := selectOrphans(kernel, set("pvc-healthy"), set("pvc-healthy", "pvc-ours"))

	if !slices.Equal(orphans, []string{"pvc-ours"}) {
		t.Errorf("orphans = %v, want [pvc-ours]", orphans)
	}

	if !slices.Equal(foreign, []string{"pvc-linstor"}) {
		t.Errorf("foreign = %v, want [pvc-linstor]", foreign)
	}
}

// fakeDrbdsetup puts a drbdsetup on PATH whose kernel holds one resource,
// pvc-x, and which records every `down` it is asked for in the returned file.
func fakeDrbdsetup(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	calls := filepath.Join(dir, "downs")
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  status) echo 'pvc-x role:Secondary' ;;\n" +
		"  down) echo \"$2\" >> " + calls + " ;;\n" +
		"esac\n"

	if err := os.WriteFile(filepath.Join(dir, "drbdsetup"), []byte(script), 0o755); err != nil {
		t.Fatalf("write the fake drbdsetup: %v", err)
	}

	t.Setenv("PATH", dir)

	return calls
}

// Without the list of its own resources the satellite cannot tell blockstor's
// from anyone else's, so it takes nothing down. The same orphan, known to be
// blockstor's, is taken down: that is the positive control.
func TestCleanKernelStateDownsNothingWithoutTheOwnedList(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	calls := fakeDrbdsetup(t)
	cleanKernelState(t.Context(), logger, nil, nil)

	if downs, err := os.ReadFile(calls); err == nil {
		t.Errorf("downed %q without knowing what is blockstor's", strings.TrimSpace(string(downs)))
	}

	cleanKernelState(t.Context(), logger, nil, set("pvc-x"))

	downs, err := os.ReadFile(calls)
	if err != nil || strings.TrimSpace(string(downs)) != "pvc-x" {
		t.Errorf("blockstor's own orphan was not taken down: %q (%v)", downs, err)
	}
}

var errProbeTimedOut = errors.New("drbdsetup status timed out")

// A probe that cannot tell keeps the resource: taking a replica down is the
// step that cannot be undone, and the reconciler adjusts it shortly after.
func TestAFailedHealthProbeKeepsTheResource(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	probe := func(_ context.Context, name string) (bool, error) {
		switch name {
		case "pvc-fine":
			return true, nil
		case "pvc-dead":
			return false, nil
		default:
			return false, errProbeTimedOut
		}
	}

	healthy := classifyKernelResources(t.Context(), logger, []string{"pvc-fine", "pvc-dead", "pvc-unknown"}, probe)

	if _, ok := healthy["pvc-unknown"]; !ok {
		t.Error("a resource whose probe failed was not kept")
	}

	if _, ok := healthy["pvc-dead"]; ok {
		t.Error("an orphan the probe saw was kept")
	}

	if _, ok := healthy["pvc-fine"]; !ok {
		t.Error("a healthy resource was not kept")
	}
}

// The owned list is the definitions of the replicas scheduled on this node,
// and nothing scheduled elsewhere.
func TestOwnedOnNodeNamesTheReplicasOfThisNode(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	replica := func(rd, node string) *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: node},
		}
	}

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(replica("pvc-a", "srv1"), replica("pvc-b", "srv1"), replica("pvc-c", "srv2")).
		Build()

	owned, err := ownedOnNode(t.Context(), c, "srv1")
	if err != nil {
		t.Fatalf("ownedOnNode: %v", err)
	}

	got := slices.Sorted(maps.Keys(owned))
	if !slices.Equal(got, []string{"pvc-a", "pvc-b"}) {
		t.Errorf("owned on srv1 = %v, want [pvc-a pvc-b]", got)
	}
}
