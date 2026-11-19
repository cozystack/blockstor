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

package satellite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errResParse = errors.New("parse error: 'resync-rate | c-plan-ahead' expected, but got 'block-size'")

// checkedReconciler is a reconciler whose drbdadm answers the .res check
// for name with refusal (nil accepts).
func checkedReconciler(t *testing.T, name string, refusal error) (*Reconciler, *storage.FakeExec, string) {
	t.Helper()

	dir := t.TempDir()
	fx := storage.NewFakeExec()

	if refusal != nil {
		fx.Expect("drbdadm --config-to-test "+filepath.Join(dir, name+".res.tmp")+
			" --config-to-exclude "+filepath.Join(dir, name+".res")+" sh-nop",
			storage.FakeResponse{Err: refusal})
	}

	rec := NewReconciler(ReconcilerConfig{
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
		Adm:          drbd.NewAdm(fx),
	})

	return rec, fx, dir
}

// A body drbdadm cannot parse must not replace the live .res: drbdadm
// reads every .res on the node, so installing it would stop adjust and
// dump-md for every resource there. The previous file stays, nothing is
// left under a name drbdadm would include, and the resource reports it.
func TestARefusedResBodyLeavesTheLiveFileInPlace(t *testing.T) {
	t.Parallel()

	refusal := errResParse
	rec, _, dir := checkedReconciler(t, "pvc-bad", refusal)

	resPath := filepath.Join(dir, "pvc-bad.res")
	previous := []byte("resource pvc-bad {\n}\n")

	if err := os.WriteFile(resPath, previous, resFilePerm); err != nil {
		t.Fatal(err)
	}

	dr, devices := renderResFileFixture("pvc-bad")
	dr.Volumes[0].DiskOptions = map[string]string{"block-size": "4096"}

	err := rec.renderResFile(context.Background(), dr, devices)
	if !errors.Is(err, refusal) {
		t.Fatalf("renderResFile = %v, want the refusal", err)
	}

	got, readErr := os.ReadFile(resPath)
	if readErr != nil || string(got) != string(previous) {
		t.Errorf("live .res = %q (%v), want the previous body kept", got, readErr)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "pvc-bad.res.") {
			t.Errorf("refused candidate %s left in the state dir", e.Name())
		}
	}
}

// An accepted body is installed under the real name, after the check,
// and the candidate is gone.
func TestAnAcceptedResBodyIsCheckedThenInstalled(t *testing.T) {
	t.Parallel()

	rec, fx, dir := checkedReconciler(t, "pvc-ok", nil)

	dr, devices := renderResFileFixture("pvc-ok")
	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("renderResFile: %v", err)
	}

	want, err := buildResFile(dr, "n1", "10.0.0.1", devices, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "pvc-ok.res"))
	if err != nil || string(got) != want {
		t.Errorf(".res = %q (%v), want the rendered body", got, err)
	}

	check := "drbdadm --config-to-test " + filepath.Join(dir, "pvc-ok.res.tmp") +
		" --config-to-exclude " + filepath.Join(dir, "pvc-ok.res") + " sh-nop"
	if !slices.Contains(fx.CommandLines(), check) {
		t.Errorf("the body was installed without the drbdadm check; calls %v", fx.CommandLines())
	}

	if _, err := os.Stat(filepath.Join(dir, "pvc-ok.res.tmp")); !os.IsNotExist(err) {
		t.Errorf("candidate still present after install: %v", err)
	}
}

// expectXMLHelp makes fx answer drbdsetup xml-help the way drbd-utils
// does, from the excerpts pkg/drbd keeps in its testdata.
func expectXMLHelp(t *testing.T, fx *storage.FakeExec) {
	t.Helper()

	for _, cmd := range drbd.DiskSectionCommands() {
		doc, err := os.ReadFile(filepath.Join("..", "drbd", "testdata", "xml-help-"+cmd+".xml"))
		if err != nil {
			t.Fatal(err)
		}

		fx.Expect("drbdsetup xml-help "+cmd, storage.FakeResponse{Stdout: doc})
	}
}

// An adopted definition can carry a disk option this node's drbd-utils
// does not know, or a value outside its range. drbdadm would refuse the
// whole .res over it, and with no .res installed the resource is never
// adjusted. The option is left out instead, as LINSTOR's satellite does,
// and the rest of the file lands; the volume's own block-size survives.
func TestADiskOptionDrbdUtilsRejectsIsLeftOutOfTheRes(t *testing.T) {
	t.Parallel()

	rec, fx, dir := checkedReconciler(t, "pvc-adopted", nil)
	expectXMLHelp(t, fx)

	dr, devices := renderResFileFixture("pvc-adopted")
	dr.DrbdOptions["DrbdOptions/Disk/some-future-option"] = "1"
	dr.DrbdOptions["DrbdOptions/Disk/al-extents"] = "6007"

	volumeOptions := map[string]string{
		"block-size":             "4096",
		"rs-discard-granularity": "8M",
		"another-future-option":  "yes",
	}
	dr.Volumes[0].DiskOptions = volumeOptions

	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("renderResFile: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "pvc-adopted.res"))
	if err != nil {
		t.Fatal(err)
	}

	for _, gone := range []string{"some-future-option", "another-future-option", "rs-discard-granularity"} {
		if strings.Contains(string(body), gone) {
			t.Errorf(".res still carries %s, which drbdadm would refuse:\n%s", gone, body)
		}
	}

	for _, kept := range []string{"block-size 4096;", "al-extents 6007;"} {
		if !strings.Contains(string(body), kept) {
			t.Errorf(".res lost %q, which drbdadm accepts:\n%s", kept, body)
		}
	}

	if len(volumeOptions) != 3 {
		t.Errorf("the intent's volume options were edited in place: %v", volumeOptions)
	}
}
