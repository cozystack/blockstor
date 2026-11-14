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

package controllers

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/keyring"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/passphrase"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

const keysNS = "blockstor-system"

type keysRig struct {
	cli    client.Client
	keys   *LUKSKeys
	master *keyring.MasterKey
}

// newKeysRig is a cluster whose passphrase Secret opens its keyring, with
// the given objects besides.
func newKeysRig(t *testing.T, unlocked bool, objs ...client.Object) *keysRig {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	ring, master, err := keyring.New([]byte("right"), passphrase.SlotNameSecret, keyring.OriginSecret, time.Now())
	if err != nil {
		t.Fatalf("keyring: %v", err)
	}

	data, err := ring.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	pass := "right"
	if !unlocked {
		pass = "wrong"
	}

	objs = append(objs,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: passphrase.KeyringSecretName, Namespace: keysNS},
			Data:       map[string][]byte{passphrase.KeyringDataKey: data},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: passphrase.UnlockSecretName, Namespace: keysNS},
			Data:       map[string][]byte{passphrase.DefaultPassphraseKey: []byte(pass)},
		})

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	return &keysRig{
		cli:    cli,
		master: master,
		keys: &LUKSKeys{
			Client:   cli,
			Reader:   cli,
			Unlocker: &passphrase.Unlocker{Secrets: passphrase.Secrets{Reader: cli, Namespace: keysNS}},
		},
	}
}

func keysReplica(keys ...*blockstoriov1alpha1.LUKSKey) *blockstoriov1alpha1.Resource {
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-k.n1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-k", NodeName: "n1"},
	}

	for i, key := range keys {
		res.Spec.Volumes = append(res.Spec.Volumes, blockstoriov1alpha1.ResourceVolumeSpec{VolumeNumber: int32(i), LUKSKey: key})
	}

	return res
}

func (r *keysRig) wrap(t *testing.T, master *keyring.MasterKey, password string) *blockstoriov1alpha1.LUKSKey {
	t.Helper()

	if master == nil {
		master = r.master
	}

	wrapped, err := master.WrapVolumePassword([]byte(password))
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	return &blockstoriov1alpha1.LUKSKey{KeyID: wrapped.KeyID, Wrapped: wrapped.Wrapped, Origin: blockstoriov1alpha1.LUKSKeyOriginGenerated}
}

func (r *keysRig) replica(t *testing.T) *blockstoriov1alpha1.Resource {
	t.Helper()

	var res blockstoriov1alpha1.Resource

	err := r.cli.Get(t.Context(), client.ObjectKey{Name: "pvc-k.n1"}, &res)
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	return &res
}

// keysVol0 is volume 0 of the rig's replica.
func keysVol0() satellite.LUKSVolume {
	return satellite.LUKSVolume{Resource: "pvc-k", Node: "n1", Volume: 0}
}

// A generated password is stored wrapped on the replica before it is
// handed back, and reads back the same.
func TestGeneratedPasswordIsStoredBeforeItIsReturned(t *testing.T) {
	t.Parallel()

	rig := newKeysRig(t, true, keysReplica())

	password, err := rig.keys.Generate(t.Context(), keysVol0())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	stored := luksKeyOf(rig.replica(t), 0)
	if stored == nil || stored.Origin != blockstoriov1alpha1.LUKSKeyOriginGenerated || stored.KeyID != rig.master.KeyID() {
		t.Fatalf("stored %+v", stored)
	}

	if bytes.Contains(stored.Wrapped, password) {
		t.Fatal("the password is stored in the clear")
	}

	again, err := rig.keys.Stored(t.Context(), keysVol0())
	if err != nil || !bytes.Equal(again, password) {
		t.Fatalf("Stored %q, %v; want the generated password", again, err)
	}
}

// A replica that already carries a password is never given another.
func TestGenerateNeverReplacesAStoredPassword(t *testing.T) {
	t.Parallel()

	rig := newKeysRig(t, true)
	existing := rig.wrap(t, nil, "existing")

	err := rig.cli.Create(t.Context(), keysReplica(existing))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = rig.keys.Generate(t.Context(), keysVol0())
	if !errors.Is(err, errLUKSKeyRace) {
		t.Fatalf("err %v, want errLUKSKeyRace", err)
	}

	if got := luksKeyOf(rig.replica(t), 0); !bytes.Equal(got.Wrapped, existing.Wrapped) {
		t.Fatal("the stored password was replaced")
	}
}

func TestStoredRefusals(t *testing.T) {
	t.Parallel()

	foreign, err := keyring.NewMasterKey()
	if err != nil {
		t.Fatalf("NewMasterKey: %v", err)
	}

	t.Run("none", func(t *testing.T) {
		t.Parallel()

		rig := newKeysRig(t, true, keysReplica())

		_, err := rig.keys.Stored(t.Context(), keysVol0())
		if !errors.Is(err, luks.ErrNoLUKSKey) {
			t.Fatalf("err %v, want ErrNoLUKSKey", err)
		}
	})

	t.Run("other master key", func(t *testing.T) {
		t.Parallel()

		rig := newKeysRig(t, true)

		err := rig.cli.Create(t.Context(), keysReplica(rig.wrap(t, foreign, "pw")))
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		_, err = rig.keys.Stored(t.Context(), keysVol0())
		if !errors.Is(err, luks.ErrMasterKeyMismatch) || !strings.Contains(err.Error(), luks.ErrMasterKeyMismatch.Error()) {
			t.Fatalf("err %v, want ErrMasterKeyMismatch in the message", err)
		}
	})

	t.Run("locked", func(t *testing.T) {
		t.Parallel()

		rig := newKeysRig(t, false)

		err := rig.cli.Create(t.Context(), keysReplica(rig.wrap(t, nil, "pw")))
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		_, err = rig.keys.Stored(t.Context(), keysVol0())
		if !errors.Is(err, luks.ErrMasterKeyLocked) {
			t.Fatalf("Stored: err %v, want ErrMasterKeyLocked", err)
		}

		_, err = rig.keys.Generate(t.Context(), satellite.LUKSVolume{Resource: "pvc-k", Node: "n1", Volume: 1})
		if !errors.Is(err, luks.ErrMasterKeyLocked) {
			t.Fatalf("Generate: err %v, want ErrMasterKeyLocked", err)
		}

		if luksKeyOf(rig.replica(t), 1) != nil {
			t.Fatal("a password was stored under a locked master key")
		}
	})
}

// A restored volume's candidates are the snapshot's recorded passwords for
// that volume, this node's first; adopting one stores it, origin kept.
func TestSnapshotCandidates(t *testing.T) {
	t.Parallel()

	rig := newKeysRig(t, true, keysReplica())
	foreign, err := keyring.NewMasterKey()
	if err != nil {
		t.Fatalf("NewMasterKey: %v", err)
	}

	imported := rig.wrap(t, nil, "peer-pw")
	imported.Origin = blockstoriov1alpha1.LUKSKeyOriginLinstorImport

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-src.snap1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-src", SnapshotName: "snap1",
			LUKSKeys: []blockstoriov1alpha1.SnapshotLUKSKey{
				{NodeName: "peer", VolumeNumber: 0, LUKSKey: *imported},
				{NodeName: "n1", VolumeNumber: 1, LUKSKey: *rig.wrap(t, nil, "other-volume")},
				{NodeName: "stranger", VolumeNumber: 0, LUKSKey: *rig.wrap(t, foreign, "foreign")},
				{NodeName: "N1", VolumeNumber: 0, LUKSKey: *rig.wrap(t, nil, "own-pw")},
			},
		},
	}

	err = rig.cli.Create(t.Context(), snap)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	vol := keysVol0()
	vol.SourceSnapshot = "pvc-src:snap1"

	candidates, err := rig.keys.SnapshotCandidates(t.Context(), vol)
	if err != nil {
		t.Fatalf("SnapshotCandidates: %v", err)
	}

	got := make([]string, 0, len(candidates))
	for _, c := range candidates {
		got = append(got, c.Node+"="+string(c.Password))
	}

	if strings.Join(got, ",") != "N1=own-pw,peer=peer-pw" {
		t.Fatalf("candidates %v, want this node's first and the other master key's left out", got)
	}

	err = candidates[1].Adopt(t.Context())
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	stored := luksKeyOf(rig.replica(t), 0)
	if stored == nil || !bytes.Equal(stored.Wrapped, imported.Wrapped) || stored.Origin != blockstoriov1alpha1.LUKSKeyOriginLinstorImport {
		t.Fatalf("adopted %+v", stored)
	}

	_, err = rig.keys.SnapshotCandidates(t.Context(), satellite.LUKSVolume{Resource: "pvc-k", Node: "n1", SourceSnapshot: "bad"})
	if !errors.Is(err, luks.ErrLUKSKeyMissing) {
		t.Fatalf("malformed source: err %v", err)
	}
}

// Every LUKS refusal that leaves a volume down is a Warning Event on the
// Resource, under its own reason.
func TestLUKSRefusalsAreWarningEvents(t *testing.T) {
	t.Parallel()

	res := &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.node-a"}}

	for _, tc := range []struct {
		err    error
		reason string
	}{
		{luks.ErrLUKSKeyMissing, "LUKSKeyMissing"},
		{luks.ErrLUKSKeyMismatch, "LUKSKeyMismatch"},
		{luks.ErrMasterKeyMismatch, "MasterKeyMismatch"},
		{luks.ErrMasterKeyLocked, "MasterKeyLocked"},
	} {
		recorder := events.NewFakeRecorder(4)
		r := &ResourceReconciler{Config: Config{Recorder: recorder}}

		message := errors.Wrap(tc.err, "luks volume 0").Error()
		r.recordPerResourceFailures(res, []*intent.ResourceApplyResult{{Name: "pvc-1", Message: message}}, logr.Discard())

		select {
		case ev := <-recorder.Events:
			if !strings.Contains(ev, "Warning "+tc.reason+" ") {
				t.Errorf("%s: Event %q", tc.reason, ev)
			}
		default:
			t.Errorf("%s: no Event", tc.reason)
		}
	}
}
