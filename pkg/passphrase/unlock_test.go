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

package passphrase_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/keyring"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

const ns = "blockstor-system"

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()

	err := corev1.AddToScheme(s)
	if err == nil {
		err = blockstoriov1alpha1.AddToScheme(s)
	}

	if err != nil {
		t.Fatalf("scheme: %v", err)
	}

	return s
}

func refConfig(name, key string) *blockstoriov1alpha1.ControllerConfig {
	return &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{Name: name, Key: key},
		},
	}
}

func passSecret(name, key, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{key: []byte(value)},
	}
}

func keyringSecret(t *testing.T, ring *keyring.Keyring) *corev1.Secret {
	t.Helper()

	data, err := ring.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: passphrase.KeyringSecretName, Namespace: ns},
		Type:       passphrase.KeyringSecretType,
		Data:       map[string][]byte{passphrase.KeyringDataKey: data},
	}
}

func newRing(t *testing.T, pass string) (*keyring.Keyring, *keyring.MasterKey) {
	t.Helper()

	ring, key, err := keyring.New([]byte(pass), passphrase.SlotNameSecret, keyring.OriginSecret, time.Now())
	if err != nil {
		t.Fatalf("keyring.New: %v", err)
	}

	return ring, key
}

func TestResolveRef(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		objs []client.Object
		want passphrase.Ref
	}{
		"no config": {want: passphrase.Ref{Name: passphrase.UnlockSecretName, Key: "MASTER_PASSPHRASE", Generated: true}},
		"no ref": {
			objs: []client.Object{&blockstoriov1alpha1.ControllerConfig{ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName}}},
			want: passphrase.Ref{Name: passphrase.UnlockSecretName, Key: "MASTER_PASSPHRASE", Generated: true},
		},
		"default key": {
			objs: []client.Object{refConfig("linstor-passphrase", "")},
			want: passphrase.Ref{Name: "linstor-passphrase", Key: "MASTER_PASSPHRASE"},
		},
		"own key": {
			objs: []client.Object{refConfig("linstor-passphrase", "pass")},
			want: passphrase.Ref{Name: "linstor-passphrase", Key: "pass"},
		},
	}

	for name, tc := range cases {
		cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(tc.objs...).Build()

		got, err := passphrase.ResolveRef(t.Context(), cli)
		if err != nil || got != tc.want {
			t.Errorf("%s: %+v, %v; want %+v", name, got, err, tc.want)
		}
	}
}

// The Secret's bytes are the passphrase verbatim: a trailing newline is
// part of it, as it is for LINSTOR.
func TestPassphraseIsVerbatim(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(passSecret("p", "MASTER_PASSPHRASE", "hunter2\n")).Build()
	secrets := passphrase.Secrets{Reader: cli, Namespace: ns}

	got, _, err := secrets.Passphrase(t.Context(), passphrase.Ref{Name: "p", Key: "MASTER_PASSPHRASE"})
	if err != nil || string(got) != "hunter2\n" {
		t.Fatalf("%q, %v", got, err)
	}

	for _, ref := range []passphrase.Ref{{Name: "absent", Key: "MASTER_PASSPHRASE"}, {Name: "p", Key: "other"}} {
		got, _, err = secrets.Passphrase(t.Context(), ref)
		if err != nil || got != nil {
			t.Errorf("%+v: %q, %v; want nothing", ref, got, err)
		}
	}
}

// No keyring is unset; a keyring the Secret does not open, or no Secret,
// is locked; the Secret's passphrase opens it.
func TestUnlockStates(t *testing.T) {
	t.Parallel()

	ring, master := newRing(t, "right")

	cases := map[string]struct {
		objs  []client.Object
		state passphrase.State
	}{
		"no keyring": {objs: []client.Object{refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "right")}, state: passphrase.StateUnset},
		"no secret":  {objs: []client.Object{refConfig("p", ""), keyringSecret(t, ring)}, state: passphrase.StateLocked},
		"wrong": {
			objs:  []client.Object{refConfig("p", ""), keyringSecret(t, ring), passSecret("p", "MASTER_PASSPHRASE", "wrong")},
			state: passphrase.StateLocked,
		},
		"newline": {
			objs:  []client.Object{refConfig("p", ""), keyringSecret(t, ring), passSecret("p", "MASTER_PASSPHRASE", "right\n")},
			state: passphrase.StateLocked,
		},
		"right": {
			objs:  []client.Object{refConfig("p", ""), keyringSecret(t, ring), passSecret("p", "MASTER_PASSPHRASE", "right")},
			state: passphrase.StateUnlocked,
		},
		"generated": {
			objs:  []client.Object{keyringSecret(t, ring), passSecret(passphrase.UnlockSecretName, "MASTER_PASSPHRASE", "right")},
			state: passphrase.StateUnlocked,
		},
	}

	for name, tc := range cases {
		cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(tc.objs...).Build()
		unlocker := &passphrase.Unlocker{Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

		status, err := unlocker.Unlock(t.Context())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if status.State != tc.state {
			t.Errorf("%s: state %s, want %s", name, status.State, tc.state)
		}

		if (status.Key != nil) != (tc.state == passphrase.StateUnlocked) {
			t.Errorf("%s: key %v in state %s", name, status.Key != nil, status.State)
		}

		if status.Key != nil && status.Key.KeyID() != master.KeyID() {
			t.Errorf("%s: unlocked another key", name)
		}
	}
}

// A keyring that does not parse is an error, not a locked cluster.
func TestUnlockReportsAMalformedKeyring(t *testing.T) {
	t.Parallel()

	bad := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: passphrase.KeyringSecretName, Namespace: ns},
		Data:       map[string][]byte{passphrase.KeyringDataKey: []byte(`{"version":1}`)},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(bad, passSecret(passphrase.UnlockSecretName, "MASTER_PASSPHRASE", "x")).Build()

	_, err := (&passphrase.Unlocker{Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}).Unlock(t.Context())
	if !errors.Is(err, keyring.ErrMalformed) {
		t.Fatalf("err %v, want ErrMalformed", err)
	}
}

// The outcome is remembered while neither Secret changes, a failed one
// too, so reconciles do not run Argon2id each time; a changed Secret is
// looked at again.
func TestUnlockIsCachedPerSecretVersion(t *testing.T) {
	t.Parallel()

	ring, _ := newRing(t, "right")
	pass := passSecret("p", "MASTER_PASSPHRASE", "wrong")
	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(refConfig("p", ""), keyringSecret(t, ring), pass).Build()
	unlocker := &passphrase.Unlocker{Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	for range 3 {
		status, err := unlocker.Unlock(t.Context())
		if err != nil || status.State != passphrase.StateLocked {
			t.Fatalf("%s, %v", status.State, err)
		}
	}

	if got := passphrase.UnlockAttempts(unlocker); got != 1 {
		t.Fatalf("%d attempts for one pair of Secret versions, want 1", got)
	}

	first, _ := unlocker.Unlock(t.Context())

	var cur corev1.Secret

	err := cli.Get(t.Context(), client.ObjectKeyFromObject(pass), &cur)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	cur.Data["MASTER_PASSPHRASE"] = []byte("right")

	err = cli.Update(t.Context(), &cur)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	second, err := unlocker.Unlock(t.Context())
	if err != nil || first.State != passphrase.StateLocked || second.State != passphrase.StateUnlocked {
		t.Fatalf("before %s, after %s, %v; want locked then unlocked", first.State, second.State, err)
	}

	third, err := unlocker.Unlock(t.Context())
	if err != nil || third.Key != second.Key || passphrase.UnlockAttempts(unlocker) != 2 {
		t.Fatal("an unchanged pair of Secrets unlocked again")
	}
}

// countingKeyringCreates fails the test on any write to a Secret but the
// keyring, and counts the keyring creates.
func countingKeyringCreates(t *testing.T, creates *int, objs ...client.Object) client.Client {
	t.Helper()

	refuse := func(obj client.Object) error {
		if _, ok := obj.(*corev1.Secret); ok && obj.GetName() != passphrase.KeyringSecretName {
			t.Errorf("wrote Secret %s", obj.GetName())
		}

		return nil
	}

	return fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			_ = refuse(obj)

			if obj.GetName() == passphrase.KeyringSecretName {
				*creates++
			}

			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			_ = refuse(obj)

			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			_ = refuse(obj)

			return c.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			_ = refuse(obj)

			return c.Delete(ctx, obj, opts...)
		},
	}).Build()
}

// The first start with a piraeus-shaped Secret and no keyring creates one
// whose only slot, `secret`, the Secret's passphrase opens. The passphrase
// Secret is never written.
func TestBootstrapCreatesTheKeyringFromTheSecret(t *testing.T) {
	t.Parallel()

	var creates int

	cli := countingKeyringCreates(t, &creates, refConfig("linstor-passphrase", ""), passSecret("linstor-passphrase", "MASTER_PASSPHRASE", "piraeus"))
	secrets := passphrase.Secrets{Reader: cli, Namespace: ns}

	created, err := passphrase.Bootstrap(t.Context(), cli, secrets, time.Now())
	if err != nil || !created || creates != 1 {
		t.Fatalf("created %v, %v, %d creates", created, err, creates)
	}

	ring, _, err := secrets.Keyring(t.Context())
	if err != nil || ring == nil {
		t.Fatalf("keyring %v, %v", ring, err)
	}

	if len(ring.Slots) != 1 || ring.Slots[0].Name != passphrase.SlotNameSecret || ring.Slots[0].Origin != keyring.OriginSecret {
		t.Fatalf("slots %+v", ring.Slots)
	}

	if _, err := ring.Opens([]byte("piraeus")); err != nil {
		t.Fatalf("the Secret's passphrase does not open the new keyring: %v", err)
	}

	var sec corev1.Secret

	err = cli.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: passphrase.KeyringSecretName}, &sec)
	if err != nil || sec.Type != passphrase.KeyringSecretType {
		t.Fatalf("keyring Secret type %q, %v", sec.Type, err)
	}

	created, err = passphrase.Bootstrap(t.Context(), cli, secrets, time.Now())
	if err != nil || created || creates != 1 {
		t.Fatalf("second pass: created %v, %v, %d creates", created, err, creates)
	}
}

// Nothing is created without a named Secret, with an empty one, or when a
// keyring exists; and none is created while a replica carries a password
// wrapped under a keyring that is gone.
func TestBootstrapLeavesWhatItMustNotTouch(t *testing.T) {
	t.Parallel()

	ring, _ := newRing(t, "x")
	carrier := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{Volumes: []blockstoriov1alpha1.ResourceVolumeSpec{{
			VolumeNumber: 0,
			LUKSKey:      &blockstoriov1alpha1.LUKSKey{KeyID: "c7324c99567678f7", Wrapped: []byte{1}, Origin: blockstoriov1alpha1.LUKSKeyOriginGenerated},
		}}},
	}

	cases := map[string]struct {
		objs []client.Object
		err  error
	}{
		"generated ref": {objs: []client.Object{passSecret(passphrase.UnlockSecretName, "MASTER_PASSPHRASE", "x")}},
		"no secret":     {objs: []client.Object{refConfig("p", "")}},
		"empty secret":  {objs: []client.Object{refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "")}},
		"keyring there": {objs: []client.Object{refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "y"), keyringSecret(t, ring)}},
		"keyring lost": {
			objs: []client.Object{refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "y"), carrier},
			err:  passphrase.ErrKeyringLost,
		},
	}

	for name, tc := range cases {
		var creates int

		cli := countingKeyringCreates(t, &creates, tc.objs...)

		created, err := passphrase.Bootstrap(t.Context(), cli, passphrase.Secrets{Reader: cli, Namespace: ns}, time.Now())
		if created || creates != 0 {
			t.Errorf("%s: created %v, %d creates", name, created, creates)
		}

		if (tc.err == nil && err != nil) || (tc.err != nil && !errors.Is(err, tc.err)) {
			t.Errorf("%s: err %v, want %v", name, err, tc.err)
		}
	}
}

// Two controllers racing: the one that loses the create is not in error.
func TestCreateKeyringLosingTheRaceIsNotAnError(t *testing.T) {
	t.Parallel()

	ring, _ := newRing(t, "x")
	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(keyringSecret(t, ring)).Build()

	created, err := passphrase.CreateKeyring(t.Context(), cli, ns, ring)
	if err != nil || created {
		t.Fatalf("created %v, %v", created, err)
	}
}

func setSecret(t *testing.T, cli client.Client, name, value string) {
	t.Helper()

	var sec corev1.Secret

	err := cli.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, &sec)
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}

	sec.Data["MASTER_PASSPHRASE"] = []byte(value)

	err = cli.Update(t.Context(), &sec)
	if err != nil {
		t.Fatalf("update %s: %v", name, err)
	}
}

func stateOf(t *testing.T, ops *passphrase.Ops) passphrase.Status {
	t.Helper()

	status, err := (&passphrase.Unlocker{Secrets: ops.Secrets}).Unlock(t.Context())
	if err != nil {
		t.Fatalf("unlock: %v", err)
	}

	return status
}

// The documented change of the Secret's passphrase unlocks at every step:
// add a slot, change the Secret, remove the old slot. And modify-passphrase
// of the Secret's passphrase keeps the old slot, superseded, until the
// Secret moves, after which the cleanup removes it.
func TestChangingTheSecretsPassphraseNeverLocks(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "one")).Build()
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := passphrase.Bootstrap(t.Context(), cli, ops.Secrets, time.Now())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	_, err = ops.Add(t.Context(), []byte("one"), []byte("two"), "next")
	if err != nil || stateOf(t, ops).State != passphrase.StateUnlocked {
		t.Fatalf("after add: %v, %s", err, stateOf(t, ops).State)
	}

	setSecret(t, cli, "p", "two")

	if stateOf(t, ops).State != passphrase.StateUnlocked {
		t.Fatal("locked after the Secret moved to the added passphrase")
	}

	err = ops.Remove(t.Context(), []byte("one"), "next")
	if !errors.Is(err, passphrase.ErrUnlockSlot) {
		t.Fatalf("removing the slot the Secret opens: %v, want ErrUnlockSlot", err)
	}

	err = ops.Remove(t.Context(), []byte("two"), passphrase.SlotNameSecret)
	if err != nil || stateOf(t, ops).State != passphrase.StateUnlocked {
		t.Fatalf("after removing the old slot: %v", err)
	}

	result, err := ops.Modify(t.Context(), []byte("two"), []byte("three"))
	if err != nil || result.SecretToUpdate != "p" {
		t.Fatalf("modify of the Secret's passphrase: %+v, %v", result, err)
	}

	removed, err := ops.CleanupSuperseded(t.Context())
	if err != nil || removed {
		t.Fatalf("cleanup before the Secret moved: %v, %v; want nothing removed", removed, err)
	}

	if stateOf(t, ops).State != passphrase.StateUnlocked {
		t.Fatal("locked after modify, before the Secret moved")
	}

	setSecret(t, cli, "p", "three")

	removed, err = ops.CleanupSuperseded(t.Context())
	if err != nil || !removed {
		t.Fatalf("cleanup after the Secret moved: %v, %v", removed, err)
	}

	status := stateOf(t, ops)
	if status.State != passphrase.StateUnlocked || len(status.Ring.Slots) != 1 {
		t.Fatalf("state %s, slots %+v", status.State, status.Ring.Slots)
	}

	if _, err := status.Ring.Opens([]byte("two")); err == nil {
		t.Fatal("the superseded passphrase still opens")
	}
}

// Under a named Secret, create-passphrase is refused while there is no
// keyring, and is a no-op with the Secret's passphrase once there is one.
func TestCreateUnderANamedSecret(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(refConfig("p", "")).Build()
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := ops.Create(t.Context(), []byte("x"))
	if !errors.Is(err, passphrase.ErrSecretManaged) {
		t.Fatalf("err %v, want ErrSecretManaged", err)
	}

	var unlock corev1.Secret
	if cli.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: passphrase.UnlockSecretName}, &unlock) == nil {
		t.Fatal("an unlock Secret was created under a named passphrase Secret")
	}
}

// create-passphrase does not mint a new master key while a replica carries
// a password wrapped under one that is gone: those volumes would never
// open again.
func TestCreateRefusesALostKeyring(t *testing.T) {
	t.Parallel()

	carrier := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{Volumes: []blockstoriov1alpha1.ResourceVolumeSpec{{
			VolumeNumber: 0,
			LUKSKey:      &blockstoriov1alpha1.LUKSKey{KeyID: "c7324c99567678f7", Wrapped: []byte{1}, Origin: blockstoriov1alpha1.LUKSKeyOriginGenerated},
		}}},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(carrier).Build()
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := ops.Create(t.Context(), []byte("x"))
	if !errors.Is(err, passphrase.ErrKeyringLost) {
		t.Fatalf("err %v, want ErrKeyringLost", err)
	}

	ring, _, err := ops.Secrets.Keyring(t.Context())
	if err != nil || ring != nil {
		t.Fatalf("a keyring was created: %v", err)
	}
}

// The review's reproducer: on a cluster locked out of its generated
// unlock Secret, removing the operator's slot would leave only the
// generated one, which nothing opens any more. Remove refuses on a locked
// cluster, refuses the last slot a human holds even when unlocked, and
// needs a passphrase that stays.
func TestRemoveNeverStrandsTheMasterKey(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).Build()
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := ops.Create(t.Context(), []byte("operator-pass"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var unlock corev1.Secret

	err = cli.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: passphrase.UnlockSecretName}, &unlock)
	if err != nil {
		t.Fatalf("get the unlock Secret: %v", err)
	}

	generated := bytes.Clone(unlock.Data[passphrase.DefaultPassphraseKey])

	err = ops.Remove(t.Context(), generated, passphrase.SlotNameOperator)
	if !errors.Is(err, passphrase.ErrLastHumanSlot) {
		t.Fatalf("unlocked, removing the only human slot: %v, want ErrLastHumanSlot", err)
	}

	err = cli.Delete(t.Context(), &unlock)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	err = ops.Remove(t.Context(), generated, passphrase.SlotNameOperator)
	if !errors.Is(err, passphrase.ErrLocked) {
		t.Fatalf("locked: %v, want ErrLocked", err)
	}

	if _, err := ops.Enter(t.Context(), []byte("operator-pass")); err != nil {
		t.Fatalf("the operator's passphrase no longer opens: %v", err)
	}
}

// Remove needs a passphrase of a slot that stays: not a wrong one, not
// the one being removed.
func TestRemoveIsAuthorisedByARemainingPassphrase(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).Build()
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := ops.Create(t.Context(), []byte("operator-pass"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = ops.Add(t.Context(), []byte("operator-pass"), []byte("recovery-pass"), "recovery")
	if err != nil {
		t.Fatalf("add: %v", err)
	}

	err = ops.Remove(t.Context(), []byte("wrong"), "recovery")
	if !errors.Is(err, keyring.ErrWrongPassphrase) {
		t.Errorf("wrong passphrase: %v", err)
	}

	err = ops.Remove(t.Context(), []byte("recovery-pass"), "recovery")
	if !errors.Is(err, passphrase.ErrRemoveSelf) {
		t.Errorf("authorised by itself: %v", err)
	}

	err = ops.Remove(t.Context(), []byte("operator-pass"), "nope")
	if !errors.Is(err, keyring.ErrSlotNotFound) {
		t.Errorf("unknown slot: %v", err)
	}

	err = ops.Remove(t.Context(), []byte("operator-pass"), "recovery")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}

	err = ops.Remove(t.Context(), []byte("operator-pass"), passphrase.SlotNameGenerated)
	if !errors.Is(err, passphrase.ErrUnlockSlot) {
		t.Errorf("the generated unlock slot: %v", err)
	}
}

// A snapshot's recorded passwords count as much as a replica's: with no
// keyring, neither the bootstrap nor create-passphrase mints a new master
// key while a snapshot carries one.
func TestASnapshotKeyMeansTheKeyringIsLost(t *testing.T) {
	t.Parallel()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc.snap1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc", SnapshotName: "snap1",
			LUKSKeys: []blockstoriov1alpha1.SnapshotLUKSKey{{
				NodeName: "n1", VolumeNumber: 0,
				LUKSKey: blockstoriov1alpha1.LUKSKey{KeyID: "c7324c99567678f7", Wrapped: []byte{1}, Origin: blockstoriov1alpha1.LUKSKeyOriginGenerated},
			}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "x"), snap).Build()
	secrets := passphrase.Secrets{Reader: cli, Namespace: ns}

	created, err := passphrase.Bootstrap(t.Context(), cli, secrets, time.Now())
	if created || !errors.Is(err, passphrase.ErrKeyringLost) {
		t.Fatalf("bootstrap: created %v, %v", created, err)
	}

	gen := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(snap.DeepCopy()).Build()

	_, err = (&passphrase.Ops{Client: gen, Secrets: passphrase.Secrets{Reader: gen, Namespace: ns}}).Create(t.Context(), []byte("x"))
	if !errors.Is(err, passphrase.ErrKeyringLost) {
		t.Fatalf("create: %v", err)
	}
}

// The review's probe: on a cluster locked out of its generated unlock
// Secret, modify of the generated passphrase succeeded, and the next
// enter-passphrase repair dropped that slot, so the new passphrase opened
// nothing. Modify is refused on a locked cluster.
func TestModifyIsRefusedOnALockedCluster(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).Build()
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	_, err := ops.Create(t.Context(), []byte("operator-pass"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var unlock corev1.Secret

	err = cli.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: passphrase.UnlockSecretName}, &unlock)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	generated := bytes.Clone(unlock.Data[passphrase.DefaultPassphraseKey])

	err = cli.Delete(t.Context(), &unlock)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = ops.Modify(t.Context(), generated, []byte("mine"))
	if !errors.Is(err, passphrase.ErrLocked) {
		t.Fatalf("modify on a locked cluster: %v, want ErrLocked", err)
	}

	_, err = ops.Modify(t.Context(), []byte("operator-pass"), []byte("mine"))
	if !errors.Is(err, passphrase.ErrLocked) {
		t.Fatalf("modify of the operator's passphrase on a locked cluster: %v, want ErrLocked", err)
	}
}

// Remove protects the slot the passphrase Secret opens when the keyring is
// written, not when the state was read: a Secret moved to another slot in
// between keeps that slot.
func TestRemoveProtectsTheSlotTheSecretOpensNow(t *testing.T) {
	t.Parallel()

	base := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "one")).Build()
	setup := &passphrase.Ops{Client: base, Secrets: passphrase.Secrets{Reader: base, Namespace: ns}}

	_, err := passphrase.Bootstrap(t.Context(), base, setup.Secrets, time.Now())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	_, err = setup.Add(t.Context(), []byte("one"), []byte("two"), "next")
	if err != nil {
		t.Fatalf("add: %v", err)
	}

	keyringGets := 0

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == passphrase.KeyringSecretName {
				keyringGets++
				// The second read is the one inside the write; the
				// operator moves the Secret to "two" just before it.
				if keyringGets == 2 {
					flipSecret(ctx, t, c, "p", "two")
				}
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	err = ops.Remove(t.Context(), []byte("one"), "next")
	if !errors.Is(err, passphrase.ErrUnlockSlot) {
		t.Fatalf("remove of the slot the Secret moved to: %v, want ErrUnlockSlot", err)
	}

	if stateOf(t, setup).State != passphrase.StateUnlocked {
		t.Fatal("the cluster is locked")
	}
}

// Modify decides whether the old passphrase is the Secret's from the
// keyring it writes: a Secret moved to another slot in between makes the
// old slot an ordinary one, replaced outright rather than superseded.
func TestModifyReadsTheSlotTheSecretOpensNow(t *testing.T) {
	t.Parallel()

	base := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "one")).Build()
	setup := &passphrase.Ops{Client: base, Secrets: passphrase.Secrets{Reader: base, Namespace: ns}}

	_, err := passphrase.Bootstrap(t.Context(), base, setup.Secrets, time.Now())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	_, err = setup.Add(t.Context(), []byte("one"), []byte("two"), "next")
	if err != nil {
		t.Fatalf("add: %v", err)
	}

	keyringGets := 0

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == passphrase.KeyringSecretName {
				keyringGets++
				if keyringGets == 2 {
					flipSecret(ctx, t, c, "p", "two")
				}
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	result, err := ops.Modify(t.Context(), []byte("one"), []byte("three"))
	if err != nil || result.SecretToUpdate != "" {
		t.Fatalf("modify: %+v, %v; want a plain replace", result, err)
	}

	status := stateOf(t, setup)
	if _, err := status.Ring.Opens([]byte("one")); err == nil {
		t.Fatal("the replaced passphrase still opens")
	}
}

// The cleanup keeps the slot the passphrase Secret opens when the keyring is
// written, not when the state was read: a Secret moved back to the
// superseded passphrase in between keeps that slot, and the cluster stays
// unlocked.
func TestCleanupKeepsTheSlotTheSecretOpensNow(t *testing.T) {
	t.Parallel()

	base := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "one")).Build()
	setup := &passphrase.Ops{Client: base, Secrets: passphrase.Secrets{Reader: base, Namespace: ns}}

	_, err := passphrase.Bootstrap(t.Context(), base, setup.Secrets, time.Now())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	result, err := setup.Modify(t.Context(), []byte("one"), []byte("two"))
	if err != nil || result.SecretToUpdate != "p" {
		t.Fatalf("modify: %+v, %v", result, err)
	}

	setSecret(t, base, "p", "two")

	passGets := 0

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			// The state is read with the Secret on "two"; right after,
			// the operator rolls the Secret back to "one".
			if key.Name == "p" {
				passGets++
				if passGets == 1 {
					defer flipSecret(ctx, t, c, "p", "one")
				}
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	removed, err := ops.CleanupSuperseded(t.Context())
	if err != nil || removed {
		t.Fatalf("cleanup: %v, %v; want the slot the Secret opens again kept", removed, err)
	}

	if stateOf(t, setup).State != passphrase.StateUnlocked {
		t.Fatal("the cluster is locked")
	}
}

// The cleanup decides on the ControllerConfig the API server holds, not
// on the one a watch saw: a Secret the operator has switched back to, but
// the watch has not seen yet, keeps the slot it opens, and the cluster
// stays unlocked.
func TestCleanupKeepsTheSlotOfASecretTheWatchHasNotSeen(t *testing.T) {
	t.Parallel()

	direct := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		refConfig("a", ""), passSecret("a", "MASTER_PASSPHRASE", "one"),
		passSecret("b", "MASTER_PASSPHRASE", "two")).Build()
	setup := &passphrase.Ops{Client: direct, Secrets: passphrase.Secrets{Reader: direct, Namespace: ns}}

	_, err := passphrase.Bootstrap(t.Context(), direct, setup.Secrets, time.Now())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// The rotation: slot X, opened by a, is superseded by slot Y, opened
	// by b, and the operator points the ControllerConfig at b.
	_, err = setup.Modify(t.Context(), []byte("one"), []byte("two"))
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	var ringSec corev1.Secret

	err = direct.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: passphrase.KeyringSecretName}, &ringSec)
	if err != nil {
		t.Fatalf("get keyring: %v", err)
	}

	ringSec.ResourceVersion = ""

	// The watch still holds b; the operator has since switched back to a.
	watch := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		refConfig("b", ""), passSecret("a", "MASTER_PASSPHRASE", "one"),
		passSecret("b", "MASTER_PASSPHRASE", "two"), &ringSec).Build()

	ops := &passphrase.Ops{Client: direct, Secrets: passphrase.Secrets{Reader: watch, Namespace: ns, Direct: direct}}

	removed, err := ops.CleanupSuperseded(t.Context())
	if err != nil || removed {
		t.Fatalf("cleanup: %v, %v; want the slot Secret a opens kept", removed, err)
	}

	if stateOf(t, setup).State != passphrase.StateUnlocked {
		t.Fatal("the cluster is locked: Secret a opens no slot")
	}
}

// The cleanup answers the steady state from the keyring alone, and while a
// superseded slot waits for the Secret to move, a shared Unlocker tries the
// Secret's passphrase once, not once per call.
func TestCleanupDerivesOncePerSecretVersion(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		refConfig("p", ""), passSecret("p", "MASTER_PASSPHRASE", "one")).Build()
	secrets := passphrase.Secrets{Reader: cli, Namespace: ns}

	_, err := passphrase.Bootstrap(t.Context(), cli, secrets, time.Now())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	unlocker := &passphrase.Unlocker{Secrets: secrets}
	cleanup := &passphrase.Ops{Client: cli, Secrets: secrets, Unlocker: unlocker}

	for range 3 {
		removed, err := cleanup.CleanupSuperseded(t.Context())
		if err != nil || removed {
			t.Fatalf("cleanup with no superseded slot: %v, %v", removed, err)
		}
	}

	if got := passphrase.UnlockAttempts(unlocker); got != 0 {
		t.Fatalf("%d passphrases tried with no superseded slot, want none", got)
	}

	_, err = (&passphrase.Ops{Client: cli, Secrets: secrets}).Modify(t.Context(), []byte("one"), []byte("two"))
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	for range 3 {
		removed, err := cleanup.CleanupSuperseded(t.Context())
		if err != nil || removed {
			t.Fatalf("cleanup before the Secret moved: %v, %v", removed, err)
		}
	}

	if got := passphrase.UnlockAttempts(unlocker); got != 1 {
		t.Fatalf("%d passphrases tried while the Secret did not change, want one", got)
	}
}

// Enter's repair of a locked cluster on the generated unlock Secret drops
// the generated slots by origin: an operator's slot that happens to be
// named "generated" holds a passphrase someone knows, and stays.
func TestEnterDropsGeneratedSlotsByOrigin(t *testing.T) {
	t.Parallel()

	ring, master, err := keyring.New([]byte("first"), passphrase.SlotNameGenerated, keyring.OriginAPI, time.Now())
	if err != nil {
		t.Fatalf("keyring.New: %v", err)
	}

	defer master.Destroy()

	_, err = ring.AddSlot(master, []byte("lost"), "unlock", keyring.OriginGenerated, time.Now())
	if err != nil {
		t.Fatalf("AddSlot: %v", err)
	}

	// No ControllerConfig, so the cluster runs on the generated unlock
	// Secret; that Secret is gone, so it is locked.
	cli := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(keyringSecret(t, ring)).Build()
	ops := &passphrase.Ops{Client: cli, Secrets: passphrase.Secrets{Reader: cli, Namespace: ns}}

	result, err := ops.Enter(t.Context(), []byte("first"))
	if err != nil || !result.Repaired || result.Status.State != passphrase.StateUnlocked {
		t.Fatalf("enter: %+v, %v; want a repaired, unlocked cluster", result, err)
	}

	status := stateOf(t, ops)

	if _, err := status.Ring.Opens([]byte("first")); err != nil {
		t.Fatalf("the operator's slot named %q was dropped", passphrase.SlotNameGenerated)
	}

	if _, err := status.Ring.Opens([]byte("lost")); err == nil {
		t.Fatal("the old generated slot survived the repair")
	}

	generated := 0

	for _, slot := range status.Ring.Slots {
		if slot.Origin == keyring.OriginGenerated {
			generated++
		}
	}

	if generated != 1 || len(status.Ring.Slots) != 2 {
		t.Fatalf("slots %+v; want the operator's and one fresh generated slot", status.Ring.Slots)
	}
}

// flipSecret is setSecret on ctx, for a change made inside a client call.
func flipSecret(ctx context.Context, t *testing.T, cli client.Client, name, value string) {
	t.Helper()

	var sec corev1.Secret

	err := cli.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &sec)
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}

	sec.Data["MASTER_PASSPHRASE"] = []byte(value)

	err = cli.Update(ctx, &sec)
	if err != nil {
		t.Fatalf("update %s: %v", name, err)
	}
}
