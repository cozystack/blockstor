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

package k8s_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

func testLUKSKey(fill byte) *crdv1alpha1.LUKSKey {
	return &crdv1alpha1.LUKSKey{
		KeyID:   "c7324c99567678f7",
		Wrapped: bytes.Repeat([]byte{fill}, 12+64+16),
		Origin:  crdv1alpha1.LUKSKeyOriginGenerated,
	}
}

func replicaWithKeys(rd, node string, keys map[int32]*crdv1alpha1.LUKSKey) *crdv1alpha1.Resource {
	res := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: node},
	}

	for vol := range int32(len(keys) + 1) {
		key, ok := keys[vol]
		if !ok {
			continue
		}

		res.Spec.Volumes = append(res.Spec.Volumes, crdv1alpha1.ResourceVolumeSpec{VolumeNumber: vol, LUKSKey: key})
	}

	return res
}

func requireEnvtest(t *testing.T) {
	t.Helper()

	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })
}

// The API server keeps a replica's wrapped password once it is set: it may
// be added to a volume that has none, but not changed or dropped while the
// volume stays; the volume itself may still go.
func TestLUKSKeyIsSetOnce(t *testing.T) {
	requireEnvtest(t)

	ctx := t.Context()
	res := replicaWithKeys("pvc-once", "node-a", nil)
	res.Spec.Volumes = []crdv1alpha1.ResourceVolumeSpec{{VolumeNumber: 0}, {VolumeNumber: 1, LUKSKey: testLUKSKey(1)}}

	err := fixture.client.Create(ctx, res)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	update := func(mutate func(*crdv1alpha1.Resource)) error {
		var cur crdv1alpha1.Resource

		err := fixture.client.Get(ctx, client.ObjectKeyFromObject(res), &cur)
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		mutate(&cur)

		return fixture.client.Update(ctx, &cur)
	}

	err = update(func(r *crdv1alpha1.Resource) { r.Spec.Volumes[0].LUKSKey = testLUKSKey(2) })
	if err != nil {
		t.Fatalf("setting a key on a volume without one: %v", err)
	}

	refused := map[string]func(*crdv1alpha1.Resource){
		"changed": func(r *crdv1alpha1.Resource) { r.Spec.Volumes[1].LUKSKey = testLUKSKey(3) },
		"removed": func(r *crdv1alpha1.Resource) { r.Spec.Volumes[1].LUKSKey = nil },
		"key ID":  func(r *crdv1alpha1.Resource) { r.Spec.Volumes[0].LUKSKey.KeyID = "0000000000000000" },
		"origin": func(r *crdv1alpha1.Resource) {
			r.Spec.Volumes[0].LUKSKey.Origin = crdv1alpha1.LUKSKeyOriginLinstorImport
		},
	}

	for name, mutate := range refused {
		err = update(mutate)
		if !apierrors.IsInvalid(err) {
			t.Errorf("%s: err %v, want Invalid", name, err)
		}
	}

	err = update(func(r *crdv1alpha1.Resource) { r.Spec.Volumes = r.Spec.Volumes[:1] })
	if err != nil {
		t.Errorf("dropping a volume with its key: %v", err)
	}

	bad := replicaWithKeys("pvc-bad", "node-a", map[int32]*crdv1alpha1.LUKSKey{0: testLUKSKey(1)})
	bad.Spec.Volumes[0].LUKSKey.KeyID = "not-hex"

	err = fixture.client.Create(ctx, bad)
	if !apierrors.IsInvalid(err) {
		t.Errorf("malformed key ID: err %v, want Invalid", err)
	}
}

// A routine REST modify rebuilds the spec from the wire shape, which has no
// keys; the wrapped passwords survive it.
func TestLUKSKeySurvivesWireRewrites(t *testing.T) {
	requireEnvtest(t)

	ctx := t.Context()
	st := k8s.New(fixture.client)

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-carry"})
	if err != nil {
		t.Fatalf("create the definition: %v", err)
	}

	err = fixture.client.Create(ctx, replicaWithKeys("pvc-carry", "node-a", map[int32]*crdv1alpha1.LUKSKey{0: testLUKSKey(7)}))
	if err != nil {
		t.Fatalf("create the replica: %v", err)
	}

	wire, err := st.Resources().Get(ctx, "pvc-carry", "node-a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	wire.Props = map[string]string{"Aux/foo": "bar"}

	err = st.Resources().Update(ctx, &wire)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	err = st.Resources().PatchResourceSpec(ctx, "pvc-carry", "node-a", func(r *apiv1.Resource) error {
		r.Props["Aux/foo"] = "baz"

		return nil
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	var got crdv1alpha1.Resource

	err = fixture.client.Get(ctx, client.ObjectKey{Name: "pvc-carry.node-a"}, &got)
	if err != nil {
		t.Fatalf("get CR: %v", err)
	}

	if len(got.Spec.Volumes) != 1 || got.Spec.Volumes[0].LUKSKey == nil ||
		!bytes.Equal(got.Spec.Volumes[0].LUKSKey.Wrapped, testLUKSKey(7).Wrapped) {
		t.Fatalf("luksKey lost: %+v", got.Spec.Volumes)
	}
}

// A snapshot carries the wrapped password of each replica its bytes come
// from, on the nodes it names or on every node when it names none, and keeps
// them across a REST modify; the API server refuses to drop them.
func TestSnapshotCopiesTheSourceReplicasLUKSKeys(t *testing.T) {
	requireEnvtest(t)

	ctx := t.Context()
	st := k8s.New(fixture.client)

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "Pvc-Snap"})
	if err != nil {
		t.Fatalf("create the definition: %v", err)
	}

	for _, res := range []*crdv1alpha1.Resource{
		replicaWithKeys("pvc-snap", "node-a", map[int32]*crdv1alpha1.LUKSKey{0: testLUKSKey(0xa0), 1: testLUKSKey(0xa1)}),
		replicaWithKeys("pvc-snap", "node-b", map[int32]*crdv1alpha1.LUKSKey{0: testLUKSKey(0xb0)}),
		replicaWithKeys("pvc-snap", "node-c", nil),
		replicaWithKeys("pvc-other", "node-a", map[int32]*crdv1alpha1.LUKSKey{0: testLUKSKey(0xff)}),
	} {
		err = fixture.client.Create(ctx, res)
		if err != nil {
			t.Fatalf("create %s: %v", res.Name, err)
		}
	}

	cases := map[string]struct {
		nodes []string
		want  map[string]byte
	}{
		"named":   {nodes: []string{"Node-A", "node-c"}, want: map[string]byte{"node-a/0": 0xa0, "node-a/1": 0xa1}},
		"unnamed": {nodes: nil, want: map[string]byte{"node-a/0": 0xa0, "node-a/1": 0xa1, "node-b/0": 0xb0}},
	}

	for name, tc := range cases {
		err = st.Snapshots().Create(ctx, &apiv1.Snapshot{ResourceName: "Pvc-Snap", Name: name, Nodes: tc.nodes})
		if err != nil {
			t.Fatalf("%s: create the snapshot: %v", name, err)
		}

		got := snapshotKeys(ctx, t, "pvc-snap."+name)
		if len(got) != len(tc.want) {
			t.Errorf("%s: keys %v, want %v", name, got, tc.want)
		}

		for at, fill := range tc.want {
			if got[at] != fill {
				t.Errorf("%s: key at %s is %#x, want %#x", name, at, got[at], fill)
			}
		}
	}

	snap, err := st.Snapshots().Get(ctx, "Pvc-Snap", "named")
	if err != nil {
		t.Fatalf("get the snapshot: %v", err)
	}

	snap.Props = map[string]string{"Aux/foo": "bar"}

	err = st.Snapshots().Update(ctx, &snap)
	if err != nil {
		t.Fatalf("update the snapshot: %v", err)
	}

	if got := snapshotKeys(ctx, t, "pvc-snap.named"); len(got) != 2 {
		t.Fatalf("keys after a REST modify: %v", got)
	}

	var raw crdv1alpha1.Snapshot

	err = fixture.client.Get(ctx, client.ObjectKey{Name: "pvc-snap.named"}, &raw)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	raw.Spec.LUKSKeys = nil

	err = fixture.client.Update(ctx, &raw)
	if !apierrors.IsInvalid(err) {
		t.Fatalf("dropping the keys: err %v, want Invalid", err)
	}
}

func snapshotKeys(ctx context.Context, t *testing.T, name string) map[string]byte {
	t.Helper()

	var snap crdv1alpha1.Snapshot

	err := fixture.client.Get(ctx, client.ObjectKey{Name: name}, &snap)
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}

	out := map[string]byte{}

	for _, key := range snap.Spec.LUKSKeys {
		out[key.NodeName+"/"+strconv.Itoa(int(key.VolumeNumber))] = key.Wrapped[0]
	}

	return out
}

// Where the field selector is not served, the copy falls back to reading
// every replica and still keeps only the definition's own.
func TestSnapshotLUKSKeysFallBackToAFullRead(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()

	err := crdv1alpha1.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	rd := crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-scan"}}
	k8s.SetOriginalName(&rd.ObjectMeta, "pvc-scan")

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&rd,
		replicaWithKeys("pvc-scan", "node-a", map[int32]*crdv1alpha1.LUKSKey{0: testLUKSKey(0xa0)}),
		replicaWithKeys("pvc-other", "node-a", map[int32]*crdv1alpha1.LUKSKey{0: testLUKSKey(0xff)}),
	).Build()

	err = k8s.New(cli).Snapshots().Create(context.Background(), &apiv1.Snapshot{ResourceName: "pvc-scan", Name: "s1"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var snap crdv1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-scan.s1"}, &snap)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(snap.Spec.LUKSKeys) != 1 || snap.Spec.LUKSKeys[0].Wrapped[0] != 0xa0 {
		t.Fatalf("keys %+v, want only pvc-scan's", snap.Spec.LUKSKeys)
	}
}

// The passphrase Secret's data key defaults to piraeus's MASTER_PASSPHRASE.
func TestPassphraseSecretKeyDefaultsToPiraeus(t *testing.T) {
	requireEnvtest(t)

	ctx := t.Context()
	cfg := &crdv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "luks-default-key"},
		Spec: crdv1alpha1.ControllerConfigSpec{
			PassphraseSecretRef: &crdv1alpha1.PassphraseSecretRef{Name: "linstor-passphrase"},
		},
	}

	err := fixture.client.Create(ctx, cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	t.Cleanup(func() { _ = fixture.client.Delete(context.Background(), cfg) })

	var got crdv1alpha1.ControllerConfig

	err = fixture.client.Get(ctx, client.ObjectKeyFromObject(cfg), &got)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.PassphraseSecretRef.Key != "MASTER_PASSPHRASE" {
		t.Fatalf("key %q, want MASTER_PASSPHRASE", got.Spec.PassphraseSecretRef.Key)
	}
}

// testLUKSKeyStdBase64 is a key whose JSON form, standard base64, carries
// '+' and '/': AES-GCM output does in nearly every real key.
func testLUKSKeyStdBase64() *crdv1alpha1.LUKSKey {
	return &crdv1alpha1.LUKSKey{
		KeyID:   "c7324c99567678f7",
		Wrapped: bytes.Repeat([]byte{0xfb, 0xff}, (12+64+16)/2),
		Origin:  crdv1alpha1.LUKSKeyOriginGenerated,
	}
}

// A replica and a snapshot carrying a real wrapped password stay writable:
// the set-once rules must read the password the way it is stored, or every
// later write to the object, the satellite's finalizer strip on delete
// among them, is refused and the replica can never be deleted.
func TestLUKSKeyOfAnyBytesKeepsTheObjectsWritable(t *testing.T) {
	requireEnvtest(t)

	ctx := t.Context()
	key := testLUKSKeyStdBase64()
	res := replicaWithKeys("pvc-bytes", "node-a", map[int32]*crdv1alpha1.LUKSKey{0: key})
	res.Finalizers = []string{"blockstor.org/test"}

	err := fixture.client.Create(ctx, res)
	if err != nil {
		t.Fatalf("create the replica: %v", err)
	}

	res.Spec.Flags = []string{apiv1.ResourceFlagDiskless}

	err = fixture.client.Update(ctx, res)
	if err != nil {
		t.Fatalf("update the replica's flags: %v", err)
	}

	changed := res.DeepCopy()
	changed.Spec.Volumes[0].LUKSKey.Wrapped = bytes.Repeat([]byte{0xfb, 0xfe}, (12+64+16)/2)

	err = fixture.client.Update(ctx, changed)
	if !apierrors.IsInvalid(err) {
		t.Errorf("changing the replica's password: err %v, want Invalid", err)
	}

	err = fixture.client.Delete(ctx, res)
	if err != nil {
		t.Fatalf("delete the replica: %v", err)
	}

	err = fixture.client.Get(ctx, client.ObjectKeyFromObject(res), res)
	if err != nil {
		t.Fatalf("get the deleting replica: %v", err)
	}

	res.Finalizers = nil

	err = fixture.client.Update(ctx, res)
	if err != nil {
		t.Fatalf("strip the finalizer of the deleting replica: %v", err)
	}

	malformed := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": crdv1alpha1.GroupVersion.String(),
		"kind":       "Resource",
		"metadata":   map[string]any{"name": "pvc-bytes.node-b"},
		"spec": map[string]any{
			"resourceDefinitionName": "pvc-bytes",
			"nodeName":               "node-b",
			"volumes": []any{map[string]any{"volumeNumber": int64(0), "luksKey": map[string]any{
				"keyID": key.KeyID, "wrapped": "-_not-std-base64", "origin": string(key.Origin),
			}}},
		},
	}}

	err = fixture.client.Create(ctx, malformed)
	if !apierrors.IsInvalid(err) {
		t.Errorf("a password that is not standard base64: err %v, want Invalid", err)
	}

	snap := &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bytes.snap", Finalizers: []string{"blockstor.org/test"}},
		Spec: crdv1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-bytes",
			SnapshotName:           "snap",
			LUKSKeys:               []crdv1alpha1.SnapshotLUKSKey{{NodeName: "node-a", VolumeNumber: 0, LUKSKey: *key}},
		},
	}

	err = fixture.client.Create(ctx, snap)
	if err != nil {
		t.Fatalf("create the snapshot: %v", err)
	}

	snap.Finalizers = nil

	err = fixture.client.Update(ctx, snap)
	if err != nil {
		t.Fatalf("strip the finalizer of the snapshot: %v", err)
	}
}
