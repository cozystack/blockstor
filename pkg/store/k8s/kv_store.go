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

package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// LabelKVInstanceHash is a deterministic hash of the KV instance name. We
// can't put the raw instance string on a k8s label (`/`, length, charset),
// so we hash it. List-by-instance still goes through this label as a
// server-side filter; the in-memory loop afterwards verifies against the
// raw spec.instance to catch the once-in-2^64 hash collision.
const LabelKVInstanceHash = "blockstor.io/kv-instance-hash"

// kvNamePrefix keeps KVEntry CRD names visually distinct from other CRDs
// when an admin runs `kubectl get kventries`.
const kvNamePrefix = "kv-"

type kvStore struct {
	c ctrlclient.Client
}

// kvCRDName turns a (instance, key) pair into a deterministic, k8s-valid
// metadata.name. The full original strings live on the CRD's spec; the
// name is opaque, just guarantees uniqueness.
func kvCRDName(instance, key string) string {
	sum := sha256.Sum256([]byte(instance + "\x00" + key))

	return kvNamePrefix + hex.EncodeToString(sum[:16])
}

// kvInstanceHash is shorter than the CRD name (8 bytes hex = 16 chars) so
// it fits comfortably under the 63-char label-value limit.
func kvInstanceHash(instance string) string {
	sum := sha256.Sum256([]byte(instance))

	return hex.EncodeToString(sum[:8])
}

// ListInstances returns the distinct instance names across all entries.
// Implemented as List-everything + dedupe; on small clusters this is
// negligible, on large clusters we'll add a per-instance index later.
func (s *kvStore) ListInstances(ctx context.Context) ([]string, error) {
	var entries crdv1alpha1.KVEntryList

	err := s.c.List(ctx, &entries)
	if err != nil {
		return nil, errors.Wrap(err, "list KVEntry CRDs")
	}

	seen := make(map[string]struct{}, len(entries.Items))
	for i := range entries.Items {
		seen[entries.Items[i].Spec.Instance] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}

	sort.Strings(out)

	return out, nil
}

// GetInstance returns the props map of the named instance, or ErrNotFound
// if the instance has no entries at all.
func (s *kvStore) GetInstance(ctx context.Context, instance string) (map[string]string, error) {
	entries, err := s.entriesForInstance(ctx, instance)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, errors.Wrapf(store.ErrNotFound, "kv instance %q", instance)
	}

	out := make(map[string]string, len(entries))
	for i := range entries {
		out[entries[i].Spec.Key] = entries[i].Spec.Value
	}

	return out, nil
}

// SetKeys applies the upstream override/delete payload. Creates the
// instance on first set (no separate Create call needed), and applies
// override + delete-keys + delete-namespace in that order.
func (s *kvStore) SetKeys(ctx context.Context, instance string, modify apiv1.GenericPropsModify) error {
	for k, v := range modify.OverrideProps {
		err := s.setOne(ctx, instance, k, v)
		if err != nil {
			return err
		}
	}

	for _, k := range modify.DeleteProps {
		err := s.deleteOne(ctx, instance, k)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}

	if len(modify.DeleteNamespace) > 0 {
		err := s.deleteNamespaces(ctx, instance, modify.DeleteNamespace)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteInstance removes every entry of the named instance. Errors with
// ErrNotFound if no entries exist.
func (s *kvStore) DeleteInstance(ctx context.Context, instance string) error {
	entries, err := s.entriesForInstance(ctx, instance)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return errors.Wrapf(store.ErrNotFound, "kv instance %q", instance)
	}

	for i := range entries {
		dErr := s.c.Delete(ctx, &entries[i])
		if dErr != nil && !apierrors.IsNotFound(dErr) {
			return errors.Wrapf(dErr, "delete KVEntry %q", entries[i].Name)
		}
	}

	return nil
}

// entriesForInstance fetches all entries belonging to one instance via the
// label index, then re-checks Spec.Instance to defend against hash
// collisions.
func (s *kvStore) entriesForInstance(ctx context.Context, instance string) ([]crdv1alpha1.KVEntry, error) {
	var raw crdv1alpha1.KVEntryList

	err := s.c.List(ctx, &raw,
		ctrlclient.MatchingLabels{LabelKVInstanceHash: kvInstanceHash(instance)})
	if err != nil {
		return nil, errors.Wrapf(err, "list KVEntries for instance %q", instance)
	}

	out := make([]crdv1alpha1.KVEntry, 0, len(raw.Items))
	for i := range raw.Items {
		if raw.Items[i].Spec.Instance == instance {
			out = append(out, raw.Items[i])
		}
	}

	return out, nil
}

// setOne is the upsert primitive. Get → mutate-or-create round trip; the
// CRD is keyed by hash so two writers on the same (instance, key) hit the
// same name and one of them gets a conflict (callers retry).
func (s *kvStore) setOne(ctx context.Context, instance, key, value string) error {
	name := kvCRDName(instance, key)

	var existing crdv1alpha1.KVEntry

	err := s.c.Get(ctx, types.NamespacedName{Name: name}, &existing)
	if err == nil {
		existing.Spec.Value = value
		// instance / key are immutable on the CRD; we don't touch them.
		uErr := s.c.Update(ctx, &existing)
		if uErr != nil {
			return errors.Wrapf(uErr, "update KVEntry %s/%s", instance, key)
		}

		return nil
	}

	if !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "get KVEntry %s/%s", instance, key)
	}

	entry := &crdv1alpha1.KVEntry{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				LabelKVInstanceHash: kvInstanceHash(instance),
			},
		},
		Spec: crdv1alpha1.KVEntrySpec{
			Instance: instance,
			Key:      key,
			Value:    value,
		},
	}

	cErr := s.c.Create(ctx, entry)
	if cErr != nil {
		return errors.Wrapf(cErr, "create KVEntry %s/%s", instance, key)
	}

	return nil
}

// deleteOne is the inverse of setOne for a single key.
func (s *kvStore) deleteOne(ctx context.Context, instance, key string) error {
	name := kvCRDName(instance, key)

	entry := &crdv1alpha1.KVEntry{ObjectMeta: metav1.ObjectMeta{Name: name}}

	err := s.c.Delete(ctx, entry)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "kv %s/%s", instance, key)
		}

		return errors.Wrapf(err, "delete KVEntry %s/%s", instance, key)
	}

	return nil
}

// deleteNamespaces removes every key under any of the listed LINSTOR
// namespace paths (slash-separated prefixes).
func (s *kvStore) deleteNamespaces(ctx context.Context, instance string, namespaces []string) error {
	entries, err := s.entriesForInstance(ctx, instance)
	if err != nil {
		return err
	}

	for i := range entries {
		key := entries[i].Spec.Key

		match := false

		for _, ns := range namespaces {
			if key == ns || strings.HasPrefix(key, ns+"/") {
				match = true

				break
			}
		}

		if !match {
			continue
		}

		dErr := s.c.Delete(ctx, &entries[i])
		if dErr != nil && !apierrors.IsNotFound(dErr) {
			return errors.Wrapf(dErr, "delete KVEntry %q", entries[i].Name)
		}
	}

	return nil
}
