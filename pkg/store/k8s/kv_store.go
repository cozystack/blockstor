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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// keyEncodeMarker substitutes for '/' in ConfigMap data keys, since
// ConfigMap keys are restricted to [-._a-zA-Z0-9]. LINSTOR property keys
// look like "DrbdOptions/Net/foo", so we encode on Set and decode on Get.
const keyEncodeMarker = "__slash__"

func encodeKey(k string) string { return strings.ReplaceAll(k, "/", keyEncodeMarker) }
func decodeKey(k string) string { return strings.ReplaceAll(k, keyEncodeMarker, "/") }

// LabelKVInstance marks ConfigMaps that back the LINSTOR KeyValueStore. We
// reuse a native Kubernetes resource rather than introducing yet another CRD
// for opaque string maps.
const LabelKVInstance = "blockstor.io/kv-instance"

// KVNamespace is the namespace blockstor stores KV ConfigMaps in. Cluster
// admins set this via the BLOCKSTOR_KV_NAMESPACE env var or the controller's
// own namespace; the default is fine for tests.
var KVNamespace = "default" //nolint:gochecknoglobals // single configurable per-process

type kvStore struct {
	c ctrlclient.Client
}

func (s *kvStore) ListInstances(ctx context.Context) ([]string, error) {
	var cms corev1.ConfigMapList

	err := s.c.List(ctx, &cms,
		ctrlclient.InNamespace(KVNamespace),
		ctrlclient.HasLabels{LabelKVInstance})
	if err != nil {
		return nil, errors.Wrap(err, "list KV ConfigMaps")
	}

	out := make([]string, 0, len(cms.Items))
	for i := range cms.Items {
		out = append(out, cms.Items[i].Labels[LabelKVInstance])
	}

	sort.Strings(out)

	return out, nil
}

func (s *kvStore) GetInstance(ctx context.Context, instance string) (map[string]string, error) {
	cm, err := s.fetchCM(ctx, instance)
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, len(cm.Data))
	for k, v := range cm.Data {
		out[decodeKey(k)] = v
	}

	return out, nil
}

// SetKeys creates the ConfigMap on first call (so callers don't need a
// separate Create), and applies override+delete atomically per the upstream
// semantics.
func (s *kvStore) SetKeys(ctx context.Context, instance string, modify apiv1.GenericPropsModify) error {
	cm, err := s.fetchCM(ctx, instance)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	if errors.Is(err, store.ErrNotFound) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      kvCMName(instance),
				Namespace: KVNamespace,
				Labels:    map[string]string{LabelKVInstance: instance},
			},
			Data: map[string]string{},
		}

		applyModify(cm, modify)

		err = s.c.Create(ctx, cm)
		if err != nil {
			return errors.Wrapf(err, "create KV ConfigMap %q", instance)
		}

		return nil
	}

	applyModify(cm, modify)

	err = s.c.Update(ctx, cm)
	if err != nil {
		return errors.Wrapf(err, "update KV ConfigMap %q", instance)
	}

	return nil
}

func (s *kvStore) DeleteInstance(ctx context.Context, instance string) error {
	cm, err := s.fetchCM(ctx, instance)
	if err != nil {
		return err
	}

	err = s.c.Delete(ctx, cm)
	if err != nil {
		return errors.Wrapf(err, "delete KV ConfigMap %q", instance)
	}

	return nil
}

func (s *kvStore) fetchCM(ctx context.Context, instance string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap

	err := s.c.Get(ctx, types.NamespacedName{Namespace: KVNamespace, Name: kvCMName(instance)}, &cm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(store.ErrNotFound, "kv instance %q", instance)
		}

		return nil, errors.Wrapf(err, "get KV ConfigMap %q", instance)
	}

	return &cm, nil
}

func applyModify(cm *corev1.ConfigMap, modify apiv1.GenericPropsModify) {
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}

	for k, v := range modify.OverrideProps {
		cm.Data[encodeKey(k)] = v
	}

	for _, k := range modify.DeleteProps {
		delete(cm.Data, encodeKey(k))
	}

	for _, ns := range modify.DeleteNamespace {
		encodedNS := encodeKey(ns)
		for k := range cm.Data {
			if k == encodedNS || (len(k) > len(encodedNS) && k[:len(encodedNS)] == encodedNS && k[len(encodedNS):len(encodedNS)+len(keyEncodeMarker)] == keyEncodeMarker) {
				delete(cm.Data, k)
			}
		}
	}
}

func kvCMName(instance string) string {
	return "blockstor-kv-" + instance
}
