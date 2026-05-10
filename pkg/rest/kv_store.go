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

package rest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// CSIVolumesInstance / CSIVolumeAnnotation are the well-known
// names linstor-csi uses to push per-PVC JSON metadata. Phase
// 10.4 routes the legacy KVEntry-shaped traffic on the
// `csi-volumes` instance to ResourceDefinition annotations
// under `blockstor.io/csi-volume-data`. Other instances fall
// through to the still-KVEntry-backed `KeyValueStore` until
// their migration lands.
const (
	CSIVolumesInstance  = "csi-volumes"
	CSIVolumeAnnotation = "blockstor.io/csi-volume-data"
)

// registerKeyValueStore wires /v1/key-value-store endpoints. linstor-csi
// uses these for its own per-volume bookkeeping.
func (s *Server) registerKeyValueStore(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/key-value-store", s.requireStore(s.handleKVList))
	mux.HandleFunc("GET /v1/key-value-store/{instance}", s.requireStore(s.handleKVGet))
	mux.HandleFunc("POST /v1/key-value-store/{instance}", s.requireStore(s.handleKVSet))
	mux.HandleFunc("PUT /v1/key-value-store/{instance}", s.requireStore(s.handleKVSet))
	mux.HandleFunc("DELETE /v1/key-value-store/{instance}", s.requireStore(s.handleKVDelete))
}

func (s *Server) handleKVList(w http.ResponseWriter, r *http.Request) {
	insts, err := s.Store.KeyValueStore().ListInstances(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	out := make([]apiv1.KV, 0, len(insts))

	for _, name := range insts {
		props, gErr := s.Store.KeyValueStore().GetInstance(r.Context(), name)
		if gErr != nil {
			continue
		}

		out = append(out, apiv1.KV{Name: name, Props: props})
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	if name == CSIVolumesInstance {
		props, err := readCSIVolumesAnnotations(r.Context(), s.Store)
		if err != nil {
			writeStoreError(w, err)

			return
		}

		writeJSON(w, http.StatusOK, apiv1.KV{Name: name, Props: props})

		return
	}

	props, err := s.Store.KeyValueStore().GetInstance(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, apiv1.KV{Name: name, Props: props})
}

func (s *Server) handleKVSet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	var modify apiv1.GenericPropsModify

	err := json.NewDecoder(r.Body).Decode(&modify)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if name == CSIVolumesInstance {
		err = applyCSIVolumesAnnotations(r.Context(), s.Store, modify)
		if err != nil {
			writeStoreError(w, err)

			return
		}

		w.WriteHeader(http.StatusOK)

		return
	}

	err = s.Store.KeyValueStore().SetKeys(r.Context(), name, modify)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	if name == CSIVolumesInstance {
		err := clearAllCSIVolumesAnnotations(r.Context(), s.Store)
		if err != nil {
			writeStoreError(w, err)

			return
		}

		w.WriteHeader(http.StatusNoContent)

		return
	}

	err := s.Store.KeyValueStore().DeleteInstance(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// readCSIVolumesAnnotations assembles the legacy KV-shaped instance
// view of the csi-volumes namespace by walking every RD and
// collecting `blockstor.io/csi-volume-data` annotations into the
// `{pvc-name: json-blob}` map golinstor expects.
func readCSIVolumesAnnotations(ctx context.Context, st store.Store) (map[string]string, error) {
	rds, err := st.ResourceDefinitions().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list ResourceDefinitions")
	}

	out := map[string]string{}

	for i := range rds {
		val := rds[i].Annotations[CSIVolumeAnnotation]
		if val == "" {
			continue
		}

		out[rds[i].Name] = val
	}

	return out, nil
}

// applyCSIVolumesAnnotations applies a GenericPropsModify envelope
// onto RD annotations: each OverrideProps entry sets the annotation
// on the matching RD; each DeleteProps entry clears it. Missing RDs
// silently skip — a PVC that hasn't yet provisioned an RD shouldn't
// fail the whole batch (the controller will catch up).
func applyCSIVolumesAnnotations(ctx context.Context, st store.Store, modify apiv1.GenericPropsModify) error {
	for pvcName, value := range modify.OverrideProps {
		err := setOneCSIVolumeAnnotation(ctx, st, pvcName, value)
		if err != nil {
			return err
		}
	}

	for _, pvcName := range modify.DeleteProps {
		err := setOneCSIVolumeAnnotation(ctx, st, pvcName, "")
		if err != nil {
			return err
		}
	}

	return nil
}

// setOneCSIVolumeAnnotation writes (or clears, when value=="") the
// CSI-volume annotation on a single RD. Treats NotFound as a soft
// skip — see applyCSIVolumesAnnotations comment.
func setOneCSIVolumeAnnotation(ctx context.Context, st store.Store, pvcName, value string) error {
	rd, err := st.ResourceDefinitions().Get(ctx, pvcName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}

		return errors.Wrapf(err, "get RD %q", pvcName)
	}

	if rd.Annotations == nil {
		rd.Annotations = map[string]string{}
	}

	if value == "" {
		delete(rd.Annotations, CSIVolumeAnnotation)
	} else {
		rd.Annotations[CSIVolumeAnnotation] = value
	}

	err = st.ResourceDefinitions().Update(ctx, &rd)
	if err != nil {
		return errors.Wrapf(err, "update RD %q", pvcName)
	}

	return nil
}

// clearAllCSIVolumesAnnotations is the per-instance DELETE
// counterpart: walk every RD and strip the CSI annotation.
// Used very rarely (`linstor c kv delete csi-volumes`).
func clearAllCSIVolumesAnnotations(ctx context.Context, st store.Store) error {
	rds, err := st.ResourceDefinitions().List(ctx)
	if err != nil {
		return errors.Wrap(err, "list ResourceDefinitions")
	}

	for i := range rds {
		if _, has := rds[i].Annotations[CSIVolumeAnnotation]; !has {
			continue
		}

		err = setOneCSIVolumeAnnotation(ctx, st, rds[i].Name, "")
		if err != nil {
			return err
		}
	}

	return nil
}
