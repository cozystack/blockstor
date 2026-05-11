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
	"maps"
	"net/http"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerControllerProperties wires the cluster-wide controller
// property bag. linstor CLI's `controller list-properties` calls
// /v1/controller/properties; `controller set-property` calls POST.
// Backed by the singleton `ControllerConfig` CRD's
// `Spec.ExtraProps` (Phase 10.4 — replaces the legacy KVEntry
// "ControllerProps" instance).
func (s *Server) registerControllerProperties(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/controller/properties", s.requireStore(s.handleControllerPropsGet))
	mux.HandleFunc("POST /v1/controller/properties", s.requireStore(s.handleControllerPropsModify))
}

// handleControllerPropsGet returns ControllerConfig.Spec.ExtraProps
// as a flat map. A missing ControllerConfig CRD returns an empty
// map (LINSTOR CLI happily renders zero properties).
func (s *Server) handleControllerPropsGet(w http.ResponseWriter, r *http.Request) {
	if s.Client == nil {
		writeJSON(w, http.StatusOK, map[string]string{})

		return
	}

	props, err := readControllerProps(r.Context(), s.Client)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, props)
}

// handleControllerPropsModify applies an OverrideProps /
// DeleteProps batch onto ControllerConfig.Spec.ExtraProps. The
// CRD is auto-created on first write (canonical name `default`).
func (s *Server) handleControllerPropsModify(w http.ResponseWriter, r *http.Request) {
	if s.Client == nil {
		writeError(w, http.StatusServiceUnavailable, "controller properties require an apiserver client")

		return
	}

	var modify apiv1.GenericPropsModify

	err := json.NewDecoder(r.Body).Decode(&modify)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	err = applyControllerProps(r.Context(), s.Client, &modify)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	w.WriteHeader(http.StatusOK)
}

// readControllerProps fetches the singleton ControllerConfig and
// returns its ExtraProps bag. NotFound is folded into an empty
// map so the LINSTOR CLI never sees a 500 on a fresh cluster.
func readControllerProps(ctx context.Context, c client.Client) (map[string]string, error) {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return map[string]string{}, nil
		}

		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	if ctrlConfig.Spec.ExtraProps == nil {
		return map[string]string{}, nil
	}

	return ctrlConfig.Spec.ExtraProps, nil
}

// applyControllerProps merges an OverrideProps / DeleteProps
// batch into ControllerConfig.Spec.ExtraProps. Creates the CRD
// on first write so a fresh cluster doesn't need an explicit
// `kubectl apply` of the ControllerConfig manifest before
// `linstor controller set-property` works.
func applyControllerProps(ctx context.Context, c client.Client, modify *apiv1.GenericPropsModify) error {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if apierrors.IsNotFound(err) {
		ctrlConfig = blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec:       blockstoriov1alpha1.ControllerConfigSpec{ExtraProps: map[string]string{}},
		}

		mergeControllerProps(&ctrlConfig, modify)

		err = c.Create(ctx, &ctrlConfig)
		if err != nil {
			return errors.Wrap(err, "create ControllerConfig")
		}

		return nil
	}

	if err != nil {
		return errors.Wrap(err, "get ControllerConfig")
	}

	mergeControllerProps(&ctrlConfig, modify)

	err = c.Update(ctx, &ctrlConfig)
	if err != nil {
		return errors.Wrap(err, "update ControllerConfig")
	}

	return nil
}

// mergeControllerProps applies the OverrideProps / DeleteProps
// merge semantic LINSTOR uses: set keys land first, then delete
// keys remove their entries. Same precedence as the upstream
// "merge into existing" rule.
func mergeControllerProps(ctrlConfig *blockstoriov1alpha1.ControllerConfig, modify *apiv1.GenericPropsModify) {
	if ctrlConfig.Spec.ExtraProps == nil {
		ctrlConfig.Spec.ExtraProps = map[string]string{}
	}

	maps.Copy(ctrlConfig.Spec.ExtraProps, modify.OverrideProps)

	for _, k := range modify.DeleteProps {
		delete(ctrlConfig.Spec.ExtraProps, k)
	}
}
