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

// registerControllerProperties wires the cluster-wide controller
// property bag. linstor CLI's `controller list-properties` calls
// /v1/controller/properties; `controller set-property` calls POST.
func (s *Server) registerControllerProperties(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/controller/properties", s.requireStore(s.handleControllerPropsGet))
	mux.HandleFunc("POST /v1/controller/properties", s.requireStore(s.handleControllerPropsModify))
}

// handleControllerPropsGet returns the controller property map. We
// store it under a fixed KV instance ("ControllerProps") so it shares
// the same scaling story as the per-resource KV state.
func (s *Server) handleControllerPropsGet(w http.ResponseWriter, r *http.Request) {
	props, err := s.Store.KeyValueStore().GetInstance(r.Context(), controllerPropsInstance)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if props == nil {
		props = map[string]string{}
	}

	writeJSON(w, http.StatusOK, props)
}

// handleControllerPropsModify applies a GenericPropsModify in one
// transaction. Set keys take precedence over delete keys when both
// reference the same key (LINSTOR's behaviour).
func (s *Server) handleControllerPropsModify(w http.ResponseWriter, r *http.Request) {
	var modify apiv1.GenericPropsModify

	err := json.NewDecoder(r.Body).Decode(&modify)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	err = applyControllerProps(r.Context(), s.Store, &modify)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	w.WriteHeader(http.StatusOK)
}

// applyControllerProps delegates to the KV store's SetKeys, which
// already implements the upstream "merge into existing" semantic. We
// keep a wrapper for the named instance + error mapping.
func applyControllerProps(ctx context.Context, st store.Store, modify *apiv1.GenericPropsModify) error {
	err := st.KeyValueStore().SetKeys(ctx, controllerPropsInstance, *modify)
	if err != nil {
		return errors.Wrap(err, "write controller props")
	}

	return nil
}

// controllerPropsInstance is the KV-store instance name we reuse for
// the cluster-wide controller property bag. Picked to match the
// upstream "ControllerProps" namespace so satellites that look it up
// by name keep working.
const controllerPropsInstance = "ControllerProps"
