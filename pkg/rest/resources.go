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
	"net/http"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerResources wires the /v1/view/resources aggregate. linstor-csi
// relies on this in its volume reconciliation loop.
func (s *Server) registerResources(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/resources", s.requireStore(s.handleResourcesView))
}

func (s *Server) handleResourcesView(w http.ResponseWriter, r *http.Request) {
	resList, err := s.Store.Resources().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Optional filters. Two wire dialects in the wild:
	//   - golinstor (csi side): comma-joined `?nodes=a,b`
	//   - python-linstor CLI:   repeat-key `?nodes=a&nodes=b` (via
	//     urlencode(doseq=True))
	// `multiValueQuery` accepts both, so `linstor r l -r foo -n bar`
	// and linstor-csi's existing requests land in the same filter.
	// Java LINSTOR honours both as case-insensitive set-membership; we
	// match that so linstor-csi's "is this resource on this node?"
	// poll returns a non-empty list when the answer is yes.
	nodeFilter := multiValueQuery(r, "nodes")
	rdFilter := multiValueQuery(r, "resources")

	out := make([]apiv1.ResourceWithVolumes, 0, len(resList))

	for i := range resList {
		if !matchAnyFold(nodeFilter, resList[i].NodeName) {
			continue
		}

		if !matchAnyFold(rdFilter, resList[i].Name) {
			continue
		}

		// Resource.Volumes is sourced from CRD Status by
		// crdToWireResource; ResourceWithVolumes is kept as a
		// distinct wrapper for backwards-compat with anything
		// still consuming the embedded shape — its Volumes field
		// shadows Resource.Volumes via field promotion ordering,
		// so the JSON output remains a single `volumes` key.
		out = append(out, apiv1.ResourceWithVolumes{
			Resource: resList[i],
			Volumes:  resList[i].Volumes,
		})
	}

	writeJSON(w, http.StatusOK, out)
}

// multiValueQuery returns the union of all values for a query
// parameter, supporting both wire dialects:
//
//   - `?key=a,b,c`        (comma-joined — golinstor)
//   - `?key=a&key=b&key=c` (repeat-key — python-linstor urlencode(doseq=True))
//
// Empty result = no filter on this key.
func multiValueQuery(r *http.Request, key string) []string {
	values := r.URL.Query()[key]
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))

	for _, v := range values {
		out = append(out, splitCSV(v)...)
	}

	return out
}

// splitCSV parses the comma-separated query value, trimming whitespace
// and dropping empty segments. Empty input means no filter.
func splitCSV(value string) []string {
	if value == "" {
		return nil
	}

	var out []string

	for s := range strings.SplitSeq(value, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}

	return out
}

// matchAnyFold reports whether candidate matches any of needles
// case-insensitively. Empty needles means "no filter — accept".
func matchAnyFold(needles []string, candidate string) bool {
	if len(needles) == 0 {
		return true
	}

	for _, n := range needles {
		if strings.EqualFold(n, candidate) {
			return true
		}
	}

	return false
}
