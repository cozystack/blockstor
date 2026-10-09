// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func linstorCSIClient(t *testing.T, base string) *lapi.Client {
	t.Helper()

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse base: %v", err)
	}

	c, err := lapi.NewClient(lapi.BaseURL(u), lapi.UserAgent("linstor-csi/v1.11.2"))
	if err != nil {
		t.Fatalf("golinstor NewClient: %v", err)
	}

	return c
}

// golinstor decodes every non-2xx answer other than a 404 as []ApiCallRc, so
// the CloneStarted object reached linstor-csi as a JSON decode error and the
// cause and correction of every clone refusal were lost before the PVC's
// events.
func TestRDCloneRefusalsReachLinstorCSIAsApiCallErrors(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-env")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "dst-taken"}); err != nil {
		t.Fatalf("seed the colliding definition: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	_, err := linstorCSIClient(t, base).ResourceDefinitions.Clone(ctx, "src-env",
		lapi.ResourceDefinitionCloneRequest{Name: "dst-taken"})

	var apiErr lapi.ApiCallError
	if !errors.As(err, &apiErr) || len(apiErr) == 0 {
		t.Fatalf("clone refusal reached golinstor as %T %v, want an ApiCallError", err, err)
	}

	if !strings.Contains(apiErr[0].Message, "dst-taken") || apiErr[0].Correction == "" {
		t.Errorf("refusal %+v lost its message or correction", apiErr[0])
	}
}

// A golinstor client other than linstor-csi decodes errors the same way, and
// may not name itself at all: golinstor sends no User-Agent of its own unless
// asked. It gets the array too.
func TestRDCloneRefusalsReachAnyGolinstorClientAsApiCallErrors(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-goenv")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "dst-gotaken"}); err != nil {
		t.Fatalf("seed the colliding definition: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse base: %v", err)
	}

	c, err := lapi.NewClient(lapi.BaseURL(u))
	if err != nil {
		t.Fatalf("golinstor NewClient: %v", err)
	}

	_, err = c.ResourceDefinitions.Clone(ctx, "src-goenv", lapi.ResourceDefinitionCloneRequest{Name: "dst-gotaken"})

	var apiErr lapi.ApiCallError
	if !errors.As(err, &apiErr) || len(apiErr) == 0 {
		t.Fatalf("clone refusal reached a plain golinstor client as %T %v, want an ApiCallError", err, err)
	}
}

// python-linstor decodes every clone answer into CloneStarted and names itself
// on every request, so its refusals keep the object.
func TestRDCloneRefusalsStayObjectsForPythonLinstor(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-pyenv")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "dst-pytaken"}); err != nil {
		t.Fatalf("seed the colliding definition: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/resource-definitions/src-pyenv/clone",
		strings.NewReader(`{"name":"dst-pytaken"}`))
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}

	req.Header.Set("User-Agent", pythonLinstorTestUserAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST clone: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the body: %v", err)
	}

	var envelope cloneStartedResponse
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Messages == nil || len(*envelope.Messages) == 0 {
		t.Errorf("refusal to python-linstor is not a CloneStarted object with messages: %s", raw)
	}
}
