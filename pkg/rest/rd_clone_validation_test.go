// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Every other door that creates a definition validates the name it is handed.
// The clone door checked only that it was non-empty, so it could create a
// definition `rd create` answers 400 for.
func TestRDCloneRefusesANameRDCreateWouldRefuse(t *testing.T) {
	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-name")

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, tc := range []struct{ name, target string }{
		{name: "dots", target: "dst.name.8"},
		{name: "leading-digit", target: "8dst"},
		{name: "space", target: "dst name"},
		{name: "past-the-ceiling-once-prefixed", target: strings.Repeat("d", 48)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code := cloneOnce(t, base, "src-name", tc.target, nil)
			if code != http.StatusBadRequest {
				t.Errorf("clone into %q = %d, want 400", tc.target, code)
			}
		})
	}
}

// The body decode is the last refusal on this endpoint that answered outside
// the envelope, and python-linstor reads `messages` off whatever comes back.
func TestRDCloneBodyDecodeRefusalsKeepTheEnvelope(t *testing.T) {
	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-dec")

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, tc := range []struct{ name, body string }{
		{name: "malformed", body: `{"name":`},
		{name: "unknown-field", body: `{"name":"dst-dec","no_such_field":1}`},
		{name: "wrong-type", body: `{"name":42}`},
		{name: "empty", body: ``},
		{name: "trailing-data", body: `{"name":"dst-dec"}garbage`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := httpPost(t, base+"/v1/resource-definitions/src-dec/clone", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read the body: %v", err)
			}

			if resp.StatusCode < http.StatusBadRequest {
				t.Fatalf("status = %d, want a refusal", resp.StatusCode)
			}

			var envelope cloneStartedResponse
			if err := json.Unmarshal(raw, &envelope); err != nil ||
				envelope.Messages == nil || len(*envelope.Messages) == 0 {
				t.Errorf("decode refusal is not a CloneStarted object with messages: %s", raw)
			}
		})
	}
}

// A volume-less clone takes no snapshot, so the derived name's ceiling does not
// apply to it, and a name `rd create` accepts has to stay possible.
func TestRDCloneAppliesTheSnapshotNameCeilingOnlyWhereASnapshotIsTaken(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-ceil")

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "shell-ceil"}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	long := "d" + strings.Repeat("x", 42)

	if code := cloneOnce(t, base, "shell-ceil", long, nil); code != http.StatusCreated {
		t.Errorf("volume-less clone into a %d-char name = %d, want 201", len(long), code)
	}

	if code := cloneOnce(t, base, "src-ceil", long+"y", nil); code != http.StatusBadRequest {
		t.Errorf("data-path clone whose snapshot name passes the ceiling = %d, want 400", code)
	}
}

// Only a missing passphrase is the caller's to fix. A Secret that could not be
// read was answered 400, which linstor-csi takes as permanent.
func TestRDCloneAnswersAnUnreadableLUKSPrerequisiteAsAServerError(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-luks", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    secretsForbiddenClient(t),
		Namespace: testRESTNamespace,
	})
	defer stop()

	code := cloneOnce(t, base, "src-luks", "dst-luks",
		map[string]any{"layer_list": []string{"DRBD", "LUKS", "STORAGE"}})
	if code != http.StatusInternalServerError {
		t.Errorf("clone with an unreadable passphrase Secret = %d, want 500", code)
	}
}

// A cluster without a passphrase is the caller's to fix, and stays a 400.
func TestRDCloneAnswersAMissingLUKSPassphraseAsAClientError(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-nopass", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	})
	defer stop()

	code := cloneOnce(t, base, "src-nopass", "dst-nopass",
		map[string]any{"layer_list": []string{"DRBD", "LUKS", "STORAGE"}})
	if code != http.StatusBadRequest {
		t.Errorf("clone on a cluster with no passphrase = %d, want 400", code)
	}
}

// A clone that names no stack takes the source's. Of a LUKS source, it needs
// the passphrase as much as one that names LUKS, and was written without it,
// for the satellite to fail on later.
func TestRDCloneOfALUKSSourceNeedsThePassphraseWithoutALayerList(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-inherit", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	})
	defer stop()

	if code := cloneOnce(t, base, "src-inherit", "dst-inherit", nil); code != http.StatusBadRequest {
		t.Errorf("clone of a LUKS source with no layer_list and no passphrase = %d, want 400", code)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "dst-inherit"); err == nil {
		t.Error("the refused clone wrote its target")
	}
}

// The replay of a finished LUKS clone writes no new LUKS volume, so a
// passphrase Secret that cannot be read at that moment does not fail it: a
// driver that lost the first answer would otherwise be refused a clone that
// is already done.
func TestRDCloneReplayOfAFinishedLUKSCloneDoesNotNeedThePassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-lreplay", []string{"DRBD", "LUKS", "STORAGE"})

	body := map[string]any{"layer_list": []string{"DRBD", "LUKS", "STORAGE"}}

	withPass := newFakeRESTClient(t)
	if err := withPass.Create(t.Context(), &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{"DrbdOptions/EncryptPassphrase": "blockstorpass"},
		},
	}); err != nil {
		t.Fatalf("seed the passphrase: %v", err)
	}

	plain, stopPlain := startServerCustom(t, &Server{
		Addr: pickFreeAddr(t), Store: st, Client: withPass, Namespace: testRESTNamespace,
	})

	first := postClone(t, plain, "src-lreplay", map[string]any{
		"name": "dst-lreplay", "use_zfs_clone": true, "layer_list": body["layer_list"],
	})
	rc := decodeCloneMessage(t, first)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d %q, want 201", first.StatusCode, rc.Message)
	}

	stopPlain()

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    secretsForbiddenClient(t),
		Namespace: testRESTNamespace,
	})
	defer stop()

	if code := cloneOnce(t, base, "src-lreplay", "dst-lreplay", body); code != http.StatusCreated {
		t.Errorf("replay of a finished LUKS clone with the passphrase unreadable = %d, want 201", code)
	}
}

// The refusal's twin: with the passphrase set, a clone of a LUKS source that
// names no stack goes through and takes the source's stack.
func TestRDCloneOfALUKSSourceWithThePassphraseInheritsItsStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-luksok", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerWithPassphrase(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-luksok", "dst-luksok", nil); code != http.StatusCreated {
		t.Fatalf("clone of a LUKS source with the passphrase set = %d, want 201", code)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "dst-luksok")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	if !apiv1.LayerInStack(rd.LayerStack, apiv1.LayerKindLUKS) {
		t.Errorf("clone stack %v, want the source's LUKS stack", rd.LayerStack)
	}
}
