// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/passphrase"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

// newKubeAppWithStore is newKubeApp plus the store, so a test can seed the
// objects a verb writes against and set controller-scope props.
func newKubeAppWithStore(t *testing.T) (*cli.App, store.Store, *bytes.Buffer) {
	t.Helper()

	scheme := runtime.NewScheme()

	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core scheme: %v", err)
	}

	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register crd scheme: %v", err)
	}

	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	backend := store.NewInMemory()

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return backend, nil
		},
		KubeFor: func(context.Context) (ctrlclient.Client, string, error) {
			return kube, testNamespace, nil
		},
	}

	return app, backend, &errBuf
}

// A LUKS layer with no cluster passphrase brings the replicas up plaintext.
// The stack reaches the satellite the same way whichever verb wrote it, so
// every writer of a layer list has to ask the same question — guarding only
// `rd create` leaves two doors open onto the same outcome.
func TestLUKSPrerequisiteHoldsOnEveryLayerListWriter(t *testing.T) {
	t.Parallel()

	stack := apiv1.LayerKindDRBD + "," + apiv1.LayerKindLUKS + "," + apiv1.LayerKindStorage

	// `rd create` gets a name the fixture did not seed: creating over the
	// seeded one is refused for existing, and the subtest would then pass
	// with the guard removed.
	for name, argv := range map[string][]string{
		"rd create": {"rd", "c", "pvc-fresh", "--layer-list", stack},
		"rd modify": {"rd", "m", "pvc-x", "--layer-list", stack},
		"rg create": {"rg", "c", "rg-x", "--layer-list", stack},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app, backend, errBuf := newKubeAppWithStore(t)

			err := backend.ResourceDefinitions().Create(t.Context(),
				&apiv1.ResourceDefinition{Name: "pvc-x"})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}

			if got := app.Run(t.Context(), argv); got == 0 {
				t.Errorf("%s wrote a LUKS stack with no cluster passphrase, exit 0 "+
					"(stderr: %s)", name, errBuf.String())
			}
		})
	}
}

// The passphrase lives only in the Secret `encryption create-passphrase`
// writes; nothing reads it from a property. A controller prop that carries
// one (put there with kubectl, since REST refuses to store it) must not
// satisfy the prerequisite, and the refusal must point at the Secret flow,
// not at the property.
func TestLUKSPrerequisiteIgnoresTheControllerProp(t *testing.T) {
	t.Parallel()

	app, backend, errBuf := newKubeAppWithStore(t)

	err := backend.ControllerProps().PatchProps(t.Context(), func(props map[string]string) error {
		props[passphrase.PropKeyCanonical] = "plaintext-in-a-prop"

		return nil
	})
	if err != nil {
		t.Fatalf("seed the controller prop: %v", err)
	}

	stack := apiv1.LayerKindDRBD + "," + apiv1.LayerKindLUKS + "," + apiv1.LayerKindStorage

	if got := app.Run(t.Context(), []string{"rd", "c", "pvc-x", "--layer-list", stack}); got == 0 {
		t.Fatalf("exit 0: a passphrase in a controller prop satisfied the LUKS prerequisite "+
			"(stderr: %s)", errBuf.String())
	}

	if !strings.Contains(errBuf.String(), "encryption create-passphrase") {
		t.Errorf("the refusal does not point at `encryption create-passphrase`: %s", errBuf.String())
	}

	if strings.Contains(errBuf.String(), passphrase.PropKeyCanonical) {
		t.Errorf("the refusal names the property, which is never read: %s", errBuf.String())
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-x"); err == nil {
		t.Error("the refused create wrote the definition")
	}
}
