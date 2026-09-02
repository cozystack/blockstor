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

// Command blockstor is the storage CLI.
//
// It speaks the Kubernetes API directly — the CRDs are the source of
// truth, so there is no control-plane hop between the operator and the
// data. Reading straight from the API also sidesteps the informer-cache
// lag the multi-replica REST apiserver has to retry around: this client
// sees its own writes.
//
// The grammar mirrors the client operators already use, long and short:
//
//	blockstor resource list          blockstor r l
//	blockstor storage-pool list      blockstor sp l
//	blockstor node list --nodes n1   blockstor n l -n n1
//
// Exit codes follow the same convention as the client it replaces: 0
// success, 2 a client-side rejection (unknown command or flag), 10 an
// API-level failure.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"

	"github.com/cozystack/blockstor/internal/cli"
)

func main() {
	app := &cli.App{
		Out:      os.Stdout,
		Err:      os.Stderr,
		In:       os.Stdin,
		StoreFor: openStore,
		KubeFor:  openKube,
	}

	os.Exit(app.Run(context.Background(), os.Args[1:]))
}

// openStore builds the CRD-backed store from the ambient kubeconfig
// (KUBECONFIG, ~/.kube/config, or the in-cluster service account).
//
// The client is deliberately NOT cached: a cache would reintroduce the
// read-your-writes lag the apiserver has to defend against, and a CLI
// process that lists once has nothing to gain from an informer.
func openStore(ctx context.Context) (store.Store, error) {
	c, _, err := openKube(ctx)
	if err != nil {
		return nil, err
	}

	return storek8s.New(c), nil
}

// openKube builds the raw client plus the namespace the controller
// runs in, for the few commands that reach objects outside the CRD
// surface (the cluster passphrase lives in a Secret).
func openKube(context.Context) (ctrlclient.Client, string, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig: %w", err)
	}

	scheme := runtime.NewScheme()

	err = crdv1alpha1.AddToScheme(scheme)
	if err != nil {
		return nil, "", fmt.Errorf("register scheme: %w", err)
	}

	err = corev1.AddToScheme(scheme)
	if err != nil {
		return nil, "", fmt.Errorf("register core scheme: %w", err)
	}

	c, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, "", fmt.Errorf("connect to the Kubernetes API: %w", err)
	}

	return c, namespace(), nil
}

// namespace resolves where the controller's own objects live — the
// cluster passphrase Secret, today.
//
// BLOCKSTOR_NAMESPACE wins wherever it is set, including inside a pod:
// an operator debugging from a shell in some unrelated pod, or running
// against a deployment that does not use the default namespace, needs
// to be able to say so and have it obeyed. The pod's own
// service-account namespace comes next, and the deployment default
// last.
func namespace() string {
	if ns := os.Getenv("BLOCKSTOR_NAMESPACE"); ns != "" {
		return ns
	}

	data, err := os.ReadFile(serviceAccountNamespaceFile)
	if err == nil && len(data) > 0 {
		return strings.TrimSpace(string(data))
	}

	return defaultNamespace
}

// serviceAccountNamespaceFile is where kubelet projects a pod's own
// namespace.
const serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// defaultNamespace matches the deployment manifests.
const defaultNamespace = "blockstor-system"
