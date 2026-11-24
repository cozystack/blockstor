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

package cli

import "context"

// KubeTarget names the cluster a command talks to when the operator
// picks one on the command line, as kubectl's --kubeconfig and
// --context do. Both empty means the ambient configuration: KUBECONFIG,
// ~/.kube/config, or the in-cluster service account.
type KubeTarget struct {
	// Kubeconfig is the --kubeconfig path.
	Kubeconfig string
	// Context is the --context name.
	Context string
}

type kubeTargetKey struct{}

// KubeTargetFrom returns the cluster the command line picked, if any.
// StoreFor and KubeFor read it from the context they are given.
func KubeTargetFrom(ctx context.Context) KubeTarget {
	target, _ := ctx.Value(kubeTargetKey{}).(KubeTarget)

	return target
}

// withKubeTarget records the --kubeconfig and --context flags on ctx.
func withKubeTarget(ctx context.Context, flags *flagSet) context.Context {
	target := KubeTarget{Kubeconfig: flags.Values["kubeconfig"], Context: flags.Values["context"]}
	if target == (KubeTarget{}) {
		return ctx
	}

	return context.WithValue(ctx, kubeTargetKey{}, target)
}
