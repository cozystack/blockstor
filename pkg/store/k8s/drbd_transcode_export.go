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

package k8s

import (
	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// SplitProps is the exported face of the store's wire→CRD property
// split for external writers that build CRDs directly (the
// linstor-migrate converter). It applies exactly the same three-way
// routing the REST write path uses:
//
//   - recognised `DrbdOptions/...` keys → the typed DRBDOptions struct;
//   - unrecognised `DrbdOptions/...` keys → the extraProps overflow map;
//   - everything else (`Aux/*`, `StorDriver/*`, `StorPoolName`, ...) →
//     the residual props map, verbatim.
//
// Keeping direct CRD writers on this helper guarantees their objects
// read back through the store's mergeProps exactly like REST-created
// ones.
func SplitProps(props map[string]string) (*crdv1alpha1.DRBDOptions, map[string]string, map[string]string) {
	typed, extra := propsToTyped(props)
	residual := stripDRBDProps(props)

	return typed, residual, extra
}
