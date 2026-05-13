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

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// mergeEffectiveProps applies one scope's prop bag onto the
// accumulator with override semantics: every present key wins over
// any existing entry. The `scope` tag is stamped on every key the
// caller layers in so the final map records the highest-precedence
// origin per key — matching upstream LINSTOR's `(R)` inheritance
// marker semantics.
//
// Pure function so callers can compose any subset of the
// Controller → RG → RD → Resource chain without re-fetching parents
// — the scopeInputs helper in pkg/effectiveprops already centralises
// the parent fetch for the satellite path; this REST surface keeps a
// thin wrapper that's testable on its own.
func mergeEffectiveProps(out apiv1.EffectiveProperties, props map[string]string, scope string) {
	for k, v := range props {
		out[k] = apiv1.EffectivePropEntry{Value: v, Scope: scope}
	}
}

// buildEffectiveProps composes the merged map across all scopes
// provided (in upstream-LINSTOR precedence order: Controller → RG
// → RD → Resource). Each non-nil bag overrides the previous level,
// per-key. Returns a non-nil empty map when every level is empty so
// callers can drop it via `omitempty` on the JSON marshal path.
func buildEffectiveProps(scopes ...effectiveScope) apiv1.EffectiveProperties {
	out := apiv1.EffectiveProperties{}

	for _, sc := range scopes {
		mergeEffectiveProps(out, sc.Props, sc.Scope)
	}

	return out
}

// effectiveScope is one rung of the override hierarchy — the
// resolved prop bag at that scope plus the scope identifier the
// final entry should carry when this rung wins. Allows callers to
// pass any subset of (Controller, RG, RD, Resource) by composing
// `effectiveScope`s in precedence order.
type effectiveScope struct {
	Props map[string]string
	Scope string
}

// effectivePropsForRD resolves the merged Controller→RG→RD view for
// one ResourceDefinition. A missing parent RG (deleted out from
// under the RD, or the RD never had one) soft-fails to "empty at
// this level" — better to surface a partial map than 500 the GET.
func effectivePropsForRD(ctx context.Context, c client.Reader, st store.Store, rd *apiv1.ResourceDefinition) (apiv1.EffectiveProperties, error) {
	ctrl, err := controllerScopeProps(ctx, c)
	if err != nil {
		return nil, err
	}

	var rgProps map[string]string

	if rd.ResourceGroupName != "" {
		rg, getErr := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
		switch {
		case getErr == nil:
			rgProps = rg.Props
		case errors.Is(getErr, store.ErrNotFound):
			// Soft-fail; the RG was deleted out from under the RD.
		default:
			return nil, errors.Wrapf(getErr, "get ResourceGroup %q", rd.ResourceGroupName)
		}
	}

	return buildEffectiveProps(
		effectiveScope{Props: ctrl, Scope: apiv1.EffectivePropScopeController},
		effectiveScope{Props: rgProps, Scope: apiv1.EffectivePropScopeResourceGroup},
		effectiveScope{Props: rd.Props, Scope: apiv1.EffectivePropScopeResourceDefinition},
	), nil
}

// effectivePropsForResource resolves the full
// Controller→RG→RD→Resource view for one replica. Used by the
// `/v1/view/resources` aggregate to populate per-replica
// `effective_props`. Parent fetch errors soft-fail to "missing
// level → empty" so a partially-migrated cluster still returns a
// usable response.
func effectivePropsForResource(ctx context.Context, c client.Reader, st store.Store, rsc *apiv1.Resource) (apiv1.EffectiveProperties, error) {
	ctrl, err := controllerScopeProps(ctx, c)
	if err != nil {
		return nil, err
	}

	rd, getRDErr := st.ResourceDefinitions().Get(ctx, rsc.Name)

	var (
		rgProps map[string]string
		rdProps map[string]string
	)

	switch {
	case getRDErr == nil:
		rdProps = rd.Props

		if rd.ResourceGroupName != "" {
			rg, getRGErr := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
			switch {
			case getRGErr == nil:
				rgProps = rg.Props
			case errors.Is(getRGErr, store.ErrNotFound):
				// Soft-fail; surface a partial map.
			default:
				return nil, errors.Wrapf(getRGErr, "get ResourceGroup %q", rd.ResourceGroupName)
			}
		}
	case errors.Is(getRDErr, store.ErrNotFound):
		// Soft-fail; the parent RD vanished — emit Controller + Resource only.
	default:
		return nil, errors.Wrapf(getRDErr, "get ResourceDefinition %q", rsc.Name)
	}

	return buildEffectiveProps(
		effectiveScope{Props: ctrl, Scope: apiv1.EffectivePropScopeController},
		effectiveScope{Props: rgProps, Scope: apiv1.EffectivePropScopeResourceGroup},
		effectiveScope{Props: rdProps, Scope: apiv1.EffectivePropScopeResourceDefinition},
		effectiveScope{Props: rsc.Props, Scope: apiv1.EffectivePropScopeResource},
	), nil
}

// controllerScopeProps fetches ControllerConfig.Spec.ExtraProps via
// the controller-runtime client. A nil client (test paths that build
// a Server without one) or a missing CRD (fresh cluster) both
// degrade to an empty map — matches readControllerProps so callers
// never see a 500 just because the singleton hasn't been created.
func controllerScopeProps(ctx context.Context, c client.Reader) (map[string]string, error) {
	if c == nil {
		return nil, nil //nolint:nilnil // optional client
	}

	var cfg blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil //nolint:nilnil // optional singleton
		}

		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	return cfg.Spec.ExtraProps, nil
}
