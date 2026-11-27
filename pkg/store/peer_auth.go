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

package store

import (
	"context"
	"maps"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// SeedPeerAuthentication gives rd, a definition about to be created, the
// DRBD peer authentication LINSTOR gives every definition it creates: a
// shared secret of its own and the sha1 digest. The secret is the one the
// request names (requested, empty for none), as LINSTOR takes drbd_secret,
// else a fresh random one.
//
// A value an operator set is kept: a secret or a digest already on the
// definition, as a clone's override or a group's props copied onto a
// spawn carry it, and one its group or the controller names, which the
// satellite resolves through the definition. Stamping over either would
// silently replace what the operator chose.
//
// Only a definition being created gets here. One that already exists is
// never touched, because switching authentication on under a running
// resource fails every handshake between a satellite that adjusted and
// one that has not yet.
func SeedPeerAuthentication(ctx context.Context, st Store, rd *apiv1.ResourceDefinition, requested string) error {
	inherited, err := inheritedDefinitionProps(ctx, st, rd)
	if err != nil {
		return err
	}

	if rd.Props == nil {
		rd.Props = map[string]string{}
	}

	switch {
	case requested != "":
		err = drbd.ValidateOptionValue(drbd.SectionNet, "shared-secret", requested)
		if err != nil {
			return err //nolint:wrapcheck // the message names the option for the caller
		}

		drbd.DropSharedSecret(rd.Props)
		rd.Props[drbd.SharedSecretProp] = requested
	case !hasProp(rd.Props, drbd.SharedSecretProp) && !hasProp(inherited, drbd.SharedSecretProp):
		secret, genErr := drbd.GenerateSharedSecret()
		if genErr != nil {
			return errors.Wrapf(genErr, "generate the DRBD shared secret of %q", rd.Name)
		}

		rd.Props[drbd.SharedSecretProp] = secret
	}

	if !hasProp(rd.Props, drbd.CramHMACAlgProp) && !hasProp(inherited, drbd.CramHMACAlgProp) {
		rd.Props[drbd.CramHMACAlgProp] = drbd.DefaultCramHMACAlg
	}

	return nil
}

// hasProp reports whether props names key exactly. The satellite renders a
// DrbdOptions key only in its canonical spelling, so a value under another
// spelling never reaches the .res and must not stop the stamp.
func hasProp(props map[string]string, key string) bool {
	_, ok := props[key]

	return ok
}

// inheritedDefinitionProps merges the props rd inherits from its group and
// the controller. A group that cannot be found or read contributes
// nothing: the create paths check the group themselves, and some of them
// proceed over an inconclusive read. Missing a group's value costs little
// here, since every replica renders the definition's own stamp, so its
// peers still agree.
func inheritedDefinitionProps(ctx context.Context, st Store, rd *apiv1.ResourceDefinition) (map[string]string, error) {
	ctrl, err := st.ControllerProps().Get(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "read controller properties for the peer authentication of %q", rd.Name)
	}

	out := maps.Clone(ctrl)
	if out == nil {
		out = map[string]string{}
	}

	if rd.ResourceGroupName == "" {
		return out, nil
	}

	rg, err := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
	if err == nil {
		maps.Copy(out, rg.Props)
	}

	return out, nil
}
