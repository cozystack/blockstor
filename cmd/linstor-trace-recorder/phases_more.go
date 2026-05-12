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

package main

import (
	"context"

	"github.com/LINBIT/golinstor/client"
	"github.com/cockroachdb/errors"
)

// Fixture names — all prefixed `trace-` so replay can filter
// pre-existing oracle state out of list responses.
const (
	traceRG1    = "trace-rg-1"
	traceRG2    = "trace-rg-2"
	traceRD1    = "trace-rd-1"
	traceRD2    = "trace-rd-2"
	traceProp1  = "Aux/trace-recorder-stamp"
	traceProp2  = "Aux/trace-recorder-purpose"
	rd1VolSize  = 65536
	rdMaxPeers  = 7
	defaultVol  = 0
	rgPlaceOne  = 1
	traceMinor1 = 1100
	// traceStampYes is the canonical "set this prop" value the
	// recorder writes. Lifted to a constant so goconst stops
	// flagging it and a future change applies cluster-wide.
	traceStampYes = "yes"
)

// phaseControllerProps pins the controller's prop bag round-trip:
// list (empty-ish), set two props, list (with the two), delete one,
// list (with one), delete the last, list (clean). The /controller
// path is the only blockstor REST surface that's pure controller
// state with no satellite contact required, so this phase is the
// cheapest sanity check after the version smoke.
func phaseControllerProps(ctx context.Context, c *client.Client) error {
	_, err := c.Controller.GetProps(ctx)
	if err != nil {
		return errors.Wrap(err, "get props (initial)")
	}

	err = c.Controller.Modify(ctx, client.GenericPropsModify{
		OverrideProps: map[string]string{
			traceProp1: "yes",
			traceProp2: "contract-recording",
		},
	})
	if err != nil {
		return errors.Wrap(err, "modify props")
	}

	_, err = c.Controller.GetProps(ctx)
	if err != nil {
		return errors.Wrap(err, "get props (after set)")
	}

	err = c.Controller.DeleteProp(ctx, traceProp1)
	if err != nil {
		return errors.Wrapf(err, "delete prop %s", traceProp1)
	}

	_, err = c.Controller.GetProps(ctx)
	if err != nil {
		return errors.Wrap(err, "get props (after first delete)")
	}

	err = c.Controller.DeleteProp(ctx, traceProp2)
	if err != nil {
		return errors.Wrapf(err, "delete prop %s", traceProp2)
	}

	_, err = c.Controller.GetProps(ctx)
	if err != nil {
		return errors.Wrap(err, "get props (after teardown)")
	}

	return nil
}

// phaseErrorReports captures the list endpoint blockstor's REST
// shim returns `[]` for (the satellite path is retired; we don't
// surface error reports). The oracle returns whatever the Java
// LINSTOR has accumulated — Normalize strips timestamps + UUIDs
// from each entry so the count is what matters.
func phaseErrorReports(ctx context.Context, c *client.Client) error {
	_, err := c.Controller.GetErrorReports(ctx)
	if err != nil {
		return errors.Wrap(err, "list error reports")
	}

	return nil
}

// phaseResourceGroups captures the RG lifecycle. Pure controller
// state — no satellite contact needed, so this can run against any
// LINSTOR oracle without registering real workers.
func phaseResourceGroups(ctx context.Context, c *client.Client) error {
	_, err := c.ResourceGroups.GetAll(ctx)
	if err != nil {
		return errors.Wrap(err, "list RGs (initial)")
	}

	for _, name := range []string{traceRG1, traceRG2} {
		err := c.ResourceGroups.Create(ctx, client.ResourceGroup{
			Name: name,
			SelectFilter: client.AutoSelectFilter{
				PlaceCount: rgPlaceOne,
			},
		})
		if err != nil {
			return errors.Wrapf(err, "create RG %s", name)
		}
	}

	_, err = c.ResourceGroups.GetAll(ctx)
	if err != nil {
		return errors.Wrap(err, "list RGs (after create)")
	}

	_, err = c.ResourceGroups.Get(ctx, traceRG1)
	if err != nil {
		return errors.Wrapf(err, "get %s", traceRG1)
	}

	err = c.ResourceGroups.Modify(ctx, traceRG1, client.ResourceGroupModify{
		OverrideProps: map[string]string{
			traceProp1: "modified",
		},
	})
	if err != nil {
		return errors.Wrapf(err, "modify %s", traceRG1)
	}

	err = rgVolumeGroupCRUD(ctx, c, traceRG1)
	if err != nil {
		return err
	}

	return rgTeardown(ctx, c)
}

// rgVolumeGroupCRUD walks the /resource-groups/<n>/volume-groups
// surface against RG1 — list, create, list, delete. Split out so
// phaseResourceGroups stays under the funlen budget.
func rgVolumeGroupCRUD(ctx context.Context, c *client.Client, rg string) error {
	err := c.ResourceGroups.CreateVolumeGroup(ctx, rg, client.VolumeGroup{
		VolumeNumber: defaultVol,
	})
	if err != nil {
		return errors.Wrapf(err, "create vol-group %s", rg)
	}

	_, err = c.ResourceGroups.GetVolumeGroups(ctx, rg)
	if err != nil {
		return errors.Wrapf(err, "list vol-groups %s", rg)
	}

	err = c.ResourceGroups.DeleteVolumeGroup(ctx, rg, defaultVol)
	if err != nil {
		return errors.Wrapf(err, "delete vol-group %s", rg)
	}

	return nil
}

// rgTeardown deletes the two test RGs and re-lists to assert empty.
func rgTeardown(ctx context.Context, c *client.Client) error {
	for _, name := range []string{traceRG1, traceRG2} {
		err := c.ResourceGroups.Delete(ctx, name)
		if err != nil {
			return errors.Wrapf(err, "delete RG %s", name)
		}
	}

	_, err := c.ResourceGroups.GetAll(ctx)
	if err != nil {
		return errors.Wrap(err, "list RGs (after teardown)")
	}

	return nil
}

// phaseResourceDefinitions captures RD + VolumeDefinition CRUD.
// Pure controller state — no autoplace, no actual placement on
// satellites. The replay harness sees the same envelope blockstor
// emits when an RD is created via REST without children.
func phaseResourceDefinitions(ctx context.Context, c *client.Client) error {
	_, err := c.ResourceDefinitions.GetAll(ctx, client.RDGetAllRequest{})
	if err != nil {
		return errors.Wrap(err, "list RDs (initial)")
	}

	err = rdCreateAndModify(ctx, c)
	if err != nil {
		return err
	}

	err = rdVolumeDefinitionCRUD(ctx, c, traceRD1)
	if err != nil {
		return err
	}

	return rdTeardown(ctx, c)
}

// rdCreateAndModify covers list-empty / create / list / get /
// modify across the two test RDs. Split out so
// phaseResourceDefinitions stays within funlen.
func rdCreateAndModify(ctx context.Context, c *client.Client) error {
	for _, name := range []string{traceRD1, traceRD2} {
		err := c.ResourceDefinitions.Create(ctx, client.ResourceDefinitionCreate{
			ResourceDefinition: client.ResourceDefinition{
				Name: name,
			},
		})
		if err != nil {
			return errors.Wrapf(err, "create RD %s", name)
		}
	}

	_, err := c.ResourceDefinitions.GetAll(ctx, client.RDGetAllRequest{})
	if err != nil {
		return errors.Wrap(err, "list RDs (after create)")
	}

	_, err = c.ResourceDefinitions.Get(ctx, traceRD1)
	if err != nil {
		return errors.Wrapf(err, "get %s", traceRD1)
	}

	err = c.ResourceDefinitions.Modify(ctx, traceRD1, client.GenericPropsModify{
		OverrideProps: map[string]string{
			traceProp1: "rd-mod",
		},
	})
	if err != nil {
		return errors.Wrapf(err, "modify %s", traceRD1)
	}

	return nil
}

// rdVolumeDefinitionCRUD walks the
// /v1/resource-definitions/<n>/volume-definitions surface for the
// given RD: create, list, get, delete.
func rdVolumeDefinitionCRUD(ctx context.Context, c *client.Client, rd string) error {
	volNum := int32(defaultVol)

	err := c.ResourceDefinitions.CreateVolumeDefinition(ctx, rd, client.VolumeDefinitionCreate{
		VolumeDefinition: client.VolumeDefinition{
			VolumeNumber: &volNum,
			SizeKib:      rd1VolSize,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "create vol-def %s", rd)
	}

	_, err = c.ResourceDefinitions.GetVolumeDefinitions(ctx, rd)
	if err != nil {
		return errors.Wrapf(err, "list vol-defs %s", rd)
	}

	_, err = c.ResourceDefinitions.GetVolumeDefinition(ctx, rd, defaultVol)
	if err != nil {
		return errors.Wrapf(err, "get vol-def %s/%d", rd, defaultVol)
	}

	err = c.ResourceDefinitions.DeleteVolumeDefinition(ctx, rd, defaultVol)
	if err != nil {
		return errors.Wrapf(err, "delete vol-def %s/%d", rd, defaultVol)
	}

	return nil
}

// rdTeardown deletes the two test RDs and re-lists empty.
func rdTeardown(ctx context.Context, c *client.Client) error {
	for _, name := range []string{traceRD1, traceRD2} {
		err := c.ResourceDefinitions.Delete(ctx, name)
		if err != nil {
			return errors.Wrapf(err, "delete RD %s", name)
		}
	}

	_, err := c.ResourceDefinitions.GetAll(ctx, client.RDGetAllRequest{})
	if err != nil {
		return errors.Wrap(err, "list RDs (after teardown)")
	}

	return nil
}
