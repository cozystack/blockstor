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

package rest

import (
	"testing"

	lapiconsts "github.com/LINBIT/golinstor"
)

// TestFailureCodesAreLinstors pins every failure code blockstor answers
// to the value golinstor, and so linstor-csi and piraeus-operator, test
// it against with errors.Is. golinstor matches a code as a bit mask, so a
// value of blockstor's own makes a client read one failure as another:
// FAIL_EXISTS_VLM_DFN on 502 read as FAIL_EXISTS_RSC.
func TestFailureCodesAreLinstors(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		got  int64
		want uint64
	}{
		"FAIL_EXISTS_NODE":           {apiCallRcFailExistsNode, lapiconsts.FailExistsNode},
		"FAIL_EXISTS_RSC_DFN":        {apiCallRcFailExistsRscDfn, lapiconsts.FailExistsRscDfn},
		"FAIL_EXISTS_RSC":            {apiCallRcFailExistsRsc, lapiconsts.FailExistsRsc},
		"FAIL_EXISTS_VLM_DFN":        {apiCallRcFailExistsVlmDfn, lapiconsts.FailExistsVlmDfn},
		"FAIL_EXISTS_STOR_POOL_DFN":  {apiCallRcFailExistsStorPoolDfn, lapiconsts.FailExistsStorPoolDfn},
		"FAIL_EXISTS_STOR_POOL":      {apiCallRcFailExistsStorPool, lapiconsts.FailExistsStorPool},
		"FAIL_EXISTS_SNAPSHOT_DFN":   {apiCallRcFailExistsSnapshotDfn, lapiconsts.FailExistsSnapshotDfn},
		"FAIL_EXISTS_RSC_GRP":        {apiCallRcFailExistsRscGrp, lapiconsts.FailExistsRscGrp},
		"FAIL_EXISTS_LIVE_MIGRATE":   {apiCallRcFailExistsLiveMigrate, lapiconsts.FailExistsLiveMigrate},
		"FAIL_NOT_FOUND_NODE":        {apiCallRcFailNotFoundNode, lapiconsts.FailNotFoundNode},
		"FAIL_NOT_FOUND_RSC_DFN":     {apiCallRcFailNotFoundRscDfn, lapiconsts.FailNotFoundRscDfn},
		"FAIL_NOT_FOUND_STOR_POOL":   {apiCallRcFailNotFoundStorPool, lapiconsts.FailNotFoundStorPool},
		"FAIL_INVLD_NODE_TYPE":       {apiCallRcFailInvldNodeType, lapiconsts.FailInvldNodeType},
		"FAIL_INVLD_VLM_NR":          {apiCallRcFailInvldVlmNr, lapiconsts.FailInvldVlmNr},
		"FAIL_INVLD_VLM_SIZE":        {apiCallRcFailInvldVlmSize, lapiconsts.FailInvldVlmSize},
		"FAIL_INVLD_STOR_POOL_NAME":  {apiCallRcFailInvldStorPoolName, lapiconsts.FailInvldStorPoolName},
		"FAIL_INVLD_PROP":            {apiCallRcFailInvldProp, lapiconsts.FailInvldProp},
		"FAIL_INVLD_LAYER_STACK":     {apiCallRcFailInvldLayerStack, lapiconsts.FailInvldLayerStack},
		"FAIL_SNAPSHOTS_NOT_SUPPORT": {apiCallRcFailSnapshotsNotSupported, lapiconsts.FailSnapshotsNotSupported},
		"FAIL_NOT_ENOUGH_NODES":      {apiCallRcFailNotEnoughNodes, lapiconsts.FailNotEnoughNodes},
		"FAIL_IN_USE":                {apiCallRcFailInUse, lapiconsts.FailInUse},
	} {
		if got := uint64(apiCallRcError | tc.got); got != tc.want {
			t.Errorf("%s: blockstor answers %#x, LINSTOR %#x", name, got, tc.want)
		}
	}
}
