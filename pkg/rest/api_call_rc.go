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

// maskInfo is upstream LINSTOR `apiconsts.MASK_INFO` — the high-bit
// marker that flags an `ApiCallRc` as informational (success) rather
// than an error. The Python CLI treats any non-negative ret_code as
// success and decodes the `message` field for the operator-facing
// log line.
//
// Write-side endpoints (POST/PUT/DELETE) all wrap their success path
// with this so the wire shape matches upstream's `[]ApiCallRc` array
// envelope — golinstor discards the body but the Python parser
// dereferences `replies[0].ret_code` unconditionally.
const maskInfo = int64(0x0001_0000_0000)

// maskWarn is the blockstor-convention "warning band" bit. Matches
// the value the contract-normalizer (`tests/contract/normalize.go`)
// expects when classifying ApiCallRc entries into <info>/<warn>/<error>
// buckets — upstream LINSTOR's true MASK_WARN is 0x8000_…L (sign bit),
// but our wire-side uses a simplified band that stays in positive int64
// territory so callers that compare `ret_code >= 0` still treat a warning
// as non-fatal. Python CLI prints any entry with a message, regardless of
// mask, so the operator still sees the advisory line.
const maskWarn = int64(0x0002_0000_0000)

// warnVlmDfnResizeShrink is the (warn | vlmdfn | mod | code) composite
// used for VolumeDefinition-shrink advisories on `PUT /v1/resource-definitions/
// {rd}/volume-definitions/{vn}`. Upstream LINSTOR rejects shrinks at the
// API layer (CtrlVlmDfnModifyApiCallHandler.ensureShrinkingIsSupported),
// but blockstor passes them through so admins who explicitly want to
// reduce the spec size — knowing they'll have to shrink the FS and run
// `lvreduce`/`zfs set volsize` out-of-band — can do so. The advisory bit
// makes the Python CLI surface the warning line so the data-loss risk is
// visible in the audit log.
//
// Sub-code 1024 sits one past upstream's WARN_VLMDFN_RESIZE_SAME_SIZE
// (1023) — clusters tailing both servers can disambiguate the two.
const warnVlmDfnResizeShrink = maskWarn | int64(1024)

// warnRscNotFound is the (warn | rsc | del | code) composite used by
// `DELETE /v1/resource-definitions/{rd}/resources/{node}` when the
// (rd, node) pair doesn't exist. CSI spec § DeleteVolume mandates
// idempotence: the driver retries until it sees success, so a 404 on
// the second-delete-after-success path breaks the retry loop.
// Upstream LINSTOR returns 200 + `WARNING: Node: …, Resource: … not
// found.` exit 0 on the same input (cli-parity-audit row #42); the
// WARN bit lets python-linstor's print loop surface the "already
// absent" line as an advisory rather than a fatal error.
//
// Sub-code 2048 sits in the warn band reserved for "delete-of-missing"
// advisories — kept distinct from warnVlmDfnResizeShrink's 1024 so
// audit-log greppers can tell the two apart.
const warnRscNotFound = maskWarn | int64(2048)

// warnRDNotFound mirrors warnRscNotFound for the parent
// `DELETE /v1/resource-definitions/{rd}` route. The same CSI
// idempotence reasoning applies: a re-issued DeleteVolume on an RD
// that has already been dropped must succeed, not 404. Sub-code 2049
// sits one past warnRscNotFound so log filtering can distinguish the
// two no-op replays.
const warnRDNotFound = maskWarn | int64(2049)

// warnRGNotFound flags a delete-of-missing on
// `DELETE /v1/resource-groups/{rg}`. Bug 66: the Python linstor CLI
// pipes the response body through its XML parser when the HTTP layer
// returns a non-2xx, so a bare 404 on a no-op replay crashes the CLI
// before it can surface the "already absent" reality. Folding into a
// 200 + WARN envelope keeps `linstor rg d` exit-0 on the second call
// (matches the Bug 56 pattern in warnRscNotFound).
const warnRGNotFound = maskWarn | int64(2050)

// warnVDNotFound flags a delete-of-missing on
// `DELETE /v1/resource-definitions/{rd}/volume-definitions/{vn}`.
// Sub-code 2051. The two NotFound shapes (RD absent, VD absent
// inside an extant RD) both fold here — the message disambiguates
// them. Required for `linstor vd d` retries from csi-driver-side
// expand/shrink replay paths.
const warnVDNotFound = maskWarn | int64(2051)

// warnVGNotFound flags a delete-of-missing on
// `DELETE /v1/resource-groups/{rg}/volume-groups/{vlmNr}`.
// Sub-code 2052. Folds two NotFound shapes (RG itself absent, or
// VG-by-number absent inside an extant RG) into 200 + WARN.
const warnVGNotFound = maskWarn | int64(2052)

// warnNodeNotFound flags a delete-of-missing on
// `DELETE /v1/nodes/{node}`. Sub-code 2053. Cozystack
// node-evacuation playbooks re-issue `linstor n d` per node on
// every retry — a 404 on the second pass tripped the python CLI's
// XML decoder and surfaced as `xml.etree.ElementTree.ParseError`
// instead of the intended idempotent success.
const warnNodeNotFound = maskWarn | int64(2053)

// warnRemoteNotFound flags a delete-of-missing on
// `DELETE /v1/remotes?remote_name=…`. Sub-code 2054. The linstor
// remote registry is process-local (in-memory), so a controller
// restart loses every entry — operators (and `linstor remote d`
// scripts) frequently retry against a fresh controller and would
// otherwise see a 404 crash in the Python CLI.
const warnRemoteNotFound = maskWarn | int64(2054)

// warnStoragePoolNotFound flags a delete-of-missing on
// `DELETE /v1/nodes/{node}/storage-pools/{pool}`. Sub-code 2055.
// The handler pre-existed Bug 66 (added in Bug 52, 93d104163) and
// already folded NotFound into 200, but tagged it as `maskInfo`
// — making the "already absent" reply indistinguishable from a
// real drop. Bug 66 promotes it to the warn band so audit-log
// greppers can tell the two outcomes apart, in line with all
// other delete handlers.
const warnStoragePoolNotFound = maskWarn | int64(2055)

// warnSnapshotNotFound is emitted by `DELETE /v1/resource-definitions/
// {rd}/snapshots/{snap}` when the snapshot (and/or its parent RD)
// doesn't exist. CSI spec § DeleteSnapshot mandates idempotence — the
// driver retries until success — so a 404 on the second-delete path
// breaks the retry loop. Upstream LINSTOR's behaviour for the same
// input is `200 + WARNING: Snapshot definition <snap> of resource <rd>
// not found.` exit 0 (cli-parity-audit row #33); flipping the mask
// from maskInfo to maskWarn lets python-linstor surface the line as a
// "WARNING" rather than misclassifying a no-op replay as a real drop.
//
// Sub-code 2050 sits one past warnRDNotFound in the warn band; the
// numbering keeps the "delete-of-missing" family contiguous so log
// filters can match the whole class with `(ret_code & 0xFFFE) >= 2048`.
const warnSnapshotNotFound = maskWarn | int64(2056)

// warnNoSatelliteConnection is emitted as a SECOND envelope entry on
// `POST /v1/nodes` when the node was created in the controller's view
// of the cluster but no satellite has registered itself for the named
// node yet. Upstream LINSTOR returns the same 200 envelope (cli-parity-
// audit row #40): a SUCCESS line first, then a WARNING with the literal
// text "No active connection to satellite '<name>'" so operators learn
// they still need to start `linstor-satellite` (or its DaemonSet pod)
// before the node can host resources. Sub-code 2051 keeps the warn-band
// numbering monotonic alongside warnRscNotFound / warnRDNotFound /
// warnSnapshotNotFound.
const warnNoSatelliteConnection = maskWarn | int64(2057)

// warnRscConnPathNotFound flags a delete-of-missing on
// `DELETE /v1/resource-definitions/{rd}/resource-connections/{a}/{b}/
// paths/{name}`. Bug 198: the pre-fix handler replied 204 + empty body
// regardless of whether the named path existed; golinstor and python-
// linstor both crash decoding the empty body as `[]ApiCallRc`. Folding
// into a 200 + WARN envelope mirrors the Bug 56 / 66 pattern used by
// every other "delete-of-missing" handler in this file. Sub-code 2058
// keeps the warn-band numbering contiguous with warnNoSatelliteConnection
// (2057).
const warnRscConnPathNotFound = maskWarn | int64(2058)

// apiCallRcFailSnapshotFinalizerStuck is emitted by `DELETE /v1/
// resource-definitions/{rd}/snapshots/{snap}` (Bug 193) when the
// Snapshot CRD's satellite-side finalizer
// (`blockstor.io.blockstor.io/satellite-snapshot`) fails to drain
// inside `snapshotDeleteWaitBudget`. The pre-fix wire shape was an
// immediate 200 + SUCCESS line that lied to the caller — the
// snapshot CRD survived under `kubectl get snapshot` with the
// finalizer still attached. The fix surfaces a 504 + envelope
// citing the stuck-state and pointing the operator at the
// satellite (cause + correction strings render in `linstor s d`'s
// CLI output unchanged).
//
// Sub-code 998 sits next to apiCallRcFailInUse (997) — both
// describe "the delete acked locally but the upstream owner of
// the resource is still holding on", which the audit-log greppers
// already cluster together. The MASK_ERROR bit is OR'd in by the
// envelope wrapper at the call site.
const apiCallRcFailSnapshotFinalizerStuck int64 = 998

// apiCallRcFailExistsSnapshotDfn mirrors upstream LINSTOR's
// `ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN` (`514 | MASK_ERROR`). Emitted by
// `DELETE /v1/resource-definitions/{rd}` when the RD still has at
// least one Snapshot child (wave2 scenario 4.W11 / UG9 §"Deleting a
// resource definition" WARNING). The MASK_ERROR bit is OR'd in by the
// `apiCallRcError` envelope wrapper at the call site; the bare 514 sub-
// code here keeps the wire shape byte-identical to upstream's `linstor
// rd d <name>` reply on the same input.
//
// Choosing 514 (rather than a fresh sub-code in our 996+ range) lets
// audit-log greppers that already classify upstream's
// FAIL_EXISTS_SNAPSHOT_DFN traffic catch blockstor's equivalent without
// a separate rule.
const apiCallRcFailExistsSnapshotDfn int64 = 514

// apiCallRcFailExistsRscDfn mirrors upstream LINSTOR's
// `ApiConsts.FAIL_EXISTS_RSC_DFN` (`501 | MASK_ERROR`). Emitted by
// `DELETE /v1/resource-groups/{rg}` when the RG still has at least one
// child ResourceDefinition (wave2 scenario 9.W02 cross-listed with
// wave1 4.5 / UG9 §"Deleting a resource group"). Upstream's
// CtrlRscGrpApiCallHandler.deleteResourceGroup checks
// `rscGrpData.hasResourceDefinitions(apiCtx)` and raises this code
// with the literal text "Cannot delete <rg name> because it has
// existing resource definitions." — the operator must clear or
// reassign the RDs first; there's no `--force`. The MASK_ERROR bit is
// OR'd in by the `apiCallRcError` envelope wrapper at the call site;
// the bare 501 sub-code here keeps the wire shape byte-identical to
// upstream's `linstor rg d <name>` reply on the same input.
//
// Choosing 501 (rather than a fresh sub-code in our 996+ range) lets
// audit-log greppers that already classify upstream's
// FAIL_EXISTS_RSC_DFN traffic catch blockstor's equivalent without a
// separate rule.
const apiCallRcFailExistsRscDfn int64 = 501

// apiCallRcFailInUse mirrors upstream LINSTOR's `ApiConsts.FAIL_IN_USE`
// (997). Used when a delete is refused because an in-flight resource
// still references the target — e.g. `sp delete` on a pool with
// replicas.
const apiCallRcFailInUse int64 = 997

// apiCallRcFailInvldVlmSize mirrors upstream LINSTOR's
// `ApiConsts.FAIL_INVLD_VLM_SIZE` (206). Used when an operator-supplied
// size violates the contract (e.g. `vd set-size` shrink without
// `--force`).
const apiCallRcFailInvldVlmSize int64 = 206

// apiCallRcFailExistsVlmDfn mirrors upstream LINSTOR's
// `ApiConsts.FAIL_EXISTS_VLM_DFN` (502). Emitted by `POST
// /v1/resource-definitions/{rd}/volume-definitions` when the
// requested VolumeNumber already has a definition under the parent
// RD. Bug 140: the in-tree handler used to surface
// `writeStoreError` on ErrAlreadyExists which carried the
// MASK_ERROR bit but no cause/correction/sub-code; scripts and
// audit-log greppers that routed on the upstream catalogue missed
// the conflict.
//
// Choosing 502 (rather than a fresh sub-code in our 996+ range)
// keeps the wire shape byte-identical to upstream's `linstor vd c`
// reply on the same input — alongside FAIL_EXISTS_RSC_DFN (501)
// and FAIL_EXISTS_SNAPSHOT_DFN (514) which are already in this
// file.
const apiCallRcFailExistsVlmDfn int64 = 502

// apiCallRcFailInvldStorPoolName mirrors upstream LINSTOR's
// `ApiConsts.FAIL_INVLD_STOR_POOL_NAME` (552). Bug 135 uses it on
// `POST /v1/nodes/{node}/storage-pools` when the requested backing
// VG / zpool isn't in the satellite's `Aux/DiscoveredVGs` /
// `Aux/DiscoveredZPools` set. The MASK_ERROR bit is OR'd in by the
// envelope wrapper at the call site; the bare 552 sub-code here
// keeps the wire shape byte-identical to upstream's `linstor sp c`
// reply on the same input.
//
// Choosing 552 (rather than a fresh sub-code in our 996+ range) lets
// audit-log greppers that already classify upstream's
// FAIL_INVLD_STOR_POOL_NAME traffic catch blockstor's equivalent
// without a separate rule.
const apiCallRcFailInvldStorPoolName int64 = 552

// ObjRefs key constants — the wire-side identifiers upstream LINSTOR
// uses to tag ApiCallRc entries with the object(s) the message refers
// to. The strings are case-sensitive (the Python CLI matches on the
// exact case); constifying them here keeps the wire-shape uniform.
const (
	objRefNode        = "Node"
	objRefRscDfn      = "RscDfn"
	objRefRscGrp      = "RscGrp"
	objRefSnapshotDfn = "SnapshotDfn"
	objRefStorPool    = "StorPool"
	objRefVlmNr       = "VlmNr"
)
