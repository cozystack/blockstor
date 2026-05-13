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
