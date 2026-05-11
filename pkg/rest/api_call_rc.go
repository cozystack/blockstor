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
