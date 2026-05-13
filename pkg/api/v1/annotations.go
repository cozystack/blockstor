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

package v1

// AutoTiebreakerSuppressedUntilAnnotation is stamped on an RD when an
// operator (or an internal cleanup path) deletes a TIE_BREAKER replica.
// While the annotation timestamp is in the future, the RD-level
// reconciler skips its auto-witness branch. Without the suppression
// window, `linstor r d <tiebreaker-node> <rd>` returns success and
// then the reconciler re-stamps a fresh witness within milliseconds,
// silently undoing operator intent.
//
// Defined here (rather than in pkg/rest or internal/controller) so
// both the REST writer (`stampTiebreakerSuppression`) and the
// controller reader (`isTiebreakerSuppressed`) share a single source
// of truth without either package importing the other — pkg/api/v1
// is the neutral, dependency-free shared layer both already import.
const AutoTiebreakerSuppressedUntilAnnotation = "blockstor.io/auto-tiebreaker-suppressed-until"
