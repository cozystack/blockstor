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

package satellite

// Day0GiForTest re-exports the package-private day0 GI derivation for
// the external test package (`satellite_test`). Kept test-only so the
// derivation stays an implementation detail — the controller / REST
// layers have no business calling it.
func Day0GiForTest(resourceName string, volumeNumber int32) string {
	return day0GiFor(resourceName, volumeNumber)
}
