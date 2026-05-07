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

// Package v1 contains hand-written Go types matching the LINSTOR REST v1
// contract for the small slice we currently expose. Once the OpenAPI
// generator is wired in, types here will be replaced by generated code with
// the upstream rest_v1_openapi.yaml as the source of truth.
package v1

// ControllerVersion mirrors `ControllerVersion` from the upstream OpenAPI
// spec. Field tags match upstream JSON exactly so golinstor unmarshals it.
type ControllerVersion struct {
	Version        string `json:"version"`
	GitHash        string `json:"git_hash"`
	BuildTime      string `json:"build_time"`
	RestAPIVersion string `json:"rest_api_version"`
}
