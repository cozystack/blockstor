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

// Package version exposes the build identity of blockstor and the LINSTOR
// REST contract version it implements. The contract version is reported in
// /v1/controller/version so that golinstor clients can negotiate.
package version

// Build-time identity of this binary.
const (
	Project   = "blockstor"
	Version   = "0.0.0-dev"
	GitCommit = "unknown"
)

// LINSTOR REST contract identity reported in /v1/controller/version.
// We mimic a recent upstream Java LINSTOR so strict golinstor clients accept
// our responses.
const (
	LinstorVersion   = "1.33.2"
	LinstorGitHash   = "blockstor"
	LinstorBuildTime = "2026-01-01T00:00:00+00:00"
	RestAPIVersion   = "1.23.0"
)
