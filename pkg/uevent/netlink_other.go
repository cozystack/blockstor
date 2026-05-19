//go:build !linux

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

package uevent

import (
	"context"

	"github.com/cockroachdb/errors"
)

// Listener is the non-Linux stub of the netlink listener type.
// Empty struct because the non-Linux build never instantiates one:
// `New` always returns an error and the satellite caller takes the
// pure-polling fallback path. The exported shape lets the
// satellite caller declare `*uevent.Listener` fields without a
// build-tag-aware indirection.
type Listener struct{}

// Events returns a nil channel on non-Linux. A nil channel blocks
// forever in `select`, which is exactly what a "no listener
// configured" caller needs: the select branch never fires and the
// caller falls through to the polling tick. Callers MUST already
// guard a nil Listener via the constructor's error return — this
// method is here for shape parity only.
func (*Listener) Events() <-chan Event {
	return nil
}

// New on non-Linux platforms always returns an error: kernel
// kobject uevent netlink is a Linux-only mechanism. The satellite
// production binary only builds for Linux but developers run unit
// tests + linters on darwin, so the non-Linux compilation target
// must still produce a working binary that compiles. The
// satellite's `cmd/satellite/main.go` treats any New error as
// "graceful fallback to pure-polling discovery", so the non-Linux
// build silently runs without the udev fast-path.
func New(_ context.Context) (*Listener, error) {
	return nil, errors.New("uevent: NETLINK_KOBJECT_UEVENT is not supported on this platform")
}
