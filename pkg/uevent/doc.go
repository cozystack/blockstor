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

// Package uevent is a tiny zero-dependency reader for kernel
// `NETLINK_KOBJECT_UEVENT` frames. It exists so the satellite can
// react to block-device mutations (wipefs / pvcreate / zpool
// destroy / drive hotplug) within milliseconds instead of waiting
// for the next discovery tick — see
// `pkg/satellite/controllers/physicaldevice_discovery.go`.
//
// The package is deliberately self-contained: a third-party udev /
// uevent library could pull in GPL code (kernel headers ship under
// GPLv2 and several Go wrappers inherit that licensing posture).
// Apache-2.0 hygiene for the blockstor module requires we hand-roll
// the ~50 LOC parser ourselves.
//
// Wire format: kernel writes one frame per kobject event, framed as
//
//	ACTION@DEVPATH\0KEY=VALUE\0KEY=VALUE\0...\0
//
// where ACTION is `add`/`remove`/`change`/`move`/`online`/`offline`,
// DEVPATH is the sysfs path under `/sys`, and the key/value pairs
// repeat ACTION + DEVPATH plus SUBSYSTEM, SEQNUM, DEVNAME (for
// devices that have a /dev node) and any subsystem-specific keys.
// We surface only the subset the satellite cares about
// (block-subsystem events) — everything else is dropped at parse
// time so the consumer's channel never fills with noise.
package uevent
