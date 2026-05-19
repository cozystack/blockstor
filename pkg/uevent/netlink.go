//go:build linux

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
	"golang.org/x/sys/unix"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// readBufferSize is the per-read buffer for netlink frames. A
// single kobject_uevent frame is bounded by `UEVENT_BUFFER_SIZE` in
// the kernel (lib/kobject_uevent.c) which has been 2048 historically
// and 4096 on modern kernels. 4 KiB matches the kernel max and is
// the size every other userspace consumer (systemd-udevd, busybox
// mdev) uses.
const readBufferSize = 4096

// netlinkGroupKernel is the multicast group the kernel posts kobject
// events onto. Bit 0 (group `1`) is the kernel-side multicast group
// every uevent lands on; bit 1 is reserved for udevd's own
// re-broadcast frames (which carry a `libudev` magic header we
// don't want to parse). Binding only to bit 0 means we see every
// kernel event with the simple `ACTION@DEVPATH\0KEY=VAL\0...`
// framing.
const netlinkGroupKernel = 1

// eventBufferSize bounds the channel between the reader goroutine
// and the consumer. A `udevadm trigger` on a busy host can fire
// hundreds of frames in a burst (one per partition, one per child
// kobject); 128 keeps the kernel buffer headroom comfortable while
// still letting the consumer's 250ms debounce coalesce a multi-disk
// rescan into a single tick.
const eventBufferSize = 128

// Listener wraps an `AF_NETLINK + NETLINK_KOBJECT_UEVENT` socket and
// parses incoming frames into Event values on a buffered channel.
// One Listener per process — the kernel multicasts each frame to
// every socket subscribed to the group, so multiple listeners would
// each receive every event. The satellite holds exactly one,
// owned by the PhysicalDeviceDiscoveryRunnable wiring.
//
// Lifecycle:
//
//   - New(ctx) opens + binds the socket and spawns the reader
//     goroutine.
//   - Events() returns the read-only channel.
//   - When ctx cancels the reader exits and closes the channel —
//     consumers MUST `select` on both the channel and ctx.Done()
//     to avoid a leaked goroutine when the parent shuts down.
//
// Failure modes:
//
//   - New() returns an error if the socket / bind syscalls fail
//     (typically `EPERM` when the satellite is missing
//     `CAP_NET_ADMIN`). The satellite caller logs and falls back
//     to pure-polling discovery — udev events are an
//     optimisation, not a correctness requirement.
//   - Read errors inside the goroutine are silently retried;
//     transient `EINTR` or short reads must not take the listener
//     out of service. A fatal `EBADF` (socket closed) cleanly
//     exits the loop.
type Listener struct {
	fd     int
	events chan Event
}

// New opens the netlink socket, binds it to the kernel multicast
// group, and starts the reader goroutine. The socket is closed
// when ctx cancels — consumers do NOT call Close themselves.
// Returns an error if the syscalls fail; the satellite caller logs
// the error and falls back to pure-polling discovery.
//
// The socket is opened with `SOCK_CLOEXEC` so a fork/exec doesn't
// leak it to a child — defence in depth, the satellite doesn't
// typically exec but `pkg/storage` shells out to lsblk / pvs /
// zpool / drbdmeta on every scan.
func New(ctx context.Context) (*Listener, error) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_KOBJECT_UEVENT)
	if err != nil {
		return nil, errors.Wrap(err, "socket(AF_NETLINK, NETLINK_KOBJECT_UEVENT)")
	}

	err = unix.Bind(fd, &unix.SockaddrNetlink{
		Family: unix.AF_NETLINK,
		Groups: netlinkGroupKernel,
	})
	if err != nil {
		_ = unix.Close(fd)

		return nil, errors.Wrap(err, "bind netlink uevent socket")
	}

	listener := &Listener{
		fd:     fd,
		events: make(chan Event, eventBufferSize),
	}

	// Bug 341: emit an INFO marker on every successful open so the
	// operator can grep the satellite log for "uevent listener
	// started" and confirm the fast-path is live. Without this,
	// "the listener silently no-op'd" looks identical in logs to
	// "the listener is up but no events are flowing", and we
	// burned hours diagnosing exactly that.
	log.FromContext(ctx).Info("uevent listener started",
		"fd", fd,
		"group", netlinkGroupKernel,
		"buffer", eventBufferSize)

	go listener.run(ctx)

	return listener, nil
}

// Events returns the channel the reader goroutine emits parsed
// block-subsystem events on. The channel closes when ctx (the one
// passed to New) cancels — consumers MUST handle the close to
// avoid a tight loop on a closed channel.
func (l *Listener) Events() <-chan Event {
	return l.events
}

// run is the reader goroutine. Loops on `read(fd)` until ctx
// cancels (which closes the FD and unblocks the read with EBADF),
// parses each frame, and emits matching block-subsystem Events
// onto the channel. Transient read errors are retried — a single
// EINTR or short read MUST NOT take the listener out of service.
func (l *Listener) run(ctx context.Context) {
	defer close(l.events)

	logger := log.FromContext(ctx).WithName("uevent-reader")
	logger.Info("uevent reader goroutine running", "fd", l.fd)

	// A goroutine that closes the FD on ctx cancellation. Read()
	// blocks indefinitely on the netlink socket; the only portable
	// way to interrupt it is to close the FD from a second
	// goroutine. The subsequent Read returns EBADF and the main
	// loop falls through to the ctx.Err() check.
	go func() {
		<-ctx.Done()
		_ = unix.Close(l.fd)
	}()

	buf := make([]byte, readBufferSize)

	for {
		n, err := unix.Read(l.fd, buf)
		if err != nil {
			// ctx cancelled → fd closed → EBADF: clean exit.
			// Any other error (EINTR, short read, transient
			// kernel hiccup): drop the frame and keep reading;
			// missing one event is fine because the periodic
			// discovery tick is the safety net.
			if ctx.Err() != nil {
				return
			}

			continue
		}

		if n <= 0 {
			continue
		}

		event, ok := ParseFrame(buf[:n])
		if !ok {
			continue
		}

		// Bug 341: V(1) per-frame trace lets the operator bump
		// log-verbosity once and see every uevent crossing the
		// socket. Cheap to leave in production at V=0 (the
		// logr.Discard short-circuits before any allocation).
		logger.V(1).Info("uevent frame",
			"action", event.Action,
			"subsystem", event.Subsystem,
			"kernel", event.Kernel,
			"devname", event.Devname)

		// Non-blocking send: if the consumer is slow, drop the
		// event rather than back-pressuring the kernel's
		// multicast queue. The periodic poll catches anything we
		// miss; the udev path is an optimisation, never the
		// source of truth.
		select {
		case l.events <- event:
		default:
		}
	}
}
