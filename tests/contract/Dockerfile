# Tier 3 contract-test image — drbd-utils against a real loopback device.
#
# Used exclusively by tests/contract/*_test.go (build-tag `contract`). The
# Go harness builds this image once per process via sync.Once with the
# fixed tag `blockstor-drbd-contract:local`, then `docker run --rm` once
# per test case.
#
# Alpine 3.19 ships drbd-utils 9.x in `community`; the slim base + single
# apk install keeps the final image well under 30 MB so re-building it
# from scratch on a clean CI runner is cheap (~10 s).
FROM alpine:3.19

RUN apk add --no-cache drbd-utils

# The harness mounts the loopback file at /dev/loop0 and the resource
# file at /etc/drbd.d/<name>.res, then runs drbdmeta or drbdadm directly
# — no entrypoint shenanigans needed.
WORKDIR /work
