# CI runner setup — `e2e-stand` self-hosted

The `e2e-stand` job in `.github/workflows/pull-request.yml` runs real
Talos+QEMU+DRBD end-to-end against a self-hosted runner labelled
`e2e-stand`. This document is the one-time setup for that runner.

## Why self-hosted, not the CNCF Oracle pool?

The pool's `oracle-vm-24cpu-96gb-x86-64` runner does not expose
`/dev/kvm` (the preflight `test -w /dev/kvm` step fails). GitHub-hosted
`ubuntu-latest` runs in an Azure VM without DRBD kernel module access.
Real-DRBD+Talos coverage therefore has to land on hardware that already
has both: the blockstor dev stand.

The stand is the canonical operator-workflow host (manual `make up` /
`tests/e2e/*.sh`); registering it as a runner reuses existing
infrastructure rather than provisioning new hardware. Concurrent PRs
serialize on the runner — acceptable for a small team.

## Register the runner

Install the GitHub Actions runner on the stand and tag it `e2e-stand`:

```bash
# On the stand:
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-linux-x64.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.328.0/actions-runner-linux-x64-2.328.0.tar.gz
tar xzf actions-runner-linux-x64.tar.gz

# From the cozystack/blockstor repo Settings → Actions → Runners → New self-hosted runner,
# copy the registration token. Then:
./config.sh \
  --url https://github.com/cozystack/blockstor \
  --token <REGISTRATION_TOKEN> \
  --name blockstor-e2e-stand \
  --labels e2e-stand,self-hosted,linux,x64 \
  --unattended

# Install as systemd service so it survives reboots:
sudo ./svc.sh install ubuntu
sudo ./svc.sh start
```

## Sanity check

After registration, the runner should appear at
<https://github.com/cozystack/blockstor/settings/actions/runners> with
status Idle and labels `e2e-stand, self-hosted, linux, x64`.

To exercise it, add the `e2e-stand` label to any PR; the `E2E (real
DRBD on Talos+QEMU)` job should pick up the runner within seconds.

## What the runner needs

- KVM: `/dev/kvm` must be writable by the runner user (`ubuntu` by
  default on the stand). Verified by the workflow's first step.
- QEMU+talosctl: installed via the workflow's `Install host prereqs`
  step on every job run, no preinstall needed.
- Disk: ~200 GB free per concurrent job in `/var/lib/blockstor` (sparse
  qcow2 grows to ~50 GB per stand). The stand has 5.9 TB NVMe at
  `/var/lib/blockstor`.
- Network: the runner spawns Talos VMs on per-cluster `10.<hash>.0.0/24`
  bridges via talosctl; no static config needed.

## Teardown

Each job ends with `make down NAME=ci-e2e` in `if: always()`, removing
the Talos VMs, libvirt bridge, and qcow2 disks for that run. Failed
jobs may leave residue under `/var/lib/blockstor/_state/ci-e2e/`; the
breakpoint step exposes the wedged cluster for inspection before
teardown fires.

## Runner versioning

Pin the runner version above (`v2.328.0`) — auto-update is disabled by
default. Bump when GitHub deprecates older runner versions (warnings in
the workflow log).
