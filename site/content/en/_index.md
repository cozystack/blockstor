---
title: Blockstor
---

{{< blocks/cover image_anchor="top" height="auto" color="primary" >}}
<div class="row align-items-center text-start cover-hero">
<div class="col-lg-6">
<h1 class="display-1 mt-0 mt-md-5 pb-4">Blockstor</h1>
<p class="lead">A Kubernetes control plane for LVM and ZFS storage with DRBD replication.</p>
<a class="btn btn-lg btn-light me-3 mb-4" href="docs/installation/">Get started <i class="fa-solid fa-arrow-right ms-2"></i></a>
<a class="btn btn-lg btn-outline-light me-3 mb-4" href="https://github.com/cozystack/blockstor">GitHub <i class="fa-brands fa-github ms-2"></i></a>
</div>
<div class="col-lg-6">
{{< replicas >}}
</div>
</div>
{{< /blocks/cover >}}

{{% blocks/lead color="white" %}}
Blockstor speaks a LINSTOR-compatible REST API, so the clients you already use keep working unchanged: `linstor-csi`, `piraeus-operator`, `ha-controller`, `golinstor` and the `linstor` CLI.

The difference is underneath. Desired state lives in CRDs, and a set of `controller-runtime` reconcilers drive the cluster toward it. There is no external database to back up, no in-memory state to lose on restart, and no controller-to-node polling to fall behind.
{{% /blocks/lead %}}

{{% blocks/section color="light" type="row" %}}

{{% blocks/feature icon="fa-solid fa-dharmachakra" title="Kubernetes-native" %}}
The state of truth lives in Kubernetes CRDs. `Resource`, `ResourceDefinition`, `ResourceGroup`, `StoragePool`, `Snapshot`, `Node` and `PhysicalDevice` are meant to be read, and where it makes sense written, by other operators. Each satellite watches the CRDs for its own node and reports what it observes through Status, with no gRPC dispatch from a central controller.
{{% /blocks/feature %}}

{{% blocks/feature icon="fa-brands fa-golang" title="Ecosystem fit" %}}
Go is what the apiserver, the kubelet, etcd, most CSI drivers and `controller-runtime` are written in, so blockstor shares their tooling, libraries and contributors.
{{% /blocks/feature %}}

{{% blocks/feature icon="fa-solid fa-feather" title="Small footprint" %}}
The controller, the apiserver and the satellite are statically linked Go binaries, with no JVM, and DRBD is a Linux kernel module rather than a userspace daemon. The images are multi-arch, `linux/amd64` and `linux/arm64`, so blockstor fits on edge and small ARM nodes.
{{% /blocks/feature %}}

{{% /blocks/section %}}

{{< cncf >}}

{{% blocks/section color="light" %}}
## Acknowledgements {.text-center}

Blockstor was inspired by LINBIT's [LINSTOR](https://linbit.com/linstor/), and it operates [DRBD](https://linbit.com/drbd/) to provide block-level replication. It deliberately speaks LINSTOR's API so that the ecosystem the LINSTOR community has built keeps working unchanged. Heartfelt thanks to LINBIT and to the wider DRBD, LINSTOR and Piraeus community.

Blockstor is licensed under [Apache 2.0](https://github.com/cozystack/blockstor/blob/main/LICENSE). LINSTOR, LINBIT and DRBD are trademarks or registered trademarks of LINBIT. Blockstor is an independent project and is not affiliated with, endorsed by, or sponsored by LINBIT.
{{% /blocks/section %}}
