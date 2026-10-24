---
title: Blockstor
---

<section id="td-cover-block-0" class="row td-cover-block td-cover-block--height-auto js-td-cover td-overlay td-overlay--dark -bg-primary">
<div class="col-12">
<div class="container td-overlay__inner">
<div class="row align-items-center cover-hero">
<div class="col-lg-6 text-start">
<p class="eyebrow">Early-stage project, developed as part of <a href="https://cozystack.io">Cozystack</a></p>
<h1>Replicated block storage, run by Kubernetes</h1>
<p class="lead">Blockstor turns the local disks of your nodes into DRBD-replicated volumes on LVM or ZFS. The whole cluster state lives in custom resources, so you operate storage the way you operate everything else: with <code>kubectl</code>, GitOps and a CLI.</p>
<div class="hero-actions">
<a class="btn btn-lg btn-accent me-2 mb-3" href="docs/quickstart/">Quickstart <i class="fa-solid fa-arrow-right ms-2"></i></a>
<a class="btn btn-lg btn-outline-light me-2 mb-3" href="docs/">Documentation</a>
<a class="btn btn-lg btn-outline-light mb-3" href="https://github.com/cozystack/blockstor"><i class="fa-brands fa-github me-2"></i>GitHub</a>
</div>
<ul class="hero-facts">
<li>LINSTOR-compatible API</li>
<li>LVM, LVM-thin, ZFS</li>
<li>DRBD 9 replication</li>
<li>amd64 and arm64</li>
</ul>
</div>
<div class="col-lg-6">
{{< replicas >}}
</div>
</div>
</div>
</div>
</section>

<section class="row bs-section">
<div class="col-12">
<div class="container">
<p class="bs-intro">Blockstor speaks the LINSTOR REST API, so the clients you already run keep working unchanged: <code>linstor-csi</code>, <code>piraeus-operator</code>, <code>ha-controller</code>, <code>golinstor</code> and the <code>linstor</code> command. What changes is underneath. There is no database to back up, no in-memory state to lose on a restart, and no controller polling nodes.</p>
</div>
</div>
</section>

<section class="row bs-section bs-section--tint">
<div class="col-12">
<div class="container">
<h2 class="text-center">From a pool to a replicated volume</h2>
<p class="section-lead text-center mx-auto">Register the storage once, describe the placement policy once, and every volume spawned from the group gets the same replication.</p>
<div class="row g-4 align-items-center">
<div class="col-lg-7">
<div class="bs-terminal">
<div class="bs-terminal__bar"><span></span><span></span><span></span><em>blockstor</em></div>
<pre><code><span class="c"># a thin pool on each node becomes a storage pool named "data"</span>
<span class="p">$</span> blockstor storage-pool create lvmthin worker-1 data my_vg/my_thinpool

<span class="c"># the group holds the policy: two replicas, placed from "data"</span>
<span class="p">$</span> blockstor resource-group create replicated --place-count 2 --storage-pool data
<span class="p">$</span> blockstor volume-group create replicated

<span class="c"># one line per volume</span>
<span class="p">$</span> blockstor resource-group spawn replicated my-volume 10G
<span class="p">$</span> blockstor resource list</code></pre>
</div>
</div>
<div class="col-lg-5">
<ol class="bs-steps">
<li><strong>Register the storage</strong><span>A storage pool points at an LVM volume group, a thin pool or a ZFS pool on a node. Blockstor can also build one from a raw disk.</span></li>
<li><strong>Describe the policy</strong><span>A resource group says how many replicas a volume gets, from which pools, and how they spread across nodes and zones.</span></li>
<li><strong>Spawn volumes, or let CSI do it</strong><span>Each spawn creates a definition that the controller places and the satellites bring up as DRBD replicas. A StorageClass does the same for every PersistentVolumeClaim.</span></li>
</ol>
<p class="mb-0"><a href="docs/quickstart/">Run it yourself in the quickstart <i class="fa-solid fa-arrow-right ms-1"></i></a></p>
</div>
</div>
</div>
</div>
</section>

<section class="row bs-section">
<div class="col-12">
<div class="container">
<h2 class="text-center">Why blockstor</h2>
<p class="section-lead text-center mx-auto">The same storage stack as LINSTOR and Piraeus, with the control plane rebuilt on Kubernetes primitives.</p>
<div class="bs-cards">
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-dharmachakra"></i></div>
<h3>Kubernetes-native state</h3>
<p>Nodes, pools, definitions, replicas and snapshots are custom resources with printer columns. Each satellite watches the slice for its own node and reports what it sees through Status. Back it up, diff it and drive it from GitOps like any other object.</p>
</div>
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-plug"></i></div>
<h3>Drop-in for the LINSTOR ecosystem</h3>
<p>The REST API matches the LINSTOR wire shape, verified against <code>golinstor</code>. CSI, the HA controller and existing runbooks carry over. A running LINSTOR cluster can be taken over in place, without copying data.</p>
</div>
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-feather"></i></div>
<h3>Small footprint</h3>
<p>Three statically linked Go services and a kernel module. No JVM, no external database, no controller-to-node protocol. Multi-arch images for <code>linux/amd64</code> and <code>linux/arm64</code> fit edge clusters and small ARM nodes.</p>
</div>
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-layer-group"></i></div>
<h3>The features you expect</h3>
<p>Replicas on LVM, LVM-thin, ZFS and file backends. Automatic placement with zones and node constraints, tiebreakers and quorum, LUKS encryption, online resize, snapshots, clones and restores, and mutual TLS on the API.</p>
</div>
</div>
</div>
</div>
</section>

<section class="row bs-section bs-section--tint">
<div class="col-12">
<div class="container">
<h2 class="text-center">Find your way</h2>
<p class="section-lead text-center mx-auto">Pick the page that matches what you are doing.</p>
<div class="bs-cards">
<a class="bs-card" href="docs/quickstart/">
<div class="bs-card__icon"><i class="fa-solid fa-bolt"></i></div>
<h3>Quickstart</h3>
<p>From an empty cluster to a replicated PersistentVolumeClaim.</p>
<span class="bs-card__more">Start here <i class="fa-solid fa-arrow-right ms-1"></i></span>
</a>
<a class="bs-card" href="docs/installation/">
<div class="bs-card__icon"><i class="fa-solid fa-download"></i></div>
<h3>Installation</h3>
<p>Requirements, the Helm chart, registering nodes and storage pools.</p>
<span class="bs-card__more">Install <i class="fa-solid fa-arrow-right ms-1"></i></span>
</a>
<a class="bs-card" href="docs/architecture/">
<div class="bs-card__icon"><i class="fa-solid fa-diagram-project"></i></div>
<h3>Architecture</h3>
<p>How the apiserver, the controller and the satellites share the work.</p>
<span class="bs-card__more">Understand <i class="fa-solid fa-arrow-right ms-1"></i></span>
</a>
<a class="bs-card" href="docs/migration/">
<div class="bs-card__icon"><i class="fa-solid fa-right-left"></i></div>
<h3>Coming from LINSTOR</h3>
<p>Take over an existing cluster in place and what differs afterwards.</p>
<span class="bs-card__more">Migrate <i class="fa-solid fa-arrow-right ms-1"></i></span>
</a>
<a class="bs-card" href="docs/cli/">
<div class="bs-card__icon"><i class="fa-solid fa-terminal"></i></div>
<h3>Using the CLI</h3>
<p>Nodes, pools, volumes and snapshots from the <code>blockstor</code> command.</p>
<span class="bs-card__more">Operate <i class="fa-solid fa-arrow-right ms-1"></i></span>
</a>
<a class="bs-card" href="developer-guide/">
<div class="bs-card__icon"><i class="fa-solid fa-code"></i></div>
<h3>Developer guide</h3>
<p>Build it, run the test tiers, and find your way around the code.</p>
<span class="bs-card__more">Contribute <i class="fa-solid fa-arrow-right ms-1"></i></span>
</a>
</div>
</div>
</div>
</section>

{{< cncf >}}

<section class="row bs-section bs-section--tint">
<div class="col-12">
<div class="container">
<h2 class="text-center">Acknowledgements</h2>
<p class="bs-ack text-center">Blockstor was inspired by LINBIT's <a href="https://linbit.com/linstor/">LINSTOR</a>, and it operates <a href="https://linbit.com/drbd/">DRBD</a> to provide block-level replication. It deliberately speaks LINSTOR's API so that the ecosystem the LINSTOR community has built keeps working unchanged. Heartfelt thanks to LINBIT and to the wider DRBD, LINSTOR and Piraeus community.</p>
<p class="bs-ack text-center mt-3">Blockstor is licensed under <a href="https://github.com/cozystack/blockstor/blob/main/LICENSE">Apache 2.0</a>. LINSTOR, LINBIT and DRBD are trademarks or registered trademarks of LINBIT. Blockstor is an independent project and is not affiliated with, endorsed by, or sponsored by LINBIT.</p>
</div>
</div>
</section>
