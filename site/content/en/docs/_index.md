---
title: "Documentation"
linkTitle: "Docs"
weight: 20
menu:
  main:
    weight: 10
description: "Install, operate and understand blockstor."
---

Blockstor is a Kubernetes control plane for LVM and ZFS storage with DRBD replication. It speaks a LINSTOR-compatible REST API, so `linstor-csi`, `piraeus-operator`, `ha-controller` and `golinstor` keep working unchanged, and it ships its own `blockstor` command-line tool.

<div class="bs-cards">
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-bolt"></i></div>
<h3><a href="quickstart/">New here</a></h3>
<p>The <strong>Quickstart</strong> takes an empty cluster to a replicated PersistentVolumeClaim. Then read the <a href="overview/">Overview</a> for what works today and what does not.</p>
<a class="bs-card__more" href="quickstart/">Quickstart <i class="fa-solid fa-arrow-right ms-1"></i></a>
</div>
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-download"></i></div>
<h3><a href="installation/">Setting up a cluster</a></h3>
<p><strong>Installation</strong> covers requirements, the chart, nodes and pools. <a href="csi/">CSI</a> connects it to StorageClasses, <a href="networking/">Networking</a> lists the ports.</p>
<a class="bs-card__more" href="installation/">Installation <i class="fa-solid fa-arrow-right ms-1"></i></a>
</div>
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-screwdriver-wrench"></i></div>
<h3><a href="operations/">Running it day to day</a></h3>
<p><strong>Operations</strong> for node maintenance, resize, snapshots and upgrades, <a href="troubleshooting/">Troubleshooting</a> when something is off, and the <a href="cli/">CLI</a> reference.</p>
<a class="bs-card__more" href="operations/">Operations <i class="fa-solid fa-arrow-right ms-1"></i></a>
</div>
<div class="bs-card">
<div class="bs-card__icon"><i class="fa-solid fa-right-left"></i></div>
<h3><a href="migration/">Coming from LINSTOR</a></h3>
<p><strong>Migrating from LINSTOR</strong> takes over a cluster in place. <a href="compatibility/">Compatibility</a> lists what the API answers differently.</p>
<a class="bs-card__more" href="migration/">Migration <i class="fa-solid fa-arrow-right ms-1"></i></a>
</div>
</div>

To understand how it works, read [Architecture](architecture/), [Resources](resources/) and [Layer stack](layer-stack/). If you want to work on blockstor itself, see the [Developer guide](/developer-guide/).

## All pages
