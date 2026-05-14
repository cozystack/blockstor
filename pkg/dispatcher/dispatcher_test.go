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

package dispatcher_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// TestExternalMetadataRouting: scenario 6.18 (StorPoolNameDrbdMeta).
// When the target Resource (or the parent RD) carries the LINSTOR-
// compatible `StorPoolNameDrbdMeta` prop, dispatcher.BuildDesired
// must stamp the value into DesiredVolume.MetaPool so the satellite
// can carve the external metadata device and the .res renderer can
// emit the matching `meta-disk <path>;` line.
//
// We exercise three scope variants:
//
//   - Resource-level prop wins over RD-level (most-specific
//     overrides — matches UG9's precedence semantics);
//   - RD-level prop is honoured when Resource doesn't set one;
//   - Diskless replicas suppress MetaPool (no backing disk →
//     nothing to attach metadata to).
//
// The data-pool routing (StorPoolName) is asserted alongside so
// regressing the existing pool resolution would also fail this test.
func TestExternalMetadataRouting(t *testing.T) {
	rdName := "pvc-1"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				// RD-scope StorPoolNameDrbdMeta — should be picked
				// up when the Resource doesn't override it.
				"StorPoolNameDrbdMeta": "ssd-meta",
			},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	cases := []struct {
		name      string
		target    *blockstoriov1alpha1.Resource
		wantPool  string
		wantMeta  string
		wantPeers int
		comment   string
	}{
		{
			name: "rd-scope-prop",
			target: &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            "data-hdd",
				},
			},
			wantPool: "data-hdd",
			wantMeta: "ssd-meta",
			comment:  "RD-level prop is honoured when Resource doesn't override",
		},
		{
			name: "resource-overrides-rd",
			target: &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            "data-hdd",
					Props: map[string]string{
						// Most-specific scope wins.
						"StorPoolNameDrbdMeta": "nvme-meta",
					},
				},
			},
			wantPool: "data-hdd",
			wantMeta: "nvme-meta",
			comment:  "Resource-scope prop overrides RD-scope",
		},
		{
			name: "diskless-suppresses-meta",
			target: &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					Flags:                  []string{"DISKLESS"},
					Props: map[string]string{
						// Even if set, a diskless replica has no
						// disk to attach metadata to. The
						// dispatcher must not propagate.
						"StorPoolNameDrbdMeta": "nvme-meta",
					},
				},
			},
			wantPool: "",
			wantMeta: "",
			comment:  "Diskless replica suppresses MetaPool",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dispatcher.BuildDesired(tc.target, nil, nil, nil, rd, nil)
			if got == nil {
				t.Fatalf("BuildDesired returned nil")
			}

			if len(got.Volumes) != 1 {
				t.Fatalf("want 1 volume, got %d", len(got.Volumes))
			}

			vol := got.Volumes[0]
			if vol.StoragePool != tc.wantPool {
				t.Errorf("%s: StoragePool=%q want %q", tc.comment, vol.StoragePool, tc.wantPool)
			}

			if vol.MetaPool != tc.wantMeta {
				t.Errorf("%s: MetaPool=%q want %q", tc.comment, vol.MetaPool, tc.wantMeta)
			}
		})
	}
}

// TestMetaPoolDefaultsToInternal: when no scope carries
// StorPoolNameDrbdMeta, MetaPool must stay empty so the renderer
// falls back to the default `meta-disk internal;` line. Guards
// against the dispatcher accidentally stamping a non-zero
// default into DesiredVolume.MetaPool.
func TestMetaPoolDefaultsToInternal(t *testing.T) {
	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			StoragePool:            "data-hdd",
		},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if len(got.Volumes) != 1 {
		t.Fatalf("want 1 volume, got %d", len(got.Volumes))
	}

	if vol := got.Volumes[0]; vol.MetaPool != "" {
		t.Errorf("MetaPool=%q want \"\" (internal default)", vol.MetaPool)
	}
}

// TestMultiVolumeRDConsistencyGroup pins scenario 4.W25 (wave2-04-
// lifecycle.md): an RD with multiple VolumeDefinitions forms ONE DRBD
// resource that owns ALL of them. The dispatcher must surface every
// VD as a separate DesiredVolume on the SAME DesiredResource (not as
// separate resources!), preserving each VD's VolumeNumber + SizeKib.
// Downstream this turns into one `.res` file with multiple
// `volume <N> { ... }` sub-blocks under each `on <node> {}` block,
// i.e. one DRBD consistency group whose snapshots are atomic across
// all volumes and whose primary state is shared.
//
// Negative — multiple SEPARATE RDs would each emit their own
// DesiredResource (not exercised here; pinning per-DesiredResource
// shape is enough to fail the dispatcher-side regression that
// flattens multi-VD RDs into single-volume payloads).
func TestMultiVolumeRDConsistencyGroup(t *testing.T) {
	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{"StorPoolName": "pool_default"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				// Two volumes on the same RD: DRBD will allocate
				// minors base+0 and base+1, the .res renderer
				// emits two `volume {}` sub-blocks per `on {}`.
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
				{VolumeNumber: 1, SizeKib: 2 * 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-multi",
			NodeName:               "n1",
		},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	// One resource, two volumes — the consistency-group shape.
	// Multiple DesiredResources here would mean the dispatcher
	// fanned one RD out into separate DRBD resources, breaking
	// the cross-volume write-order guarantee.
	if len(got.Volumes) != 2 {
		t.Fatalf("want 2 volumes on one DesiredResource, got %d", len(got.Volumes))
	}

	for i, want := range []struct {
		vn   int32
		size int64
	}{
		{vn: 0, size: 1024 * 1024},
		{vn: 1, size: 2 * 1024 * 1024},
	} {
		v := got.Volumes[i]
		if v.VolumeNumber != want.vn {
			t.Errorf("Volumes[%d].VolumeNumber=%d want %d", i, v.VolumeNumber, want.vn)
		}

		if v.SizeKib != want.size {
			t.Errorf("Volumes[%d].SizeKib=%d want %d", i, v.SizeKib, want.size)
		}

		// Both VDs without per-VD override resolve to the RD-
		// default pool — same backing pool, one consistency group.
		if v.StoragePool != "pool_default" {
			t.Errorf("Volumes[%d].StoragePool=%q want %q", i, v.StoragePool, "pool_default")
		}
	}
}

// TestPerVolumeStorPoolOverride pins scenario 4.W26 (wave2-04-
// lifecycle.md): per-VolumeDefinition `Props["StorPoolName"]`
// overrides the RD-level default. Different VDs on the same RD can
// land on different storage pools (e.g. fast NVMe for vol 0, slow
// HDD for vol 1) on the same satellite — the dispatcher must surface
// each VD's resolved pool independently on its DesiredVolume.
//
// The same-ProviderKind gate (issue 76) is enforced ACROSS REPLICAS
// of the same VD by the placer (it never sees per-VD pool
// selection), so per-VD overrides stay orthogonal to that gate at
// this layer.
//
// Cases:
//
//  1. vol 0 + vol 1 both have explicit, distinct per-VD overrides —
//     each independently overrides the RD default.
//  2. vol 0 overrides; vol 1 has no per-VD prop → falls through to
//     the RD default. Mixed-mode RDs must work.
//  3. Diskless replica: per-VD pool prop is ignored (no disk).
func TestPerVolumeStorPoolOverride(t *testing.T) {
	rdName := "pvc-multi-pool"

	mkRD := func() *blockstoriov1alpha1.ResourceDefinition {
		return &blockstoriov1alpha1.ResourceDefinition{
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				Props: map[string]string{"StorPoolName": "pool_default"},
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{
						VolumeNumber: 0,
						SizeKib:      1024 * 1024,
						Props:        map[string]string{"StorPoolName": "pool_nvme"},
					},
					{
						VolumeNumber: 1,
						SizeKib:      2 * 1024 * 1024,
						Props:        map[string]string{"StorPoolName": "pool_hdd"},
					},
				},
			},
		}
	}

	t.Run("both-vds-override", func(t *testing.T) {
		rd := mkRD()
		target := &blockstoriov1alpha1.Resource{
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               "n1",
			},
		}

		got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
		if got == nil || len(got.Volumes) != 2 {
			t.Fatalf("want 2 volumes; got %+v", got)
		}

		if got.Volumes[0].StoragePool != "pool_nvme" {
			t.Errorf("vol 0 pool=%q want pool_nvme (per-VD override)", got.Volumes[0].StoragePool)
		}

		if got.Volumes[1].StoragePool != "pool_hdd" {
			t.Errorf("vol 1 pool=%q want pool_hdd (per-VD override)", got.Volumes[1].StoragePool)
		}
	})

	t.Run("mixed-only-vol0-overrides", func(t *testing.T) {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				Props: map[string]string{"StorPoolName": "pool_default"},
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{
						VolumeNumber: 0,
						SizeKib:      1024 * 1024,
						Props:        map[string]string{"StorPoolName": "pool_nvme"},
					},
					// No per-VD prop → falls through to RD default.
					{VolumeNumber: 1, SizeKib: 1024 * 1024},
				},
			},
		}
		target := &blockstoriov1alpha1.Resource{
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               "n1",
			},
		}

		got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
		if got == nil || len(got.Volumes) != 2 {
			t.Fatalf("want 2 volumes; got %+v", got)
		}

		if got.Volumes[0].StoragePool != "pool_nvme" {
			t.Errorf("vol 0 pool=%q want pool_nvme (per-VD override)", got.Volumes[0].StoragePool)
		}

		if got.Volumes[1].StoragePool != "pool_default" {
			t.Errorf("vol 1 pool=%q want pool_default (RD-fallback when no per-VD prop)",
				got.Volumes[1].StoragePool)
		}
	})

	t.Run("diskless-ignores-per-vd-pool", func(t *testing.T) {
		rd := mkRD()
		target := &blockstoriov1alpha1.Resource{
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               "n1",
				Flags:                  []string{"DISKLESS"},
			},
		}

		got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
		if got == nil || len(got.Volumes) != 2 {
			t.Fatalf("want 2 volumes; got %+v", got)
		}

		for i, v := range got.Volumes {
			if v.StoragePool != "" {
				t.Errorf("vol %d pool=%q want \"\" (DISKLESS suppresses per-VD pool routing)",
					i, v.StoragePool)
			}
		}
	})
}

// TestPeerAddressPrefersKubeInternalNIC: Bug 48 — when a peer's Node
// CRD advertises both a pod-CIDR NetInterface in slot [0] (the
// piraeus-operator-with-externalController-url pathology, where
// satellite-pod IP rather than host IP gets written) and a named
// "k8s-internal" NetInterface carrying the routable corev1.Node
// InternalIP, the dispatcher must pick the InternalIP for the peer
// `address=` line. Otherwise DRBD-9 hands a pod-CIDR address to the
// peer satellite, which can't route to it cross-node, and the RD
// flaps in `Connecting`.
//
// Negative case: a single pod-CIDR NetInterface with no "k8s-
// internal" alongside must NOT abort — the dispatcher carries the
// pod-CIDR address through, on the theory that single-NIC
// satellite-pod-IP clusters (host networking / hostNetwork:true)
// are still legitimate.
func TestPeerAddressPrefersKubeInternalNIC(t *testing.T) {
	rdName := "pvc-1"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	cases := []struct {
		name        string
		peerIfaces  []blockstoriov1alpha1.NodeNetInterface
		wantPeerAdr string
		comment     string
	}{
		{
			name: "k8s-internal-wins-over-positional-pod-cidr",
			peerIfaces: []blockstoriov1alpha1.NodeNetInterface{
				// piraeus rewrote [0] with the satellite pod IP.
				{Name: "default", Address: "10.244.0.5"},
				// register / label-sync published the host
				// InternalIP under the well-known name.
				{Name: "k8s-internal", Address: "10.51.0.3"},
			},
			wantPeerAdr: "10.51.0.3",
			comment:     "k8s-internal must override pod-CIDR positional[0]",
		},
		{
			name: "single-pod-cidr-interface-falls-through",
			peerIfaces: []blockstoriov1alpha1.NodeNetInterface{
				{Name: "default", Address: "10.244.0.5"},
			},
			wantPeerAdr: "10.244.0.5",
			comment:     "single NIC carries through; no abort, no rewrite",
		},
		{
			name: "k8s-internal-only",
			peerIfaces: []blockstoriov1alpha1.NodeNetInterface{
				{Name: "k8s-internal", Address: "10.51.0.3"},
			},
			wantPeerAdr: "10.51.0.3",
			comment:     "k8s-internal alone is used directly",
		},
		{
			name: "default-named-preferred-over-unnamed",
			peerIfaces: []blockstoriov1alpha1.NodeNetInterface{
				{Name: "drbd-net", Address: "10.244.0.5"},
				{Name: "default", Address: "10.51.0.3"},
			},
			wantPeerAdr: "10.51.0.3",
			comment:     "explicit default wins over arbitrary other names",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			peerID := int32(1)
			targetID := int32(0)

			target := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            "data-hdd",
				},
				Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &targetID},
			}

			peer := blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-n2"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n2",
					StoragePool:            "data-hdd",
				},
				Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &peerID},
			}

			nodes := []blockstoriov1alpha1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "n1"},
					Spec: blockstoriov1alpha1.NodeSpec{
						Type: "Satellite",
						NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{
							{Name: "default", Address: "10.51.0.2"},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "n2"},
					Spec: blockstoriov1alpha1.NodeSpec{
						Type:          "Satellite",
						NetInterfaces: tc.peerIfaces,
					},
				},
			}

			got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{peer}, nodes, nil, rd, nil)
			if got == nil {
				t.Fatalf("BuildDesired returned nil")
			}

			gotAddr := got.DrbdOptions["peer.n2.address"]
			if gotAddr != tc.wantPeerAdr {
				t.Errorf("%s: peer.n2.address=%q want %q (drbdOpts=%v)",
					tc.comment, gotAddr, tc.wantPeerAdr, got.DrbdOptions)
			}
		})
	}
}

// TestHostNetworkSatelliteUsesNodeIPNotPodIP: scenario 3.W06.
//
// LinstorSatelliteConfiguration with `hostNetwork: true` restarts
// satellite pods sharing the host network namespace. Under that
// configuration, the satellite pod IP equals the host's InternalIP,
// and LINSTOR must reconfigure each `.res` so `peer.<peer>.address`
// resolves to the host IP — not to a stale pod-CIDR address piraeus-
// operator wrote into `Spec.NetInterfaces[0]` before the switch
// (Bug 48: piraeus rewrites the positional [0] NetInterface with the
// satellite pod IP, which under CNI networking is a pod-CIDR address
// that doesn't route cross-node).
//
// Three cases pin the contract on the dispatcher side:
//
//   - hostNetwork true, host IP published under `k8s-internal`:
//     dispatcher picks the host IP regardless of any leftover
//     pod-CIDR sitting in `default` from the pre-switch state.
//   - hostNetwork true with only the host-IP `default` NIC (clean
//     install, no `k8s-internal` published yet): host IP carries
//     through — single-NIC clusters keep working.
//   - hostNetwork false (CNI networking, pre-switch baseline):
//     `default` holds the pod-CIDR satellite-pod IP and no
//     `k8s-internal` is registered; the dispatcher carries the
//     pod-CIDR address through unchanged. The test pins that
//     pre-switch behaviour so a regression in either direction —
//     stamping host IP when none was published, OR failing to pick
//     `k8s-internal` post-switch — fails the suite.
//
// The W06 e2e scenario verifies `.res` carries host IPs on a live
// stand; this unit test pins the dispatcher's IP selection in
// isolation so we catch the failure mode without spinning the stand.
func TestHostNetworkSatelliteUsesNodeIPNotPodIP(t *testing.T) {
	rdName := "pvc-1"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	// Routable host InternalIPs and pod-CIDR addresses for the two
	// satellite nodes. The dispatcher must always prefer the host
	// IP when both are visible on the Node CRD.
	const (
		n1HostIP = "10.51.0.2"
		n1PodIP  = "10.244.0.5"
		n2HostIP = "10.51.0.3"
		n2PodIP  = "10.244.0.6"
	)

	cases := []struct {
		name        string
		n2Ifaces    []blockstoriov1alpha1.NodeNetInterface
		wantPeerAdr string
		comment     string
	}{
		{
			name: "host-network-with-k8s-internal-overrides-stale-pod-cidr",
			n2Ifaces: []blockstoriov1alpha1.NodeNetInterface{
				// Stale entry: piraeus wrote the pod IP into
				// positional [0] before the hostNetwork switch.
				{Name: "default", Address: n2PodIP},
				// register / label-sync published the host
				// InternalIP under the well-known name.
				{Name: "k8s-internal", Address: n2HostIP},
			},
			wantPeerAdr: n2HostIP,
			comment:     "post-W06: k8s-internal host IP wins over leftover pod-CIDR in default[0]",
		},
		{
			name: "host-network-host-ip-in-default-single-nic",
			n2Ifaces: []blockstoriov1alpha1.NodeNetInterface{
				// Clean install with hostNetwork=true from the
				// start: satellite pod IP == host IP, so the only
				// advertised NIC carries the host address.
				{Name: "default", Address: n2HostIP},
			},
			wantPeerAdr: n2HostIP,
			comment:     "single-NIC hostNetwork clusters carry the host IP through default",
		},
		{
			name: "container-network-pre-switch-baseline",
			n2Ifaces: []blockstoriov1alpha1.NodeNetInterface{
				// Pre-W06 state: CNI networking, pod-CIDR is all
				// we have — dispatcher must not invent a host IP
				// out of thin air.
				{Name: "default", Address: n2PodIP},
			},
			wantPeerAdr: n2PodIP,
			comment:     "pre-switch (container network) carries pod-CIDR through",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			targetID := int32(0)
			peerID := int32(1)

			target := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            "data-hdd",
				},
				Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &targetID},
			}

			peer := blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-n2"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n2",
					StoragePool:            "data-hdd",
				},
				Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &peerID},
			}

			nodes := []blockstoriov1alpha1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "n1"},
					Spec: blockstoriov1alpha1.NodeSpec{
						Type: "Satellite",
						NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{
							{Name: "default", Address: n1HostIP},
							{Name: "k8s-internal", Address: n1HostIP},
						},
						// Pin the local replica's address to a known
						// value via PrefNic so any regression in the
						// peer-side lookup doesn't bleed in through
						// the target-side branch.
						Props: map[string]string{"PrefNic": "default"},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "n2"},
					Spec: blockstoriov1alpha1.NodeSpec{
						Type:          "Satellite",
						NetInterfaces: tc.n2Ifaces,
					},
				},
			}

			_ = n1PodIP // referenced for symmetry / future cases

			got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{peer}, nodes, nil, rd, nil)
			if got == nil {
				t.Fatalf("BuildDesired returned nil")
			}

			gotAddr := got.DrbdOptions["peer.n2.address"]
			if gotAddr != tc.wantPeerAdr {
				t.Errorf("%s: peer.n2.address=%q want %q (drbdOpts=%v)",
					tc.comment, gotAddr, tc.wantPeerAdr, got.DrbdOptions)
			}
		})
	}
}

// TestPrefNicSteersDRBDAddress: scenario 3.W03.
//
// `linstor node set-property <node> PrefNic <nic>` (or the equivalent
// pool-scope `linstor storage-pool set-property <node> <pool>
// PrefNic <nic>`) must steer DRBD replication to the named
// NetInterface: the target's own `address` (rendered into `.res` as
// `on <node> { address ... }`) AND every `peer.<peer>.address` must
// resolve to the chosen interface's IP — not the default endpoint,
// not the 0.0.0.0 placeholder.
//
// Cases:
//
//   - pool-level PrefNic on each node's pool: both target and peer
//     addresses follow that pool's PrefNic.
//   - node-level PrefNic via `Node.Spec.Props["PrefNic"]`: applies
//     to every StoragePool on that node (UG9: safer than pool-level
//     — includes the diskless pool too).
//   - pool-level overrides node-level on the same node (most-specific
//     scope wins, per UG9 prop precedence).
//
// We exercise both target (`address`) and peer (`peer.n2.address`)
// so a regression that only fixes one side still fails the test.
func TestPrefNicSteersDRBDAddress(t *testing.T) {
	rdName := "pvc-1"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	// Two nodes, each with two NICs: a "default" (pod-CIDR-ish) and
	// a "nic_10G" the operator wants DRBD to use.
	const (
		n1Default = "10.244.0.2"
		n1Fast    = "192.168.43.231"
		n2Default = "10.244.0.5"
		n2Fast    = "192.168.43.232"
		poolName  = "data-hdd"
	)

	makeNodes := func(n1Props, n2Props map[string]string) []blockstoriov1alpha1.Node {
		return []blockstoriov1alpha1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "n1"},
				Spec: blockstoriov1alpha1.NodeSpec{
					Type:  "Satellite",
					Props: n1Props,
					NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{
						{Name: "default", Address: n1Default},
						{Name: "nic_10G", Address: n1Fast},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "n2"},
				Spec: blockstoriov1alpha1.NodeSpec{
					Type:  "Satellite",
					Props: n2Props,
					NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{
						{Name: "default", Address: n2Default},
						{Name: "nic_10G", Address: n2Fast},
					},
				},
			},
		}
	}

	makePool := func(node string, props map[string]string) blockstoriov1alpha1.StoragePool {
		return blockstoriov1alpha1.StoragePool{
			ObjectMeta: metav1.ObjectMeta{Name: node + "-" + poolName},
			Spec: blockstoriov1alpha1.StoragePoolSpec{
				NodeName: node,
				PoolName: poolName,
				Props:    props,
			},
		}
	}

	cases := []struct {
		name           string
		nodes          []blockstoriov1alpha1.Node
		pools          []blockstoriov1alpha1.StoragePool
		wantTargetAddr string
		wantPeerAddr   string
		comment        string
	}{
		{
			name:  "pool-level-prefnic-steers-both-sides",
			nodes: makeNodes(nil, nil),
			pools: []blockstoriov1alpha1.StoragePool{
				makePool("n1", map[string]string{"PrefNic": "nic_10G"}),
				makePool("n2", map[string]string{"PrefNic": "nic_10G"}),
			},
			wantTargetAddr: n1Fast,
			wantPeerAddr:   n2Fast,
			comment:        "pool PrefNic on each node selects the fast NIC for both endpoints",
		},
		{
			name: "node-level-prefnic-steers-both-sides",
			nodes: makeNodes(
				map[string]string{"PrefNic": "nic_10G"},
				map[string]string{"PrefNic": "nic_10G"},
			),
			pools: []blockstoriov1alpha1.StoragePool{
				// no pool-level PrefNic — node-level must take effect
				makePool("n1", nil),
				makePool("n2", nil),
			},
			wantTargetAddr: n1Fast,
			wantPeerAddr:   n2Fast,
			comment:        "Node.Spec.Props[PrefNic] applies cluster-wide on that node",
		},
		{
			name: "pool-level-overrides-node-level",
			nodes: makeNodes(
				// node says "default" but pool pins "nic_10G" → pool wins
				map[string]string{"PrefNic": "default"},
				map[string]string{"PrefNic": "default"},
			),
			pools: []blockstoriov1alpha1.StoragePool{
				makePool("n1", map[string]string{"PrefNic": "nic_10G"}),
				makePool("n2", map[string]string{"PrefNic": "nic_10G"}),
			},
			wantTargetAddr: n1Fast,
			wantPeerAddr:   n2Fast,
			comment:        "most-specific scope wins per UG9 prop precedence",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			targetID := int32(0)
			peerID := int32(1)

			target := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            poolName,
				},
				Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &targetID},
			}

			peer := blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-n2"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n2",
					StoragePool:            poolName,
				},
				Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &peerID},
			}

			got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{peer}, tc.nodes, tc.pools, rd, nil)
			if got == nil {
				t.Fatalf("BuildDesired returned nil")
			}

			if gotAddr := got.DrbdOptions["address"]; gotAddr != tc.wantTargetAddr {
				t.Errorf("%s: target address=%q want %q (drbdOpts=%v)",
					tc.comment, gotAddr, tc.wantTargetAddr, got.DrbdOptions)
			}

			if gotAddr := got.DrbdOptions["peer.n2.address"]; gotAddr != tc.wantPeerAddr {
				t.Errorf("%s: peer.n2.address=%q want %q (drbdOpts=%v)",
					tc.comment, gotAddr, tc.wantPeerAddr, got.DrbdOptions)
			}
		})
	}
}

// TestAutoQuorumDisabledPassesManualSettings: scenario 7.W01
// (wave2-07 §7.W01, UG9 lines 4233-4279). Models the operator
// workflow where auto-quorum is opted out at the cluster scope and
// the per-RD quorum / on-no-quorum policy is set explicitly:
//
//  1. Cluster/RG-scope `DrbdOptions/auto-quorum=disabled` reaches
//     the dispatcher via effectiveProps but is a LINSTOR-internal
//     section-less key — it must round-trip into DrbdOptions on
//     the wire so the satellite splitDRBDOptions can drop it
//     (it lives in the LINSTOR-only namespace, not a DRBD section).
//
//  2. Per-RD `DrbdOptions/Resource/quorum=majority` and
//     `DrbdOptions/Resource/on-no-quorum=suspend-io` flow through
//     verbatim. Without this guard, an effective-props refactor
//     that dropped section-prefixed keys would silently revert the
//     manual policy to DRBD's defaults the moment the operator
//     disabled auto-quorum — exactly the regression UG9 warns
//     about. Acceptable on-no-quorum values from upstream:
//     `suspend-io` (freeze I/O until quorum returns) and `io-error`
//     (fail I/O so writers see EIO and can retry elsewhere).
//
//  3. Resource-scope override beats RD-scope: a Resource carrying
//     its own `DrbdOptions/Resource/quorum=off` wins. Mirrors
//     LINSTOR's priority-props rule (Resource > RD > RG > Ctrl).
func TestAutoQuorumDisabledPassesManualSettings(t *testing.T) {
	t.Parallel()

	rdName := "pvc-quorum"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	cases := []struct {
		name           string
		effectiveProps map[string]string
		targetProps    map[string]string
		wantQuorum     string
		wantOnNoQuorum string
		wantAutoQuorum string
		comment        string
	}{
		{
			name: "rd-scope-majority-suspend-io",
			effectiveProps: map[string]string{
				// Cluster scope disabled the auto-quorum reconciler.
				"DrbdOptions/auto-quorum": "disabled",
				// Operator manually set the per-RD policy.
				"DrbdOptions/Resource/quorum":       "majority",
				"DrbdOptions/Resource/on-no-quorum": "suspend-io",
			},
			wantQuorum:     "majority",
			wantOnNoQuorum: "suspend-io",
			wantAutoQuorum: "disabled",
			comment:        "RD-scope manual policy flows to wire when auto-quorum is opted out",
		},
		{
			name: "rd-scope-off-io-error",
			effectiveProps: map[string]string{
				"DrbdOptions/auto-quorum":           "disabled",
				"DrbdOptions/Resource/quorum":       "off",
				"DrbdOptions/Resource/on-no-quorum": "io-error",
			},
			wantQuorum:     "off",
			wantOnNoQuorum: "io-error",
			wantAutoQuorum: "disabled",
			comment:        "off + io-error is a legitimate manual choice on a 1-replica RD",
		},
		{
			name: "resource-overrides-rd",
			effectiveProps: map[string]string{
				// Hierarchy collapsed into one bag by the resolver:
				// RD wanted majority, Resource overrides to off, and
				// the cluster-scope disable marker still rides along.
				"DrbdOptions/auto-quorum":           "disabled",
				"DrbdOptions/Resource/quorum":       "off",
				"DrbdOptions/Resource/on-no-quorum": "io-error",
			},
			wantQuorum:     "off",
			wantOnNoQuorum: "io-error",
			wantAutoQuorum: "disabled",
			comment:        "Resource > RD precedence — final merged map carries the most-specific value",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target := &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            "data-hdd",
					Props:                  tc.targetProps,
				},
			}

			got := dispatcher.BuildDesired(target, nil, nil, nil, rd, tc.effectiveProps)
			if got == nil {
				t.Fatalf("BuildDesired returned nil")
			}

			if v := got.DrbdOptions["DrbdOptions/Resource/quorum"]; v != tc.wantQuorum {
				t.Errorf("%s: quorum=%q want %q (drbdOpts=%v)",
					tc.comment, v, tc.wantQuorum, got.DrbdOptions)
			}

			if v := got.DrbdOptions["DrbdOptions/Resource/on-no-quorum"]; v != tc.wantOnNoQuorum {
				t.Errorf("%s: on-no-quorum=%q want %q (drbdOpts=%v)",
					tc.comment, v, tc.wantOnNoQuorum, got.DrbdOptions)
			}

			// Section-less LINSTOR-only marker round-trips on the
			// wire — the satellite splitDRBDOptions filters it out
			// before .res rendering (see pkg/satellite/reconciler.go
			// splitDRBDOptions doc comment). The dispatcher must
			// NOT strip it here: stripping would break operators
			// who inspect the wire payload to confirm the cluster
			// policy reached the satellite.
			if v := got.DrbdOptions["DrbdOptions/auto-quorum"]; v != tc.wantAutoQuorum {
				t.Errorf("%s: auto-quorum=%q want %q (drbdOpts=%v)",
					tc.comment, v, tc.wantAutoQuorum, got.DrbdOptions)
			}

			// Manual quorum keys must NOT leak into Props (wire-side
			// non-DRBD bag); they belong in DrbdOptions so the
			// .res renderer picks them up.
			if _, ok := got.Props["DrbdOptions/Resource/quorum"]; ok {
				t.Errorf("%s: quorum leaked into Props bag: %v", tc.comment, got.Props)
			}
		})
	}
}

// TestDRBDLUKSStorageLayerStackPropagates_W10 pins scenario 9.W10's
// dispatcher-side contract: when an RD carries
// LayerStack=["DRBD","LUKS","STORAGE"] (the wire shape that
// `linstor rg c --layer-list drbd,luks,storage` + spawn produces via
// the cross-listed sticky-inheritance path), `BuildDesired` must
// propagate the exact slice onto DesiredResource.LayerStack — same
// length, same order — so the satellite's `needsLUKS` check
// (pkg/satellite/reconciler.go) lights up `applyLUKS` for the
// cryptsetup layer between DRBD and the underlying storage.
//
// The order matters: DRBD-above-LUKS means DRBD replicates ciphertext
// (the whole point of the allowed ordering), and STORAGE-terminal
// anchors the backing disk at the bottom of the chain. A regression
// that flipped the slot order or dropped LUKS entirely would silently
// fall back to needsLUKS=false → no encryption layer, which is a
// data-at-rest leak.
func TestDRBDLUKSStorageLayerStackPropagates_W10(t *testing.T) {
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-luks"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-luks",
			NodeName:               "n1",
			StoragePool:            "data-hdd",
		},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	wantStack := []string{"DRBD", "LUKS", "STORAGE"}

	if len(got.LayerStack) != len(wantStack) {
		t.Fatalf("DesiredResource.LayerStack: got %v, want %v",
			got.LayerStack, wantStack)
	}

	for i, want := range wantStack {
		if got.LayerStack[i] != want {
			t.Errorf("LayerStack[%d]: got %q, want %q",
				i, got.LayerStack[i], want)
		}
	}

	// Verify the LUKS slot lands strictly between DRBD and STORAGE —
	// the property pkg/satellite/reconciler.applyLUKS depends on
	// when it composes cryptsetup over the raw storage devices.
	// A flat assertion on the slice already enforces this, but the
	// explicit index check pins the semantic intent against a future
	// refactor that might widen the allowed shape.
	drbdIdx := -1
	luksIdx := -1
	storageIdx := -1

	for i, layer := range got.LayerStack {
		switch layer {
		case "DRBD":
			drbdIdx = i
		case "LUKS":
			luksIdx = i
		case "STORAGE":
			storageIdx = i
		}
	}

	if drbdIdx < 0 || luksIdx <= drbdIdx || storageIdx <= luksIdx {
		t.Errorf("LUKS slot must sit between DRBD and STORAGE; got order %v "+
			"(drbd=%d luks=%d storage=%d)",
			got.LayerStack, drbdIdx, luksIdx, storageIdx)
	}
}

// TestMultiPathConnectionRendersTwoPaths pins scenario 3.7 end-to-end
// from the dispatcher's perspective: when the parent RD carries the
// `Cozystack/ResourceConnectionPaths/<a>/<b>` prop (written by the
// REST `POST /resource-connections/{a}/{b}/paths` handler), the
// produced DesiredResource MUST surface those paths on
// DesiredResource.Connections in the same (NodeA, NodeB, Path) shape
// the satellite's .res renderer expects.
//
// Until pkg/rest's POST handler landed (this same commit), the RD
// could not carry the prop in the first place and the test was
// pending on dispatcher.connectionsFromRD existing. Now both ends
// exist; this asserts the on-RD wire shape flows through to the
// renderer's Connections slice with two distinct paths.
//
// Why we don't also call drbd.Build here: the renderer's path-block
// shape is pinned in pkg/drbd/conffile_test.go::TestRenderMultiPathConnection;
// duplicating that assertion would couple this test to .res
// formatting changes. We keep the dispatcher test focused on the
// translation step it owns: RD props → DesiredConnection slice.
func TestMultiPathConnectionRendersTwoPaths(t *testing.T) {
	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				// JSON shape matches what pkg/rest writes via the
				// `POST .../paths` handler. Keep in sync with
				// pkg/rest/resource_connections.go.
				"Cozystack/ResourceConnectionPaths/n1/n2": `[` +
					`{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"},` +
					`{"name":"path2","node_a_address":"10.2.2.5","node_b_address":"10.2.2.6"}` +
					`]`,
			},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			StoragePool:            "data-hdd",
		},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	conns := got.GetConnections()
	if len(conns) != 1 {
		t.Fatalf("Connections: got %d, want 1", len(conns))
	}

	conn := conns[0]
	if conn.NodeA != "n1" || conn.NodeB != "n2" {
		t.Errorf("pair: got (%q, %q), want (n1, n2)", conn.NodeA, conn.NodeB)
	}

	if len(conn.Paths) != 2 {
		t.Fatalf("Paths: got %d, want 2: %+v", len(conn.Paths), conn.Paths)
	}

	// Paths are sorted by Name in the dispatcher to anchor a
	// deterministic .res render; we expect path1 then path2.
	if conn.Paths[0].Name != "path1" || conn.Paths[0].AddressA != "10.1.1.5" || conn.Paths[0].AddressB != "10.1.1.6" {
		t.Errorf("paths[0]: got %+v, want path1/10.1.1.5/10.1.1.6", conn.Paths[0])
	}

	if conn.Paths[1].Name != "path2" || conn.Paths[1].AddressA != "10.2.2.5" || conn.Paths[1].AddressB != "10.2.2.6" {
		t.Errorf("paths[1]: got %+v, want path2/10.2.2.5/10.2.2.6", conn.Paths[1])
	}

	// Negative: an RD with no resource-connection prop must NOT
	// produce a phantom Connections entry. Guards against the
	// dispatcher conjuring an empty slice that would otherwise make
	// the .res render diverge ("connection { }") and trigger a
	// drbdadm-adjust loop.
	noProps := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	plain := dispatcher.BuildDesired(target, nil, nil, nil, noProps, nil)
	if len(plain.GetConnections()) != 0 {
		t.Errorf("RD with no connection prop produced Connections=%+v, want empty", plain.GetConnections())
	}
}
