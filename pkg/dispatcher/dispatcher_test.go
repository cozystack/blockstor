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
