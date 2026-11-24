// SPDX-License-Identifier: Apache-2.0

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

package linstormigrate

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The fixture under testdata/dump is read at the storage version of
// LINSTOR 1.33's CRDs, v1-31-1. firstServedColumns lists, per table the
// converter loads, the columns that version has and v1-15-0, the first
// version the CRDs serve and the one a plain `kubectl get` reads, does
// not. The API server prunes them from every object it serves there.
//
//nolint:gochecknoglobals // fixed test table
var firstServedColumns = map[string][]string{
	"layerdrbdresources": {"tcp_port_list"},
	"resourcegroups":     {"peer_slots", "x_replicas_on_different"},
}

const firstServedVersion = "internal.linstor.linbit.com/v1-15-0"

// firstServedDump writes the fixture as the API server serves it at
// v1-15-0 into a new directory: the columns that version lacks pruned
// and every object's apiVersion set to it.
func firstServedDump(t *testing.T) string {
	t.Helper()

	src := filepath.Join("testdata", "dump")
	dst := t.TempDir()

	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	for _, entry := range entries {
		table := strings.TrimSuffix(entry.Name(), dumpFileSuffix)

		raw, err := os.ReadFile(filepath.Join(src, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}

		var list struct {
			Items []map[string]any `json:"items"`
		}

		err = json.Unmarshal(raw, &list)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}

		for _, item := range list.Items {
			item["apiVersion"] = firstServedVersion

			spec, _ := item["spec"].(map[string]any)
			for _, column := range firstServedColumns[table] {
				delete(spec, column)
			}
		}

		out, err := json.Marshal(list)
		if err != nil {
			t.Fatalf("marshal %s: %v", entry.Name(), err)
		}

		err = os.WriteFile(filepath.Join(dst, entry.Name()), out, 0o600)
		if err != nil {
			t.Fatalf("write %s: %v", entry.Name(), err)
		}
	}

	return dst
}

func loadFixture(t *testing.T) *Dump {
	t.Helper()

	dump, err := LoadDump(filepath.Join("testdata", "dump"))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	return dump
}

// drbdRowOf returns the live DRBD row of one replica of the fixture.
func drbdRowOf(t *testing.T, dump *Dump, node, rd string) *LayerDrbdResourceRow {
	t.Helper()

	for _, lri := range dump.LayerResourceIDs {
		if lri.NodeName != node || lri.ResourceName != rd || lri.SnapshotName != "" || lri.LayerResourceKind != "DRBD" {
			continue
		}

		for i := range dump.LayerDrbdResources {
			if dump.LayerDrbdResources[i].LayerResourceID == lri.LayerResourceID {
				return &dump.LayerDrbdResources[i]
			}
		}
	}

	t.Fatalf("fixture has no DRBD row for %s on %s", rd, node)

	return nil
}

func portOf(port *int32) any {
	if port == nil {
		return nil
	}

	return *port
}

// A dump read the way the runbook used to read it, every table at the
// first version its CRD serves, has lost every replica's port and the
// resource groups' columns with it. It is refused, naming the version
// it was read at and the runbook, and a live port map does not make it
// acceptable: the map restores the ports, not the other columns.
func TestADumpReadAtTheFirstServedVersionIsRefused(t *testing.T) {
	dump, err := LoadDump(firstServedDump(t))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	for _, opts := range []Options{
		{LUKS: fixtureLUKS(t)},
		{LUKS: fixtureLUKS(t), DRBDPorts: map[string]int32{"pvc-vol1": 7001, "pvc-vol3": 7003, "pvc-vol5": 7005}},
	} {
		_, err = ConvertWithOptions(dump, opts)
		if !errors.Is(err, ErrDumpNotAtStorageVersion) {
			t.Fatalf("Convert err = %v, want ErrDumpNotAtStorageVersion", err)
		}

		for _, want := range []string{"7 of 7 DRBD replica rows", firstServedVersion, "storage version", "runbook"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q does not say %q", err, want)
			}
		}
	}
}

// The storage-version fixture records the version it was read at and
// converts, each replica taking its port from its own row.
func TestADumpReadAtTheStorageVersionConverts(t *testing.T) {
	dump := loadFixture(t)

	for _, table := range []string{"layerdrbdresources", "resourcegroups", "nodes"} {
		if got := dump.APIVersions[table]; !slices.Equal(got, []string{"internal.linstor.linbit.com/v1-31-1"}) {
			t.Errorf("APIVersions[%s] = %v, want the storage version", table, got)
		}
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for _, name := range []string{"pvc-vol1.node-a", "pvc-vol1.node-b", "pvc-vol1.node-c"} {
		if got := findResource(t, res, name).Spec.DRBDPort; portOf(got) != int32(7001) {
			t.Errorf("%s DRBDPort = %v, want 7001 from tcp_port_list", name, portOf(got))
		}
	}

	if got := findRD(t, res, "pvc-vol1").Spec.DRBDPort; portOf(got) != int32(7001) {
		t.Errorf("pvc-vol1 DRBDPort = %v, want the shared 7001", portOf(got))
	}
}

// One pruned row is enough to refuse: a dump assembled from two reads
// is as lossy as one read at the wrong version.
func TestOnePortlessReplicaRowIsRefused(t *testing.T) {
	dump := loadFixture(t)
	drbdRowOf(t, dump, "NODE-B", "PVC-VOL5").TCPPortList = nil

	_, err := convertForTest(t, dump)
	if !errors.Is(err, ErrDumpNotAtStorageVersion) || !strings.Contains(err.Error(), "1 of 7") {
		t.Fatalf("Convert err = %v, want ErrDumpNotAtStorageVersion for 1 of 7 rows", err)
	}
}

// A LINSTOR from before the port moved to the replica keeps it on the
// definition, and its replica rows have no tcp_port_list at any
// version. That is not a pruned dump, and each replica takes its
// definition's port.
func TestADumpFromBeforeThePortMovedConverts(t *testing.T) {
	dump := loadFixture(t)

	for i := range dump.LayerDrbdResources {
		dump.LayerDrbdResources[i].TCPPortList = nil
	}

	for i := range dump.LayerDrbdResourceDefinitions {
		row := &dump.LayerDrbdResourceDefinitions[i]
		if row.SnapshotName == "" {
			row.TCPPort = ptr(int32(7100 + i))
		}
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if got := findResource(t, res, "pvc-vol5.node-b").Spec.DRBDPort; portOf(got) != int32(7102) {
		t.Errorf("pvc-vol5.node-b DRBDPort = %v, want its definition's 7102", portOf(got))
	}

	if got := findRD(t, res, "pvc-vol5").Spec.DRBDPort; portOf(got) != int32(7102) {
		t.Errorf("pvc-vol5 DRBDPort = %v, want 7102", portOf(got))
	}
}

// Replicas of one definition the database gives different ports keep
// their own, with no live map, and the definition gets none.
func TestEachReplicaKeepsItsDatabasePort(t *testing.T) {
	dump := loadFixture(t)
	drbdRowOf(t, dump, "NODE-A", "PVC-VOL5").TCPPortList = ptr("[7042]")
	drbdRowOf(t, dump, "NODE-B", "PVC-VOL5").TCPPortList = ptr("[7051]")

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for name, want := range map[string]int32{"pvc-vol5.node-a": 7042, "pvc-vol5.node-b": 7051} {
		if got := findResource(t, res, name).Spec.DRBDPort; portOf(got) != want {
			t.Errorf("%s DRBDPort = %v, want %d", name, portOf(got), want)
		}
	}

	if got := findRD(t, res, "pvc-vol5").Spec.DRBDPort; got != nil {
		t.Errorf("pvc-vol5 DRBDPort = %d, want none: its replicas listen on different ports", *got)
	}

	if hasWarning(res, "pvc-vol5: no DRBD port") {
		t.Errorf("a definition whose ports are in the database was reported portless; warnings %v", res.Warnings)
	}
}

// With a live map the kernel's port is kept, and a database port that
// disagrees with it is reported for that replica alone.
func TestTheLiveMapCrossChecksTheDatabasePort(t *testing.T) {
	dump := loadFixture(t)

	res, err := ConvertWithOptions(dump, Options{
		LUKS:             fixtureLUKS(t),
		ReplicaDRBDPorts: map[string]map[string]int32{"pvc-vol1": {"node-a": 7099, "node-b": 7001, "node-c": 7001}},
	})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if got := findResource(t, res, "pvc-vol1.node-a").Spec.DRBDPort; portOf(got) != int32(7099) {
		t.Errorf("pvc-vol1.node-a DRBDPort = %v, want the kernel's 7099", portOf(got))
	}

	if !hasWarning(res, "resource pvc-vol1.node-a: LINSTOR's database gives DRBD port 7001, the kernel listens on 7099") {
		t.Errorf("disagreement not reported; warnings %v", res.Warnings)
	}

	if hasWarning(res, "pvc-vol1.node-b: LINSTOR's database") {
		t.Errorf("an agreeing replica was reported; warnings %v", res.Warnings)
	}
}

// A port list blockstor cannot carry, several ports or a value that is
// not a list of ports, gives the replica no port from the database and
// says so.
func TestAPortListBlockstorCannotCarryIsReported(t *testing.T) {
	for list, want := range map[string]string{
		"[7001, 7002]": "resource pvc-vol1.node-a: the database gives this replica 2 DRBD ports",
		"[0]":          `resource pvc-vol1.node-a: tcp_port_list "[0]" is not a list of ports`,
		`"7001"`:       "is not a list of ports",
	} {
		dump := loadFixture(t)
		drbdRowOf(t, dump, "NODE-A", "PVC-VOL1").TCPPortList = ptr(list)

		res, err := convertForTest(t, dump)
		if err != nil {
			t.Fatalf("%s: Convert: %v", list, err)
		}

		if got := findResource(t, res, "pvc-vol1.node-a").Spec.DRBDPort; got != nil {
			t.Errorf("%s: pvc-vol1.node-a DRBDPort = %d, want none", list, *got)
		}

		if !hasWarning(res, want) {
			t.Errorf("%s: not reported as %q; warnings %v", list, want, res.Warnings)
		}

		if got := findRD(t, res, "pvc-vol1").Spec.DRBDPort; got != nil {
			t.Errorf("%s: pvc-vol1 DRBDPort = %d, want none: node-a has no port", list, *got)
		}
	}
}

// A live map whose node names match no replica leaves the database's
// ports in place, and the report says they went unchecked rather than
// that the replicas lose their ports.
func TestAnUnmatchedLiveMapKeepsTheDatabasePorts(t *testing.T) {
	dump := loadFixture(t)

	res, err := ConvertWithOptions(dump, Options{
		LUKS:             fixtureLUKS(t),
		ReplicaDRBDPorts: map[string]map[string]int32{"pvc-vol1": {"k8s-a": 7099, "k8s-b": 7099}},
	})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if got := findResource(t, res, "pvc-vol1.node-b").Spec.DRBDPort; portOf(got) != int32(7001) {
		t.Errorf("pvc-vol1.node-b DRBDPort = %v, want the database's 7001", portOf(got))
	}

	if !hasWarning(res, "pvc-vol1: no node in the live port map (k8s-a, k8s-b) hosts a replica of it") ||
		!hasWarning(res, "the database's ports are used, unchecked against the kernel") {
		t.Errorf("unmatched map not reported as unchecked; warnings %v", res.Warnings)
	}

	if hasWarning(res, "pvc-vol1: no DRBD port for") {
		t.Errorf("replicas with database ports reported portless; warnings %v", res.Warnings)
	}
}

// A resource group's peer_slots is carried, and an
// x_replicas_on_different blockstor has no field for is reported as
// dropped by name and value rather than lost silently.
func TestResourceGroupColumnsOfTheStorageVersion(t *testing.T) {
	dump := loadFixture(t)

	for i := range dump.ResourceGroups {
		if dump.ResourceGroups[i].ResourceGroupName == "SC-ENCRYPTED" {
			dump.ResourceGroups[i].XReplicasOnDifferent = `{"Aux/site":1}`
		}
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for _, group := range res.ResourceGroups {
		want := int32(0)
		if group.Name == "sc-replicated" {
			want = 7
		}

		if group.Spec.PeerSlots != want {
			t.Errorf("resource group %s peerSlots = %d, want %d", group.Name, group.Spec.PeerSlots, want)
		}
	}

	if !hasWarning(res, `resource group sc-encrypted: x_replicas_on_different {"Aux/site":1} dropped`) {
		t.Errorf("x_replicas_on_different not reported; warnings %v", res.Warnings)
	}

	if hasWarning(res, "sc-replicated: x_replicas_on_different") {
		t.Errorf("an empty x_replicas_on_different was reported; warnings %v", res.Warnings)
	}
}
