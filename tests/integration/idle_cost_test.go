// SPDX-License-Identifier: Apache-2.0

//go:build integration

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

package integration

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	restclient "k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// busyController names the controller's API requests in busyRequests.
const busyController = "controller"

// busyRequests counts the API requests each component of the busy
// stack sends, by method and resource.
type busyRequests struct {
	mu sync.Mutex
	by map[string]map[string]int
}

func newBusyRequests() *busyRequests {
	return &busyRequests{by: map[string]map[string]int{}}
}

// config is cfg with every request counted for who.
func (r *busyRequests) config(cfg *restclient.Config, who string) *restclient.Config {
	counted := restclient.CopyConfig(cfg)
	counted.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return busyCounting{next: next, requests: r, who: who}
	})

	return counted
}

func (r *busyRequests) add(who, what string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.by[who] == nil {
		r.by[who] = map[string]int{}
	}

	r.by[who][what]++
}

// take returns the requests of who since the last call, the total and
// the kinds that made most of them, and starts the count over.
func (r *busyRequests) take(who string) (int, string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	counts := r.by[who]
	delete(r.by, who)

	kinds := make([]string, 0, len(counts))
	total := 0

	for kind, n := range counts {
		kinds = append(kinds, kind)
		total += n
	}

	sort.Slice(kinds, func(i, j int) bool { return counts[kinds[i]] > counts[kinds[j]] })

	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds[:min(len(kinds), 6)] {
		parts = append(parts, fmt.Sprintf("%s %d", kind, counts[kind]))
	}

	return total, strings.Join(parts, ", ")
}

type busyCounting struct {
	next     http.RoundTripper
	requests *busyRequests
	who      string
}

func (c busyCounting) RoundTrip(req *http.Request) (*http.Response, error) {
	c.requests.add(c.who, busyRequestKind(req))

	return c.next.RoundTrip(req) //nolint:wrapcheck // a transport passes the error on as it is
}

// busyRequestKind is a request's method and resource, with /status for
// a status write and WATCH for a watch.
func busyRequestKind(req *http.Request) string {
	method := req.Method
	if req.URL.Query().Get("watch") == "true" {
		method = "WATCH"
	}

	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")

	for i, part := range parts {
		if part != "v1alpha1" && part != "v1" {
			continue
		}

		tail := parts[i+1:]
		if len(tail) == 0 {
			break
		}

		kind := tail[0]
		if len(tail) >= 3 && tail[2] == "status" {
			kind += "/status"
		}

		return method + " " + kind
	}

	return method + " " + req.URL.Path
}

// seedVolume creates a volume the way createVolume does, trying each
// call again for a while: the REST server can answer a volume definition
// for a definition it just created with NotFound, and the seeding is
// not what the benchmark measures.
func (s *busyStack) seedVolume(ctx context.Context, name, preferred string) error {
	calls := []struct {
		path string
		body any
		want int
	}{
		{"/v1/resource-definitions", apiv1.ResourceDefinitionCreate{
			ResourceDefinition: apiv1.ResourceDefinition{Name: name, ResourceGroupName: busyRG},
		}, 0},
		{"/v1/resource-definitions/" + name + "/volume-definitions", apiv1.VolumeDefinitionCreate{
			VolumeDefinition: apiv1.VolumeDefinition{SizeKib: busyVolKi},
		}, 0},
		{"/v1/resource-definitions/" + name + "/autoplace", apiv1.AutoPlaceRequest{
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, NodeNameList: []string{preferred}, StoragePool: busyPool},
		}, 1},
		{"/v1/resource-definitions/" + name + "/autoplace", apiv1.AutoPlaceRequest{
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: apiv1.LaxInt32(len(s.nodes)), StoragePool: busyPool},
		}, len(s.nodes)},
	}

	for _, call := range calls {
		var err error

		for range 10 {
			err = s.post(ctx, call.path, call.body)
			if err == nil || strings.Contains(err.Error(), "already exists") {
				err = nil

				break
			}

			time.Sleep(500 * time.Millisecond)
		}

		if err != nil {
			return err
		}

		if call.want > 0 {
			err = s.waitReplicas(ctx, name, call.want)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// processCPU is the CPU time this process has used so far.
func processCPU() time.Duration {
	var usage syscall.Rusage

	err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	if err != nil {
		return 0
	}

	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// TestIdleCostOfBusySatellites measures what three nodes holding about
// a hundred replicas each cost while nothing happens: the external
// tools each satellite runs, at the CPU each costs on the field nodes,
// the API requests of the controller and of each satellite, and the CPU
// of this process, which runs all of them. The satellites run without
// a CPU limit, the way they now run in the field, so the measure is
// what they ask for rather than what a limit lets them have.
//
// LINSTOR's satellite runs nothing while nothing changes; on the field
// cluster it uses 2 to 3 millicores. The test fails when a node's tools
// or the process take more than the budgets.
//
// It takes minutes, so it runs only when BURST_BENCH is set.
// BUSY_EXISTING sets how many volumes each node holds (the field nodes
// hold 115), IDLE_SECONDS how long the idle window is, and
// IDLE_TOOL_BUDGET_M and IDLE_PROCESS_BUDGET_M the budgets in
// millicores.
func TestIdleCostOfBusySatellites(t *testing.T) {
	if os.Getenv("BURST_BENCH") == "" {
		t.Skip("benchmark; set BURST_BENCH=1 to run it")
	}

	existing := busyEnvInt("BUSY_EXISTING", 115)
	window := time.Duration(busyEnvInt("IDLE_SECONDS", 60)) * time.Second
	toolBudget := busyEnvInt("IDLE_TOOL_BUDGET_M", 20)
	processBudget := busyEnvInt("IDLE_PROCESS_BUDGET_M", 100)

	if os.Getenv("BUSY_LOG") != "" {
		ctrl.SetLogger(zap.New(zap.WriteTo(os.Stdout)))
	}

	s := startBusyStack(t, []string{"n1", "n2", "n3"})
	ctx := t.Context()

	idle := make([]string, existing)
	seeded := time.Now()

	var seeders sync.WaitGroup

	jobs := make(chan int)

	for range 10 {
		seeders.Go(func() {
			for i := range jobs {
				idle[i] = fmt.Sprintf("pvc-idle-%03d", i)

				err := s.seedVolume(ctx, idle[i], s.nodes[i%len(s.nodes)])
				if err != nil {
					t.Errorf("seed volume %d: %v", i, err)
				}
			}
		})
	}

	for i := range existing {
		jobs <- i
	}

	close(jobs)
	seeders.Wait()

	if t.Failed() {
		t.FailNow()
	}

	t.Logf("seeded %d volumes in %v", existing, time.Since(seeded).Round(time.Second))

	for _, cpu := range s.cpus {
		cpu.cores = 64
		cpu.limited.Store(true)
	}

	s.startNodeAgents(ctx, t, idle)
	s.quiet.Store(true)

	// Let the agents settle first: the dump, its first writes and the
	// passes they wake.
	time.Sleep(30 * time.Second)

	before := map[string]time.Duration{}
	for node, cpu := range s.cpus {
		before[node] = cpu.usedSoFar()
		cpu.tools()
	}

	for _, who := range append([]string{busyController}, s.nodes...) {
		s.requests.take(who)
	}

	start, startCPU := time.Now(), processCPU()

	time.Sleep(window)

	elapsed := time.Since(start).Seconds()
	process := int((processCPU() - startCPU).Seconds() * 1000 / elapsed)

	var over []string

	for _, node := range s.nodes {
		used := int((s.cpus[node].usedSoFar() - before[node]).Seconds() * 1000 / elapsed)
		requests, kinds := s.requests.take(node)

		t.Logf("%s: tools %dm (%s); %.1f API requests/s (%s)", node, used, s.cpus[node].tools(),
			float64(requests)/elapsed, kinds)

		if used > toolBudget {
			over = append(over, fmt.Sprintf("%s tools %dm", node, used))
		}
	}

	requests, kinds := s.requests.take(busyController)
	t.Logf("controller: %.1f API requests/s (%s)", float64(requests)/elapsed, kinds)
	t.Logf("process (controller, REST server and three satellites): %dm", process)

	if process > processBudget {
		over = append(over, fmt.Sprintf("process %dm", process))
	}

	if len(over) > 0 {
		t.Errorf("idle cost over budget (tools %dm per node, process %dm): %v", toolBudget, processBudget, over)
	}
}
