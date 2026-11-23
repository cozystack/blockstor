// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

// constructionSlack is what the process spends before registration starts and
// what the kubelet adds on top of the kill arithmetic: container start, flag
// parsing, ctrl.NewManager, and the failing probe's own timeout.
const constructionSlack = 10 * time.Second

// Kubelet's defaults for the probe fields a manifest leaves unset.
const (
	defaultProbePeriod           = 10 * time.Second
	defaultProbeFailureThreshold = 3
)

// The manifests that run a binary calling NewManager.
//
//nolint:gochecknoglobals // a fixed list of repository paths, read by one test
var managerManifests = []string{
	"stand/blockstor-deploy.yaml",
	"stand/blockstor-apiserver-deploy.yaml",
	"config/manager/manager.yaml",
}

// Nothing serves /healthz while NewManager waits on the API server, so the wait
// has to end before the kubelet kills the container. A 60s budget outlived the
// kill in every manifest that ships, which turned the outage the wait rides out
// into a restart with no cause in the log. The kill deadline is read from the
// manifests rather than restated here, so a probe tightened later fails this
// instead of silently reopening the gap.
func TestIndexRegistrationBudgetEndsBeforeLivenessKills(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "..")

	for _, manifest := range managerManifests {
		kills := livenessKillDeadlines(t, filepath.Join(root, manifest))
		if len(kills) == 0 {
			t.Errorf("%s: no Deployment container with a liveness probe; the check would pass vacuously", manifest)

			continue
		}

		for container, kill := range kills {
			if indexRegistrationBudget+constructionSlack > kill {
				t.Errorf("%s container %s: the kubelet kills at %s, but registration may wait %s after %s of "+
					"construction, with /healthz not yet served", manifest, container, kill,
					indexRegistrationBudget, constructionSlack)
			}
		}
	}
}

// livenessKillDeadlines returns, per container with a liveness probe, the time
// after start at which three failed probes have restarted it.
func livenessKillDeadlines(t *testing.T, path string) map[string]time.Duration {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	kills := map[string]time.Duration{}

	for {
		var deploy appsv1.Deployment

		err := decoder.Decode(&deploy)
		if errors.Is(err, io.EOF) {
			return kills
		}

		if err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}

		if deploy.Kind != "Deployment" {
			continue
		}

		for i := range deploy.Spec.Template.Spec.Containers {
			container := &deploy.Spec.Template.Spec.Containers[i]

			probe := container.LivenessProbe
			if probe == nil {
				continue
			}

			period := defaultProbePeriod
			if probe.PeriodSeconds > 0 {
				period = time.Duration(probe.PeriodSeconds) * time.Second
			}

			threshold := int32(defaultProbeFailureThreshold)
			if probe.FailureThreshold > 0 {
				threshold = probe.FailureThreshold
			}

			kills[container.Name] = time.Duration(probe.InitialDelaySeconds)*time.Second +
				time.Duration(threshold-1)*period
		}
	}
}
