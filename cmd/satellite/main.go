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

// Command satellite is the per-node agent that owns local DRBD/LVM/ZFS
// state and reconciles it with what the blockstor-controller dictates.
//
// Phase 3 milestone (this file): boot the binary, register with the
// controller via gRPC, log the hello round-trip. DRBD/LVM/ZFS work lands
// in subsequent slices, each behind the same `apply / observe` contract.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/satellite"
)

func main() {
	os.Exit(run())
}

// run is split out so deferred cancellation actually runs before exit; main
// only ever calls os.Exit(run()) so there are no defers in the same frame
// as the os.Exit call.
func run() int {
	var (
		controllerAddr string
		nodeName       string
		stateDir       string
	)

	flag.StringVar(&controllerAddr, "controller", "blockstor-controller:7000",
		"gRPC address of the blockstor controller")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"),
		"name this satellite registers under (defaults to NODE_NAME env)")
	flag.StringVar(&stateDir, "state-dir", "/var/lib/blockstor-satellite",
		"directory the satellite uses to persist DRBD .res files and per-resource state")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if nodeName == "" {
		logger.Error("node-name is required (pass --node-name or set NODE_NAME)")

		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	agent := satellite.NewAgent(satellite.Config{
		NodeName:       nodeName,
		ControllerAddr: controllerAddr,
		StateDir:       stateDir,
		DialTimeout:    10 * time.Second,
		Logger:         logger,
	})

	logger.Info("blockstor-satellite starting",
		"node_name", nodeName,
		"controller", controllerAddr,
		"state_dir", stateDir)

	err := agent.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("satellite exited", "err", err)

		return 1
	}

	return 0
}
