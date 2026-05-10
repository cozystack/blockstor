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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/loopfile"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// loopfileDirPerm is the mkdir mode for the loopfile pool directory
// when --loopfile-dir doesn't already exist. 0o700 because the sparse
// files inside are block-device backers and shouldn't be world-readable.
const loopfileDirPerm = 0o700

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
		listenAddr     string
		advertised     string
		lvmPoolName    string
		lvmVG          string
		lvmThinPool    string
	)

	flag.StringVar(&controllerAddr, "controller", "blockstor-controller:7000",
		"gRPC address of the blockstor controller")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"),
		"name this satellite registers under (defaults to NODE_NAME env)")
	flag.StringVar(&stateDir, "state-dir", "/var/lib/blockstor-satellite",
		"directory the satellite uses to persist DRBD .res files and per-resource state")
	flag.StringVar(&listenAddr, "listen", ":7000",
		"bind address for the satellite-side gRPC server (controller dials this for ApplyResources)")

	// advertised-endpoint flag — actual default is computed AFTER
	// flag.Parse() because it depends on --listen's port. Initial
	// value here is just an empty string + a placeholder doc.
	flag.StringVar(&advertised, "advertised-endpoint", "",
		"host:port the controller should dial back at (defaults to $POD_IP:<listen-port>)")
	flag.StringVar(&lvmPoolName, "lvm-pool-name", "",
		"register an LVM-thin pool under this LINSTOR pool name (empty disables LVM)")
	flag.StringVar(&lvmVG, "lvm-vg", "",
		"LVM volume group backing the lvm-pool-name pool")
	flag.StringVar(&lvmThinPool, "lvm-thinpool", "",
		"LVM thinpool LV backing the lvm-pool-name pool")

	var (
		lvmThickPoolName string
		lvmThickVG       string
	)

	flag.StringVar(&lvmThickPoolName, "lvm-thick-pool-name", "",
		"register an LVM (classic, thick) pool under this LINSTOR pool name (empty disables)")
	flag.StringVar(&lvmThickVG, "lvm-thick-vg", "",
		"LVM volume group backing the lvm-thick-pool-name pool (defaults to lvm-thick-pool-name)")

	var (
		loopfilePoolName string
		loopfileDir      string
	)

	flag.StringVar(&loopfilePoolName, "loopfile-pool-name", "",
		"register a loopfile (sparse-file + losetup) pool under this LINSTOR pool name (empty disables)")
	flag.StringVar(&loopfileDir, "loopfile-dir", "/var/lib/blockstor-pool",
		"directory the loopfile-pool-name pool stores its sparse files in")

	var (
		zfsPoolName string
		zfsZpool    string
		zfsThin     bool
	)

	flag.StringVar(&zfsPoolName, "zfs-pool-name", "",
		"register a ZFS pool under this LINSTOR pool name (empty disables)")
	flag.StringVar(&zfsZpool, "zfs-zpool", "",
		"backing zpool name (defaults to zfs-pool-name)")
	flag.BoolVar(&zfsThin, "zfs-thin", true,
		"create sparse zvols (ZFS_THIN). Set to false for thick provisioning.")

	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if nodeName == "" {
		logger.Error("node-name is required (pass --node-name or set NODE_NAME)")

		return 1
	}

	// Compute the advertised endpoint default if --advertised-endpoint
	// wasn't explicitly set. We pull the port from --listen so a
	// non-default listen port (e.g. when DRBD's tcp-port-range starts
	// at 7000 and gRPC has to move) doesn't require a second flag.
	if advertised == "" {
		host := os.Getenv("POD_IP")
		port := portFromListen(listenAddr)

		if host != "" && port != "" {
			advertised = host + ":" + port
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	providers := map[string]storage.Provider{}

	if lvmPoolName != "" {
		if lvmVG == "" || lvmThinPool == "" {
			logger.Error("lvm-pool-name set but lvm-vg / lvm-thinpool missing")

			return 1
		}

		providers[lvmPoolName] = lvm.NewThin(
			lvm.ThinConfig{VolumeGroup: lvmVG, ThinPool: lvmThinPool},
			storage.RealExec{})
	}

	if lvmThickPoolName != "" {
		vg := lvmThickVG
		if vg == "" {
			vg = lvmThickPoolName
		}

		providers[lvmThickPoolName] = lvm.NewThick(
			lvm.ThickConfig{VolumeGroup: vg},
			storage.RealExec{})
	}

	if loopfilePoolName != "" {
		err := os.MkdirAll(loopfileDir, loopfileDirPerm)
		if err != nil {
			logger.Error("create loopfile dir", "err", err)

			return 1
		}

		providers[loopfilePoolName] = loopfile.NewProvider(
			loopfile.Config{Dir: loopfileDir},
			storage.RealExec{})
	}

	if zfsPoolName != "" {
		zpool := zfsZpool
		if zpool == "" {
			zpool = zfsPoolName
		}

		providers[zfsPoolName] = zfs.NewProvider(
			zfs.Config{Pool: zpool, Thin: zfsThin},
			storage.RealExec{})
	}

	// Wipe stale .res files from previous incarnations of this
	// satellite process. Each pod restart should hand drbdadm a
	// clean slate — the controller will re-Apply every Resource
	// CRD on this node within seconds, so we don't lose state, just
	// the cruft from prior runs (csi-sanity leftovers, RDs deleted
	// while the satellite was down, malformed .res from earlier
	// release versions, etc). Without this drbdadm fails on parse
	// errors from any one stale file even when the new RD's render
	// is clean.
	cleanStateDir(stateDir, logger)

	restCfg, mgrFactory := buildControllerRuntime(logger)

	agent := satellite.NewAgent(satellite.Config{
		NodeName:           nodeName,
		ControllerAddr:     controllerAddr,
		ListenAddr:         listenAddr,
		AdvertisedEndpoint: advertised,
		StateDir:           stateDir,
		Providers:          providers,
		DialTimeout:        10 * time.Second,
		Logger:             logger,
		RESTConfig:         restCfg,
		ManagerFactory:     mgrFactory,
	})

	providerNames := make([]string, 0, len(providers))
	for name := range providers {
		providerNames = append(providerNames, name)
	}

	logger.Info("blockstor-satellite starting",
		"node_name", nodeName,
		"controller", controllerAddr,
		"state_dir", stateDir,
		"listen", listenAddr,
		"providers", providerNames)

	err := agent.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("satellite exited", "err", err)

		return 1
	}

	return 0
}

// buildControllerRuntime returns (restCfg, factory) when an
// in-cluster Kubernetes config is reachable — the production
// path under the DaemonSet. When the config can't be loaded
// (off-cluster `go run`, unit tests with no kubeconfig), both
// return values are nil and the agent falls back to the gRPC-
// only path. Phase 10.1: the c-r manager runs alongside gRPC
// so CRD events drive the same apply chain.
func buildControllerRuntime(logger *slog.Logger) (*rest.Config, satellite.ManagerFactory) {
	cfg, err := ctrl.GetConfig()
	if err != nil {
		logger.Info("no Kubernetes config; skipping controller-runtime manager",
			"reason", err)

		return nil, nil
	}

	factory := func(restCfg *rest.Config, nodeName string, rec *satellite.Reconciler) (manager.Manager, error) {
		return controllers.NewManager(restCfg, controllers.Config{
			NodeName: nodeName,
			Apply:    rec,
			Exec:     storage.RealExec{},
		})
	}

	return cfg, factory
}

// portFromListen extracts the port number from a Go-style listen
// address ("host:port", ":port"). Returns empty when the address
// doesn't include a port — caller falls back to whatever default
// they chose. Doesn't validate the host part.
func portFromListen(addr string) string {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return ""
	}

	return addr[idx+1:]
}

// cleanStateDir wipes every *.res file in dir on satellite startup.
// The controller re-Applies every Resource CRD on this node shortly
// after Hello, so the contents are reproducible — we don't persist
// satellite-side state across restarts. Best-effort: log and continue
// on errors so a single missing dir doesn't stall the whole startup.
func cleanStateDir(dir string, logger *slog.Logger) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		// Missing dir is fine — the satellite's first Apply will
		// create it on demand.
		return
	}

	removed := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".res") {
			// Leave global_common.conf and any operator-supplied
			// non-rendered files alone.
			continue
		}

		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil {
			logger.Warn("clean state-dir entry", "path", path, "err", err)

			continue
		}

		removed++
	}

	if removed > 0 {
		logger.Info("wiped stale .res files on startup", "dir", dir, "removed", removed)
	}
}
