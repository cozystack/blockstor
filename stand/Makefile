## Dev-stand targets — talos+qemu cluster, piraeus, smoke. Included from the
## top-level Makefile so `make up NAME=foo` works from the repo root.

NAME ?= blockstor
WORKERS ?= 3
CONTROLPLANES ?= 1
EXTENSIONS ?= siderolabs/drbd,siderolabs/zfs

WORK_DIR := .work/$(NAME)
TALOSCONFIG := $(WORK_DIR)/talosconfig
KUBECONFIG := $(WORK_DIR)/kubeconfig

export TALOSCONFIG
export KUBECONFIG

.PHONY: stand-help up down reset piraeus oracle build-images blockstor pools smoke smoke-blockstor burnin-blockstor e2e e2e-list use list stand-clean

stand-help: ## list dev-stand targets
	@echo "stand targets (use NAME=<cluster>):"
	@grep -hE '^[a-zA-Z_-]+:.*?##' stand/Makefile | awk -F':.*?##' '{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

up: ## create talos+qemu cluster (NAME=<n>)
	@mkdir -p $(WORK_DIR)
	@./stand/up.sh "$(NAME)" "$(CONTROLPLANES)" "$(WORKERS)" "$(EXTENSIONS)" "$(WORK_DIR)"

down: ## destroy cluster (NAME=<n>)
	@./stand/down.sh "$(NAME)" "$(WORK_DIR)"

reset: down up ## down + up

piraeus: ## install piraeus-operator + linstor-csi (NAME=<n>)
	@./stand/install-piraeus.sh "$(WORK_DIR)"

oracle: ## install Java LINSTOR controller as in-cluster oracle (NAME=<n>)
	@./stand/install-oracle.sh "$(WORK_DIR)"

build-images: ## build + push blockstor/blockstor-satellite to localhost:5000 (shared across all stands)
	@./stand/build-images.sh

blockstor: ## install blockstor CRDs + controller + satellites (NAME=<n>)
	@./stand/install-blockstor.sh "$(WORK_DIR)"

pools: ## provision real-disk pools on workers (NAME=<n>, TYPE=zfs|lvm|both)
	@./stand/install-pools.sh "$(WORK_DIR)" "$${TYPE:-both}"

smoke: ## run piraeus-stack smoke (PVC + linstor-csi) (NAME=<n>)
	@./tests/smoke.sh "$(WORK_DIR)"

smoke-blockstor: ## blockstor end-to-end smoke (RD → 2 replicas → bytes-perfect failover) (NAME=<n>)
	@./tests/smoke-blockstor.sh "$(WORK_DIR)"

burnin-blockstor: ## continuous PVC churn for DURATION seconds (default 86400 = 24h) (NAME=<n>, DURATION=<sec>)
	@./tests/burnin-blockstor.sh "$(WORK_DIR)" "$${DURATION:-86400}"

e2e: ## run a single e2e scenario (NAME=<cluster>, SCENARIO=<basename without .sh>)
	@test -n "$${SCENARIO}" || (echo "usage: make e2e NAME=<cluster> SCENARIO=<name>"; exit 2)
	@./tests/e2e/$${SCENARIO}.sh "$(WORK_DIR)"

e2e-list: ## list available e2e scenarios
	@ls tests/e2e/*.sh | xargs -n1 basename | grep -v '^lib\.sh$$' | sed 's/\.sh$$//'

use: ## print TALOSCONFIG/KUBECONFIG exports for shell eval
	@echo "export TALOSCONFIG=$(abspath $(TALOSCONFIG))"
	@echo "export KUBECONFIG=$(abspath $(KUBECONFIG))"

list: ## list active stands
	@ls -1 .work 2>/dev/null || true

stand-clean: ## remove all .work dirs (does NOT destroy clusters)
	@rm -rf .work
