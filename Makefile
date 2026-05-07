SHELL := bash
NAME ?= blockstor
WORKERS ?= 3
CONTROLPLANES ?= 1
EXTENSIONS ?= siderolabs/drbd

WORK_DIR := .work/$(NAME)
TALOSCONFIG := $(WORK_DIR)/talosconfig
KUBECONFIG := $(WORK_DIR)/kubeconfig

export TALOSCONFIG
export KUBECONFIG

.PHONY: help up down reset piraeus oracle smoke use list clean

help: ## show this help
	@grep -hE '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk -F':.*?##' '{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

up: ## create talos+qemu cluster (NAME=<n>)
	@mkdir -p $(WORK_DIR)
	@./stand/up.sh "$(NAME)" "$(CONTROLPLANES)" "$(WORKERS)" "$(EXTENSIONS)" "$(WORK_DIR)"

down: ## destroy cluster
	@./stand/down.sh "$(NAME)" "$(WORK_DIR)"

reset: down up ## down + up

piraeus: ## install piraeus-operator + linstor-csi
	@./stand/install-piraeus.sh "$(WORK_DIR)"

oracle: ## install Java LINSTOR controller as in-cluster oracle
	@./stand/install-oracle.sh "$(WORK_DIR)"

smoke: ## run smoke tests
	@./tests/smoke.sh "$(WORK_DIR)"

use: ## print TALOSCONFIG/KUBECONFIG exports for shell eval
	@echo "export TALOSCONFIG=$(abspath $(TALOSCONFIG))"
	@echo "export KUBECONFIG=$(abspath $(KUBECONFIG))"

list: ## list active stands
	@ls -1 .work 2>/dev/null || true

clean: ## remove all .work dirs (does NOT destroy clusters)
	@rm -rf .work
