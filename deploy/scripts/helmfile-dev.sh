#!/usr/bin/env bash
set -euo pipefail

# Wrapper around helmfile that pins the Helm plugin/data locations.
# This avoids cases where `helm plugin list` works in one shell, but
# `helmfile apply` cannot see the `helm-diff` plugin.

export HELM_DATA_HOME="${HELMFILE_DEV_HELM_DATA_HOME:-$HOME/.local/share/helm}"
export HELM_PLUGINS="${HELMFILE_DEV_HELM_PLUGINS:-$HELM_DATA_HOME/plugins}"
export HELM_CONFIG_HOME="${HELMFILE_DEV_HELM_CONFIG_HOME:-$HOME/.config/helm}"
export HELM_CACHE_HOME="${HELMFILE_DEV_HELM_CACHE_HOME:-$HOME/.cache/helm}"

exec helmfile "$@"
