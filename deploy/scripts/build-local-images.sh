#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TAG="${1:-dev}"

build() {
  local image="$1"
  local context="$2"
  shift 2
  echo "==> Building ${image}:${TAG} from ${context}"
  docker build -t "${image}:${TAG}" "$@" "${ROOT_DIR}/${context}"
}

build "haltefest/backend" "services/backend"
build "haltefest/parser" "services/parser"
build "haltefest/ml" "services/ml" --build-arg "MODEL_NAME=chandar-lab/NeoBERT"
build "haltefest/indexer" "services/indexer"
build "haltefest/llm-analysis" "services/llm_analysis"
build "haltefest/reranker" "services/reranker"

echo "Local images built with tag '${TAG}'."
