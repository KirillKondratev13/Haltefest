#!/usr/bin/env bash
set -euo pipefail

# Loads locally built application images into all Kubernetes node containers
# whose names match the current cluster nodes returned by kubectl.
#
# Default tag:
#   ./deploy/scripts/load-local-images-into-nodes.sh
#
# Custom tag:
#   ./deploy/scripts/load-local-images-into-nodes.sh dev
#
# Override node list manually if needed:
#   NODE_NAMES="desktop-control-plane" ./deploy/scripts/load-local-images-into-nodes.sh dev

TAG="${1:-dev}"

IMAGES=(
  "haltefest/backend:${TAG}"
  "haltefest/parser:${TAG}"
  "haltefest/ml:${TAG}"
  "haltefest/indexer:${TAG}"
  "haltefest/llm-analysis:${TAG}"
  "haltefest/reranker:${TAG}"
)

if [[ -n "${NODE_NAMES:-}" ]]; then
  mapfile -t NODES < <(printf '%s\n' ${NODE_NAMES})
else
  mapfile -t NODES < <(kubectl get nodes -o name | sed 's#node/##')
fi

if [[ "${#NODES[@]}" -eq 0 ]]; then
  echo "No Kubernetes nodes found via kubectl." >&2
  exit 1
fi

for image in "${IMAGES[@]}"; do
  docker image inspect "${image}" >/dev/null
done

echo "==> Target nodes: ${NODES[*]}"
echo "==> Images to import:"
printf ' - %s\n' "${IMAGES[@]}"

for node in "${NODES[@]}"; do
  echo "==> Importing images into node ${node}"
  for image in "${IMAGES[@]}"; do
    echo "   -> ${image}"
    docker save "${image}" | docker exec -i "${node}" ctr -n k8s.io images import -
  done
done

echo "Local images imported into node containers successfully."
