#!/usr/bin/env bash
set -euo pipefail

# Installs the local Kubernetes deployment toolchain needed by this repo:
# - helmfile
# - yq
# - helm-diff plugin
#
# Default mode is user-local install into ~/.local/bin.
# Optional system-wide install:
#   INSTALL_DIR=/usr/local/bin USE_SUDO=1 ./deploy/scripts/install-dev-tools.sh

INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
USE_SUDO="${USE_SUDO:-0}"

if [[ "${USE_SUDO}" == "1" ]]; then
  INSTALL_CMD=(sudo install)
else
  INSTALL_CMD=(install)
fi

TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMPDIR}"
}
trap cleanup EXIT

ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64)
    HF_ARCH="amd64"
    YQ_ARCH="amd64"
    ;;
  aarch64|arm64)
    HF_ARCH="arm64"
    YQ_ARCH="arm64"
    ;;
  *)
    echo "Unsupported architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

mkdir -p "${INSTALL_DIR}"

echo "==> Resolving latest helmfile release"
HELMFILE_TAG="$(curl -fsSL https://api.github.com/repos/helmfile/helmfile/releases/latest | jq -r '.tag_name')"
HELMFILE_VER="${HELMFILE_TAG#v}"

echo "==> Installing helmfile ${HELMFILE_TAG} into ${INSTALL_DIR}"
curl -fsSL -o "${TMPDIR}/helmfile.tar.gz" \
  "https://github.com/helmfile/helmfile/releases/download/${HELMFILE_TAG}/helmfile_${HELMFILE_VER}_linux_${HF_ARCH}.tar.gz"
tar -xzf "${TMPDIR}/helmfile.tar.gz" -C "${TMPDIR}"
"${INSTALL_CMD[@]}" -m 0755 "${TMPDIR}/helmfile" "${INSTALL_DIR}/helmfile"

echo "==> Installing yq into ${INSTALL_DIR}"
curl -fsSL \
  "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_${YQ_ARCH}" \
  -o "${TMPDIR}/yq"
"${INSTALL_CMD[@]}" -m 0755 "${TMPDIR}/yq" "${INSTALL_DIR}/yq"

if [[ "${INSTALL_DIR}" == "$HOME/.local/bin" ]]; then
  if ! grep -qs 'HOME/.local/bin' "$HOME/.bashrc"; then
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
    echo "==> Added ~/.local/bin to ~/.bashrc"
  fi
  if ! grep -qs 'HELM_DATA_HOME=' "$HOME/.bashrc"; then
    echo 'export HELM_DATA_HOME="$HOME/.local/share/helm"' >> "$HOME/.bashrc"
    echo 'export HELM_PLUGINS="$HELM_DATA_HOME/plugins"' >> "$HOME/.bashrc"
    echo "==> Added HELM_DATA_HOME/HELM_PLUGINS to ~/.bashrc"
  fi
  export PATH="$HOME/.local/bin:$PATH"
  export HELM_DATA_HOME="$HOME/.local/share/helm"
  export HELM_PLUGINS="$HELM_DATA_HOME/plugins"
else
  export PATH="${INSTALL_DIR}:$PATH"
fi

if ! helm plugin list | awk 'NR>1 {print $1}' | grep -qx diff; then
  echo "==> Installing helm-diff plugin"
  helm plugin install https://github.com/databus23/helm-diff
else
  echo "==> helm-diff plugin already installed"
fi

echo
echo "Installed tool versions:"
helm version --short
helmfile version
yq --version
helm plugin list
