#!/bin/bash

#   Copyright The containerd Authors.

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

set -euo pipefail

CONTEXT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/"
REPO="${CONTEXT}../../"
SNAPSHOTTER_SOCK_PATH=/run/containerd-stargz-grpc/containerd-stargz-grpc.sock

source "${CONTEXT}/const.sh"
source "${REPO}/script/util/utils.sh"

CNI_PLUGINS_VERSION=$(get_version_from_arg "${REPO}/Dockerfile" "CNI_PLUGINS_VERSION")
CRI_TOOLS_VERSION=$(get_version_from_arg "${REPO}/Dockerfile" "CRI_TOOLS_VERSION")
PAUSE_IMAGE_NAME=$(get_version_from_arg "${REPO}/Dockerfile" "PAUSE_IMAGE_NAME_TEST")
GINKGO_VERSION=v1.16.5

if [ "${CRI_NO_RECREATE:-}" != "true" ] ; then
    echo "Preparing node image..."

    TARGET_STAGE=
    if [ "${BUILTIN_SNAPSHOTTER:-}" == "true" ] ; then
        TARGET_STAGE="--target kind-builtin-snapshotter"
    fi

    docker build ${DOCKER_BUILD_ARGS:-} -t "${NODE_BASE_IMAGE_NAME}" ${TARGET_STAGE} "${REPO}"
    docker build ${DOCKER_BUILD_ARGS:-} -t "${PREPARE_NODE_IMAGE}" --target containerd-base "${REPO}"
fi

TMP_CONTEXT=$(mktemp -d)
IMAGE_LIST=$(mktemp)
function cleanup {
    local ORG_EXIT_CODE="${1}"
    rm -rf "${TMP_CONTEXT}" || true
    rm "${IMAGE_LIST}" || true
    exit "${ORG_EXIT_CODE}"
}
trap 'cleanup "$?"' EXIT SIGHUP SIGINT SIGQUIT SIGTERM

ADDITIONAL_INST=
if [ "${BUILTIN_SNAPSHOTTER:-}" == "true" ] ; then
    # Special configuration for CRI containerd + builtin stargz snapshotter
    cat <<EOF > "${TMP_CONTEXT}/containerd.hack.toml"
version = 2

[debug]
  format = "json"
  level = "debug"
[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "${PAUSE_IMAGE_NAME}"
[plugins."io.containerd.grpc.v1.cri".containerd]
  default_runtime_name = "runc"
  snapshotter = "stargz"
  disable_snapshot_annotations = false
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.test-handler]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.snapshotter.v1.stargz"]
cri_keychain_image_service_path = "${SNAPSHOTTER_SOCK_PATH}"
metadata_store = "memory"
[plugins."io.containerd.snapshotter.v1.stargz".cri_keychain]
enable_keychain = true
EOF
    ADDITIONAL_INST="COPY containerd.hack.toml /etc/containerd/config.toml"
fi

cat <<EOF > "${TMP_CONTEXT}/test.conflist"
{
  "cniVersion": "0.4.0",
  "name": "containerd-net",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "promiscMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [
          [{
            "subnet": "10.88.0.0/16"
          }]
        ],
        "routes": [
          { "dst": "0.0.0.0/0" }
        ]
      }
    },
    {
      "type": "portmap",
      "capabilities": {"portMappings": true}
    }
  ]
}
EOF

USE_METADATA_STORE="memory"
if [ "${METADATA_STORE:-}" != "" ] ; then
    USE_METADATA_STORE="${METADATA_STORE}"
fi

FUSE_MANAGER_CONFIG=""
if [ "${FUSE_MANAGER:-}" == "true" ] ; then
    FUSE_MANAGER_CONFIG='listen_path = "/run/containerd-stargz-grpc/cri.sock"
[fuse_manager]
enable = true'
fi

SNAPSHOTTER_CONFIG_FILE=/etc/containerd-stargz-grpc/config.toml
if [ "${BUILTIN_SNAPSHOTTER:-}" == "true" ] ; then
    SNAPSHOTTER_CONFIG_FILE=/etc/containerd/config.toml
fi

USE_FUSE_PASSTHROUGH="false"
if [ "${FUSE_PASSTHROUGH:-}" != "" ] ; then
    USE_FUSE_PASSTHROUGH="${FUSE_PASSTHROUGH}"
    if [ "${BUILTIN_SNAPSHOTTER:-}" == "true" ] && [ "${FUSE_PASSTHROUGH}" == "true" ] ; then
        echo "builtin snapshotter + fuse passthrough test is unsupported"
        exit 1
    fi
fi

if [ "${TRANSFER_SERVICE:-}" == "true" ] ; then
    cp "${CONTEXT}/config.containerd.transfer.toml" "${TMP_CONTEXT}/"
    ADDITIONAL_INST="${ADDITIONAL_INST}
COPY config.containerd.transfer.toml /etc/containerd/config.toml"
fi

# Prepare the testing node
cat <<EOF > "${TMP_CONTEXT}/Dockerfile"
# Legacy builder that doesn't support TARGETARCH should set this explicitly using --build-arg.
# If TARGETARCH isn't supported by the builder, the default value is "amd64".

FROM ${NODE_BASE_IMAGE_NAME}
ARG TARGETARCH

ENV PATH=$PATH:/usr/local/go/bin
ENV GOPATH=/go
# Do not install git and its dependencies here which will cause failure of building the image
RUN apt-get update && apt-get install -y --no-install-recommends make && \
    curl -Ls https://dl.google.com/go/go1.24.0.linux-\${TARGETARCH:-amd64}.tar.gz | tar -C /usr/local -xz && \
    go install github.com/onsi/ginkgo/ginkgo@${GINKGO_VERSION} && \
    mkdir -p \${GOPATH}/src/github.com/kubernetes-sigs/cri-tools /tmp/cri-tools && \
    curl -sL https://github.com/kubernetes-sigs/cri-tools/archive/refs/tags/v${CRI_TOOLS_VERSION}.tar.gz | tar -C /tmp/cri-tools -xz && \
    mv /tmp/cri-tools/cri-tools-${CRI_TOOLS_VERSION}/* \${GOPATH}/src/github.com/kubernetes-sigs/cri-tools/ && \
    cd \${GOPATH}/src/github.com/kubernetes-sigs/cri-tools && \
    make && make install -e BINDIR=\${GOPATH}/bin && \
    curl -Ls https://github.com/containernetworking/plugins/releases/download/v${CNI_PLUGINS_VERSION}/cni-plugins-linux-\${TARGETARCH:-amd64}-v${CNI_PLUGINS_VERSION}.tgz | tar xzv -C /opt/cni/bin && \
    systemctl disable kubelet

COPY ./test.conflist /etc/cni/net.d/test.conflist

${ADDITIONAL_INST}

RUN <<EEE
cat <<EOT >> "${SNAPSHOTTER_CONFIG_FILE}"
${FUSE_MANAGER_CONFIG}
EOT
EEE

RUN if [ "${BUILTIN_SNAPSHOTTER:-}" != "true" ] ; then \
      sed -i '1imetadata_store = "${USE_METADATA_STORE}"' "${SNAPSHOTTER_CONFIG_FILE}" && \
      echo '[fuse]' >> "${SNAPSHOTTER_CONFIG_FILE}" && \
      echo "passthrough = ${USE_FUSE_PASSTHROUGH}" >> "${SNAPSHOTTER_CONFIG_FILE}" ; \
    fi

ENTRYPOINT [ "/usr/local/bin/entrypoint", "/sbin/init" ]
EOF
docker build -t "${NODE_TEST_IMAGE_NAME}" ${DOCKER_BUILD_ARGS:-} "${TMP_CONTEXT}"

echo "Testing..."
"${CONTEXT}/test-legacy.sh" "${IMAGE_LIST}"
"${CONTEXT}/test-stargz.sh" "${IMAGE_LIST}"
