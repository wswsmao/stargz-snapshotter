ARG NERDCTL_VERSION=1.7.7
ARG OS_IMAGE=mirrors.tencent.com/tencentos/tencentos4-minimal:4.4-v20250331

FROM --platform=$BUILDPLATFORM golang:1.24-bullseye AS snapshotter-dev
ARG TARGETARCH
ARG GOARM
ARG SNAPSHOTTER_BUILD_FLAGS
ARG CTR_REMOTE_BUILD_FLAGS
COPY . $GOPATH/src/github.com/containerd/stargz-snapshotter
ARG CGO_ENABLED
RUN cd $GOPATH/src/github.com/containerd/stargz-snapshotter && \
    PREFIX=/out/ GOARCH=${TARGETARCH:-amd64} GO_BUILD_FLAGS=${SNAPSHOTTER_BUILD_FLAGS} make && \
    mkdir /stargz && \
    cp $GOPATH/src/github.com/containerd/stargz-snapshotter/helm/scripts/config.toml /stargz/config.toml && \
    cp $GOPATH/src/github.com/containerd/stargz-snapshotter/helm/scripts/stargz-run.sh /stargz/stargz-run.sh

FROM $OS_IMAGE AS snapshotter-builder

COPY --from=snapshotter-dev /out/* /opt/stargz/bin/
COPY --from=snapshotter-dev /stargz/config.toml /etc/containerd-stargz-grpc/config.toml
COPY --from=snapshotter-dev /stargz/stargz-run.sh /opt/stargz/bin/stargz-run.sh
ENTRYPOINT [ "/opt/stargz/bin/stargz-run.sh" ]
