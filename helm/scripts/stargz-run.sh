#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

CONTAINER_RUNTIME_CONFIG="/etc/containerd/config.toml"
SNAPSHOTTER_GRPC_SOCKET="/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"

function wait_service_active(){
    local wait_time="$1"
    local sleep_time="$2"
    local service="$3"

    nsenter -t 1 -m systemctl restart $service

    # Wait for containerd to be running
    while [ "$wait_time" -gt 0 ]; do
        if nsenter -t 1 -m systemctl is-active --quiet $service; then
            echo "$service is running"
            return 0  
        else
            sleep "$sleep_time"
            wait_time=$((wait_time-sleep_time))
        fi
    done

    echo "Timeout reached. $service may not be running."
    nsenter -t 1 -m systemctl status $service
    return 1  
}

function configure_snapshotter() {

    echo "configuring snapshotter"

    # Copy the container runtime config to a backup
    cp "$CONTAINER_RUNTIME_CONFIG" "$CONTAINER_RUNTIME_CONFIG".bak.stargz


    # When trying to edit the config file that is mounted by docker with `sed -i`, the error would happend:
    # sed: cannot rename /etc/containerd/config.tomlpmdkIP: Device or resource busy  
    # The reason is that `sed`` with option `-i` creates new file, and then replaces the old file with the new one, 
    # which definitely will change the file inode. But the file is mounted by docker, which means we are not allowed to 
    # change its inode from within docker container.
    # 
    # So we copy the original file to a backup, make changes to the backup, and then overwrite the original file with the backup.
    cp "$CONTAINER_RUNTIME_CONFIG" "$CONTAINER_RUNTIME_CONFIG".bak
    # Check and add nydus proxy plugin in the config
    if grep -q '\[proxy_plugins.stargz\]' "$CONTAINER_RUNTIME_CONFIG".bak; then
        echo "the config has configured the stargz proxy plugin!"
    else
        echo "Not found stargz proxy plugin!"
        cat <<EOF >>"$CONTAINER_RUNTIME_CONFIG".bak
        
    [proxy_plugins.stargz]
        type = "snapshot"
        address = "$SNAPSHOTTER_GRPC_SOCKET"
EOF
    fi

    if grep -q 'disable_snapshot_annotations' "$CONTAINER_RUNTIME_CONFIG".bak; then
        sed -i -e "s|disable_snapshot_annotations = .*|disable_snapshot_annotations = false|" \
                "${CONTAINER_RUNTIME_CONFIG}".bak
    else
        sed -i '/\[plugins\..*\.containerd\]/a\disable_snapshot_annotations = false' \
                "${CONTAINER_RUNTIME_CONFIG}".bak
    fi

    sed -i -e '/\[plugins\..*\.containerd\]/,/snapshotter =/ s/snapshotter = "[^"]*"/snapshotter = "stargz"/' "${CONTAINER_RUNTIME_CONFIG}".bak
    
    cat "${CONTAINER_RUNTIME_CONFIG}".bak >  "${CONTAINER_RUNTIME_CONFIG}"
}

function deploy(){
    echo "deploying stargz-snapshotter"

    configure_snapshotter
    /opt/stargz/bin/containerd-stargz-grpc --log-level=debug --config=/etc/containerd-stargz-grpc/config.toml &
    wait_service_active 30 5 containerd
    sleep infinity
}

deploy
