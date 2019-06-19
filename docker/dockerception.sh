#!/bin/bash -x

DOCKER_SOCKET=/var/run/docker.sock
DOCKER_GROUP=docker

if [[ -S ${DOCKER_SOCKET} ]]; then
    groupadd ${DOCKER_GROUP}
    chgrp ${DOCKER_GROUP} ${DOCKER_SOCKET}
    usermod -aG ${DOCKER_GROUP} flink
fi

exec /docker-entrypoint.sh "$@"
