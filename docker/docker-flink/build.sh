#!/usr/bin/env bash
set -e

# get this scripts parent directory
WDIR="$PWD"; [ "$PWD" = "/" ] && WDIR=""
case "$0" in
  /*) SCRIPT_DIR="${0}";;
  *) SCRIPT_DIR="$WDIR/${0#./}";;
esac
SCRIPT_DIR="${SCRIPT_DIR%/*}"

if [[ -z ${DOCKER_CONTAINER_NAME} ]]; then
  DOCKER_CONTAINER_NAME="$(basename $SCRIPT_DIR)"
fi

docker build $SCRIPT_DIR \
  -t ${DOCKER_REGISTRY_URL:-localhost:5000}/beam/${DOCKER_CONTAINER_NAME}:${FLINK_VERSION:-1.8} \
  --build-arg DOCKER_GID_HOST=$(ls -ng /var/run/docker.sock | cut -f3 -d' ') \
  --build-arg FLINK_VERSION=${FLINK_VERSION:-1.8}

docker push ${DOCKER_REGISTRY_URL:-localhost:5000}/beam/${DOCKER_CONTAINER_NAME}:${FLINK_VERSION:-1.8}
