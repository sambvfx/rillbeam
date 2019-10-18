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

if [[ -z $BEAM_DIR ]]; then
  BEAM_DIR=~/projects/beam
fi

# Extract flink version from the build.gradle
if [[ -z $FLINK_VERSION ]]; then
  FLINK_VERSION=$(cat $BEAM_DIR/runners/flink/1.8/build.gradle | grep flink_version | cut -d"'" -f 2)
fi

docker build $SCRIPT_DIR \
  -t ${DOCKER_REGISTRY_URL:-localhost:5000}/beam/${DOCKER_CONTAINER_NAME}:${FLINK_VERSION} \
  --build-arg DOCKER_GID_HOST=$(ls -ng /var/run/docker.sock | cut -f3 -d' ') \
  --build-arg FLINK_VERSION=${FLINK_VERSION}

docker push ${DOCKER_REGISTRY_URL:-localhost:5000}/beam/${DOCKER_CONTAINER_NAME}:${FLINK_VERSION}
