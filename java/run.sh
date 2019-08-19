#!/usr/bin/env bash

set -e

# get this scripts parent directory
WDIR="$PWD"; [ "$PWD" = "/" ] && WDIR=""
case "$0" in
  /*) SCRIPT_DIR="${0}";;
  *) SCRIPT_DIR="$WDIR/${0#./}";;
esac
SCRIPT_DIR="${SCRIPT_DIR%/*}"

cd $SCRIPT_DIR

NAME="${1}"

if [[ ! $NAME ]]; then
  echo "Provide a new name: \`run.sh {name}\`"
  exit 1
fi


if [[ ! -d $SCRIPT_DIR/.repository ]]; then
    mkdir $SCRIPT_DIR/.repository
fi


docker run --rm \
  -v $SCRIPT_DIR/.repository:/root/.m2/repository \
  -v $SCRIPT_DIR:/opt/app/java \
  -v `dirname $SCRIPT_DIR`/.cred/render-pubsub.json:/opt/app/.cred/render-pubsub.json \
  --workdir /opt/app/java/experiments \
  --env GOOGLE_APPLICATION_CREDENTIALS=/opt/app/.cred/render-pubsub.json \
  maven:3-jdk-8 \
    mvn compile exec:java \
        -Dexec.mainClass=org.apache.beam.examples.$NAME \
        -Dexec.args="${@:2}" \
        -Pdirect-runner
