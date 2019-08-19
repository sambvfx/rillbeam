#!/usr/bin/env bash

set -e

# get this scripts parent directory
WDIR="$PWD"; [ "$PWD" = "/" ] && WDIR=""
case "$0" in
  /*) SCRIPT_DIR="${0}";;
  *) SCRIPT_DIR="$WDIR/${0#./}";;
esac
SCRIPT_DIR="${SCRIPT_DIR%/*}"

NAME="${1}"

if [[ ! $NAME ]]; then
  echo "Provide a new name"
  exit 1
fi

docker run --rm \
    -v $SCRIPT_DIR/.repository:/root/.m2/repository \
    -v $SCRIPT_DIR:/opt/app/java \
    --workdir /opt/app maven:3-jdk-8 \
    mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion=2.14.0 \
    -DgroupId=org.example \
    -DartifactId=$NAME \
    -Dversion="0.1" \
    -Dpackage=org.apache.beam.examples \
    -DinteractiveMode=false
