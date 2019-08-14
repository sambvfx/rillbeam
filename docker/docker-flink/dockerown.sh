#!/usr/bin/env bash
chown flink /var/run/docker.sock
exec /docker-entrypoint.sh "$@"
