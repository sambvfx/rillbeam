#!/usr/bin/env bash


shopt -s nullglob
for f in /docker_entrypoint.d/*.sh; do
  source $f;
done

exec gosu $USER bash --rcfile /.bashrc_denv
