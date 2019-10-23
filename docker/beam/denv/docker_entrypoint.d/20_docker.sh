#!/usr/bin/env bash

if [[ $DOCKER_GID ]]; then
  if [[ -e /var/run/docker.sock ]]; then
    groupmod -g $DOCKER_GID -o docker
    if ! groups $USER | grep -q docker; then
      usermod -aG docker $USER
    fi
    echo "chgrp docker /var/run/docker.sock"
    chgrp docker /var/run/docker.sock
  fi
fi
