#!/usr/bin/env bash

set -e

if [ $(getent group $GROUP) ]; then
  groupmod -g $GID -o $GROUP
else
  groupadd -g $GID $GROUP
fi

useradd -r -u $UID -g $GROUP $USER

if ! [[ -d /home/$USER ]]; then
  mkdir /home/$USER
fi

chown $USER /home/$USER
export HOME=/home/$USER
