FROM openjdk:8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y libltdl7

RUN apt-get update; \
  apt-get -y install apt-transport-https ca-certificates curl gnupg software-properties-common; \
  curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -; \
  apt-key fingerprint 0EBFCD88; \
  add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/debian \
       $(lsb_release -cs) \
       stable" ;\
  apt-get update; \
  apt-get -y install docker-ce

# install gosu for a better su+exec command
ARG GOSU_VERSION=1.10
RUN dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')" \
 && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch" \
 && chmod +x /usr/local/bin/gosu \
 && gosu nobody true

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python get-pip.py virtualenv

ARG DOCKER_GID=1001
RUN groupadd -for -g $DOCKER_GID docker

ENV WORKDIR=/opt/apache/beam
RUN mkdir -p $WORKDIR
WORKDIR $WORKDIR

COPY ./docker_entrypoint.sh /docker_entrypoint.sh
COPY ./docker_entrypoint.d /docker_entrypoint.d
COPY ./.bashrc_denv /.bashrc_denv
ENTRYPOINT ["/docker_entrypoint.sh"]

VOLUME /var/lib/docker
