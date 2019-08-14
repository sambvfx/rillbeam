ARG FLINK_VERSION=1.8

FROM flink:$FLINK_VERSION

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y libltdl7

ARG DOCKER_GID_HOST=1001
RUN groupadd -for -g $DOCKER_GID_HOST docker

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

RUN usermod -aG docker flink

COPY ./dockerown.sh /dockerown.sh

ENTRYPOINT ["/dockerown.sh"]