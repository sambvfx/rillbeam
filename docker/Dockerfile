FROM flink:1.8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y libltdl7

# add flink user to docker group
# flink (uid=9999) should exist via the flink:1.8 container
RUN groupadd --system --gid=9998 docker && usermod -a -G docker flink
