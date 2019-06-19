FROM flink:1.8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y libltdl7

COPY dockerception.sh /
ENTRYPOINT ["/dockerception.sh"]
