## Setup GCP

This is only required if using GCP pubsub streaming examples.

```
PROJECT_FULLNAME=dataflow-241218
ROLE=pubsub.editor
ROLE_SUFFIX=pubsub
PROJECT_NAME=${PROJECT_FULLNAME%-*}
SERVICE_USER=$PROJECT_NAME-$ROLE_SUFFIX
export GOOGLE_APPLICATION_CREDENTIALS=~/dev/beam-tests/$SERVICE_USER.json
gcloud --project $PROJECT_FULLNAME iam service-accounts create $SERVICE_USER
gcloud projects add-iam-policy-binding $PROJECT_FULLNAME --member "serviceAccount:$SERVICE_USER@$PROJECT_FULLNAME.iam.gserviceaccount.com" --role "roles/$ROLE"
gcloud iam service-accounts keys create $GOOGLE_APPLICATION_CREDENTIALS --iam-account $SERVICE_USER@$PROJECT_FULLNAME.iam.gserviceaccount.com
```

## Direct Runner

To run various experiments using the direct runner:
```bash
source venv/bin/activate
python -m rillbeam.experiments.flowbased
```

## Flink Runner

As of this writing, apache_beam 2.14 is compatible with flink 1.8.

### Using Homebrew

Install and start [docker](https://docs.docker.com/v17.12/docker-for-mac/install/).

Install flink 1.8:

```bash
brew install apache-flink
```

Start flink:

```bash
/usr/local/Cellar/apache-flink/1.8.0/libexec/bin/start-cluster.sh
```

Get the beam source:

```bash
git clone https://github.com/apache/beam.git
cd beam
git checkout release-2.13.0
```

Run the job server:

```bash
./gradlew -p runners/flink/1.8/job-server runShadow -P flinkMasterUrl=localhost:8081
```

Then:

```bash
cd rillbeam
python -m virtualenv .venv
source .venv/bin/activate
pip install -U apache_beam[gcp]==2.14.0
pip install -U -r requirements.txt
python -m rillbeam.experiments.flowbased --defaults flink
```

### Using Docker

You need beam source code to build docker container(s).

```bash
git clone https://github.com/apache/beam.git
cd beam
git checkout release-2.14.0
```

Flink requires pulling the python sdk image from a docker registry. At luma we use dockereg:5000/, locally you need to [start your own](https://docs.docker.com/registry/deploying/).

> TIP: Add `DOCKER_REGISTRY_URL="localhost:5000"` to your _~/.bashrc_profile_.

Build the python sdk container.

```bash
./gradlew -p sdks/python/container docker -P docker-repository-root="${DOCKER_REGISTRY_URL}/beam" -P docker-tag=2.14.0.luma01
```

Upload sdk python container to the registry.

```bash
docker push ${DOCKER_REGISTRY_URL}/beam/python:2.14.0.luma01
```

Build job-server container:

```bash
./gradlew -p runners/flink/1.8/job-server-container docker -P docker-repository-root="${DOCKER_REGISTRY_URL}/beam" -P docker-tag=2.14.0.luma01
```
```bash
docker push ${DOCKER_REGISTRY_URL}/beam/flink-job-server:2.14.0.luma01
```

Build docker-flink container:

```bash
rillbeam/docker/docker-flink/build.sh
```

Start flink and beam job-server containers:

```bash
cd rillbeam/docker/osx
docker-compose pull && docker-compose up
```

Then:

```bash
cd rillbeam
source venv/bin/activate
python -m rillbeam.experiments.flowbased --defaults flink
```

> Note: You can increase the number of taskmanager (workers) by doing: 
```bash
cd rillbeam/docker/osx
docker-compose scale taskmanager=4
```

## GCP Dataflow Runner

```bash
source venv/bin/activate
python -m rillbeam.experiments.flowbased --defaults dataflow
```

# Development Tips

## Python

I use a different virtual env for development vs testing stock beam.

Note: Running this from a shell with a venv worked, but running the task from IntelliJ did not.

```
./gradlew :sdks:python:sdist
pip install sdks/python/build/apache-beam-2.16.0.dev0.zip[gcp]
```

linting

```
./gradlew :sdks:python:test-suites:tox:py2:lintPy27
```


# Java Development

Run via direct-runner:

```bash
rillbeam/java/run.sh PubSub
```
