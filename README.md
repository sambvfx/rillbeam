## Setup

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

As of this writing, apache_beam 2.13 is compatible with flink 1.8.

You need beam source code to build docker container(s).

```bash
git clone https://github.com/apache/beam.git
cd beam
```

Flink requires pulling the python sdk image from a docker registry. At luma we use dockereg:5000/, locally you need to [start your own](https://docs.docker.com/registry/deploying/).

> TIP: Add `DOCKER_REGISTRY_URL="localhost:5000"` to your _~/.bashrc_profile_.

Build the python sdk container.

```bash
./gradlew beam-sdks-python-container:docker -P docker-repository-root="${DOCKER_REGISTRY_URL}/beam" -P docker-tag=2.13
```

Upload sdk python container to the registry.

```bash
docker push ${DOCKER_REGISTRY_URL}/beam/beam-python:2.13
```

Build job-server container:

```bash
./gradlew beam-runners-flink-1.8-job-server-container:docker -P docker-repository-root="${DOCKER_REGISTRY_URL}/beam" -P docker-tag=2.13
```

Build beam-flink container:

```bash
cd rillbeam/docker
docker build -t ${DOCKER_REGISTRY_URL}/beam/beam-flink:2.13-1.8 -f Dockerfile .
```

> TIP: Add `DOCKER_BIN=\`which docker\` ` to your _~/.bashrc_profile_.

Start a fresh shell to pick this up.


Start flink and beam job-server containers:

```bash
cd rillbeam/docker
docker-compose up
```


Then:

```bash
cd rillbeam
source venv/bin/activate
python -m rillbeam.experiments.flowbased --runner flink
```

> Note: You can increase the number of taskmanager (workers) by doing: 
```bash
cd rillbeam/docker
docker-compose scale taskmanager=4
```

## GCP Dataflow Runner

```bash
source venv/bin/activate
python -m rillbeam.experiments.flowbased --runner dataflow
```
