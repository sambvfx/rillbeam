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

Build python container:

```bash
./gradlew beam-sdks-python-container:docker
```

Build job-server container:

```bash
./gradlew beam-runners-flink-1.8-job-server-container:docker
```

Tag container with version:

```bash
docker tag $USER-docker-apache.bintray.io/beam/flink-job-server:latest flink-job-server:1.8
```

Build beam-flink container:

```bash
cd rillbeam/docker
docker build -t beam-flink:1.8 -f Dockerfile .
```

Add this to your `~/.bashrc_profile` (or similar).

```bash
DOCKER_BIN=`which docker`
```

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
