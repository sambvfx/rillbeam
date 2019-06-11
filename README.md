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

As of this writing, apache_beam 2.12 is compatible with flink 1.7.

To send work to flink, first read [this issue](https://issues.apache.org/jira/browse/BEAM-7379).

Alternately, if homebrew is not your style, try the docker flink services:
```bash
docker pull flink:1.7
cd docker
docker-compose up
```

Then:

```bash
source venv/bin/activate
python -m rillbeam.experiments.flowbased --runner flink
```

Note: if using a single node flink cluster, you must increase the number of 
task slots per manager in `conf/flink-conf.yaml` in order to run these examples:

```yaml
# The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.

taskmanager.numberOfTaskSlots: 8
```

## GCP Dataflow Runner

```bash
source venv/bin/activate
python -m rillbeam.experiments.flowbased --runner dataflow
```
