## BUILDING

You need a clone of apache beam. By default it assumes this is located at `/Users/${USER}/projects/beam`. If it lives somewhere different first `export BEAM_ROOT=/path/to/beam`.

Other environment variables for the build are:

- `DOCKER_REGISTRY_URL`: the docker registry to upload containers to (defaults to localhost:5000)
- `BEAM_VERSION`: used as the docker tag for the containers. (defaults to beam git branch name; e.g. release-2.16.0)
- `FLINK_VERSION`: used for building our flink container. (defaults to the version found in ${BEAM_ROOT}/runners/flink/1.8/build.gradle)

These can be overloaded as needed.

> There is also an experiemental way to build within a docker container if you don't want all the java / gradle stuff on your machine.
> 
> First run:
> ```bash
> make denv
> ```

Build the beam pieces:

```bash
make beam-build
```

> If you entered the docker build env, you'll need to exit this for the next steps.


This will build and upload to the registry these containers:

- `$(DOCKER_REGISTRY_URL)/beam/flink-job-server:$(BEAM_VERSION)`
- `$(DOCKER_REGISTRY_URL)/beam/java_sdk:$(BEAM_VERSION)`
- `$(DOCKER_REGISTRY_URL)/beam/python2.7_sdk:$(BEAM_VERSION)`

Next build the flink container:

```bash
make docker-flink
```

And now start up the stack:

```bash
make start
```

This will start up the job-server and flink. Note there are limitations with certain types of beam graphs running on OSX that need to be resolved.

# RUNNING

Activate your virtual environment.

```bash
source venv/bin/activate
```

#### DirectRunner

```bash
python -m rillbeam.experiments.flowbased
```

#### FlinkRunner

```bash
python -m rillbeam.experiments.flowbased --defaults flink
```

# DEBUG

### Flink install using Homebrew

> Make sure to get the flink version that matches what beam expects.

Install flink:

```bash
brew install https://raw.github.com/Homebrew/homebrew-core/9312171d224f9ab2f32b57abea3f1c99d5fc4332/Formula/apache-flink.rb
```

Start flink:

```bash
/usr/local/Cellar/apache-flink/1.8.0/libexec/bin/start-cluster.sh
```

# Development Tips

## Flink

You can increase the number of taskmanager (workers) by doing: 
```bash
cd rillbeam/docker/osx
docker-compose scale taskmanager=4
```

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

I've added `rillbeam/java/run.sh` helper to aid in compiling and submitting Java beam pipelines. This is executed via a maven docker container.

Execute with direct-runner:

```bash
rillbeam/java/run.sh PubSub
```

Execute via flink directly:

```bash
rillbeam/java/run.sh PubSub --runner=FlinkRunner --flinkMaster=host.docker.internal:8081 --filesToStage=target/PubSub-bundled-0.1.jar
```

> Note: Use of `host.docker.internal` is because run.sh runs a docker container which needs to talk to flink.
 
*NOT WORKING / IN PROGRESS*
Execute via flink via PortableRunner:

```bash
java/run.sh PubSub --runner=PortableRunner --jobEndpoint=host.docker.internal:8099 --filesToStage=target/PubSub-bundled-0.1.jar
```
