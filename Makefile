SHELL:=/bin/bash

# ------------------------------------------------------------------------------
# ARGS

# Beam repo
BEAM_ROOT:=$(if $(BEAM_ROOT),$(BEAM_ROOT),~/projects/beam)
BEAM_ROOT:=$(shell python -c "import os; print(os.path.realpath(os.path.expandvars(os.path.expanduser('$(BEAM_ROOT)'))))")
# Used for docker tags
BEAM_VERSION:=$(if $(BEAM_VERSION),$(BEAM_VERSION),$(shell git --git-dir=${BEAM_ROOT}/.git branch | grep \* | cut -d ' ' -f2))
# Specify the registry to upload containers to
DOCKER_REGISTRY_URL:=$(if $(DOCKER_REGISTRY_URL),$(DOCKER_REGISTRY_URL),localhost:5000)
# Extract flink version from the build.gradle
FLINK_VERSION:=$(if $(FLINK_VERSION),$(FLINK_VERSION),$(shell cat ${BEAM_ROOT}/runners/flink/1.8/build.gradle | grep flink_version | cut -d"'" -f 2))

# Platform
UNAME:=$(shell uname -s)
DEFAULT_PLATFORM=linux
ifeq ($(UNAME), Darwin)
	DEFAULT_PLATFORM=osx
endif
PLATFORM:=$(if $(PLATFORM),$(PLATFORM),$(DEFAULT_PLATFORM))


debug:
	echo BEAM_ROOT: $(BEAM_ROOT)
	echo BEAM_VERSION: $(BEAM_VERSION)
	echo FLINK_VERSION: $(FLINK_VERSION)
	echo DOCKER_REGISTRY_URL: $(DOCKER_REGISTRY_URL)
	echo PLATFORM: $(PLATFORM)


# ------------------------------------------------------------------------------
# BEAM-BUILD


denv-build:
	docker build docker/beam/denv \
		-f docker/beam/denv/Dockerfile \
		-t beam/denv:$(BEAM_VERSION) \
		--build-arg DOCKER_GID=`ls -ng /var/run/docker.sock | cut -f3 -d' '`


denv-run:
	docker run --rm -it \
		--net=host \
		-v $(BEAM_ROOT):/opt/apache/beam \
		-v $(PWD)/Makefile:/opt/apache/beam/Makefile \
		-v /var/run/docker.sock:/var/run/docker.sock \
		--env DOCKER_GID=`ls -ng /var/run/docker.sock | cut -f3 -d' '` \
		--env USER=`id -un` \
		--env UID=`id -u` \
		--env GROUP=`id -gn` \
		--env GID=`id -g` \
		--env BEAM_ROOT=/opt/apache/beam \
		beam/denv:$(BEAM_VERSION)


denv: denv-build denv-run

# ------------------------------------------------------------------------------

beam.clean:
	$(BEAM_ROOT)/gradlew clean -P disableSpotlessCheck=true


beam.job-server:
	$(BEAM_ROOT)/gradlew -s -p runners/flink/1.8/job-server-container docker \
	    -P docker-repository-root=$(DOCKER_REGISTRY_URL)/beam \
	    -P docker-tag=$(BEAM_VERSION) \
	    -P disableSpotlessCheck=true

	docker push $(DOCKER_REGISTRY_URL)/beam/flink-job-server:$(BEAM_VERSION)


beam.sdk_java:
	$(BEAM_ROOT)/gradlew -s -p sdks/java/container docker \
	    -P docker-repository-root=$(DOCKER_REGISTRY_URL)/beam \
	    -P docker-tag=$(BEAM_VERSION) \
	    -P disableSpotlessCheck=true

	docker push $(DOCKER_REGISTRY_URL)/beam/java_sdk:$(BEAM_VERSION)


beam.sdk_python27:
	$(BEAM_ROOT)/gradlew -s -p sdks/python/container/py2 docker \
		-P docker-repository-root=$(DOCKER_REGISTRY_URL)/beam \
		-P docker-tag=$(BEAM_VERSION) \
		-P disableSpotlessCheck=true

	docker push $(DOCKER_REGISTRY_URL)/beam/python2.7_sdk:$(BEAM_VERSION)


beam-build: beam.job-server beam.sdk_java beam.sdk_python27


# ------------------------------------------------------------------------------


docker-flink:
	docker build docker/docker-flink \
		-f docker/docker-flink/Dockerfile \
		-t $(DOCKER_REGISTRY_URL)/beam/docker-flink:$(FLINK_VERSION) \
		--build-arg DOCKER_GID_HOST=`ls -ng /var/run/docker.sock | cut -f3 -d' '` \
		--build-arg FLINK_VERSION=$(FLINK_VERSION)


start:
	DOCKER_REGISTRY_URL=$(DOCKER_REGISTRY_URL) \
	BEAM_VERSION=$(BEAM_VERSION) \
	FLINK_VERSION=$(FLINK_VERSION) \
	docker-compose -f docker/compose/$(PLATFORM)/docker-compose.yaml up


# ------------------------------------------------------------------------------
# SETUP

env:
	if [[ -d .venv ]]; then \
		echo "virtualenv .venv already exists"; \
	else \
		virtualenv .venv; \
		source .venv/bin/activate; \
		pip install -e .; \
		pip install -e $(BEAM_ROOT)/sdks/python[gcp]; \
	fi;


protos: env
	source .venv/bin/activate; \
	python $(BEAM_ROOT)/sdks/python/gen_protos.py;


job-server: env
	source .venv/bin/activate; \
	python -m apache_beam.runners.portability.local_job_service_main -p 8099;


test-job: env
	source .venv/bin/activate; \
	python -m rillbeam.experiments.flowbased --defaults local;
