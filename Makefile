SHELL := $(shell which bash)

ENV_DEV_FILE := "./environment-dev.yaml"
ENV_FILE := "./environment.yaml"

CONDA_BIN = $(shell which conda)
CONDA_ROOT = $(shell $(CONDA_BIN) info --base)
CONDA_ENV_NAME ?= "neetesh"
CONDA_ENV_PREFIX = $(shell conda env list | grep $(CONDA_ENV_NAME) | sort | awk '{$$1=""; print $$0}' | tr -d '*\| ')
CONDA_ACTIVATE := source $(CONDA_ROOT)/etc/profile.d/conda.sh ; conda activate $(CONDA_ENV_NAME) && PATH=${CONDA_ENV_PREFIX}/bin:${PATH};

install:
	$(CONDA_BIN) env update -n $(CONDA_ENV_NAME) -f $(ENV_DEV_FILE)

environment:
	$(CONDA_BIN) remove -n $(CONDA_ENV_NAME) --all -y --force-remove
	$(CONDA_BIN) env update -n $(CONDA_ENV_NAME) -f $(ENV_DEV_FILE)

HTTP_PORT ?= 7878
HTTP_HOST ?= "0.0.0.0"
run:
	$(CONDA_ACTIVATE) uvicorn src.service:app --host $(HTTP_HOST) --port $(HTTP_PORT) --reload

#to start jupyter notebook
demo:
#

#building training docker
docker_build:
#

#running training docker
docker_run:
#

#docker-build-for-inference
package_docker:
#

#run docker inference engine
publish_docker:
#