SHELL := $(shell which bash)
CUR_DIR := $(CURDIR)

ENV_DEV_FILE := "./environment-dev.yml"
ENV_FILE := "./environment.yml"

CONDA_DEV_ENV_NAME ?= "recommendation-system-dev"
CONDA_ENV_NAME ?= "recommendation-system"

CONDA_BIN = $(shell which conda)
CONDA_ROOT = $(shell $(CONDA_BIN) info --base)

CONDA_DEV_ENV_PREFIX = $(shell conda env list | grep $(CONDA_DEV_ENV_NAME) | sort | awk '{$$1=""; print $$0}' | tr -d '*\| ')
CONDA_ENV_PREFIX = $(shell conda env list | grep $(CONDA_ENV_NAME) | sort | awk '{$$1=""; print $$0}' | tr -d '*\| ')

CONDA_DEV_ACTIVATE := source $(CONDA_ROOT)/etc/profile.d/conda.sh ; conda activate $(CONDA_DEV_ENV_NAME) && PATH=${CONDA_DEV_ENV_PREFIX}/bin:${PATH};
CONDA_ACTIVATE := source $(CONDA_ROOT)/etc/profile.d/conda.sh ; conda activate $(CONDA_ENV_NAME) && PATH=${CONDA_ENV_PREFIX}/bin:${PATH};

create_dev_env:
	$(CONDA_BIN) remove -n $(CONDA_DEV_ENV_NAME) --all -y --force-remove
	$(CONDA_BIN) env update -n $(CONDA_DEV_ENV_NAME) -f $(ENV_DEV_FILE)

create_env:
	$(CONDA_BIN) remove -n $(CONDA_ENV_NAME) --all -y --force-remove
	$(CONDA_BIN) env update -n $(CONDA_ENV_NAME) -f $(ENV_FILE)

#To start jupyter notebook (dev mode)
jupyter:
	#$(MAKE) create_dev_env
	#$(CONDA_DEV_ACTIVATE) python -m ipykernel install --user --name=$(CONDA_DEV_ENV_NAME)
	$(CONDA_DEV_ACTIVATE) jupyter notebook --MultiKernelManager.default_kernel_name=$(CONDA_DEV_ENV_NAME) "./notebooks/demo.ipynb"

HTTP_PORT ?= 8000
HTTP_HOST ?= "0.0.0.0"

#To run server for API (prod mode)
run:
	$(MAKE) create_env
	$(CONDA_ACTIVATE) uvicorn src.service.app:app --host $(HTTP_HOST) --port $(HTTP_PORT) --reload

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
