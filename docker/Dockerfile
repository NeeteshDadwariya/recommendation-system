#Getting conda image
FROM continuumio/conda-ci-linux-64-python3.7

#Directory work
#RUN su mkdir -p /opt/app
WORKDIR /opt/app
COPY . /opt/app/

RUN make run
