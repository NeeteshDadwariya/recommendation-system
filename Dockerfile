FROM python:3
WORKDIR /opt/app
COPY .idea /opt/app
RUN pip install -r requirements.txt
