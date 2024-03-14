FROM python:3.6.12-slim-buster

WORKDIR /opt/oracle
RUN echo 'running docker, install dependencies of docker file '
ADD requirements.txt /app/requirements.txt
RUN echo 'now installing from req.txt if any left'
RUN pip install -r /app/requirements.txt
RUN echo 'copying everything to app'
COPY . /opt/oracle

ENTRYPOINT exec gunicorn -b 0.0.0.0:$PORT -t $TIME wsgi -w $WORKERS -k gevent --worker-connections=1000