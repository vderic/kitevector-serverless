#FROM python:3.10-slim
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH="${PYTHONPATH}:/app"

ENV PYTHONUNBUFFERED True

ENV APP_HOME="/app"

RUN apt-get update && apt-get install -y python3.10 python3-pip
RUN apt-get install -y apt-transport-https curl sudo

COPY ../.. /app

WORKDIR /app

RUN dir -s /app

RUN cat requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install --no-cache-dir .

ENV PORT=8080
EXPOSE $PORT
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 kitevectorserverless.httpd.indexnode:app
#CMD ["gunicorn", "--bind",  "0.0.0.0:$PORT",  "--workers",  "1",  "--threads", "8", "--timeout", "0", "kitevectorserverless.httpd.indexnode:app"]
