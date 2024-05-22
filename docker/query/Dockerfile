FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH "${PYTHONPATH}:/app"

ENV PYTHONUNBUFFERED True

ENV APP_HOME "/app"

RUN apt-get install -y apt-transport-https ca-certificates gnupg curl sudo

# ENV PORT 5000

WORKDIR $APP_HOME

COPY . ./

RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install --no-cache-dir .

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 kitevectorserverless.httpd.indexnode:app
