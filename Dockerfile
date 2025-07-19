FROM python:3.12-alpine

WORKDIR /app

RUN apk add --no-cache build-base libffi-dev && \
    mkdir tmp

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./app ./app
