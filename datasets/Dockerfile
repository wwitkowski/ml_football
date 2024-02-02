FROM python:3.10-slim

ARG IMAGE_TAG=latest

WORKDIR /app
COPY etl/requirements.txt requirements.txt

RUN pip3 install -r requirements.txt
RUN mkdir /app/data/

COPY etl/ ./etl
COPY database/ ./database/
COPY datasets ./datasets
